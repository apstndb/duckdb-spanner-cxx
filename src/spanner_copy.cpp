#include "spanner_copy.hpp"
#include "spanner_client.hpp"
#include "spanner_types.hpp"
#include "spanner_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "google/cloud/spanner/client.h"
#include "google/cloud/spanner/mutations.h"
#include <mutex>

namespace duckdb {

namespace spanner = google::cloud::spanner;

// Spanner's per-commit mutation limit (as of Dec 2023)
static constexpr idx_t SPANNER_MUTATION_LIMIT = 80000;

// ─── Bind data ──────────────────────────────────────────────────────────────

struct SpannerCopyBindData : public FunctionData {
	std::string database;
	std::string endpoint;
	std::string table_name;
	vector<string> column_names;
	vector<LogicalType> column_types;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<SpannerCopyBindData>();
		copy->database = database;
		copy->endpoint = endpoint;
		copy->table_name = table_name;
		copy->column_names = column_names;
		copy->column_types = column_types;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<SpannerCopyBindData>();
		return database == o.database && table_name == o.table_name && endpoint == o.endpoint;
	}
};

// ─── Global state ───────────────────────────────────────────────────────────

struct SpannerCopyGlobalState : public GlobalFunctionData {
	std::shared_ptr<spanner::Client> client;
	std::string table;

	// Accumulated mutations to commit
	std::vector<spanner::Mutation> pending_mutations;
	idx_t pending_row_count = 0;
	idx_t total_rows = 0;
	idx_t total_commits = 0;
	std::mutex lock;
};

// ─── Local state ────────────────────────────────────────────────────────────

struct SpannerCopyLocalState : public LocalFunctionData {
	std::vector<spanner::Mutation> local_mutations;
	idx_t local_row_count = 0;
};

// ─── Helper: flush pending mutations ────────────────────────────────────────

static void FlushMutations(SpannerCopyGlobalState &global) {
	if (global.pending_mutations.empty()) {
		return;
	}

	auto mutations = std::move(global.pending_mutations);
	global.pending_mutations.clear();
	global.pending_row_count = 0;

	auto commit_result = global.client->Commit(
	    [&mutations](spanner::Transaction const &) -> spanner::Mutations {
		    return spanner::Mutations(mutations.begin(), mutations.end());
	    });

	if (!commit_result.ok()) {
		throw IOException("Spanner commit failed: %s", commit_result.status().message());
	}
	global.total_commits++;
}

// ─── Bind ───────────────────────────────────────────────────────────────────

static unique_ptr<FunctionData> SpannerCopyBind(ClientContext &context, CopyFunctionBindInput &input,
                                                 const vector<string> &names,
                                                 const vector<LogicalType> &sql_types) {
	auto bind_data = make_uniq<SpannerCopyBindData>();

	// file_path is the target table name
	bind_data->table_name = input.info.file_path;
	bind_data->column_names = names;
	bind_data->column_types = sql_types;

	// Get database from copy options
	for (auto &kv : input.info.options) {
		if (kv.first == "database") {
			if (!kv.second.empty()) {
				bind_data->database = kv.second[0].ToString();
			}
		} else if (kv.first == "endpoint") {
			if (!kv.second.empty()) {
				bind_data->endpoint = kv.second[0].ToString();
			}
		}
	}

	if (bind_data->database.empty()) {
		throw InvalidInputException("COPY TO spanner requires 'database' option. "
		                            "Example: COPY ... TO 'table' (FORMAT spanner, database 'projects/...')");
	}

	return std::move(bind_data);
}

// ─── Init Global ────────────────────────────────────────────────────────────

static unique_ptr<GlobalFunctionData> SpannerCopyInitGlobal(ClientContext &context, FunctionData &bind_data,
                                                             const string &file_path) {
	auto &data = bind_data.Cast<SpannerCopyBindData>();
	auto state = make_uniq<SpannerCopyGlobalState>();

	auto db = ParseDatabaseName(data.database);
	state->client = GetOrCreateClient(db, data.endpoint);
	auto [schema, table] = ParseTableName(data.table_name);
	state->table = table;

	return std::move(state);
}

// ─── Init Local ─────────────────────────────────────────────────────────────

static unique_ptr<LocalFunctionData> SpannerCopyInitLocal(ExecutionContext &context, FunctionData &bind_data) {
	return make_uniq<SpannerCopyLocalState>();
}

// ─── Sink ───────────────────────────────────────────────────────────────────

static void SpannerCopySink(ExecutionContext &context, FunctionData &bind_data,
                             GlobalFunctionData &gstate, LocalFunctionData &lstate, DataChunk &input) {
	auto &data = bind_data.Cast<SpannerCopyBindData>();
	auto &global = gstate.Cast<SpannerCopyGlobalState>();
	auto &local = lstate.Cast<SpannerCopyLocalState>();

	// Build a single mutation with all rows in this chunk
	auto builder = spanner::InsertOrUpdateMutationBuilder(global.table, data.column_names);

	for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
		std::vector<spanner::Value> values;
		for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
			auto duck_val = input.data[col_idx].GetValue(row_idx);
			values.push_back(DuckDBValueToSpanner(duck_val, data.column_types[col_idx]));
		}
		builder.AddRow(values);
	}

	local.local_mutations.push_back(builder.Build());
	local.local_row_count += input.size();
}

// ─── Combine ────────────────────────────────────────────────────────────────

static void SpannerCopyCombine(ExecutionContext &context, FunctionData &bind_data,
                                GlobalFunctionData &gstate, LocalFunctionData &lstate) {
	auto &global = gstate.Cast<SpannerCopyGlobalState>();
	auto &local = lstate.Cast<SpannerCopyLocalState>();

	std::lock_guard<std::mutex> guard(global.lock);
	global.total_rows += local.local_row_count;

	for (auto &m : local.local_mutations) {
		global.pending_mutations.push_back(std::move(m));
	}
	global.pending_row_count += local.local_row_count;

	// Flush if we've exceeded the mutation limit
	if (global.pending_row_count >= SPANNER_MUTATION_LIMIT) {
		FlushMutations(global);
	}
}

// ─── Finalize ───────────────────────────────────────────────────────────────

static void SpannerCopyFinalize(ClientContext &context, FunctionData &bind_data,
                                 GlobalFunctionData &gstate) {
	auto &global = gstate.Cast<SpannerCopyGlobalState>();

	// Flush remaining mutations
	FlushMutations(global);
}

// ─── Registration ───────────────────────────────────────────────────────────

void RegisterSpannerCopyFunction(ExtensionLoader &loader) {
	CopyFunction copy_func("spanner");
	copy_func.copy_to_bind = SpannerCopyBind;
	copy_func.copy_to_initialize_global = SpannerCopyInitGlobal;
	copy_func.copy_to_initialize_local = SpannerCopyInitLocal;
	copy_func.copy_to_sink = SpannerCopySink;
	copy_func.copy_to_combine = SpannerCopyCombine;
	copy_func.copy_to_finalize = SpannerCopyFinalize;
	copy_func.extension = "spanner";

	loader.RegisterFunction(copy_func);
}

} // namespace duckdb
