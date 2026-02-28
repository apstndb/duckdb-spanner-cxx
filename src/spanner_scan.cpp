#include "spanner_scan.hpp"
#include "spanner_client.hpp"
#include "spanner_types.hpp"
#include "spanner_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "google/cloud/spanner/client.h"
#include "google/cloud/spanner/options.h"
#include <deque>

namespace duckdb {

namespace spanner = google::cloud::spanner;

// ─── Bind data ──────────────────────────────────────────────────────────────

struct SpannerScanBindData : public TableFunctionData {
	std::string database;
	std::string table;
	std::string endpoint;
	std::string index;
	absl::optional<spanner::RequestPriority> priority;
	std::string exact_staleness_secs;
	std::string max_staleness_secs;
	std::string read_timestamp;
	std::string min_read_timestamp;

	vector<string> column_names;
	vector<LogicalType> column_types;
};

// ─── Global state ───────────────────────────────────────────────────────────

struct SpannerScanGlobalState : public GlobalTableFunctionState {
	std::deque<vector<Value>> rows;
	idx_t position = 0;
	bool done = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

// ─── Schema discovery via INFORMATION_SCHEMA ────────────────────────────────

static void DiscoverTableSchema(spanner::Client &client, const std::string &table_name,
                                const std::string &endpoint,
                                vector<string> &names, vector<LogicalType> &types) {
	auto parsed = ParseTableName(table_name);
	auto &schema = parsed.first;
	auto &table = parsed.second;

	spanner::SqlStatement stmt =
	    schema.empty()
	        ? spanner::SqlStatement(
	              "SELECT TABLE_SCHEMA, COLUMN_NAME, SPANNER_TYPE "
	              "FROM INFORMATION_SCHEMA.COLUMNS "
	              "WHERE TABLE_SCHEMA IN ('', 'public') AND TABLE_NAME = @table "
	              "ORDER BY ORDINAL_POSITION",
	              {{"table", spanner::Value(table)}})
	        : spanner::SqlStatement(
	              "SELECT TABLE_SCHEMA, COLUMN_NAME, SPANNER_TYPE "
	              "FROM INFORMATION_SCHEMA.COLUMNS "
	              "WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table "
	              "ORDER BY ORDINAL_POSITION",
	              {{"schema", spanner::Value(schema)}, {"table", spanner::Value(table)}});

	auto rows = client.ExecuteQuery(std::move(stmt));

	std::string detected_schema;
	for (auto &row_status : rows) {
		if (!row_status.ok()) {
			throw IOException("Failed to query INFORMATION_SCHEMA: %s", row_status.status().message());
		}
		auto &row = *row_status;

		auto schema_val = row.get<std::string>(0);
		auto col_name = row.get<std::string>(1);
		auto spanner_type = row.get<std::string>(2);

		if (!schema_val.ok() || !col_name.ok() || !spanner_type.ok()) {
			continue;
		}

		if (detected_schema.empty()) {
			detected_schema = *schema_val;
		} else if (*schema_val != detected_schema) {
			// Skip columns from other schema (for unqualified table name)
			continue;
		}

		names.push_back(*col_name);

		// Use appropriate parser based on detected dialect
		if (detected_schema.empty()) {
			// GoogleSQL: '' schema
			types.push_back(ParseSpannerType(*spanner_type));
		} else {
			// PostgreSQL: 'public' schema
			types.push_back(ParsePgSpannerType(*spanner_type));
		}
	}

	if (names.empty()) {
		throw IOException("Table '%s' not found or has no columns", table_name);
	}
}

// ─── Bind ───────────────────────────────────────────────────────────────────

static unique_ptr<FunctionData> SpannerScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<SpannerScanBindData>();

	bind_data->database = input.inputs[0].GetValue<string>();
	bind_data->table = input.inputs[1].GetValue<string>();

	for (auto &kv : input.named_parameters) {
		auto val = kv.second;
		if (val.IsNull()) {
			continue;
		}
		auto str_val = val.ToString();

		if (kv.first == "endpoint") {
			bind_data->endpoint = str_val;
		} else if (kv.first == "index") {
			bind_data->index = str_val;
		} else if (kv.first == "priority") {
			bind_data->priority = ParsePriority(str_val);
		} else if (kv.first == "exact_staleness_secs") {
			bind_data->exact_staleness_secs = str_val;
		} else if (kv.first == "max_staleness_secs") {
			bind_data->max_staleness_secs = str_val;
		} else if (kv.first == "read_timestamp") {
			bind_data->read_timestamp = str_val;
		} else if (kv.first == "min_read_timestamp") {
			bind_data->min_read_timestamp = str_val;
		}
	}

	// Validate timestamp bound
	BuildTimestampBound(bind_data->exact_staleness_secs, bind_data->max_staleness_secs,
	                    bind_data->read_timestamp, bind_data->min_read_timestamp);

	// Discover schema via INFORMATION_SCHEMA
	auto db = ParseDatabaseName(bind_data->database);
	auto client = GetOrCreateClient(db, bind_data->endpoint);
	DiscoverTableSchema(*client, bind_data->table, bind_data->endpoint, names, return_types);

	bind_data->column_names = names;
	bind_data->column_types = return_types;
	return std::move(bind_data);
}

// ─── Init Global ────────────────────────────────────────────────────────────

static unique_ptr<GlobalTableFunctionState>
SpannerScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<SpannerScanBindData>();
	auto state = make_uniq<SpannerScanGlobalState>();

	auto db = ParseDatabaseName(bind_data.database);
	auto client = GetOrCreateClient(db, bind_data.endpoint);

	auto ts_bound = BuildTimestampBound(bind_data.exact_staleness_secs, bind_data.max_staleness_secs,
	                                    bind_data.read_timestamp, bind_data.min_read_timestamp);

	// Build column list for projection pushdown
	auto [schema, table] = ParseTableName(bind_data.table);
	spanner::KeySet key_set = spanner::KeySet::All();

	// Determine which columns to read (projection pushdown)
	vector<string> columns;
	vector<LogicalType> read_types;
	if (!input.column_ids.empty()) {
		for (auto &col_id : input.column_ids) {
			if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
				continue;
			}
			if (col_id < bind_data.column_names.size()) {
				columns.push_back(bind_data.column_names[col_id]);
				read_types.push_back(bind_data.column_types[col_id]);
			}
		}
	}
	if (columns.empty()) {
		columns = bind_data.column_names;
		read_types = bind_data.column_types;
	}

	// Client::Read() requires a Transaction (not SingleUseOptions).
	// ReadOnlyOptions supports: strong read, exact_staleness, read_timestamp.
	// max_staleness and min_read_timestamp are only valid for single-use transactions.
	spanner::Transaction::ReadOnlyOptions ro_opts;
	if (ts_bound.kind == TimestampBoundConfig::kExactStaleness) {
		ro_opts = spanner::Transaction::ReadOnlyOptions(std::chrono::seconds(ts_bound.seconds));
	} else if (ts_bound.kind == TimestampBoundConfig::kReadTimestamp) {
		auto tp = std::chrono::system_clock::time_point(
		    std::chrono::duration_cast<std::chrono::system_clock::duration>(
		        std::chrono::seconds(ts_bound.seconds) + std::chrono::nanoseconds(ts_bound.nanos)));
		auto ts = spanner::MakeTimestamp(tp);
		ro_opts = spanner::Transaction::ReadOnlyOptions(ts.value());
	} else if (ts_bound.kind == TimestampBoundConfig::kMaxStaleness ||
	           ts_bound.kind == TimestampBoundConfig::kMinReadTimestamp) {
		throw InvalidInputException(
		    "max_staleness_secs and min_read_timestamp are not supported with spanner_scan. "
		    "Use exact_staleness_secs or read_timestamp instead.");
	}
	auto txn = spanner::MakeReadOnlyTransaction(ro_opts);

	auto read_opts = BuildRequestOptions(bind_data.priority);
	if (!bind_data.index.empty()) {
		read_opts.set<spanner::ReadIndexNameOption>(bind_data.index);
	}

	auto rows = client->Read(std::move(txn), table, std::move(key_set), columns, std::move(read_opts));

	// Buffer all rows
	for (auto &row_status : rows) {
		if (!row_status.ok()) {
			throw IOException("Spanner read error: %s", row_status.status().message());
		}
		auto &row = *row_status;

		vector<Value> row_values;
		auto &values = row.values();
		for (idx_t i = 0; i < read_types.size(); i++) {
			row_values.push_back(SpannerValueToDuckDB(values[i], read_types[i]));
		}
		state->rows.push_back(std::move(row_values));
	}

	return std::move(state);
}

// ─── Function ───────────────────────────────────────────────────────────────

static void SpannerScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<SpannerScanGlobalState>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	idx_t batch_size = STANDARD_VECTOR_SIZE;

	while (count < batch_size && state.position < state.rows.size()) {
		auto &row = state.rows[state.position];
		for (idx_t col = 0; col < output.ColumnCount(); col++) {
			output.data[col].SetValue(count, row[col]);
		}
		state.position++;
		count++;
	}

	if (state.position >= state.rows.size()) {
		state.done = true;
	}

	output.SetCardinality(count);
}

// ─── Registration ───────────────────────────────────────────────────────────

void RegisterSpannerScanFunction(ExtensionLoader &loader) {
	TableFunction scan("spanner_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   SpannerScanFunction, SpannerScanBind, SpannerScanInitGlobal);

	scan.named_parameters["endpoint"] = LogicalType::VARCHAR;
	scan.named_parameters["index"] = LogicalType::VARCHAR;
	scan.named_parameters["priority"] = LogicalType::VARCHAR;
	scan.named_parameters["exact_staleness_secs"] = LogicalType::BIGINT;
	scan.named_parameters["max_staleness_secs"] = LogicalType::BIGINT;
	scan.named_parameters["read_timestamp"] = LogicalType::VARCHAR;
	scan.named_parameters["min_read_timestamp"] = LogicalType::VARCHAR;
	scan.projection_pushdown = true;

	loader.RegisterFunction(scan);
}

} // namespace duckdb
