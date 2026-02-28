#include "spanner_query.hpp"
#include "spanner_client.hpp"
#include "spanner_types.hpp"
#include "spanner_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "google/cloud/spanner/client.h"
#include "google/cloud/spanner/query_options.h"
#include "google/cloud/spanner/request_priority.h"
#include "google/cloud/spanner/results.h"
#include <deque>
#include <optional>

namespace duckdb {

namespace spanner = google::cloud::spanner;

// ─── Bind data ──────────────────────────────────────────────────────────────

struct SpannerQueryBindData : public TableFunctionData {
	std::string database;
	std::string sql;
	std::string endpoint;
	std::string params_json;
	absl::optional<spanner::RequestPriority> priority;
	std::string exact_staleness_secs;
	std::string max_staleness_secs;
	std::string read_timestamp;
	std::string min_read_timestamp;

	vector<string> column_names;
	vector<LogicalType> column_types;
};

// ─── Global state ───────────────────────────────────────────────────────────

struct SpannerQueryGlobalState : public GlobalTableFunctionState {
	std::deque<vector<Value>> rows;
	idx_t position = 0;
	bool done = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

// ─── Helper: parse params JSON and build SqlStatement ───────────────────────

static spanner::SqlStatement BuildSqlStatement(const std::string &sql, const std::string &params_json) {
	if (params_json.empty()) {
		return spanner::SqlStatement(sql);
	}

	throw NotImplementedException(
	    "Parameterized queries are not yet supported in spanner_query_raw. "
	    "Embed parameter values directly in the SQL string for now.");
}

// ─── Helper: map proto TypeCode to DuckDB LogicalType ───────────────────────

static LogicalType TypeCodeToDuckDB(google::spanner::v1::TypeCode code) {
	switch (code) {
	case google::spanner::v1::TypeCode::BOOL:
		return LogicalType::BOOLEAN;
	case google::spanner::v1::TypeCode::INT64:
		return LogicalType::BIGINT;
	case google::spanner::v1::TypeCode::FLOAT32:
		return LogicalType::FLOAT;
	case google::spanner::v1::TypeCode::FLOAT64:
		return LogicalType::DOUBLE;
	case google::spanner::v1::TypeCode::NUMERIC:
		return LogicalType::DECIMAL(38, 9);
	case google::spanner::v1::TypeCode::STRING:
		return LogicalType::VARCHAR;
	case google::spanner::v1::TypeCode::JSON:
		return LogicalType::VARCHAR;
	case google::spanner::v1::TypeCode::BYTES:
	case google::spanner::v1::TypeCode::PROTO:
		return LogicalType::BLOB;
	case google::spanner::v1::TypeCode::DATE:
		return LogicalType::DATE;
	case google::spanner::v1::TypeCode::TIMESTAMP:
		return LogicalType::TIMESTAMP_TZ;
	case google::spanner::v1::TypeCode::ENUM:
		return LogicalType::BIGINT;
	default:
		return LogicalType::VARCHAR;
	}
}

// ─── Schema discovery via first row inspection ──────────────────────────────

static void DiscoverSchemaFromRow(const spanner::Row &row,
                                  vector<string> &names, vector<LogicalType> &types) {
	// Get column names
	for (auto &col : row.columns()) {
		names.push_back(col);
	}

	// Get types by inspecting each Value's internal proto type
	for (auto &val : row.values()) {
		// Use spanner_internal::ToProto to extract the proto type from Value
		auto val_copy = val; // ToProto takes by value (moves)
		auto [type_proto, _] = google::cloud::spanner_internal::ToProto(std::move(val_copy));
		types.push_back(TypeCodeToDuckDB(type_proto.code()));
	}
}

// ─── Bind ───────────────────────────────────────────────────────────────────

static unique_ptr<FunctionData> SpannerQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<SpannerQueryBindData>();

	// Positional parameters
	bind_data->database = input.inputs[0].GetValue<string>();
	bind_data->sql = input.inputs[1].GetValue<string>();

	// Named parameters
	for (auto &kv : input.named_parameters) {
		auto val = kv.second;
		if (val.IsNull()) {
			continue;
		}
		auto str_val = val.ToString();

		if (kv.first == "endpoint") {
			bind_data->endpoint = str_val;
		} else if (kv.first == "params") {
			bind_data->params_json = str_val;
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
	auto ts_bound = BuildTimestampBound(bind_data->exact_staleness_secs, bind_data->max_staleness_secs,
	                                    bind_data->read_timestamp, bind_data->min_read_timestamp);

	// Discover schema by executing query with LIMIT 1
	auto db = ParseDatabaseName(bind_data->database);
	auto client = GetOrCreateClient(db, bind_data->endpoint);

	auto stmt = BuildSqlStatement("SELECT * FROM (" + bind_data->sql + ") sub LIMIT 1",
	                              bind_data->params_json);
	auto su_opts = ToSingleUseOptions(ts_bound);
	auto req_opts = BuildRequestOptions(bind_data->priority);
	auto rows = client->ExecuteQuery(std::move(su_opts), std::move(stmt), std::move(req_opts));

	bool schema_found = false;
	for (auto &row_status : rows) {
		if (!row_status.ok()) {
			throw IOException("Spanner schema discovery error: %s", row_status.status().message());
		}
		if (!schema_found) {
			DiscoverSchemaFromRow(*row_status, names, return_types);
			schema_found = true;
		}
		break; // Only need first row
	}

	if (!schema_found) {
		throw IOException(
		    "Query returned no rows, so column types could not be inferred. "
		    "The google-cloud-cpp client does not expose result set metadata for empty results. "
		    "Ensure the query returns at least one row, or use spanner_scan for table reads.");
	}

	bind_data->column_names = names;
	bind_data->column_types = return_types;
	return std::move(bind_data);
}

// ─── Init Global ────────────────────────────────────────────────────────────

static unique_ptr<GlobalTableFunctionState>
SpannerQueryInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<SpannerQueryBindData>();
	auto state = make_uniq<SpannerQueryGlobalState>();

	auto db = ParseDatabaseName(bind_data.database);
	auto client = GetOrCreateClient(db, bind_data.endpoint);

	auto ts_bound = BuildTimestampBound(bind_data.exact_staleness_secs, bind_data.max_staleness_secs,
	                                    bind_data.read_timestamp, bind_data.min_read_timestamp);

	auto stmt = BuildSqlStatement(bind_data.sql, bind_data.params_json);
	auto su_opts = ToSingleUseOptions(ts_bound);
	auto req_opts = BuildRequestOptions(bind_data.priority);
	auto rows = client->ExecuteQuery(std::move(su_opts), std::move(stmt), std::move(req_opts));

	// Buffer all rows
	for (auto &row_status : rows) {
		if (!row_status.ok()) {
			throw IOException("Spanner query error: %s", row_status.status().message());
		}
		auto &row = *row_status;

		vector<Value> row_values;
		auto &values = row.values();
		for (idx_t i = 0; i < bind_data.column_types.size(); i++) {
			row_values.push_back(SpannerValueToDuckDB(values[i], bind_data.column_types[i]));
		}
		state->rows.push_back(std::move(row_values));
	}

	return std::move(state);
}

// ─── Function ───────────────────────────────────────────────────────────────

static void SpannerQueryFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<SpannerQueryGlobalState>();

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

void RegisterSpannerQueryFunction(ExtensionLoader &loader) {
	// spanner_query_raw: the underlying VTab function
	TableFunction query_raw("spanner_query_raw", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                        SpannerQueryFunction, SpannerQueryBind, SpannerQueryInitGlobal);

	query_raw.named_parameters["endpoint"] = LogicalType::VARCHAR;
	query_raw.named_parameters["params"] = LogicalType::VARCHAR;
	query_raw.named_parameters["priority"] = LogicalType::VARCHAR;
	query_raw.named_parameters["exact_staleness_secs"] = LogicalType::BIGINT;
	query_raw.named_parameters["max_staleness_secs"] = LogicalType::BIGINT;
	query_raw.named_parameters["read_timestamp"] = LogicalType::VARCHAR;
	query_raw.named_parameters["min_read_timestamp"] = LogicalType::VARCHAR;
	loader.RegisterFunction(query_raw);
}

} // namespace duckdb
