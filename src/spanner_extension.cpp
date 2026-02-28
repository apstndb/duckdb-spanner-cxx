#define DUCKDB_EXTENSION_MAIN

#include "spanner_extension.hpp"
#include "spanner_copy.hpp"
#include "spanner_query.hpp"
#include "spanner_scan.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// ─── Scalar macros ──────────────────────────────────────────────────────────

// clang-format off
static const DefaultMacro SPANNER_MACROS[] = {
    // spanner_params(s): convert a STRUCT to JSON string for Spanner query params
    {DEFAULT_SCHEMA, "spanner_params", {"s", nullptr}, {{nullptr, nullptr}},
     "CAST(to_json(s) AS VARCHAR)"},

    // spanner_typed(val, typ): wrap a value with an explicit Spanner type name
    {DEFAULT_SCHEMA, "spanner_typed", {"val", "typ", nullptr}, {{nullptr, nullptr}},
     "json_object('value', val, 'type', typ)"},

    // interval_to_iso8601(i): convert a DuckDB INTERVAL to ISO 8601 duration string
    {DEFAULT_SCHEMA, "interval_to_iso8601", {"i", nullptr}, {{nullptr, nullptr}},
     "CASE WHEN datepart('year', i) = 0 AND datepart('month', i) = 0 AND datepart('day', i) = 0"
     " AND datepart('hour', i) = 0 AND datepart('minute', i) = 0"
     " AND datepart('microsecond', i) = 0"
     " THEN 'PT0S'"
     " ELSE"
     " 'P' ||"
     " CASE WHEN datepart('year', i) != 0 THEN CAST(datepart('year', i) AS VARCHAR) || 'Y' ELSE '' END ||"
     " CASE WHEN datepart('month', i) != 0 THEN CAST(datepart('month', i) AS VARCHAR) || 'M' ELSE '' END ||"
     " CASE WHEN datepart('day', i) != 0 THEN CAST(datepart('day', i) AS VARCHAR) || 'D' ELSE '' END ||"
     " CASE WHEN datepart('hour', i) != 0 OR datepart('minute', i) != 0"
     " OR datepart('second', i) != 0 OR datepart('microsecond', i) % 1000000 != 0"
     " THEN 'T' ||"
     " CASE WHEN datepart('hour', i) != 0 THEN CAST(datepart('hour', i) AS VARCHAR) || 'H' ELSE '' END ||"
     " CASE WHEN datepart('minute', i) != 0 THEN CAST(datepart('minute', i) AS VARCHAR) || 'M' ELSE '' END ||"
     " CASE WHEN datepart('second', i) != 0 OR datepart('microsecond', i) % 1000000 != 0"
     " THEN CAST(datepart('second', i) AS VARCHAR) ||"
     " CASE WHEN datepart('microsecond', i) % 1000000 != 0"
     " THEN '.' || LPAD(CAST(datepart('microsecond', i) % 1000000 AS VARCHAR), 6, '0')"
     " ELSE '' END || 'S'"
     " ELSE '' END"
     " ELSE '' END"
     " END"},

    // _spanner_scalar_type_name(t): map DuckDB scalar type name to Spanner type name
    {DEFAULT_SCHEMA, "_spanner_scalar_type_name", {"t", nullptr}, {{nullptr, nullptr}},
     "CASE t"
     " WHEN 'BOOLEAN' THEN 'BOOL'"
     " WHEN 'TINYINT' THEN 'INT64'"
     " WHEN 'SMALLINT' THEN 'INT64'"
     " WHEN 'INTEGER' THEN 'INT64'"
     " WHEN 'BIGINT' THEN 'INT64'"
     " WHEN 'UTINYINT' THEN 'INT64'"
     " WHEN 'USMALLINT' THEN 'INT64'"
     " WHEN 'UINTEGER' THEN 'INT64'"
     " WHEN 'UBIGINT' THEN 'NUMERIC'"
     " WHEN 'HUGEINT' THEN 'NUMERIC'"
     " WHEN 'UHUGEINT' THEN 'NUMERIC'"
     " WHEN 'FLOAT' THEN 'FLOAT32'"
     " WHEN 'DOUBLE' THEN 'FLOAT64'"
     " WHEN 'VARCHAR' THEN 'STRING'"
     " WHEN 'BLOB' THEN 'BYTES'"
     " WHEN 'DATE' THEN 'DATE'"
     " WHEN 'TIMESTAMP' THEN 'TIMESTAMP'"
     " WHEN 'TIMESTAMP WITH TIME ZONE' THEN 'TIMESTAMP'"
     " WHEN 'UUID' THEN 'UUID'"
     " WHEN 'INTERVAL' THEN 'INTERVAL'"
     " WHEN 'JSON' THEN 'JSON'"
     " WHEN 'TIME' THEN 'STRING'"
     " WHEN 'BIT' THEN 'BYTES'"
     " ELSE NULL"
     " END"},

    // _spanner_type_name(t): map DuckDB type name (scalar or array) to Spanner type name
    {DEFAULT_SCHEMA, "_spanner_type_name", {"t", nullptr}, {{nullptr, nullptr}},
     "CASE"
     " WHEN t LIKE 'DECIMAL%[]' THEN 'ARRAY<NUMERIC>'"
     " WHEN t LIKE 'DECIMAL%' THEN 'NUMERIC'"
     " WHEN t LIKE '%[]' THEN 'ARRAY<' || _spanner_scalar_type_name(replace(t, '[]', '')) || '>'"
     " ELSE _spanner_scalar_type_name(t)"
     " END"},

    // spanner_value(val): auto-detect Spanner type from DuckDB type
    {DEFAULT_SCHEMA, "spanner_value", {"val", nullptr}, {{nullptr, nullptr}},
     "CASE"
     " WHEN typeof(val) IN ('BLOB', 'BIT')"
     " THEN json_object('value', base64(TRY_CAST(val AS BLOB)), 'type', _spanner_type_name(typeof(val)))"
     " WHEN typeof(val) = 'TIMESTAMP'"
     " THEN json_object('value', strftime(TRY_CAST(val AS TIMESTAMP), '%Y-%m-%dT%H:%M:%S.%fZ'), 'type', _spanner_type_name(typeof(val)))"
     " WHEN typeof(val) = 'TIMESTAMP WITH TIME ZONE'"
     " THEN json_object('value', strftime(TRY_CAST(val AS TIMESTAMPTZ) AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%fZ'), 'type', _spanner_type_name(typeof(val)))"
     " WHEN typeof(val) = 'INTERVAL'"
     " THEN json_object('value', interval_to_iso8601(TRY_CAST(val AS INTERVAL)), 'type', _spanner_type_name(typeof(val)))"
     " WHEN typeof(val) IN ('UBIGINT', 'HUGEINT', 'UHUGEINT')"
     " THEN json_object('value', CAST(val AS VARCHAR), 'type', _spanner_type_name(typeof(val)))"
     " WHEN typeof(val) IN ('UBIGINT[]', 'HUGEINT[]', 'UHUGEINT[]')"
     " THEN json_object('value', TRY_CAST(val AS VARCHAR[]), 'type', _spanner_type_name(typeof(val)))"
     " WHEN typeof(val) = 'BLOB[]'"
     " THEN json_object('value', list_transform(TRY_CAST(val AS BLOB[]), x -> base64(x)), 'type', _spanner_type_name(typeof(val)))"
     " WHEN typeof(val) = 'TIMESTAMP[]'"
     " THEN json_object('value', list_transform(TRY_CAST(val AS TIMESTAMP[]), x -> strftime(x, '%Y-%m-%dT%H:%M:%S.%fZ')), 'type', _spanner_type_name(typeof(val)))"
     " WHEN typeof(val) = 'TIMESTAMP WITH TIME ZONE[]'"
     " THEN json_object('value', list_transform(TRY_CAST(val AS TIMESTAMPTZ[]), x -> strftime(x AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%fZ')), 'type', _spanner_type_name(typeof(val)))"
     " WHEN typeof(val) = 'INTERVAL[]'"
     " THEN json_object('value', list_transform(TRY_CAST(val AS INTERVAL[]), x -> interval_to_iso8601(x)), 'type', _spanner_type_name(typeof(val)))"
     " WHEN typeof(val) LIKE 'DECIMAL%[]'"
     " THEN json_object('value', TRY_CAST(val AS VARCHAR[]), 'type', _spanner_type_name(typeof(val)))"
     " WHEN typeof(val) LIKE 'DECIMAL%'"
     " THEN json_object('value', CAST(val AS VARCHAR), 'type', _spanner_type_name(typeof(val)))"
     " WHEN _spanner_type_name(typeof(val)) IS NOT NULL"
     " THEN json_object('value', val, 'type', _spanner_type_name(typeof(val)))"
     " ELSE json_object('value', val, 'type', typeof(val))"
     " END"},

    // Terminator
    {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};
// clang-format on

// ─── Table macros ───────────────────────────────────────────────────────────

// clang-format off
static const DefaultTableMacro SPANNER_TABLE_MACROS[] = {
    // spanner_query: user-facing wrapper around spanner_query_raw
    // Note: max 7 named params due to DefaultTableMacro[8] array with null terminator
    {DEFAULT_SCHEMA, "spanner_query",
     {"database", "sql", nullptr},
     {{"params", "NULL"}, {"endpoint", "NULL"},
      {"exact_staleness_secs", "NULL"}, {"max_staleness_secs", "NULL"},
      {"read_timestamp", "NULL"}, {"min_read_timestamp", "NULL"},
      {"priority", "NULL"}},
     "SELECT * FROM spanner_query_raw("
     " database, sql,"
     " endpoint := endpoint,"
     " params := spanner_params(params),"
     " exact_staleness_secs := exact_staleness_secs,"
     " max_staleness_secs := max_staleness_secs,"
     " read_timestamp := read_timestamp,"
     " min_read_timestamp := min_read_timestamp,"
     " priority := priority"
     ")"},

    // Terminator
    {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};
// clang-format on

// ─── Extension entry ────────────────────────────────────────────────────────

static void LoadInternal(ExtensionLoader &loader) {
	RegisterSpannerQueryFunction(loader);
	RegisterSpannerScanFunction(loader);
	RegisterSpannerCopyFunction(loader);

	// Register scalar macros
	for (idx_t index = 0; SPANNER_MACROS[index].name != nullptr; index++) {
		auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(SPANNER_MACROS[index]);
		loader.RegisterFunction(*info);
	}

	// Register table macros
	for (idx_t index = 0; SPANNER_TABLE_MACROS[index].name != nullptr; index++) {
		auto info = DefaultTableFunctionGenerator::CreateTableMacroInfo(SPANNER_TABLE_MACROS[index]);
		loader.RegisterFunction(*info);
	}
}

void SpannerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string SpannerExtension::Name() {
	return "spanner";
}

std::string SpannerExtension::Version() const {
#ifdef EXT_VERSION_SPANNER
	return EXT_VERSION_SPANNER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(spanner, loader) {
	duckdb::LoadInternal(loader);
}
}
