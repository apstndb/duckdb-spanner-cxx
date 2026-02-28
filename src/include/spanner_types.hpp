#pragma once

#include "duckdb.hpp"
#include "google/cloud/spanner/row.h"
#include "google/cloud/spanner/value.h"
#include <string>

namespace duckdb {

/// Parse a GoogleSQL SPANNER_TYPE string (e.g., "BOOL", "STRING(MAX)", "ARRAY<INT64>")
/// and return the corresponding DuckDB LogicalType.
LogicalType ParseSpannerType(const std::string &type_str);

/// Parse a PG dialect SPANNER_TYPE string (e.g., "boolean", "bigint", "character varying(256)")
/// and return the corresponding DuckDB LogicalType.
LogicalType ParsePgSpannerType(const std::string &type_str);

/// Convert a single Spanner Value to a DuckDB Value, given the expected DuckDB type.
Value SpannerValueToDuckDB(const google::cloud::spanner::Value &spanner_val,
                           const LogicalType &target_type);

/// Convert a DuckDB Value to a Spanner Value for use in mutations.
google::cloud::spanner::Value DuckDBValueToSpanner(const Value &duck_val, const LogicalType &type);

} // namespace duckdb
