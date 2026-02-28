#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

/// Register spanner_scan table function.
void RegisterSpannerScanFunction(ExtensionLoader &loader);

} // namespace duckdb
