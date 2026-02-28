#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

/// Register spanner_query and spanner_query_raw table functions.
void RegisterSpannerQueryFunction(ExtensionLoader &loader);

} // namespace duckdb
