#pragma once

#include "duckdb.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

/// Register spanner CopyFunction (COPY TO).
void RegisterSpannerCopyFunction(ExtensionLoader &loader);

} // namespace duckdb
