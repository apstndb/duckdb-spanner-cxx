#pragma once

#include "google/cloud/spanner/client.h"
#include <memory>
#include <string>

namespace duckdb {

/// Get or create a cached Spanner client for the given database and endpoint.
/// - endpoint empty + SPANNER_EMULATOR_HOST set → emulator mode (auto)
/// - endpoint empty + no env → production (default credentials)
/// - endpoint non-empty → explicit emulator endpoint
std::shared_ptr<google::cloud::spanner::Client>
GetOrCreateClient(const google::cloud::spanner::Database &db, const std::string &endpoint = "");

} // namespace duckdb
