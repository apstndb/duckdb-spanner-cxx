#pragma once

#include "google/cloud/spanner/database.h"
#include "google/cloud/spanner/request_priority.h"
#include "google/cloud/spanner/timestamp.h"
#include "google/cloud/spanner/transaction.h"
#include "absl/types/optional.h"
#include <string>
#include <utility>

namespace duckdb {

/// Parse "projects/P/instances/I/databases/D" into a spanner::Database.
/// Throws on invalid format.
google::cloud::spanner::Database ParseDatabaseName(const std::string &resource_name);

/// Parse "schema.table" into (schema, table). Returns ("", table) if unqualified.
std::pair<std::string, std::string> ParseTableName(const std::string &name);

/// Timestamp bound configuration, matching the Rust implementation.
struct TimestampBoundConfig {
	enum Kind { kNone, kExactStaleness, kMaxStaleness, kReadTimestamp, kMinReadTimestamp };
	Kind kind = kNone;
	int64_t seconds = 0;
	int32_t nanos = 0;
};

/// Build TimestampBoundConfig from named parameters.
/// Validates mutual exclusivity (at most one bound).
TimestampBoundConfig BuildTimestampBound(const std::string &exact_staleness_secs,
                                         const std::string &max_staleness_secs,
                                         const std::string &read_timestamp,
                                         const std::string &min_read_timestamp);

/// Convert TimestampBoundConfig to google-cloud-cpp Transaction::SingleUseOptions.
google::cloud::spanner::Transaction::SingleUseOptions
ToSingleUseOptions(const TimestampBoundConfig &config);

/// Parse priority string ("low", "medium", "high") to spanner::RequestPriority.
/// Returns nullopt for empty string. Throws on invalid value.
absl::optional<google::cloud::spanner::RequestPriority> ParsePriority(const std::string &s);

/// Build google::cloud::Options for request priority.
google::cloud::Options BuildRequestOptions(absl::optional<google::cloud::spanner::RequestPriority> priority);

} // namespace duckdb
