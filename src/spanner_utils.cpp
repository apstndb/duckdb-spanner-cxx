#include "spanner_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "google/cloud/spanner/options.h"
#include "google/cloud/spanner/timestamp.h"
#include "absl/time/time.h"
#include <chrono>
#include <regex>
#include <sstream>

namespace duckdb {

namespace spanner = google::cloud::spanner;

google::cloud::spanner::Database ParseDatabaseName(const std::string &resource_name) {
	// Expected format: "projects/P/instances/I/databases/D"
	static const std::regex re(R"(^projects/([^/]+)/instances/([^/]+)/databases/([^/]+)$)");
	std::smatch match;
	if (!std::regex_match(resource_name, match, re)) {
		throw InvalidInputException(
		    "Invalid Spanner database name '%s'. Expected format: projects/<project>/instances/<instance>/databases/<database>",
		    resource_name);
	}
	return spanner::Database(match[1].str(), match[2].str(), match[3].str());
}

std::pair<std::string, std::string> ParseTableName(const std::string &name) {
	auto dot_pos = name.find('.');
	if (dot_pos == std::string::npos) {
		return {"", name};
	}
	return {name.substr(0, dot_pos), name.substr(dot_pos + 1)};
}

static spanner::Timestamp ParseRfc3339Timestamp(const std::string &s) {
	absl::Time t;
	std::string err;
	if (!absl::ParseTime(absl::RFC3339_full, s, &t, &err)) {
		throw InvalidInputException("Invalid RFC 3339 timestamp: '%s': %s", s, err);
	}
	auto ts = spanner::MakeTimestamp(t);
	if (!ts.ok()) {
		throw InvalidInputException("Timestamp out of range: '%s': %s", s, ts.status().message());
	}
	return *ts;
}

static void ParseRfc3339(const std::string &s, int64_t &seconds, int32_t &nanos) {
	auto ts = ParseRfc3339Timestamp(s);
	auto tp = ts.get<std::chrono::system_clock::time_point>().value();
	auto epoch = tp.time_since_epoch();
	auto secs = std::chrono::duration_cast<std::chrono::seconds>(epoch);
	auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(epoch - secs);
	seconds = secs.count();
	nanos = static_cast<int32_t>(ns.count());
}

TimestampBoundConfig BuildTimestampBound(const std::string &exact_staleness_secs,
                                         const std::string &max_staleness_secs,
                                         const std::string &read_timestamp,
                                         const std::string &min_read_timestamp) {
	int count = 0;
	TimestampBoundConfig config;

	if (!exact_staleness_secs.empty()) {
		count++;
		config.kind = TimestampBoundConfig::kExactStaleness;
		config.seconds = std::stoll(exact_staleness_secs);
	}
	if (!max_staleness_secs.empty()) {
		count++;
		config.kind = TimestampBoundConfig::kMaxStaleness;
		config.seconds = std::stoll(max_staleness_secs);
	}
	if (!read_timestamp.empty()) {
		count++;
		config.kind = TimestampBoundConfig::kReadTimestamp;
		ParseRfc3339(read_timestamp, config.seconds, config.nanos);
	}
	if (!min_read_timestamp.empty()) {
		count++;
		config.kind = TimestampBoundConfig::kMinReadTimestamp;
		ParseRfc3339(min_read_timestamp, config.seconds, config.nanos);
	}

	if (count > 1) {
		throw InvalidInputException(
		    "At most one timestamp bound parameter can be specified "
		    "(exact_staleness_secs, max_staleness_secs, read_timestamp, min_read_timestamp)");
	}

	return config;
}

google::cloud::spanner::Transaction::SingleUseOptions
ToSingleUseOptions(const TimestampBoundConfig &config) {
	using Transaction = spanner::Transaction;
	switch (config.kind) {
	case TimestampBoundConfig::kExactStaleness:
		// ReadOnlyOptions(nanoseconds) = exact staleness
		return Transaction::SingleUseOptions(
		    Transaction::ReadOnlyOptions(std::chrono::seconds(config.seconds)));
	case TimestampBoundConfig::kMaxStaleness:
		// SingleUseOptions(nanoseconds) = max staleness (bounded)
		return Transaction::SingleUseOptions(std::chrono::seconds(config.seconds));
	case TimestampBoundConfig::kReadTimestamp: {
		// ReadOnlyOptions(Timestamp) = read at exact timestamp
		auto tp = std::chrono::system_clock::time_point(
		    std::chrono::duration_cast<std::chrono::system_clock::duration>(
		        std::chrono::seconds(config.seconds) + std::chrono::nanoseconds(config.nanos)));
		auto ts = spanner::MakeTimestamp(tp);
		return Transaction::SingleUseOptions(Transaction::ReadOnlyOptions(ts.value()));
	}
	case TimestampBoundConfig::kMinReadTimestamp: {
		// SingleUseOptions(Timestamp) = min read timestamp (bounded)
		auto tp = std::chrono::system_clock::time_point(
		    std::chrono::duration_cast<std::chrono::system_clock::duration>(
		        std::chrono::seconds(config.seconds) + std::chrono::nanoseconds(config.nanos)));
		auto ts = spanner::MakeTimestamp(tp);
		return Transaction::SingleUseOptions(ts.value());
	}
	default:
		return Transaction::SingleUseOptions(Transaction::ReadOnlyOptions()); // Strong read
	}
}

absl::optional<spanner::RequestPriority> ParsePriority(const std::string &s) {
	if (s.empty()) {
		return absl::nullopt;
	}
	std::string lower = s;
	for (auto &c : lower) {
		c = std::tolower(c);
	}
	if (lower == "low") {
		return spanner::RequestPriority::kLow;
	} else if (lower == "medium") {
		return spanner::RequestPriority::kMedium;
	} else if (lower == "high") {
		return spanner::RequestPriority::kHigh;
	}
	throw InvalidInputException("Invalid priority '%s'. Expected 'low', 'medium', or 'high'", s);
}

google::cloud::Options BuildRequestOptions(absl::optional<spanner::RequestPriority> priority) {
	google::cloud::Options opts;
	if (priority.has_value()) {
		opts.set<spanner::RequestPriorityOption>(*priority);
	}
	return opts;
}

} // namespace duckdb
