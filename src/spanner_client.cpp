#include "spanner_client.hpp"
#include "google/cloud/spanner/client.h"
#include "google/cloud/options.h"
#include "google/cloud/common_options.h"
#include <cstdlib>
#include <mutex>
#include <unordered_map>

namespace duckdb {

namespace spanner = google::cloud::spanner;

static std::mutex client_cache_mutex;
static std::unordered_map<std::string, std::shared_ptr<spanner::Client>> client_cache;

std::shared_ptr<spanner::Client>
GetOrCreateClient(const spanner::Database &db, const std::string &endpoint) {
	// Cache key: "projects/P/instances/I/databases/D" optionally + "@endpoint"
	std::string key = db.FullName();
	if (!endpoint.empty()) {
		key += "@" + endpoint;
	}

	std::lock_guard<std::mutex> lock(client_cache_mutex);
	auto it = client_cache.find(key);
	if (it != client_cache.end()) {
		return it->second;
	}

	google::cloud::Options options;
	if (!endpoint.empty()) {
		options.set<google::cloud::EndpointOption>(endpoint);
	}

	auto conn = spanner::MakeConnection(db, options);
	auto client = std::make_shared<spanner::Client>(std::move(conn));
	client_cache[key] = client;
	return client;
}

} // namespace duckdb
