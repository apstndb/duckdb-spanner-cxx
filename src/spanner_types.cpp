#include "spanner_types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "google/cloud/spanner/bytes.h"
#include "google/cloud/spanner/json.h"
#include "google/cloud/spanner/numeric.h"
#include "google/cloud/spanner/timestamp.h"
#include <chrono>
#include <sstream>

namespace duckdb {

namespace spanner = google::cloud::spanner;

// ─── GoogleSQL type parser ──────────────────────────────────────────────────

namespace {

class SpannerTypeParser {
public:
	explicit SpannerTypeParser(const std::string &input) : input_(input), pos_(0) {
	}

	LogicalType Parse() {
		SkipWhitespace();
		auto ident = ParseIdent();
		// uppercase
		std::string upper;
		upper.reserve(ident.size());
		for (auto c : ident) {
			upper += static_cast<char>(std::toupper(c));
		}
		return ParseTypeFromIdent(upper);
	}

private:
	void SkipWhitespace() {
		while (pos_ < input_.size() && std::isspace(input_[pos_])) {
			pos_++;
		}
	}

	char Peek() const {
		return pos_ < input_.size() ? input_[pos_] : '\0';
	}

	bool Consume(char expected) {
		if (Peek() == expected) {
			pos_++;
			return true;
		}
		return false;
	}

	std::string ParseIdent() {
		auto start = pos_;
		while (pos_ < input_.size() && (std::isalnum(input_[pos_]) || input_[pos_] == '_')) {
			pos_++;
		}
		return input_.substr(start, pos_ - start);
	}

	void SkipLength() {
		SkipWhitespace();
		if (Peek() == '(') {
			while (pos_ < input_.size() && input_[pos_] != ')') {
				pos_++;
			}
			if (pos_ < input_.size()) {
				pos_++; // skip ')'
			}
		}
	}

	LogicalType ParseTypeFromIdent(const std::string &ident) {
		if (ident == "BOOL") {
			return LogicalType::BOOLEAN;
		}
		if (ident == "INT64") {
			return LogicalType::BIGINT;
		}
		if (ident == "FLOAT32") {
			return LogicalType::FLOAT;
		}
		if (ident == "FLOAT64") {
			return LogicalType::DOUBLE;
		}
		if (ident == "NUMERIC") {
			return LogicalType::DECIMAL(38, 9);
		}
		if (ident == "STRING") {
			SkipLength();
			return LogicalType::VARCHAR;
		}
		if (ident == "BYTES") {
			SkipLength();
			return LogicalType::BLOB;
		}
		if (ident == "DATE") {
			return LogicalType::DATE;
		}
		if (ident == "TIMESTAMP") {
			return LogicalType::TIMESTAMP_TZ;
		}
		if (ident == "JSON") {
			return LogicalType::VARCHAR;
		}
		if (ident == "UUID") {
			return LogicalType::UUID;
		}
		if (ident == "INTERVAL") {
			return LogicalType::INTERVAL;
		}
		if (ident == "ARRAY") {
			SkipWhitespace();
			Consume('<');
			auto elem = Parse();
			SkipWhitespace();
			Consume('>');
			return LogicalType::LIST(elem);
		}
		if (ident == "STRUCT") {
			SkipWhitespace();
			Consume('<');
			auto fields = ParseStructFields();
			SkipWhitespace();
			Consume('>');
			child_list_t<LogicalType> children;
			for (auto &f : fields) {
				children.push_back(std::make_pair(f.first, f.second));
			}
			return LogicalType::STRUCT(std::move(children));
		}
		if (ident == "PROTO" || ident == "ENUM") {
			// Skip <fqn>
			SkipWhitespace();
			Consume('<');
			while (pos_ < input_.size() && input_[pos_] != '>') {
				pos_++;
			}
			Consume('>');
			// PROTO → BLOB, ENUM → BIGINT
			return ident == "PROTO" ? LogicalType::BLOB : LogicalType::BIGINT;
		}

		// Unknown type fallback
		return LogicalType::VARCHAR;
	}

	std::vector<std::pair<std::string, LogicalType>> ParseStructFields() {
		std::vector<std::pair<std::string, LogicalType>> fields;
		while (true) {
			SkipWhitespace();
			if (Peek() == '>' || pos_ >= input_.size()) {
				break;
			}
			auto name = ParseIdent();
			if (name.empty()) {
				break;
			}
			SkipWhitespace();
			auto field_ident = ParseIdent();
			std::string upper;
			for (auto c : field_ident) {
				upper += static_cast<char>(std::toupper(c));
			}
			auto type = ParseTypeFromIdent(upper);
			fields.emplace_back(name, type);
			SkipWhitespace();
			Consume(',');
		}
		return fields;
	}

	std::string input_;
	size_t pos_;
};

} // anonymous namespace

LogicalType ParseSpannerType(const std::string &type_str) {
	SpannerTypeParser parser(type_str);
	return parser.Parse();
}

// ─── PG dialect type parser ─────────────────────────────────────────────────

LogicalType ParsePgSpannerType(const std::string &type_str) {
	auto s = type_str;
	// Trim
	while (!s.empty() && std::isspace(s.front())) {
		s.erase(s.begin());
	}
	while (!s.empty() && std::isspace(s.back())) {
		s.pop_back();
	}

	// Handle array: "T[]"
	if (s.size() >= 2 && s.substr(s.size() - 2) == "[]") {
		auto elem = ParsePgSpannerType(s.substr(0, s.size() - 2));
		return LogicalType::LIST(elem);
	}

	// Lowercase
	std::string lower;
	lower.reserve(s.size());
	for (auto c : s) {
		lower += static_cast<char>(std::tolower(c));
	}

	// Strip optional length specifier (e.g., "(256)")
	auto base = lower;
	auto paren = base.find('(');
	if (paren != std::string::npos) {
		base = base.substr(0, paren);
		// Trim trailing whitespace
		while (!base.empty() && std::isspace(base.back())) {
			base.pop_back();
		}
	}

	if (base == "boolean" || base == "bool") {
		return LogicalType::BOOLEAN;
	}
	if (base == "bigint" || base == "int8") {
		return LogicalType::BIGINT;
	}
	if (base == "real" || base == "float4") {
		return LogicalType::FLOAT;
	}
	if (base == "double precision" || base == "float8") {
		return LogicalType::DOUBLE;
	}
	if (base == "numeric") {
		return LogicalType::DECIMAL(38, 9);
	}
	if (base == "character varying" || base == "varchar" || base == "text") {
		return LogicalType::VARCHAR;
	}
	if (base == "bytea") {
		return LogicalType::BLOB;
	}
	if (base == "date") {
		return LogicalType::DATE;
	}
	if (base == "timestamp with time zone" || base == "timestamptz") {
		return LogicalType::TIMESTAMP_TZ;
	}
	if (base == "jsonb") {
		return LogicalType::VARCHAR;
	}
	if (base == "uuid") {
		return LogicalType::UUID;
	}

	// Unknown → VARCHAR
	return LogicalType::VARCHAR;
}

// ─── Value conversion: Spanner → DuckDB ─────────────────────────────────────

Value SpannerValueToDuckDB(const spanner::Value &spanner_val, const LogicalType &target_type) {
	// Check for NULL
	// google-cloud-cpp spanner::Value doesn't have a direct is_null(),
	// but trying to get a value from a NULL returns a non-ok status.
	// We use get<std::optional<T>> pattern instead.

	switch (target_type.id()) {
	case LogicalTypeId::BOOLEAN: {
		auto v = spanner_val.get<std::optional<bool>>();
		if (v.ok() && v->has_value()) {
			return Value::BOOLEAN(v->value());
		}
		return Value(LogicalType::BOOLEAN);
	}
	case LogicalTypeId::BIGINT: {
		auto v = spanner_val.get<std::optional<std::int64_t>>();
		if (v.ok() && v->has_value()) {
			return Value::BIGINT(v->value());
		}
		return Value(LogicalType::BIGINT);
	}
	case LogicalTypeId::FLOAT: {
		auto v = spanner_val.get<std::optional<float>>();
		if (v.ok() && v->has_value()) {
			return Value::FLOAT(v->value());
		}
		return Value(LogicalType::FLOAT);
	}
	case LogicalTypeId::DOUBLE: {
		auto v = spanner_val.get<std::optional<double>>();
		if (v.ok() && v->has_value()) {
			return Value::DOUBLE(v->value());
		}
		return Value(LogicalType::DOUBLE);
	}
	case LogicalTypeId::DECIMAL: {
		auto v = spanner_val.get<std::optional<spanner::Numeric>>();
		if (v.ok() && v->has_value()) {
			auto str = v->value().ToString();
			return Value(str).DefaultCastAs(target_type);
		}
		return Value(target_type);
	}
	case LogicalTypeId::VARCHAR: {
		auto v = spanner_val.get<std::optional<std::string>>();
		if (v.ok() && v->has_value()) {
			return Value(v->value());
		}
		// Try JSON
		auto jv = spanner_val.get<std::optional<spanner::Json>>();
		if (jv.ok() && jv->has_value()) {
			return Value(std::string(jv->value()));
		}
		return Value(LogicalType::VARCHAR);
	}
	case LogicalTypeId::BLOB: {
		auto v = spanner_val.get<std::optional<spanner::Bytes>>();
		if (v.ok() && v->has_value()) {
			auto raw = v->value().get<std::string>();
			return Value::BLOB(raw);
		}
		return Value(LogicalType::BLOB);
	}
	case LogicalTypeId::DATE: {
		auto v = spanner_val.get<std::optional<absl::CivilDay>>();
		if (v.ok() && v->has_value()) {
			auto day = v->value();
			auto date = Date::FromDate(static_cast<int32_t>(day.year()),
			                           static_cast<int32_t>(day.month()),
			                           static_cast<int32_t>(day.day()));
			return Value::DATE(date);
		}
		return Value(LogicalType::DATE);
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		auto v = spanner_val.get<std::optional<spanner::Timestamp>>();
		if (v.ok() && v->has_value()) {
			auto tp = v->value().get<std::chrono::system_clock::time_point>().value();
			auto epoch_us = std::chrono::duration_cast<std::chrono::microseconds>(
			    tp.time_since_epoch());
			return Value::TIMESTAMPTZ(timestamp_tz_t(epoch_us.count()));
		}
		return Value(LogicalType::TIMESTAMP_TZ);
	}
	case LogicalTypeId::UUID: {
		// Spanner stores UUID as string
		auto v = spanner_val.get<std::optional<std::string>>();
		if (v.ok() && v->has_value()) {
			return Value(v->value()).DefaultCastAs(LogicalType::UUID);
		}
		return Value(LogicalType::UUID);
	}
	case LogicalTypeId::INTERVAL: {
		// Spanner INTERVAL is returned as string
		auto v = spanner_val.get<std::optional<std::string>>();
		if (v.ok() && v->has_value()) {
			return Value(v->value()).DefaultCastAs(LogicalType::INTERVAL);
		}
		return Value(LogicalType::INTERVAL);
	}
	case LogicalTypeId::LIST: {
		// Arrays: decompose via proto to access elements
		auto val_copy = spanner_val;
		auto [type_proto, value_proto] = google::cloud::spanner_internal::ToProto(std::move(val_copy));

		if (value_proto.kind_case() == google::protobuf::Value::kNullValue) {
			return Value(target_type);
		}

		auto child_type = ListType::GetChildType(target_type);
		vector<Value> children;

		if (value_proto.kind_case() == google::protobuf::Value::kListValue) {
			auto &elem_type = type_proto.array_element_type();
			for (int i = 0; i < value_proto.list_value().values_size(); i++) {
				auto elem_val = google::cloud::spanner_internal::FromProto(
				    elem_type, value_proto.list_value().values(i));
				children.push_back(SpannerValueToDuckDB(elem_val, child_type));
			}
		}
		return Value::LIST(child_type, std::move(children));
	}
	case LogicalTypeId::STRUCT: {
		// Structs: decompose via proto to access fields
		auto val_copy = spanner_val;
		auto [type_proto, value_proto] = google::cloud::spanner_internal::ToProto(std::move(val_copy));

		if (value_proto.kind_case() == google::protobuf::Value::kNullValue) {
			return Value(target_type);
		}

		auto &child_types = StructType::GetChildTypes(target_type);
		child_list_t<Value> children;

		if (value_proto.kind_case() == google::protobuf::Value::kListValue) {
			auto &struct_type = type_proto.struct_type();
			for (idx_t i = 0; i < child_types.size() &&
			     i < static_cast<idx_t>(value_proto.list_value().values_size()); i++) {
				auto field_type = struct_type.fields(static_cast<int>(i)).type();
				auto field_val = google::cloud::spanner_internal::FromProto(
				    field_type, value_proto.list_value().values(static_cast<int>(i)));
				children.push_back(std::make_pair(
				    child_types[i].first,
				    SpannerValueToDuckDB(field_val, child_types[i].second)));
			}
		}
		return Value::STRUCT(std::move(children));
	}
	default:
		// Fallback: try string
		auto v = spanner_val.get<std::optional<std::string>>();
		if (v.ok() && v->has_value()) {
			return Value(v->value());
		}
		return Value(target_type);
	}
}

// ─── Value conversion: DuckDB → Spanner ─────────────────────────────────────

spanner::Value DuckDBValueToSpanner(const Value &duck_val, const LogicalType &type) {
	if (duck_val.IsNull()) {
		// Return typed NULL
		switch (type.id()) {
		case LogicalTypeId::BOOLEAN:
			return spanner::Value(std::optional<bool>());
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::TINYINT:
			return spanner::Value(std::optional<std::int64_t>());
		case LogicalTypeId::FLOAT:
			return spanner::Value(std::optional<double>());
		case LogicalTypeId::DOUBLE:
			return spanner::Value(std::optional<double>());
		case LogicalTypeId::DECIMAL:
			return spanner::Value(std::optional<spanner::Numeric>());
		case LogicalTypeId::DATE:
			return spanner::Value(std::optional<absl::CivilDay>());
		case LogicalTypeId::TIMESTAMP_TZ:
		case LogicalTypeId::TIMESTAMP:
			return spanner::Value(std::optional<spanner::Timestamp>());
		case LogicalTypeId::BLOB:
			return spanner::Value(std::optional<spanner::Bytes>());
		default:
			return spanner::Value(std::optional<std::string>());
		}
	}

	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return spanner::Value(duck_val.GetValue<bool>());
	case LogicalTypeId::TINYINT:
		return spanner::Value(static_cast<std::int64_t>(duck_val.GetValue<int8_t>()));
	case LogicalTypeId::SMALLINT:
		return spanner::Value(static_cast<std::int64_t>(duck_val.GetValue<int16_t>()));
	case LogicalTypeId::INTEGER:
		return spanner::Value(static_cast<std::int64_t>(duck_val.GetValue<int32_t>()));
	case LogicalTypeId::BIGINT:
		return spanner::Value(duck_val.GetValue<int64_t>());
	case LogicalTypeId::FLOAT:
		return spanner::Value(static_cast<double>(duck_val.GetValue<float>()));
	case LogicalTypeId::DOUBLE:
		return spanner::Value(duck_val.GetValue<double>());
	case LogicalTypeId::DECIMAL: {
		auto str = duck_val.ToString();
		auto numeric = spanner::MakeNumeric(str);
		if (numeric.ok()) {
			return spanner::Value(*numeric);
		}
		return spanner::Value(str);
	}
	case LogicalTypeId::VARCHAR:
		return spanner::Value(duck_val.GetValue<string>());
	case LogicalTypeId::BLOB: {
		auto blob = duck_val.GetValueUnsafe<string>();
		return spanner::Value(spanner::Bytes(blob));
	}
	case LogicalTypeId::DATE: {
		auto date = duck_val.GetValue<date_t>();
		int32_t year, month, day;
		Date::Convert(date, year, month, day);
		return spanner::Value(absl::CivilDay(year, month, day));
	}
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP: {
		auto ts = duck_val.GetValue<timestamp_t>();
		auto micros = Timestamp::GetEpochMicroSeconds(ts);
		auto tp = std::chrono::system_clock::time_point(std::chrono::microseconds(micros));
		return spanner::Value(spanner::MakeTimestamp(tp).value());
	}
	case LogicalTypeId::UUID: {
		auto str = duck_val.ToString();
		return spanner::Value(str);
	}
	default:
		return spanner::Value(duck_val.ToString());
	}
}

} // namespace duckdb
