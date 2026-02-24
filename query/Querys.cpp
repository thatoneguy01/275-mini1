#include "Querys.hpp"
#include <memory>
#include <any>
#include <stdexcept>
#include <typeinfo>
#include "../dob/dob_parse_utils.hpp"
#include "../dob/DobCsv.hpp"

namespace query {

    namespace {
        // ...existing code...

        // Parse field to int64 (for numeric columns)
        int64_t parse_numeric(std::string_view field) {
            // For numeric columns, try to parse as int64 first, then as double
            int64_t v{};
            auto result = std::from_chars(field.data(), field.data() + field.size(), v);
            if (result.ec == std::errc{}) {
                return v;
            }
            // If integer parsing fails, try double
            double d = 0.0;
            std::from_chars(field.data(), field.data() + field.size(), d);
            return static_cast<int64_t>(d);
        }

        // Cast any value to int64
        int64_t to_numeric(const std::any& val) {
            const std::type_info& valType = val.type();
            if (valType == typeid(int)) return std::any_cast<int>(val);
            if (valType == typeid(int16_t)) return std::any_cast<int16_t>(val);
            if (valType == typeid(int32_t)) return std::any_cast<int32_t>(val);
            if (valType == typeid(int64_t)) return std::any_cast<int64_t>(val);
            if (valType == typeid(uint8_t)) return std::any_cast<uint8_t>(val);
            if (valType == typeid(uint32_t)) return std::any_cast<uint32_t>(val);
            if (valType == typeid(double)) return static_cast<int64_t>(std::any_cast<double>(val));
            if (valType == typeid(float)) return static_cast<int64_t>(std::any_cast<float>(val));
            throw std::bad_any_cast();
        }

        // Parse string field as string (strip quotes)
        std::string parse_string(std::string_view field) {
            if (!field.empty() && field.front() == '"' && field.back() == '"') {
                field = field.substr(1, field.size() - 2);
            }
            return std::string(field);
        }

        // Cast any value to string
        std::string to_string(const std::any& val) {
            const std::type_info& valType = val.type();
            if (valType == typeid(std::string)) return std::any_cast<std::string>(val);
            if (valType == typeid(const char*)) return std::any_cast<const char*>(val);
            if (valType == typeid(std::string_view)) return std::string(std::any_cast<std::string_view>(val));
            throw std::bad_any_cast();
        }

        // Parse bool field
        bool parse_bool(std::string_view field) {
            return field == "1" || field == "true" || field == "True" || field == "TRUE";
        }

        // Cast any value to bool
        bool to_bool(const std::any& val) {
            const std::type_info& valType = val.type();
            if (valType == typeid(bool)) return std::any_cast<bool>(val);
            if (valType == typeid(int)) return std::any_cast<int>(val) != 0;
            throw std::bad_any_cast();
        }
    }

    // Query implementations

    class AndQuery : public Query {
    private:
        std::vector<std::unique_ptr<Query>> subqueries_;

    public:
        AndQuery(std::vector<std::unique_ptr<Query>> subqueries)
            : subqueries_(std::move(subqueries)) {}

        template<typename... Queries>
        AndQuery(Queries&&... queries) : subqueries_() {
            (subqueries_.push_back(std::forward<Queries>(queries)), ...);
        }

        bool eval(std::string_view row) override {
            if (subqueries_.empty()) { return false; }
            for (const auto& subquery : subqueries_) {
                if (!subquery->eval(row)) { return false; }
            }
            return true;
        }
    };

    class OrQuery : public Query {
    private:
        std::vector<std::unique_ptr<Query>> subqueries_;

    public:
        OrQuery(std::vector<std::unique_ptr<Query>> subqueries)
            : subqueries_(std::move(subqueries)) {}

        template<typename... Queries>
        OrQuery(Queries&&... queries) : subqueries_() {
            (subqueries_.push_back(std::forward<Queries>(queries)), ...);
        }

        bool eval(std::string_view row) override {
            if (subqueries_.empty()) { return false; }
            for (const auto& subquery : subqueries_) {
                if (subquery->eval(row)) { return true; }
            }
            return false;
        }
    };

    class NotQuery : public Query {
    private:
        std::unique_ptr<Query> subquery_;
    public:
        explicit NotQuery(std::unique_ptr<Query> subquery)
            : subquery_(std::move(subquery)) {}

        bool eval(std::string_view row) override {
            return !subquery_->eval(row);
        }
    };

    class MatchQuery : public Query {
    private:
        int columnIndex_;
        dob::ColumnCategory category_;
        std::any value_;
        const std::type_info* columnType_;

    public:
        MatchQuery(std::string_view column, const std::any& value) {
            auto info = dob::column_info(column);
            if (!info) {
                throw std::invalid_argument("Column name not found: " + std::string(column));
            }
            columnIndex_ = info->first;
            category_ = info->second;
            value_ = value;
            columnType_ = nullptr;  // Not needed anymore since we have category
        }

        bool eval(std::string_view row) override {
            static thread_local std::vector<std::string_view> fields;
            dob::split_csv_line(row, fields);

            if (columnIndex_ >= static_cast<int>(fields.size())) {
                return false;
            }

            std::string_view field = fields[columnIndex_];

            // Compare based on category
            if (category_ == dob::ColumnCategory::STRING) {
                return parse_string(field) == to_string(value_);
            } else if (category_ == dob::ColumnCategory::BOOLEAN) {
                return parse_bool(field) == to_bool(value_);
            } else { // NUMERIC
                return parse_numeric(field) == to_numeric(value_);
            }
        }
    };

    class RangeQuery : public Query {
    private:
        int columnIndex_;
        dob::ColumnCategory category_;
        std::any minValue_;
        std::any maxValue_;

    public:
        RangeQuery(std::string_view column, const std::any& minValue, const std::any& maxValue) {
            auto info = dob::column_info(column);
            if (!info) {
                throw std::invalid_argument("Column name not found: " + std::string(column));
            }

            columnIndex_ = info->first;
            category_ = info->second;

            // Check if the column type supports range queries (BOOLEAN is not supported)
            if (category_ == dob::ColumnCategory::BOOLEAN) {
                throw std::invalid_argument(
                    "Range queries are not supported for BOOL columns: " +
                    std::string(column)
                );
            }

            minValue_ = minValue;
            maxValue_ = maxValue;
        }

        bool eval(std::string_view row) override {
            static thread_local std::vector<std::string_view> fields;
            dob::split_csv_line(row, fields);

            if (columnIndex_ >= static_cast<int>(fields.size())) {
                return false;
            }

            std::string_view field = fields[columnIndex_];

            if (category_ == dob::ColumnCategory::STRING) {
                // Range check on string values
                std::string parsed = parse_string(field);
                std::string minVal = to_string(minValue_);
                std::string maxVal = to_string(maxValue_);
                return parsed >= minVal && parsed <= maxVal;
            } else if (category_ == dob::ColumnCategory::BOOLEAN) {
                throw std::invalid_argument("Range queries are not supported for BOOL columns");
            } else {
                // Range check on numeric values
                int64_t parsed = parse_numeric(field);
                int64_t minVal = to_numeric(minValue_);
                int64_t maxVal = to_numeric(maxValue_);
                return parsed >= minVal && parsed <= maxVal;
            }
        }
    };

} // namespace query
