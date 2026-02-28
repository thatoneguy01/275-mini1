#include "Querys.hpp"
#include <memory>
#include <any>
#include <stdexcept>
#include <typeinfo>
#include "../dob/DobCsv.hpp"

namespace query {

    namespace {
        // ...existing code...

        // Parse field to int64 (for numeric columns)
        double parse_numeric(std::string_view field) {
            double val = 0.0;
            std::from_chars(field.data(), field.data() + field.size(), val);
            return val;
        }

        // Parse string field as string (strip quotes)
        std::string parse_string(std::string_view field) {
            if (!field.empty() && field.front() == '"' && field.back() == '"') {
                field = field.substr(1, field.size() - 2);
            }
            return std::string(field);
        }

        // Parse bool field
        bool parse_bool(std::string_view field) {
            return field == "1" || field == "true" || field == "True" || field == "TRUE";
        }

        // Safe string extraction from std::any - handles const char*
        std::string safe_any_cast_string(const std::any& value) {
            if (value.type() == typeid(std::string)) {
                return std::any_cast<std::string>(value);
            } else if (value.type() == typeid(const char*)) {
                return std::string(std::any_cast<const char*>(value));
            } else if (value.type() == typeid(char*)) {
                return std::string(std::any_cast<char*>(value));
            }
            throw std::bad_any_cast();
        }

        // Safe numeric extraction from std::any
        double safe_any_cast_numeric(const std::any& value) {
            if (value.type() == typeid(double)) {
                return std::any_cast<double>(value);
            } else if (value.type() == typeid(int)) {
                return static_cast<double>(std::any_cast<int>(value));
            } else if (value.type() == typeid(long)) {
                return static_cast<double>(std::any_cast<long>(value));
            } else if (value.type() == typeid(float)) {
                return static_cast<double>(std::any_cast<float>(value));
            }
            throw std::bad_any_cast();
        }

        // Safe bool extraction from std::any
        bool safe_any_cast_bool(const std::any& value) {
            if (value.type() == typeid(bool)) {
                return std::any_cast<bool>(value);
            }
            throw std::bad_any_cast();
        }

    }

    // Query implementations
    AndQuery::AndQuery(std::vector<std::unique_ptr<Query>> subqueries)
        : subqueries_(std::move(subqueries)) {}

    template<typename... Queries>
    AndQuery::AndQuery(Queries&&... queries) : subqueries_() {
        (subqueries_.push_back(std::forward<Queries>(queries)), ...);
    }

    bool AndQuery::eval(std::string_view row)  {
        if (subqueries_.empty()) { return false; }
        for (const auto& subquery : subqueries_) {
            if (!subquery->eval(row)) { return false; }
        }
        return true;
    }

    OrQuery::OrQuery(std::vector<std::unique_ptr<Query>> subqueries)
        : subqueries_(std::move(subqueries)) {}

    template<typename... Queries>
    OrQuery::OrQuery(Queries&&... queries) : subqueries_() {
        (subqueries_.push_back(std::forward<Queries>(queries)), ...);
    }

    bool OrQuery::eval(std::string_view row)  {
        if (subqueries_.empty()) { return false; }
        for (const auto& subquery : subqueries_) {
            if (subquery->eval(row)) { return true; }
        }
        return false;
    }

    NotQuery::NotQuery(std::unique_ptr<Query> subquery) : subquery_(std::move(subquery)) {}

    bool NotQuery::eval(std::string_view row)  {
        return !subquery_->eval(row);
    }

    MatchQuery::MatchQuery(std::string_view column, const std::any& value) {
        auto info = dob::column_info(column);
        if (!info) {
            throw std::invalid_argument("Column name not found: " + std::string(column));
        }
        columnIndex_ = info->first;
        category_ = info->second;
        value_ = value;
        columnType_ = nullptr;  // Not needed anymore since we have category
    }

    bool MatchQuery::eval(std::string_view row)  {
        static thread_local std::vector<std::string_view> fields;
        dob::split_csv_line(row, fields);

        if (columnIndex_ >= static_cast<int>(fields.size())) {
            return false;
        }

        std::string_view field = fields[columnIndex_];

        // Compare based on category
        switch (category_) {
            case dob::ColumnCategory::STRING: {
                std::string parsed = parse_string(field);
                std::string val = safe_any_cast_string(value_);
                return parsed == val;
            }
            case dob::ColumnCategory::BOOLEAN: {
                bool parsed = parse_bool(field);
                bool val = safe_any_cast_bool(value_);
                return parsed == val;
            }
            case dob::ColumnCategory::NUMERIC: {
                double parsed = parse_numeric(field);
                double val = safe_any_cast_numeric(value_);
                return parsed == val;
            }
            default:
                throw std::runtime_error("Unsupported column category");
        }
    };


    RangeQuery::RangeQuery(std::string_view column, const std::any& minValue, const std::any& maxValue) {
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

    bool RangeQuery::eval(std::string_view row) {
        static thread_local std::vector<std::string_view> fields;
        dob::split_csv_line(row, fields);

        if (columnIndex_ >= static_cast<int>(fields.size())) {
            return false;
        }

        std::string_view field = fields[columnIndex_];

        if (category_ == dob::ColumnCategory::STRING) {
            // Range check on string values
            std::string parsed = parse_string(field);
            std::string minVal = safe_any_cast_string(minValue_);
            std::string maxVal = safe_any_cast_string(maxValue_);
            return parsed >= minVal && parsed <= maxVal;
        } else if (category_ == dob::ColumnCategory::BOOLEAN) {
            throw std::invalid_argument("Range queries are not supported for BOOL columns");
        } else {
            // Range check on numeric values
            double parsed = parse_numeric(field);
            double minVal = safe_any_cast_numeric(minValue_);
            double maxVal = safe_any_cast_numeric(maxValue_);
            return parsed >= minVal && parsed <= maxVal;
        }
    };

} // namespace query
