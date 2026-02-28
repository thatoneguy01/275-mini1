#pragma once

#include <string_view>
#include <vector>
#include <memory>
#include <any>
#include <type_traits>
#include "../dob/DobParseUtils.hpp"

namespace query {
    class Query {
    public:
        virtual ~Query() = default;

        // Evaluate the query against a CSV row
        virtual bool eval(std::string_view row) = 0;
    };

    // Logical AND query - all subqueries must match
    class AndQuery : public Query {
    private:
        std::vector<std::unique_ptr<Query>> subqueries_;
    public:
        explicit AndQuery(std::vector<std::unique_ptr<Query>> subqueries);

        template<typename... Queries>
        explicit AndQuery(Queries&&... queries);

        bool eval(std::string_view row) override;
    };

    // Logical OR query - any subquery must match
    class OrQuery : public Query {
    private:
        std::vector<std::unique_ptr<Query>> subqueries_;
    public:
        explicit OrQuery(std::vector<std::unique_ptr<Query>> subqueries);

        template<typename... Queries>
        explicit OrQuery(Queries&&... queries);

        bool eval(std::string_view row) override;
    };

    class NotQuery : public Query {
    private:
        std::unique_ptr<Query> subquery_;
    public:
        explicit NotQuery(std::unique_ptr<Query> subquery);

        bool eval(std::string_view row) override;
    };

    // Equality match query - field equals a value
    class MatchQuery : public Query {
    private:
        int columnIndex_;
        dob::ColumnCategory category_;
        std::any value_;
        const std::type_info* columnType_;

    public:
        MatchQuery(std::string_view column, const std::any& value);

        template<typename T, typename = std::enable_if_t<!std::is_same_v<std::decay_t<T>, std::any>>>
        MatchQuery(std::string_view column, T&& value)
            : MatchQuery(column, std::any(std::forward<T>(value))) {}

        bool eval(std::string_view row) override;
    };

    // Range query - field is between min and max values
    // Supports: numeric columns and string columns
    // Does not support: boolean columns
    class RangeQuery : public Query {
    private:
        int columnIndex_;
        dob::ColumnCategory category_;
        std::any minValue_;
        std::any maxValue_;

    public:
        RangeQuery(std::string_view column, const std::any& minValue, const std::any& maxValue);

        template<typename T, typename U, typename = std::enable_if_t<!std::is_same_v<std::decay_t<T>, std::any> && !std::is_same_v<std::decay_t<U>, std::any>>>
        RangeQuery(std::string_view column, T&& minValue, U&& maxValue)
            : RangeQuery(column, std::any(std::forward<T>(minValue)), std::any(std::forward<U>(maxValue))) {}

        bool eval(std::string_view row) override;
    };

} // namespace query
