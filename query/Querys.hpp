#pragma once

#include <string_view>
#include <vector>
#include <memory>
#include <any>

namespace query {
    class Query {
    public:
        virtual ~Query() = default;

        // Evaluate the query against a CSV row
        virtual bool eval(std::string_view row) = 0;
    };

    // Logical AND query - all subqueries must match
    class AndQuery : public Query {
    public:
        explicit AndQuery(std::vector<std::unique_ptr<Query>> subqueries);

        template<typename... Queries>
        explicit AndQuery(Queries&&... queries);

        bool eval(std::string_view row) override;
    };

    // Logical OR query - any subquery must match
    class OrQuery : public Query {
    public:
        explicit OrQuery(std::vector<std::unique_ptr<Query>> subqueries);

        template<typename... Queries>
        explicit OrQuery(Queries&&... queries);

        bool eval(std::string_view row) override;
    };

    // Equality match query - field equals a value
    class MatchQuery : public Query {
    public:
        MatchQuery(std::string_view column, const std::any& value);

        bool eval(std::string_view row) override;
    };

    // Range query - field is between min and max values
    // Supports: numeric columns and string columns
    // Does not support: boolean columns
    class RangeQuery : public Query {
    public:
        RangeQuery(std::string_view column, const std::any& minValue, const std::any& maxValue);

        bool eval(std::string_view row) override;
    };

} // namespace query
