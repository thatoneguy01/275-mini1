#include "AbstractQuery.hpp"

namespace query {
    // Query is an abstract base class with pure virtual methods

    class AndQuery : public Query {
    public:
        AndQuery(std::vector<std::unique_ptr<Query>> subqueries)
            : subqueries_(std::move(subqueries)) {}

        std::vector<dob::DobJobApplication> execute(CsvIndexedFile& file) override {
            auto queryIterator = subqueries_.begin();
            if (queryIterator == subqueries_.end()) {
                return {};
            }

            std::vector<dob::DobJobApplication> result = (*queryIterator)->execute(file);
            std::unordered_set<dob::DobJobApplication> resultSet(result.begin(), result.end());
            ++queryIterator;

            for (; queryIterator != subqueries_.end() && !resultsSet.empty(); ++queryIterator) {
                std::vector<dob::DobJobApplication> subResult = (*queryIterator)->execute(file);
                std::unordered_set<dob::DobJobApplication> subResultSet(subResult.begin(), subResult.end());

                for (auto it = resultSet.begin(); it != resultSet.end();) {
                    if (subResultSet.find(*it) == subResultSet.end()) {
                        it = resultSet.erase(it);
                    } else {
                        ++it;
                    }
                }
            }
        }
    };

    class OrQuery : public Query {
    public:
        OrQuery(std::vector<std::unique_ptr<Query>> subqueries)
            : subqueries_(std::move(subqueries)) {}

        std::vector<dob::DobJobApplication> execute(CsvIndexedFile& file) override {
            std::unordered_set<dob::DobJobApplication> resultSet;

            for (const auto& subquery : subqueries_) {
                std::vector<dob::DobJobApplication> subResult = subquery->execute(file);
                resultSet.insert(subResult.begin(), subResult.end());
            }

            return std::vector<dob::DobJobApplication>(resultSet.begin(), resultSet.end());
        }
    };

    class MatchQuery : public Query {
    public:

        std::vector<dob::DobJobApplication> execute(CsvIndexedFile& file) override {
            std::vector<dob::DobJobApplication> results;
            for (size_t i = 0; i < file.row_count(); ++i) {
                std::string row = file.read_row(i);
                results.push_back(dob::parse_row(row));
            }
            return results;
        }
    };
} // query