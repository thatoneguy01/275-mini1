#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

#include "../csv/CsvIndexedFile.hpp"
#include "../query/Querys.hpp"

namespace {

std::unique_ptr<query::Query> make_simple_match_query()
{
    return std::make_unique<query::MatchQuery>("borough", std::any(1.0));
}

} // namespace

int main(int argc, char** argv)
{
    std::string csv_path = "DOB_Job_Application_Filings_20260215.csv";
    std::size_t query_iterations = 100;

    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--csv" && i + 1 < argc) {
            csv_path = argv[++i];
        } else if (arg == "--iterations" && i + 1 < argc) {
            query_iterations = static_cast<std::size_t>(std::stoull(argv[++i]));
        }
    }

    // Resolve CSV path
    std::filesystem::path resolved_csv_path(csv_path);
    std::filesystem::path idx_path = resolved_csv_path;
    idx_path += ".idx";

    if (!std::filesystem::exists(resolved_csv_path)) {
        std::filesystem::path parent_path = std::filesystem::path("..") / csv_path;
        if (std::filesystem::exists(parent_path)) {
            resolved_csv_path = parent_path;
            idx_path = resolved_csv_path;
            idx_path += ".idx";
        }
    }

    if (!std::filesystem::exists(resolved_csv_path)) {
        std::cerr << "CSV file not found: " << csv_path << '\n';
        return 1;
    }

    // Remove old index to force rebuild
    std::error_code ec;
    std::filesystem::remove(idx_path, ec);

    // Build index
    CsvIndexedFile csv(resolved_csv_path.string());

    // Run query iterations
    auto query = make_simple_match_query();
    std::size_t total_matches = 0;

    for (std::size_t i = 0; i < query_iterations; ++i) {
        total_matches += csv.query(*query).size();
    }

    // Print result count to prevent optimization
    std::cout << total_matches << '\n';

    return 0;
}



