#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "../csv/CsvIndexedFile.hpp"
#include "../query/Querys.hpp"

namespace {

struct BenchConfig {
    std::string csv_path = "DOB_Job_Application_Filings_20260215.csv";
    std::size_t query_iters = 5;
    std::size_t index_build_iters = 2;
    bool generate_csv = false;
    std::size_t rows = 20000;
    std::size_t cols = 90;
    std::uint64_t seed = 12345;
};

struct BenchResult {
    std::string name;
    std::size_t iterations = 0;
    double total_ms = 0.0;
    double avg_ms = 0.0;
    std::size_t items = 0;
};

std::string quoted(const std::string& value)
{
    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('"');
    out += value;
    out.push_back('"');
    return out;
}

std::string field_for_column(std::size_t col, std::size_t row, std::mt19937& rng)
{
    (void)rng;
    if (col == 0) {
        return std::to_string(row + 1000);
    }
    if (col == 2) {
        return std::to_string(row % 5);
    }
    if (col == 4) {
        return quoted((row % 2 == 0) ? "MAIN ST" : "OAK ST");
    }
    if (col == 8) {
        return quoted("NEW YORK");
    }
    if (col == 9) {
        return quoted("NY");
    }
    if (col == 10) {
        return quoted("10001");
    }
    if (col == 15) {
        return quoted((row % 3 == 0) ? "A1" : "A2");
    }
    if (col == 16) {
        return quoted((row % 4 == 0) ? "ISSUED" : "PENDING");
    }
    if (col == 60) {
        return (row % 2 == 0) ? "1" : "0";
    }
    if (col == 85) {
        return "40.7128";
    }
    if (col == 86) {
        return "-74.0060";
    }

    return "0";
}

void write_csv(const std::filesystem::path& path, const BenchConfig& config)
{
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("Failed to write CSV file");
    }

    std::mt19937 rng(static_cast<std::mt19937::result_type>(config.seed));

    for (std::size_t row = 0; row < config.rows; ++row) {
        std::string line;
        line.reserve(config.cols * 6);

        for (std::size_t col = 0; col < config.cols; ++col) {
            if (col > 0) {
                line.push_back(',');
            }
            line += field_for_column(col, row, rng);
        }

        line.push_back('\n');
        out << line;
    }
}

std::unique_ptr<query::Query> make_simple_match_query()
{
    return std::make_unique<query::MatchQuery>("borough", std::any(3.0));
}

std::unique_ptr<query::Query> make_simple_range_query()
{
    return std::make_unique<query::RangeQuery>(
        "job_number", std::any(1000.0), std::any(25000.0));
}

std::unique_ptr<query::Query> make_simple_string_match_query()
{
    return std::make_unique<query::MatchQuery>(
        "job_status", std::any(std::string("ISSUED")));
}

std::unique_ptr<query::Query> make_and_query_two_conditions()
{
    std::vector<std::unique_ptr<query::Query>> subs;
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "borough", std::any(1.0)));
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "job_status", std::any(std::string("ISSUED"))));

    return std::make_unique<query::AndQuery>(std::move(subs));
}

std::unique_ptr<query::Query> make_and_query_three_conditions()
{
    std::vector<std::unique_ptr<query::Query>> subs;
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "borough", std::any(1.0)));
    subs.emplace_back(std::make_unique<query::RangeQuery>(
        "job_number", std::any(1000.0), std::any(50000.0)));
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "job_status", std::any(std::string("ISSUED"))));

    return std::make_unique<query::AndQuery>(std::move(subs));
}

std::unique_ptr<query::Query> make_and_query_four_conditions()
{
    std::vector<std::unique_ptr<query::Query>> subs;
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "borough", std::any(1.0)));
    subs.emplace_back(std::make_unique<query::RangeQuery>(
        "job_number", std::any(1000.0), std::any(50000.0)));
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "job_status", std::any(std::string("ISSUED"))));
    subs.emplace_back(std::make_unique<query::RangeQuery>(
        "filing_date", std::any(20200101.0), std::any(20240101.0)));

    return std::make_unique<query::AndQuery>(std::move(subs));
}

std::unique_ptr<query::Query> make_or_query_two_conditions()
{
    std::vector<std::unique_ptr<query::Query>> subs;
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "borough", std::any(1.0)));
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "borough", std::any(2.0)));

    return std::make_unique<query::OrQuery>(std::move(subs));
}

std::unique_ptr<query::Query> make_or_query_four_conditions()
{
    std::vector<std::unique_ptr<query::Query>> subs;
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "job_status", std::any(std::string("ISSUED"))));
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "job_status", std::any(std::string("PENDING"))));
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "job_status", std::any(std::string("APPROVED"))));
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "job_status", std::any(std::string("COMPLETED"))));

    return std::make_unique<query::OrQuery>(std::move(subs));
}

std::unique_ptr<query::Query> make_not_query()
{
    auto sub = std::make_unique<query::MatchQuery>(
        "residential", std::any(true));
    return std::make_unique<query::NotQuery>(std::move(sub));
}

std::unique_ptr<query::Query> make_complex_nested_query()
{
    // ((A AND B) OR (C AND D))
    std::vector<std::unique_ptr<query::Query>> and_left;
    and_left.emplace_back(std::make_unique<query::MatchQuery>(
        "borough", std::any(1.0)));
    and_left.emplace_back(std::make_unique<query::MatchQuery>(
        "job_status", std::any(std::string("ISSUED"))));
    auto left = std::make_unique<query::AndQuery>(std::move(and_left));

    std::vector<std::unique_ptr<query::Query>> and_right;
    and_right.emplace_back(std::make_unique<query::RangeQuery>(
        "job_number", std::any(1000.0), std::any(50000.0)));
    and_right.emplace_back(std::make_unique<query::MatchQuery>(
        "job_status", std::any(std::string("PENDING"))));
    auto right = std::make_unique<query::AndQuery>(std::move(and_right));

    std::vector<std::unique_ptr<query::Query>> or_subs;
    or_subs.emplace_back(std::move(left));
    or_subs.emplace_back(std::move(right));

    return std::make_unique<query::OrQuery>(std::move(or_subs));
}

std::unique_ptr<query::Query> make_range_heavy_query()
{
    std::vector<std::unique_ptr<query::Query>> subs;
    subs.emplace_back(std::make_unique<query::RangeQuery>(
        "job_number", std::any(1000.0), std::any(50000.0)));
    subs.emplace_back(std::make_unique<query::RangeQuery>(
        "filing_date", std::any(20200101.0), std::any(20240101.0)));
    subs.emplace_back(std::make_unique<query::RangeQuery>(
        "latitude", std::any(40.5), std::any(40.9)));

    return std::make_unique<query::AndQuery>(std::move(subs));
}

std::unique_ptr<query::Query> make_mixed_query()
{
    std::vector<std::unique_ptr<query::Query>> subs;
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "borough", std::any(1.0)));
    subs.emplace_back(std::make_unique<query::RangeQuery>(
        "job_number", std::any(1000.0), std::any(50000.0)));
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "job_status", std::any(std::string("ISSUED"))));
    subs.emplace_back(std::make_unique<query::MatchQuery>(
        "residential", std::any(true)));

    return std::make_unique<query::AndQuery>(std::move(subs));
}

template <typename Fn>
BenchResult run_bench(const std::string& name, std::size_t iterations, Fn&& fn)
{
    auto start = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < iterations; ++i) {
        fn();
    }
    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;

    BenchResult result;
    result.name = name;
    result.iterations = iterations;
    result.total_ms = elapsed.count();
    result.avg_ms = result.total_ms / static_cast<double>(iterations);
    return result;
}

void print_result(const BenchResult& result)
{
    std::cout << std::left << std::setw(30) << result.name
              << "  iters=" << std::setw(4) << result.iterations
              << "  total_ms=" << std::setw(10) << std::fixed << std::setprecision(2)
              << result.total_ms
              << "  avg_ms=" << std::setw(8) << std::fixed << std::setprecision(2)
              << result.avg_ms;

    if (result.items > 0) {
        std::cout << "  items=" << result.items;
    }

    std::cout << '\n';
}

BenchConfig parse_args(int argc, char** argv)
{
    BenchConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto take = [&](std::size_t& out) {
            if (i + 1 < argc) {
                out = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        };

        if (arg == "--csv") {
            if (i + 1 < argc) {
                config.csv_path = argv[++i];
            }
        } else if (arg == "--rows") {
            take(config.rows);
        } else if (arg == "--cols") {
            take(config.cols);
        } else if (arg == "--iterations") {
            take(config.query_iters);
        } else if (arg == "--index-iters") {
            take(config.index_build_iters);
        } else if (arg == "--seed") {
            if (i + 1 < argc) {
                config.seed = static_cast<std::uint64_t>(std::stoull(argv[++i]));
            }
        } else if (arg == "--generate") {
            config.generate_csv = true;
        }
    }

    if (config.cols < 87) {
        config.cols = 87;
    }

    return config;
}

} // namespace

int main(int argc, char** argv)
{
    BenchConfig config = parse_args(argc, argv);

    // Try to find CSV file - check relative to executable, then check parent directory
    std::filesystem::path csv_path(config.csv_path);
    std::filesystem::path idx_path = csv_path;
    idx_path += ".idx";

    // If relative path doesn't exist, try parent directory
    if (!std::filesystem::exists(csv_path)) {
        std::filesystem::path parent_path = std::filesystem::path("..") / config.csv_path;
        if (std::filesystem::exists(parent_path)) {
            csv_path = parent_path;
            idx_path = csv_path;
            idx_path += ".idx";
        }
    }

    // Check if CSV exists after path resolution
    if (!std::filesystem::exists(csv_path)) {
        if (config.generate_csv) {
            std::cout << "Generating synthetic CSV...\n";
            try {
                write_csv(csv_path, config);
            } catch (const std::exception& e) {
                std::cerr << "CSV generation failed: " << e.what() << '\n';
                return 1;
            }
        } else {
            std::cerr << "CSV file not found: " << config.csv_path << '\n';
            std::cerr << "Searched in:\n";
            std::cerr << "  - " << std::filesystem::absolute(config.csv_path) << '\n';
            std::cerr << "  - " << std::filesystem::absolute(std::filesystem::path("..") / config.csv_path) << '\n';
            std::cerr << "Use --csv <path> to specify the file, or --generate to create a synthetic CSV\n";
            return 1;
        }
    }

    auto csv_size = std::filesystem::file_size(csv_path);
    std::cout << "======================================================\n";
    std::cout << "CSV file: " << config.csv_path << '\n';
    std::cout << "CSV size: " << (static_cast<double>(csv_size) / (1024.0 * 1024.0)) << " MB\n";
    std::cout << "Index iters: " << config.index_build_iters
              << "  Query iters: " << config.query_iters << '\n';
    std::cout << "======================================================\n\n";

    // ===== INDEX BENCHMARKS =====
    std::cout << "--- INDEX BENCHMARKS ---\n";
    BenchResult index_build = run_bench("index_build", config.index_build_iters, [&]() {
        std::error_code ec;
        std::filesystem::remove(idx_path, ec);
        CsvIndexedFile csv_temp(csv_path.string());
        (void)csv_temp.row_count();
    });
    print_result(index_build);

    BenchResult index_load = run_bench("index_load", config.index_build_iters, [&]() {
        CsvIndexedFile csv_temp(csv_path.string());
        (void)csv_temp.row_count();
    });
    print_result(index_load);


    // ===== QUERY EXECUTION BENCHMARKS =====
    std::cout << "\n--- QUERY EXECUTION BENCHMARKS ---\n";

    // Cache the CsvIndexedFile for all query executions
    CsvIndexedFile csv(csv_path.string());
    std::cout << "Loaded CSV with " << csv.row_count() << " rows\n";
    std::cout << "Running " << config.query_iters << " iterations per query...\n\n";

    std::size_t sink = 0;

    auto simple_match_query = make_simple_match_query();
    BenchResult query_simple_match = run_bench("query_simple_match", config.query_iters, [&]() {
        sink += csv.query(*simple_match_query).size();
    });
    query_simple_match.items = sink;
    std::cout << "  Result: " << query_simple_match.items << " total matches across "
              << config.query_iters << " iterations ";
    print_result(query_simple_match);

    sink = 0;
    auto simple_range_query = make_simple_range_query();
    BenchResult query_simple_range = run_bench("query_simple_range", config.query_iters, [&]() {
        sink += csv.query(*simple_range_query).size();
    });
    query_simple_range.items = sink;
    std::cout << "  Result: " << query_simple_range.items << " total matches across "
              << config.query_iters << " iterations ";
    print_result(query_simple_range);

    sink = 0;
    auto simple_string_query = make_simple_string_match_query();
    BenchResult query_simple_string = run_bench("query_simple_string", config.query_iters, [&]() {
        sink += csv.query(*simple_string_query).size();
    });
    query_simple_string.items = sink;
    std::cout << "  Result: " << query_simple_string.items << " total matches across "
              << config.query_iters << " iterations ";
    print_result(query_simple_string);

    sink = 0;
    auto and_two_query = make_and_query_two_conditions();
    BenchResult query_and_two = run_bench("query_and_two_cond", config.query_iters, [&]() {
        sink += csv.query(*and_two_query).size();
    });
    query_and_two.items = sink;
    std::cout << "  Result: " << query_and_two.items << " total matches across "
              << config.query_iters << " iterations ";
    print_result(query_and_two);

    sink = 0;
    auto and_three_query = make_and_query_three_conditions();
    BenchResult query_and_three = run_bench("query_and_three_cond", config.query_iters, [&]() {
        sink += csv.query(*and_three_query).size();
    });
    query_and_three.items = sink;
    std::cout << "  Result: " << query_and_three.items << " total matches across "
              << config.query_iters << " iterations ";
    print_result(query_and_three);

    sink = 0;
    auto and_four_query = make_and_query_four_conditions();
    BenchResult query_and_four = run_bench("query_and_four_cond", config.query_iters, [&]() {
        sink += csv.query(*and_four_query).size();
    });
    query_and_four.items = sink;
    std::cout << "  Result: " << query_and_four.items << " total matches across "
              << config.query_iters << " iterations ";
    print_result(query_and_four);

    sink = 0;
    auto or_two_query = make_or_query_two_conditions();
    BenchResult query_or_two = run_bench("query_or_two_cond", config.query_iters, [&]() {
        sink += csv.query(*or_two_query).size();
    });
    query_or_two.items = sink;
    std::cout << "  Result: " << query_or_two.items << " total matches across "
              << config.query_iters << " iterations ";
    print_result(query_or_two);

    sink = 0;
    auto or_four_query = make_or_query_four_conditions();
    BenchResult query_or_four = run_bench("query_or_four_cond", config.query_iters, [&]() {
        sink += csv.query(*or_four_query).size();
    });
    query_or_four.items = sink;
    std::cout << "  Result: " << query_or_four.items << " total matches across "
              << config.query_iters << " iterations ";
    print_result(query_or_four);

    sink = 0;
    auto not_query = make_not_query();
    BenchResult query_not = run_bench("query_not", config.query_iters, [&]() {
        sink += csv.query(*not_query).size();
    });
    query_not.items = sink;
    std::cout << "  Result: " << query_not.items << " total matches across "
              << config.query_iters << " iterations ";
    print_result(query_not);

    sink = 0;
    auto complex_nested_query = make_complex_nested_query();
    BenchResult query_complex_nested = run_bench("query_complex_nested", config.query_iters, [&]() {
        sink += csv.query(*complex_nested_query).size();
    });
    query_complex_nested.items = sink;
    std::cout << "  Result: " << query_complex_nested.items << " total matches across "
              << config.query_iters << " iterations ";
    print_result(query_complex_nested);

    sink = 0;
    auto range_heavy_query = make_range_heavy_query();
    BenchResult query_range_heavy = run_bench("query_range_heavy", config.query_iters, [&]() {
        sink += csv.query(*range_heavy_query).size();
    });
    query_range_heavy.items = sink;
    std::cout << "  Result: " << query_range_heavy.items << " total matches across "
              << config.query_iters << " iterations ";
    print_result(query_range_heavy);

    sink = 0;
    auto mixed_query = make_mixed_query();
    BenchResult query_mixed = run_bench("query_mixed", config.query_iters, [&]() {
        sink += csv.query(*mixed_query).size();
    });
    query_mixed.items = sink;
    std::cout << "  Result: " << query_mixed.items << " total matches across "
              << config.query_iters << " iterations ";
    print_result(query_mixed);

    std::cout << "\n======================================================\n";
    std::cout << "Benchmarks complete!\n";
    std::cout << "======================================================\n";

    return 0;
}

