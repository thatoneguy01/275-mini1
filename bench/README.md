# Benchmarks

Two benchmark programs for measuring CSV indexing and query performance:

1. **benchmarks** - Comprehensive suite covering multiple query permutations
2. **benchmark_profile** - Detailed profiling of index build and query execution

## benchmarks - Comprehensive Performance Testing

Measures performance across:
- Index build and load operations
- 10 different query execution patterns

### Build and run

```powershell
cmake -S . -B cmake-build-debug
cmake --build cmake-build-debug --target benchmarks
cd cmake-build-debug
.\benchmarks.exe
```

### Command-line options

- `--csv <path>`: Path to CSV file (default: `DOB_Job_Application_Filings_20260215.csv`)
- `--generate`: Generate a synthetic CSV instead of using an existing one
- `--rows <N>`: Number of rows for synthetic CSV (default: 20000)
- `--cols <N>`: Number of columns for synthetic CSV (default: 90, minimum 87)
- `--iterations <N>`: Number of iterations for query execution (default: 5)
- `--index-iters <N>`: Number of iterations for index build/load (default: 2)
- `--seed <N>`: RNG seed for synthetic CSV generation (default: 12345)

### Query patterns benchmarked

- **Simple**: Single match/range query on one column
- **AND queries**: 2, 3, and 4-condition AND combinations
- **OR queries**: 2-condition (same column) and 4-condition (different columns)
- **NOT query**: Negation of a match condition
- **Complex nested**: `(A AND B) OR (C AND D)` structure
- **Range heavy**: Multiple range queries on numeric columns
- **Mixed query**: Combination of match (numeric, string, boolean) and range

## benchmark_profile - Execution Profiling with perf

Lightweight binary optimized for profiling with Linux `perf` tool. Measures CPU-level behavior of:
- Index building from CSV
- Query execution against the index
- Combined workload of both phases

### Build and run with perf

```bash
# Build
cmake --build cmake-build-debug --target benchmark_profile

# Profile the entire workload
perf record -g ./cmake-build-debug/bench/benchmark_profile.exe
perf report

# Profile with call graph
perf record -g --call-graph=dwarf ./cmake-build-debug/bench/benchmark_profile.exe
perf report

# Profile with flame graph output
perf record -g ./cmake-build-debug/bench/benchmark_profile.exe
perf script > out.perf
# Then convert to flame graph using flamegraph.pl
```

### Command-line options

- `--csv <path>`: Path to CSV file (default: `DOB_Job_Application_Filings_20260215.csv`)
- `--iterations <N>`: Number of query iterations (default: 100)

### Example usage

```bash
# Profile with more iterations for better sampling
./benchmark_profile.exe --iterations 1000 &
perf record -p $! -g

# Profile with specific CSV
perf record -g ./benchmark_profile.exe --csv custom_data.csv --iterations 500

# High-resolution profiling (requires elevated permissions)
perf record -g -F 99 ./benchmark_profile.exe

# Record with specific events
perf record -g -e cycles,cache-misses,branch-misses ./benchmark_profile.exe
```

### Output interpretation

The binary outputs a single number: the total matches found across all query iterations. This prevents compiler optimization from eliminating the work being measured.

Use `perf report` to view:
- **Hot functions**: Which functions consume most CPU time
- **Call chains**: How functions call each other
- **Instruction-level detail**: Which assembly instructions are expensive

### Analyzing the profile

Common perf commands for analysis:

```bash
# View flat profile
perf report

# View with caller/callee relationships
perf report -g caller

# View annotated source code (requires debug symbols)
perf annotated

# Generate diff between two runs
perf diff perf.data.old perf.data.new

# Per-function breakdown
perf stat --detailed ./benchmark_profile.exe
```

### Debug symbols

For better source-level profiling, ensure the binary is built with debug symbols:

```bash
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -B cmake-build-debug
cmake --build cmake-build-debug --target benchmark_profile
perf record -g ./cmake-build-debug/bench/benchmark_profile.exe
perf report -s symbol
```

## Running both benchmarks

```powershell
# Comprehensive suite with default settings
.\benchmarks.exe

# Profiling with more query iterations
.\benchmark_profile.exe --iterations 20

# Both against the same CSV file
.\benchmarks.exe --csv my_data.csv --iterations 3
.\benchmark_profile.exe --csv my_data.csv --iterations 10
```


