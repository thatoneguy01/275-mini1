# benchmarks executable
add_executable(benchmarks
        benchmark_main.cpp
)

target_link_libraries(benchmarks
        PRIVATE
        csv
        query
        dob
)

target_compile_features(benchmarks PRIVATE cxx_std_20)

# Warnings should be local to the target (not global)
if (MSVC)
    target_compile_options(benchmarks PRIVATE /W4 /permissive-)
else()
    target_compile_options(benchmarks PRIVATE
            -Wall
            -Wextra
            -Wpedantic
            -Wconversion
            -Wshadow
    )
endif()

# profiling benchmark executable
add_executable(benchmark_profile
        benchmark_profile.cpp
        benchmark_profile.cpp
)

target_link_libraries(benchmark_profile
        PRIVATE
        csv
        query
        dob
)

target_compile_features(benchmark_profile PRIVATE cxx_std_20)

# Warnings should be local to the target (not global)
if (MSVC)
    target_compile_options(benchmark_profile PRIVATE /W4 /permissive-)
else()
    target_compile_options(benchmark_profile PRIVATE
            -Wall
            -Wextra
            -Wpedantic
            -Wconversion
            -Wshadow
    )
endif()



