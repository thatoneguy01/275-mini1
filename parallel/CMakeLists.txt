add_library(parallel STATIC
    ChunkWorker.cpp
    ChunkWorker.hpp
    ParallelQueryProcessor.cpp
    ParallelQueryProcessor.hpp
    ThreadPool.cpp
    ThreadPool.hpp
)

target_include_directories(parallel PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)

target_link_libraries(parallel PUBLIC
    dob
    query
)

target_compile_features(parallel PRIVATE cxx_std_20)

# Warnings should be local to the target (not global)
if (MSVC)
    target_compile_options(parallel PRIVATE /W4 /permissive-)
else()
    target_compile_options(parallel PRIVATE
            -O3
            -Wall
            -Wextra
            -Wpedantic
            -Wconversion
            -Wshadow
    )
endif()

