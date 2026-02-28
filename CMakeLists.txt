cmake_minimum_required(VERSION 3.20)

project(my_project
        VERSION 0.1
        LANGUAGES CXX
)

# Add subdirectories for library components
add_subdirectory(dob)
add_subdirectory(csv)
add_subdirectory(query)
add_subdirectory(bench)

# Main library target (INTERFACE library for aggregating component libraries)
add_library(mini1 INTERFACE)

# Link subdirectory libraries
target_link_libraries(mini1 INTERFACE
        dob
        csv
        query
)

# Include directories
target_include_directories(mini1 INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# C++ standard
target_compile_features(mini1 INTERFACE cxx_std_20)
