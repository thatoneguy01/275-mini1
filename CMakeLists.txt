cmake_minimum_required(VERSION 3.20)

project(my_project
        VERSION 0.1
        LANGUAGES CXX
)

add_subdirectory(dob)

# Example of another module
# add_subdirectory(other_module)

# Example executable using dob
add_executable(example example_main.cpp
        main.cpp)
target_link_libraries(example PRIVATE dob)
