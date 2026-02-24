# query library definition
add_library(query
        Querys.cpp
)

target_include_directories(query
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_features(query PUBLIC cxx_std_20)

# Warnings should be local to the target (not global)
if (MSVC)
    target_compile_options(query PRIVATE /W4 /permissive-)
else()
    target_compile_options(query PRIVATE
            -Wall
            -Wextra
            -Wpedantic
            -Wconversion
            -Wshadow
    )
endif()

