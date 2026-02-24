# csv library definition
add_library(csv
        CsvIndexedFile.cpp
)

target_include_directories(csv
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_features(csv PUBLIC cxx_std_20)

# Warnings should be local to the target (not global)
if (MSVC)
    target_compile_options(csv PRIVATE /W4 /permissive-)
else()
    target_compile_options(csv PRIVATE
            -Wall
            -Wextra
            -Wpedantic
            -Wconversion
            -Wshadow
    )
endif()

