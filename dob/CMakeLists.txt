# dob library definition
add_library(dob
        dob_job_application.cpp
)

target_include_directories(dob
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_features(dob PUBLIC cxx_std_20)

# Warnings should be local to the target (not global)
if (MSVC)
    target_compile_options(dob PRIVATE /W4 /permissive-)
else()
    target_compile_options(dob PRIVATE
            -Wall
            -Wextra
            -Wpedantic
            -Wconversion
            -Wshadow
    )
endif()
