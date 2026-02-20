#pragma once

#include <vector>
#include "../csv/CsvIndexedFile.hpp"
#include "../dob/dob_job_application.hpp"

namespace query {
    class Query {
        public:
            // Execute the query and return results as a vector of DobJobApplication
            virtual std::vector<dob::DobJobApplication> execute(CsvIndexedFile& file) = 0;
        };
} // query

