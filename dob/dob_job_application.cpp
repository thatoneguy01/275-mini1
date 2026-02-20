#include "dob_job_application.h"

#include <vector>
#include <limits>
#include <charconv>

#include "dob_csv.h"
#include "dob_parse_utils.h"

namespace dob {

    DobJobApplication parse_row(std::string_view line)
    {
        static thread_local std::vector<std::string_view> fields;
        fields.reserve(100);

        split_csv_line(line, fields);

        DobJobApplication r{};

        size_t i = 0;

        r.job_number = parse_int32(fields[i++]);
        r.doc_number = parse_int16(fields[i++]);
        r.borough    = (uint8_t)parse_int32(fields[i++]);

        r.house_number.assign(fields[i++]);
        r.street_name.assign(fields[i++]);

        r.block = parse_int32(fields[i++]);
        r.lot   = parse_int16(fields[i++]);
        r.bin   = parse_int32(fields[i++]);

        // Continue mapping remaining columns sequentially…
        // (You will paste the rest of the mapping here based on column order)

        if (fields.size() > 86) {
            r.latitude = std::numeric_limits<double>::quiet_NaN();
            std::from_chars(fields[85].data(),
                            fields[85].data()+fields[85].size(),
                            r.latitude);

            r.longitude = std::numeric_limits<double>::quiet_NaN();
            std::from_chars(fields[86].data(),
                            fields[86].data()+fields[86].size(),
                            r.longitude);
        }

        return r;
    }

}
