#pragma once

#include <string_view>
#include <vector>

namespace dob {

    inline void split_csv_line(
        std::string_view line,
        std::vector<std::string_view>& out)
    {
        out.clear();
        size_t start = 0;
        bool in_quotes = false;

        for (size_t i = 0; i < line.size(); ++i) {
            char c = line[i];

            if (c == '"') in_quotes = !in_quotes;
            else if (c == ',' && !in_quotes) {
                out.emplace_back(line.substr(start, i - start));
                start = i + 1;
            }
        }

        out.emplace_back(line.substr(start));
    }

}
