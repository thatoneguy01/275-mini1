#pragma once

#include <string_view>
#include <vector>
#include <cstring>

namespace dob {

    inline void split_csv_line(
        const std::string_view& line,
        std::vector<std::string_view>& out
        )
    {
        out.reserve(96);

        const char* begin = line.data();
        const char* ptr = begin;
        const char* end = begin + line.size();

        const char* field_start = ptr;
        bool in_quotes = false;

        while (ptr < end)
        {
            const char* next_quote = (const char*)memchr(ptr, '"', end - ptr);
            const char* next_comma = (const char*)memchr(ptr, ',', end - ptr);
            const char* next = nullptr;
            if (!next_quote) next = next_comma;
            else if (!next_comma) next = next_quote;
            else next = std::min(next_quote, next_comma);
            if (!next) break;
            if (*next == '"')
            {
                in_quotes = !in_quotes;
                ptr = next + 1;
                continue;
            }
            if (*next == ',' && !in_quotes)
            {
                out.emplace_back(field_start, next - field_start);
                field_start = next + 1;
            }
            ptr = next + 1;
        }
        out.emplace_back(field_start, end - field_start);
    }

}
