#pragma once

#include <string_view>
#include <charconv>
#include <cstdint>

#include "dob_types.h"

namespace dob {

    inline int32_t parse_int32(std::string_view s) {
        int32_t v{};
        std::from_chars(s.data(), s.data()+s.size(), v);
        return v;
    }

    inline int16_t parse_int16(std::string_view s) {
        int32_t tmp{};
        std::from_chars(s.data(), s.data()+s.size(), tmp);
        return static_cast<int16_t>(tmp);
    }

    inline int64_t parse_money_cents(std::string_view s) {
        if (s.empty()) return 0;

        bool neg = false;
        size_t i = 0;

        if (s[0] == '$') i++;
        if (i < s.size() && s[i] == '-') { neg = true; i++; }

        int64_t dollars = 0;
        int64_t cents = 0;
        bool after_decimal = false;

        for (; i < s.size(); ++i) {
            char c = s[i];
            if (c == '.') { after_decimal = true; continue; }
            if (!after_decimal) dollars = dollars*10 + (c-'0');
            else cents = cents*10 + (c-'0');
        }

        int64_t total = dollars*100 + cents;
        return neg ? -total : total;
    }

    inline Date parse_date(std::string_view s) {
        if (s.size() < 10) return 0;

        uint32_t mm = (s[0]-'0')*10 + (s[1]-'0');
        uint32_t dd = (s[3]-'0')*10 + (s[4]-'0');
        uint32_t yyyy =
            (s[6]-'0')*1000 +
            (s[7]-'0')*100 +
            (s[8]-'0')*10 +
            (s[9]-'0');

        return yyyy*10000 + mm*100 + dd;
    }

}
