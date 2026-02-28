#pragma once

#include <string_view>
#include <charconv>
#include <cstdint>
#include <unordered_map>
#include <utility>
#include <optional>

#include "DobTypes.hpp"

namespace dob {

    // Column category for query evaluation
    enum class ColumnCategory { STRING, BOOLEAN, NUMERIC };

    // Column info map: column name -> (index, category)
    inline const std::unordered_map<std::string_view, std::pair<int, ColumnCategory>> COLUMN_INFO_MAP = {
        // Numeric columns
        {"job_number", {0, ColumnCategory::NUMERIC}},
        {"doc_number", {1, ColumnCategory::NUMERIC}},
        {"borough", {2, ColumnCategory::NUMERIC}},
        {"block", {5, ColumnCategory::NUMERIC}},
        {"lot", {6, ColumnCategory::NUMERIC}},
        {"bin", {7, ColumnCategory::NUMERIC}},
        {"community_board", {11, ColumnCategory::NUMERIC}},
        {"council_district", {12, ColumnCategory::NUMERIC}},
        {"census_tract", {13, ColumnCategory::NUMERIC}},
        {"filing_date", {22, ColumnCategory::NUMERIC}},
        {"issuance_date", {23, ColumnCategory::NUMERIC}},
        {"expiration_date", {24, ColumnCategory::NUMERIC}},
        {"latest_action_date", {25, ColumnCategory::NUMERIC}},
        {"special_action_date", {26, ColumnCategory::NUMERIC}},
        {"signoff_date", {27, ColumnCategory::NUMERIC}},
        {"existing_dwelling_units", {44, ColumnCategory::NUMERIC}},
        {"proposed_dwelling_units", {45, ColumnCategory::NUMERIC}},
        {"existing_stories", {46, ColumnCategory::NUMERIC}},
        {"proposed_stories", {47, ColumnCategory::NUMERIC}},
        {"existing_height", {48, ColumnCategory::NUMERIC}},
        {"proposed_height", {49, ColumnCategory::NUMERIC}},
        {"initial_cost", {50, ColumnCategory::NUMERIC}},
        {"total_est_fee", {51, ColumnCategory::NUMERIC}},
        {"paid_fee", {52, ColumnCategory::NUMERIC}},
        {"job_no_good_count", {68, ColumnCategory::NUMERIC}},
        {"latitude", {85, ColumnCategory::NUMERIC}},
        {"longitude", {86, ColumnCategory::NUMERIC}},

        // String columns
        {"house_number", {3, ColumnCategory::STRING}},
        {"street_name", {4, ColumnCategory::STRING}},
        {"city", {8, ColumnCategory::STRING}},
        {"state", {9, ColumnCategory::STRING}},
        {"zip", {10, ColumnCategory::STRING}},
        {"nta_name", {14, ColumnCategory::STRING}},
        {"job_type", {15, ColumnCategory::STRING}},
        {"job_status", {16, ColumnCategory::STRING}},
        {"building_type", {17, ColumnCategory::STRING}},
        {"building_class", {18, ColumnCategory::STRING}},
        {"work_type", {19, ColumnCategory::STRING}},
        {"permit_type", {20, ColumnCategory::STRING}},
        {"filing_status", {21, ColumnCategory::STRING}},
        {"owner_type", {28, ColumnCategory::STRING}},
        {"owner_name", {29, ColumnCategory::STRING}},
        {"owner_business_name", {30, ColumnCategory::STRING}},
        {"owner_house_number", {31, ColumnCategory::STRING}},
        {"owner_street_name", {32, ColumnCategory::STRING}},
        {"owner_city", {33, ColumnCategory::STRING}},
        {"owner_state", {34, ColumnCategory::STRING}},
        {"owner_zip", {35, ColumnCategory::STRING}},
        {"owner_phone", {36, ColumnCategory::STRING}},
        {"applicant_first_name", {37, ColumnCategory::STRING}},
        {"applicant_last_name", {38, ColumnCategory::STRING}},
        {"applicant_business_name", {39, ColumnCategory::STRING}},
        {"applicant_professional_title", {40, ColumnCategory::STRING}},
        {"applicant_license", {41, ColumnCategory::STRING}},
        {"applicant_professional_cert", {42, ColumnCategory::STRING}},
        {"applicant_business_phone", {43, ColumnCategory::STRING}},
        {"zoning_district_1", {53, ColumnCategory::STRING}},
        {"zoning_district_2", {54, ColumnCategory::STRING}},
        {"zoning_district_3", {55, ColumnCategory::STRING}},
        {"zoning_district_4", {56, ColumnCategory::STRING}},
        {"zoning_district_5", {57, ColumnCategory::STRING}},
        {"special_district_1", {58, ColumnCategory::STRING}},
        {"special_district_2", {59, ColumnCategory::STRING}},

        // Boolean columns
        {"residential", {60, ColumnCategory::BOOLEAN}},
        {"plumbing", {61, ColumnCategory::BOOLEAN}},
        {"sprinkler", {62, ColumnCategory::BOOLEAN}},
        {"fire_alarm", {63, ColumnCategory::BOOLEAN}},
        {"mechanical", {64, ColumnCategory::BOOLEAN}},
        {"boiler", {65, ColumnCategory::BOOLEAN}},
        {"fuel_burning", {66, ColumnCategory::BOOLEAN}},
        {"curb_cut", {67, ColumnCategory::BOOLEAN}},
    };

    // Get column index and category by name
    inline std::optional<std::pair<int, ColumnCategory>> column_info(std::string_view column_name) {
        auto it = COLUMN_INFO_MAP.find(column_name);
        if (it != COLUMN_INFO_MAP.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    template <typename T>
    inline T parse_simple(std::string_view s) {
        T tmp;
        std::from_chars(s.data(), s.data()+s.size(), tmp);
        return tmp;
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
