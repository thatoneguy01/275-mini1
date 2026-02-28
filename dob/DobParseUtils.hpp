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

    // Column info map: object field name -> (csv index, category)
    // Types are based on DobJobApplication struct field types and actual CSV data
    inline const std::unordered_map<std::string_view, std::pair<int, ColumnCategory>> COLUMN_INFO_MAP = {
        // Core identifiers (all NUMERIC in struct)
        {"job_number", {0, ColumnCategory::NUMERIC}},              // Job # (int32_t)
        {"doc_number", {1, ColumnCategory::NUMERIC}},              // Doc # (int16_t)
        {"borough", {2, ColumnCategory::NUMERIC}},                 // Borough (uint8_t)
        {"bin", {7, ColumnCategory::NUMERIC}},                     // Bin # (int32_t)

        // Location
        {"house_number", {3, ColumnCategory::STRING}},             // House # (std::string)
        {"street_name", {4, ColumnCategory::STRING}},              // Street Name (std::string)
        {"block", {5, ColumnCategory::NUMERIC}},                   // Block (int32_t)
        {"lot", {6, ColumnCategory::NUMERIC}},                     // Lot (int16_t)
        {"city", {76, ColumnCategory::STRING}},                    // City (std::string)
        {"state", {77, ColumnCategory::STRING}},                   // State (std::string)
        {"zip", {78, ColumnCategory::STRING}},                     // Zip (std::string)
        {"community_board", {13, ColumnCategory::NUMERIC}},        // Community - Board (int16_t)
        {"council_district", {92, ColumnCategory::NUMERIC}},       // GIS_COUNCIL_DISTRICT (int16_t, see struct)
        {"census_tract", {93, ColumnCategory::NUMERIC}},           // GIS_CENSUS_TRACT (int32_t, see struct)
        {"nta_name", {94, ColumnCategory::STRING}},                // GIS_NTA_NAME (std::string)
        {"latitude", {90, ColumnCategory::NUMERIC}},               // GIS_LATITUDE (double)
        {"longitude", {91, ColumnCategory::NUMERIC}},              // GIS_LONGITUDE (double)

        // Job classification
        {"job_type", {8, ColumnCategory::STRING}},                 // Job Type (std::string)
        {"job_status", {9, ColumnCategory::STRING}},               // Job Status (std::string)
        {"building_type", {12, ColumnCategory::STRING}},           // Building Type (std::string)
        {"building_class", {88, ColumnCategory::STRING}},          // BUILDING_CLASS (char[4])
        {"work_type", {80, ColumnCategory::STRING}},               // Job Description (std::string)
        {"permit_type", {21, ColumnCategory::STRING}},             // eFiling Filed (std::string)
        {"filing_status", {48, ColumnCategory::STRING}},           // Fee Status (std::string)

        // Dates (all NUMERIC - stored as Date type which is numeric)
        {"filing_date", {40, ColumnCategory::NUMERIC}},            // Pre- Filing Date (Date)
        {"issuance_date", {44, ColumnCategory::NUMERIC}},          // Approved (Date)
        {"expiration_date", {45, ColumnCategory::NUMERIC}},        // Fully Permitted (Date)
        {"latest_action_date", {11, ColumnCategory::NUMERIC}},     // Latest Action Date (Date)
        {"special_action_date", {87, ColumnCategory::NUMERIC}},    // SPECIAL_ACTION_DATE (Date)
        {"signoff_date", {85, ColumnCategory::NUMERIC}},           // SIGNOFF_DATE (Date)

        // Owner info (all STRING except maybe some)
        {"owner_type", {69, ColumnCategory::STRING}},              // Owner Type (std::string)
        {"owner_name", {71, ColumnCategory::STRING}},              // Owner's First Name (std::string - combined)
        {"owner_business_name", {73, ColumnCategory::STRING}},     // Owner's Business Name (std::string)
        {"owner_house_number", {74, ColumnCategory::STRING}},      // Owner's House Number (std::string)
        {"owner_street_name", {75, ColumnCategory::STRING}},       // Owner'sHouse Street Name (std::string)
        {"owner_city", {76, ColumnCategory::STRING}},              // City (std::string, shared)
        {"owner_state", {77, ColumnCategory::STRING}},             // State (std::string, shared)
        {"owner_zip", {78, ColumnCategory::STRING}},               // Zip (std::string, shared)
        {"owner_phone", {79, ColumnCategory::STRING}},             // Owner'sPhone # (std::string)

        // Applicant info (all STRING)
        {"applicant_first_name", {35, ColumnCategory::STRING}},    // Applicant's First Name (std::string)
        {"applicant_last_name", {36, ColumnCategory::STRING}},     // Applicant's Last Name (std::string)
        {"applicant_professional_title", {37, ColumnCategory::STRING}}, // Applicant Professional Title (std::string)
        {"applicant_license", {38, ColumnCategory::STRING}},       // Applicant License # (std::string)
        {"applicant_professional_cert", {39, ColumnCategory::STRING}}, // Professional Cert (std::string)
        {"applicant_business_name", {39, ColumnCategory::STRING}}, // Professional Cert (std::string - mapped, check if needed)

        // Dimensions / units (all NUMERIC - stored as int16_t or int32_t)
        {"existing_dwelling_units", {59, ColumnCategory::NUMERIC}},    // Existing Dwelling Units (int16_t)
        {"proposed_dwelling_units", {60, ColumnCategory::NUMERIC}},    // Proposed Dwelling Units (int16_t)
        {"existing_stories", {55, ColumnCategory::NUMERIC}},           // ExistingNo. of Stories (int16_t)
        {"proposed_stories", {56, ColumnCategory::NUMERIC}},           // Proposed No. of Stories (int16_t)
        {"existing_height", {57, ColumnCategory::NUMERIC}},            // Existing Height (int32_t)
        {"proposed_height", {58, ColumnCategory::NUMERIC}},            // Proposed Height (int32_t)

        // Financial (all NUMERIC - stored as int64_t in cents)
        {"initial_cost_cents", {46, ColumnCategory::NUMERIC}},     // Initial Cost (int64_t)
        {"total_est_fee_cents", {47, ColumnCategory::NUMERIC}},    // Total Est. Fee (int64_t)
        {"paid_fee_cents", {41, ColumnCategory::NUMERIC}},         // Paid (int64_t)

        // Zoning (all STRING)
        {"zoning_district_1", {64, ColumnCategory::STRING}},       // Zoning Dist1 (std::string)
        {"zoning_district_2", {65, ColumnCategory::STRING}},       // Zoning Dist2 (std::string)
        {"zoning_district_3", {66, ColumnCategory::STRING}},       // Zoning Dist3 (std::string)
        {"zoning_district_4", {67, ColumnCategory::STRING}},       // Special District 1 (std::string)
        {"zoning_district_5", {68, ColumnCategory::STRING}},       // Special District 2 (std::string)
        {"special_district_1", {67, ColumnCategory::STRING}},      // Special District 1 (std::string)
        {"special_district_2", {68, ColumnCategory::STRING}},      // Special District 2 (std::string)

        // Flags (all BOOLEAN - struct Flags with bit fields)
        {"residential", {15, ColumnCategory::BOOLEAN}},            // Landmarked (uint8_t:1)
        {"plumbing", {22, ColumnCategory::BOOLEAN}},               // Plumbing (uint8_t:1)
        {"sprinkler", {28, ColumnCategory::BOOLEAN}},              // Sprinkler (uint8_t:1)
        {"fire_alarm", {29, ColumnCategory::BOOLEAN}},             // Fire Alarm (uint8_t:1)
        {"mechanical", {23, ColumnCategory::BOOLEAN}},             // Mechanical (uint8_t:1)
        {"boiler", {24, ColumnCategory::BOOLEAN}},                 // Boiler (uint8_t:1)
        {"fuel_burning", {25, ColumnCategory::BOOLEAN}},           // Fuel Burning (uint8_t:1)
        {"curb_cut", {32, ColumnCategory::BOOLEAN}},               // Curb Cut (uint8_t:1)

        // Other numeric fields
        {"job_no_good_count", {89, ColumnCategory::NUMERIC}},      // JOB_NO_GOOD_COUNT (uint8_t)
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
