#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <array>
#include <functional>
#include <type_traits>
#include <cstddef>
#include "DobTypes.hpp"

namespace dob {

struct DobJobApplication {

    // Core identifiers
    int32_t job_number{};
    int16_t doc_number{};
    uint8_t borough{};
    int32_t bin{};

    // Location
    std::string house_number;
    std::string street_name;
    int32_t block{};
    int16_t lot{};

    std::string city;
    std::string state;
    std::string zip;

    int16_t community_board{};
    int16_t council_district{};
    int32_t census_tract{};
    std::string nta_name;

    double latitude{};
    double longitude{};

    // Job classification
    std::string job_type;
    std::string job_status;
    std::string building_type;
    char building_class[4]{};

    std::string work_type;
    std::string permit_type;
    std::string filing_status;

    // Dates
    Date filing_date{};
    Date issuance_date{};
    Date expiration_date{};
    Date latest_action_date{};
    Date special_action_date{};
    Date signoff_date{};

    // Owner info
    std::string owner_type;
    std::string owner_name;
    std::string owner_business_name;
    std::string owner_house_number;
    std::string owner_street_name;
    std::string owner_city;
    std::string owner_state;
    std::string owner_zip;
    std::string owner_phone;

    // Applicant info
    std::string applicant_first_name;
    std::string applicant_last_name;
    std::string applicant_business_name;
    std::string applicant_professional_title;
    std::string applicant_license;
    std::string applicant_professional_cert;
    std::string applicant_business_phone;

    // Dimensions / units
    int16_t existing_dwelling_units{};
    int16_t proposed_dwelling_units{};
    int16_t existing_stories{};
    int16_t proposed_stories{};
    int32_t existing_height{};
    int32_t proposed_height{};

    // Financial
    int64_t initial_cost_cents{};
    int64_t total_est_fee_cents{};
    int64_t paid_fee_cents{};

    // Zoning
    std::string zoning_district_1;
    std::string zoning_district_2;
    std::string zoning_district_3;
    std::string zoning_district_4;
    std::string zoning_district_5;

    std::string special_district_1;
    std::string special_district_2;

    // Flags
    struct Flags {
        uint8_t residential : 1;
        uint8_t plumbing : 1;
        uint8_t sprinkler : 1;
        uint8_t fire_alarm : 1;
        uint8_t mechanical : 1;
        uint8_t boiler : 1;
        uint8_t fuel_burning : 1;
        uint8_t curb_cut : 1;

        auto operator<=>(const Flags&) const = default;
        bool operator==(const Flags&) const = default;
    } flags{};

    uint8_t job_no_good_count{};

    auto operator<=>(const DobJobApplication&) const = default;
    bool operator==(const DobJobApplication&) const = default;

    std::array<char, 4> building_class_key() const {
        return {building_class[0], building_class[1], building_class[2], building_class[3]};
    }

    uint8_t flags_value() const {
        return static_cast<uint8_t>((flags.residential ? 1u : 0u)
            | (flags.plumbing ? 2u : 0u)
            | (flags.sprinkler ? 4u : 0u)
            | (flags.fire_alarm ? 8u : 0u)
            | (flags.mechanical ? 16u : 0u)
            | (flags.boiler ? 32u : 0u)
            | (flags.fuel_burning ? 64u : 0u)
            | (flags.curb_cut ? 128u : 0u));
    }
};

inline void hash_combine(std::size_t& seed, std::size_t value) {
    seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
}

}

namespace std {

template <>
struct hash<dob::DobJobApplication> {
    size_t operator()(const dob::DobJobApplication& value) const {
        size_t seed = 0;
        const auto add = [&seed](const auto& item) {
            ::dob::hash_combine(seed, std::hash<std::decay_t<decltype(item)>>{}(item));
        };

        add(value.job_number);
        add(value.doc_number);
        add(value.borough);
        add(value.bin);
        add(value.house_number);
        add(value.street_name);
        add(value.block);
        add(value.lot);
        add(value.city);
        add(value.state);
        add(value.zip);
        add(value.community_board);
        add(value.council_district);
        add(value.census_tract);
        add(value.nta_name);
        add(value.latitude);
        add(value.longitude);
        add(value.job_type);
        add(value.job_status);
        add(value.building_type);
        add(value.work_type);
        add(value.permit_type);
        add(value.filing_status);
        add(value.filing_date);
        add(value.issuance_date);
        add(value.expiration_date);
        add(value.latest_action_date);
        add(value.special_action_date);
        add(value.signoff_date);
        add(value.owner_type);
        add(value.owner_name);
        add(value.owner_business_name);
        add(value.owner_house_number);
        add(value.owner_street_name);
        add(value.owner_city);
        add(value.owner_state);
        add(value.owner_zip);
        add(value.owner_phone);
        add(value.applicant_first_name);
        add(value.applicant_last_name);
        add(value.applicant_business_name);
        add(value.applicant_professional_title);
        add(value.applicant_license);
        add(value.applicant_professional_cert);
        add(value.applicant_business_phone);
        add(value.existing_dwelling_units);
        add(value.proposed_dwelling_units);
        add(value.existing_stories);
        add(value.proposed_stories);
        add(value.existing_height);
        add(value.proposed_height);
        add(value.initial_cost_cents);
        add(value.total_est_fee_cents);
        add(value.paid_fee_cents);
        add(value.zoning_district_1);
        add(value.zoning_district_2);
        add(value.zoning_district_3);
        add(value.zoning_district_4);
        add(value.zoning_district_5);
        add(value.special_district_1);
        add(value.special_district_2);
        add(value.job_no_good_count);
        add(value.flags_value());

        for (const char c : value.building_class_key()) {
            add(c);
        }

        return seed;
    }
};

}

namespace dob {

DobJobApplication parse_row(std::string_view line);

}
