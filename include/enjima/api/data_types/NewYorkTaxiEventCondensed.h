//
// Created by t86kim on 19/11/24.
//

#ifndef ENJIMA_NYT_EVENT_CONDENSED_H
#define ENJIMA_NYT_EVENT_CONDENSED_H

#include <string>

namespace enjima::api::data_types {
    using MedallionT = std::array<char, 48>;

    class NewYorkTaxiEventCondensed {
    public:
        NewYorkTaxiEventCondensed() : fareAmount_(0.0), tipAmount_(0.0), timestamp_(0) {}

        NewYorkTaxiEventCondensed(const std::string& medallion, const std::string& pickupDatetime,
                const std::string& dropOffDatetime, int pickupCellWE, int pickupCellNS, int dropOffCellWE,
                int dropOffCellNS, double fareAmount, double tipAmount, uint64_t timestamp)
            : pickupCellWE_(pickupCellWE), pickupCellNS_(pickupCellNS), dropOffCellWE_(dropOffCellWE),
              dropOffCellNS_(dropOffCellNS), fareAmount_(fareAmount), tipAmount_(tipAmount), timestamp_(timestamp)
        {
            strcpy(medallion_.data(), medallion.c_str());
            strcpy(pickUpDatetime_.data(), pickupDatetime.c_str());
            strcpy(dropOffDatetime_.data(), dropOffDatetime.c_str());
        }

        [[nodiscard]] const MedallionT& GetMedallionArray() const
        {
            return medallion_;
        }

        [[nodiscard]] int GetPickupCellWE() const
        {
            return pickupCellWE_;
        }

        [[nodiscard]] int GetPickUpCellNS() const
        {
            return pickupCellNS_;
        }

        [[nodiscard]] int GetDropOffCellWE() const
        {
            return dropOffCellWE_;
        }

        [[nodiscard]] int GetDropOffCellNS() const
        {
            return dropOffCellNS_;
        }

        [[nodiscard]] double GetFareAmount() const
        {
            return fareAmount_;
        }

        [[nodiscard]] double GetTipAmount() const
        {
            return tipAmount_;
        }

        [[nodiscard]] uint64_t GetTimestamp() const
        {
            return timestamp_;
        }

    private:
        MedallionT medallion_{'\0'};                // an identifier of the taxi
        std::array<char, 20> pickUpDatetime_{'\0'}; // time when the passenger(s) were picked up
        std::array<char, 20> dropOffDatetime_{'\0'};// time when the passenger(s) were dropped off
        int pickupCellWE_{-1};
        int pickupCellNS_{-1};
        int dropOffCellWE_{-1};
        int dropOffCellNS_{-1};
        // Above 4 fields could be replaced with pair as following
        // std::pair<int, int> pickupCell_;
        // std::pair<int, int> dropOffCell_;
        double fareAmount_;// fare amount in dollars
        double tipAmount_; // tip in dollars
        uint64_t timestamp_;
    };
}// namespace enjima::api::data_types


#endif// ENJIMA_NYT_EVENT_CONDENSED_H