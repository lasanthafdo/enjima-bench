//
// Created by t86kim on 19/11/24.
//

#ifndef ENJIMA_NYT_EVENT_H
#define ENJIMA_NYT_EVENT_H

#include <string>
#include <utility>

namespace enjima::api::data_types {
    class NewYorkTaxiEvent {
    public:
        NewYorkTaxiEvent()
            : tripTimeInSecs_(0), tripDistance_(0.0), pickupLongitude_(0.0), pickupLatitude_(0.0),
              dropOffLongitude_(0.0), dropOffLatitude_(0.0), fareAmount_(0.0), surcharge_(0.0), mtaTax_(0.0),
              tipAmount_(0.0), tollsAmount_(0.0), totalAmount_(0.0), timestamp_(0)
        {
        }

        NewYorkTaxiEvent(const std::string& medallion, const std::string& hackLicense, const std::string& vendorId,
                const std::string& pickupDatetime, const std::string& dropOffDatetime, uint32_t tripTimeInSecs,
                double tripDistance, double pickupLongitude, double pickupLatitude, double dropOffLongitude,
                double dropOffLatitude, const std::string& paymentType, double fareAmount, double surcharge,
                double mtaTax, double tipAmount, double tollsAmount, double totalAmount, uint64_t timestamp)
            : tripTimeInSecs_(tripTimeInSecs), tripDistance_(tripDistance), pickupLongitude_(pickupLongitude),
              pickupLatitude_(pickupLatitude), dropOffLongitude_(dropOffLongitude), dropOffLatitude_(dropOffLatitude),
              fareAmount_(fareAmount), surcharge_(surcharge), mtaTax_(mtaTax), tipAmount_(tipAmount),
              tollsAmount_(tollsAmount), totalAmount_(totalAmount), timestamp_(timestamp)
        {
            strcpy(medallion_.data(), medallion.c_str());
            strcpy(hackLicense_.data(), hackLicense.c_str());
            strcpy(vendorId_.data(), vendorId.c_str());
            strcpy(pickupDatetime_.data(), pickupDatetime.c_str());
            strcpy(dropOffDatetime_.data(), dropOffDatetime.c_str());
            strcpy(paymentType_.data(), paymentType.c_str());
        }

        [[nodiscard]] std::string GetMedallion() const
        {
            return medallion_.data();
        }

        [[nodiscard]] std::string GetHackLicense() const
        {
            return hackLicense_.data();
        }

        [[nodiscard]] std::string GetVendorId() const
        {
            return vendorId_.data();
        }

        [[nodiscard]] std::array<char, 4> GetVendorIdArray() const
        {
            return vendorId_;
        }


        [[nodiscard]] std::string GetPickupDatetime() const
        {
            return pickupDatetime_.data();
        }

        [[nodiscard]] std::string GetDropOffDatetime() const
        {
            return dropOffDatetime_.data();
        }

        [[nodiscard]] uint64_t GetTripTimeInSecs() const
        {
            return tripTimeInSecs_;
        }

        [[nodiscard]] double GetTripDistance() const
        {
            return tripDistance_;
        }

        [[nodiscard]] double GetPickupLongitude() const
        {
            return pickupLongitude_;
        }

        [[nodiscard]] double GetPickupLatitude() const
        {
            return pickupLatitude_;
        }

        [[nodiscard]] double GetDropOffLongitude() const
        {
            return dropOffLongitude_;
        }

        [[nodiscard]] double GetDropOffLatitude() const
        {
            return dropOffLatitude_;
        }

        [[nodiscard]] std::string GetPaymentType() const
        {
            return paymentType_.data();
        }

        [[nodiscard]] double GetFareAmount() const
        {
            return fareAmount_;
        }

        [[nodiscard]] double GetSurcharge() const
        {
            return surcharge_;
        }

        [[nodiscard]] double GetMtaTax() const
        {
            return mtaTax_;
        }

        [[nodiscard]] double GetTipAmount() const
        {
            return tipAmount_;
        }

        [[nodiscard]] double GetTollsAmount() const
        {
            return tollsAmount_;
        }

        [[nodiscard]] double GetTotalAmount() const
        {
            return totalAmount_;
        }

        [[nodiscard]] uint64_t GetTimestamp() const
        {
            return timestamp_;
        }

    private:
        std::array<char, 48> medallion_{'\0'};      // an identifier of the taxi
        std::array<char, 48> hackLicense_{'\0'};    // an identifier of the taxi license
        std::array<char, 4> vendorId_{'\0'};        // an identifier for the vendor of the taxi
        std::array<char, 20> pickupDatetime_{'\0'}; // time when the passenger(s) were picked up
        std::array<char, 20> dropOffDatetime_{'\0'};// time when the passenger(s) were dropped off
        uint64_t tripTimeInSecs_;                   // duration of the trip
        double tripDistance_;                       // trip distance in miles
        double pickupLongitude_;                    // longitude coordinate of the pickup location
        double pickupLatitude_;                     // latitude coordinate of the pickup location
        double dropOffLongitude_;                   // longitude coordinate of the dropOff location
        double dropOffLatitude_;                    // latitude coordinate of the dropOff location
        std::array<char, 4> paymentType_{'\0'};     // the payment method – credit card or cash
        double fareAmount_;                         // fare amount in dollars
        double surcharge_;                          // surcharge in dollar
        double mtaTax_;                             // tax in dollars
        double tipAmount_;                          // tip in dollars
        double tollsAmount_;                        // bridge and tunnel tolls in dollars
        double totalAmount_;                        // total paid amount in dollars
        uint64_t timestamp_;
    };
}// namespace enjima::api::data_types


#endif// ENJIMA_NYT_EVENT_H