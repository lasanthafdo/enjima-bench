//
// Created by t86kim on 19/11/24.
//

#ifndef ENJIMA_NYT_EVENT_ODQ_H
#define ENJIMA_NYT_EVENT_ODQ_H

#include "NewYorkTaxiInterimEventTypes.h"

#include <string>
#include <utility>

namespace enjima::api::data_types {
    class NewYorkTaxiEventODQ {
    public:
        NewYorkTaxiEventODQ()
            : pickupDatetime_(0), dropOffDatetime_(0), tripTimeInSecs_(0), tripDistance_(0.0), pickupCellId_(0),
              dropOffCellId_(0), timestamp_(0)
        {
        }

        NewYorkTaxiEventODQ(CustomUUIDType medallion, CustomUUIDType hackLicense, const std::string& vendorId,
                int pickupDatetime, int dropOffDatetime, uint32_t tripTimeInSecs, double tripDistance, int pickupCellId,
                int dropOffCellId, uint64_t timestamp)
            : medallion_(medallion), hackLicense_(hackLicense), pickupDatetime_(pickupDatetime),
              dropOffDatetime_(dropOffDatetime), tripTimeInSecs_(tripTimeInSecs), tripDistance_(tripDistance),
              pickupCellId_(pickupCellId), dropOffCellId_(dropOffCellId), timestamp_(timestamp)
        {
            strcpy(vendorId_.data(), vendorId.c_str());
        }


        [[nodiscard]] std::string GetVendorId() const
        {
            return vendorId_.data();
        }

        [[nodiscard]] std::array<char, 4> GetVendorIdArray() const
        {
            return vendorId_;
        }

        [[nodiscard]] int GetPickupDatetime() const
        {
            return pickupDatetime_;
        }

        [[nodiscard]] int GetDropOffDatetime() const
        {
            return dropOffDatetime_;
        }

        [[nodiscard]] uint64_t GetTripTimeInSecs() const
        {
            return tripTimeInSecs_;
        }

        [[nodiscard]] double GetTripDistance() const
        {
            return tripDistance_;
        }

        [[nodiscard]] int GetPickupCellId() const
        {
            return pickupCellId_;
        }

        [[nodiscard]] int GetDropOffCellId() const
        {
            return dropOffCellId_;
        }

        [[nodiscard]] uint64_t GetTimestamp() const
        {
            return timestamp_;
        }

    private:
        CustomUUIDType medallion_;          // an identifier of the taxi
        CustomUUIDType hackLicense_;        // an identifier of the taxi license
        std::array<char, 4> vendorId_{'\0'};// an identifier for the vendor of the taxi
        int pickupDatetime_;                // time when the passenger(s) were picked up
        int dropOffDatetime_;               // time when the passenger(s) were dropped off
        uint64_t tripTimeInSecs_;           // duration of the trip
        double tripDistance_;               // trip distance in miles
        int pickupCellId_;                  // cell id of the pickup location
        int dropOffCellId_;                 // cell id of the dropOff location
        uint64_t timestamp_;
    };
}// namespace enjima::api::data_types


#endif// ENJIMA_NYT_EVENT_ODQ_H