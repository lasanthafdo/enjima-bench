#include <utility>

//
// Created by t86kim on 22/11/24
//

#ifndef ENJIMA_NYT_DQ_INTERIM_EVENT_TYPES_H
#define ENJIMA_NYT_DQ_INTERIM_EVENT_TYPES_H

namespace enjima::api::data_types {

    class NYTDQProjectedEvent {
    public:
        NYTDQProjectedEvent() = default;

        NYTDQProjectedEvent(std::array<char, 4> vendorId, int pickupCellId, int dropOffCellId, double tripDistance)
            : vendorId_(vendorId), pickupCellId_(pickupCellId), dropOffCellId_(dropOffCellId),
              tripDistance_(tripDistance)
        {
        }

        [[nodiscard]] std::string GetVendorId() const
        {
            return vendorId_.data();
        }

        [[nodiscard]] int GetPickupCellId() const
        {
            return pickupCellId_;
        }

        [[nodiscard]] int GetDropOffCellId() const
        {
            return dropOffCellId_;
        }

        [[nodiscard]] double GetTripDistance() const
        {
            return tripDistance_;
        }

    private:
        std::array<char, 4> vendorId_{'\0'};
        int pickupCellId_{-1};
        int dropOffCellId_{-1};
        double tripDistance_{0.0};
    };

    class NYTDQWindowEvent {
    public:
        NYTDQWindowEvent() = default;

        NYTDQWindowEvent(int cellId, uint64_t tripCount, double avgTripDistance)
            : cellId_(cellId), tripCount_(tripCount), avgTripDistance_(avgTripDistance)
        {
        }

        [[nodiscard]] int GetCellId() const
        {
            return cellId_;
        }

        [[nodiscard]] uint32_t GetTripCount() const
        {
            return tripCount_;
        }

        [[nodiscard]] double GetAvgTripDistance() const
        {
            return avgTripDistance_;
        }

    private:
        int cellId_{-1};
        uint32_t tripCount_{0};
        double avgTripDistance_{0.0};
    };
}//namespace enjima::api::data_types

#endif//ENJIMA_NYT_DQ_INTERIM_EVENT_TYPES_H