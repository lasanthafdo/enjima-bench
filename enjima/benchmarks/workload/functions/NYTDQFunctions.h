//
// Created by t86kim on 19/11/24.
//

#ifndef ENJIMA_NYT_DQ_FUNCTIONS_H
#define ENJIMA_NYT_DQ_FUNCTIONS_H

#include "enjima/api/data_types/NewYorkTaxiDQInterimEventTypes.h"
#include "enjima/api/data_types/NewYorkTaxiEvent.h"

#include "enjima/api/JoinFunction.h"
#include "enjima/api/KeyExtractionFunction.h"
#include "enjima/api/KeyedAggregateFunction.h"
#include "enjima/api/MapFunction.h"
#include "enjima/api/MergeableKeyedAggregateFunction.h"
#include "enjima/api/SinkFunction.h"
#include "enjima/api/data_types/NewYorkTaxiEventODQ.h"
#include "enjima/benchmarks/workload/functions/NYTUtilities.h"
#include "spdlog/spdlog.h"

#include <boost/functional/hash.hpp>
#include <iostream>

using NYTFullEventT = enjima::api::data_types::NewYorkTaxiEvent;
using NYTODQEventT = enjima::api::data_types::NewYorkTaxiEventODQ;
using NYTDQProjEventT = enjima::api::data_types::NYTDQProjectedEvent;
using NYTDQWinT = enjima::api::data_types::NYTDQWindowEvent;

template<typename Key, typename T, typename HashFunc = std::hash<Key>>
using NYTDQFunctionsHashMap = std::unordered_map<Key, T, HashFunc>;

namespace enjima::workload::functions {
    class NYTDQMapProjectFunction : public enjima::api::MapFunction<NYTFullEventT, NYTDQProjEventT> {
    public:
        NYTDQProjEventT operator()(const NYTFullEventT& inputEvent) override
        {
            auto puId =
                    areaMapper_.GetTransformedCellID(inputEvent.GetPickupLatitude(), inputEvent.GetPickupLongitude());
            auto doId =
                    areaMapper_.GetTransformedCellID(inputEvent.GetDropOffLatitude(), inputEvent.GetDropOffLongitude());

            return {inputEvent.GetVendorIdArray(), puId, doId, inputEvent.GetTripDistance()};
        }

    private:
        benchmarks::workload::AreaMapper areaMapper_{0.25};
    };

    class NYTODQMapProjectFunction : public enjima::api::MapFunction<NYTODQEventT, NYTDQProjEventT> {
    public:
        NYTDQProjEventT operator()(const NYTODQEventT& inputEvent) override
        {
            return {inputEvent.GetVendorIdArray(), inputEvent.GetPickupCellId(), inputEvent.GetDropOffCellId(),
                    inputEvent.GetTripDistance()};
        }
    };

    class NYTDQNoOpSinkFunction : public enjima::api::SinkFunction<NYTDQWinT> {
    public:
        void Execute([[maybe_unused]] uint64_t timestamp, [[maybe_unused]] NYTDQWinT inputEvent) override
        {
            assert(timestamp > 0 && inputEvent.GetCellId() > 0 && inputEvent.GetCellId() <= 360'000 &&
                    inputEvent.GetTripCount() >= 0 && inputEvent.GetAvgTripDistance() >= 0);
        }
    };

    class NYTDQTripAggFunction : public enjima::api::KeyedAggregateFunction<NYTDQProjEventT, NYTDQWinT> {
    private:
        std::vector<NYTDQWinT> resultVec_;
        NYTDQFunctionsHashMap<int, uint32_t> countMap_;
        NYTDQFunctionsHashMap<int, double> distanceMap_;

    public:
        void operator()(const NYTDQProjEventT& inputEvent) override
        {
            auto cellId = inputEvent.GetPickupCellId();
            assert(cellId > 0 && cellId < 360'000);
            if (!countMap_.contains(cellId)) {
                countMap_.emplace(cellId, 0);
                distanceMap_.emplace(cellId, 0);
            }
            countMap_.at(cellId)++;
            distanceMap_.at(cellId) += inputEvent.GetTripDistance();
        }

        std::vector<NYTDQWinT>& GetResult() override
        {
            if (resultVec_.size() < countMap_.size()) {
                resultVec_.reserve(countMap_.size());
            }
            resultVec_.clear();
            for (const auto& [cellId, count]: countMap_) {
                if (count > 0) {
                    auto avgTripDistance = distanceMap_.at(cellId) / count;
                    resultVec_.emplace_back(cellId, count, avgTripDistance);
                }
                countMap_[cellId] = 0;
                distanceMap_[cellId] = 0;
            }
            return resultVec_;
        }
    };
}// namespace enjima::workload::functions

#endif//ENJIMA_NYT_DQ_FUNCTIONS_H