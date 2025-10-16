//
// Created by t86kim on 19/11/24.
//

#ifndef ENJIMA_NYT_FUNCTIONS_H
#define ENJIMA_NYT_FUNCTIONS_H

#include "enjima/api/data_types/NewYorkTaxiEvent.h"
#include "enjima/api/data_types/NewYorkTaxiEventCondensed.h"
#include "enjima/api/data_types/NewYorkTaxiInterimEventTypes.h"

#include "enjima/api/JoinFunction.h"
#include "enjima/api/KeyExtractionFunction.h"
#include "enjima/api/KeyedAggregateFunction.h"
#include "enjima/api/MapFunction.h"
#include "enjima/api/MergeableKeyedAggregateFunction.h"
#include "enjima/api/SinkFunction.h"
#include "enjima/benchmarks/workload/functions/NYTUtilities.h"
#include "spdlog/spdlog.h"

#include <boost/functional/hash.hpp>
#include <iostream>

using NYTEventT = enjima::api::data_types::NewYorkTaxiEventCondensed;
using NYTEventProjT = enjima::api::data_types::NYTEventProjected;
using NYTEmptyReportT = enjima::api::data_types::NYTEmptyTaxiReport;
using NYTEmptyCountT = enjima::api::data_types::NYTEmptyTaxiCountReport;
using NYTProfitReportT = enjima::api::data_types::NYTProfitReport;
using NYTProfitabilityT = enjima::api::data_types::NYTProfitabilityReport;
using UUIDTypeT = enjima::api::data_types::CustomUUIDType;

template<typename Key, typename T, typename HashFunc = std::hash<Key>>
using NYTFunctionsHashMap = std::unordered_map<Key, T, HashFunc>;

namespace enjima::workload::functions {
    class NYTMapProjectFunction : public enjima::api::MapFunction<NYTEventT, NYTEventProjT> {
    public:
        NYTEventProjT operator()(const NYTEventT& inputEvent) override
        {
            std::string taxiIdStr{inputEvent.GetMedallionArray().data()};
            auto taxiUuid = UUIDTypeT(taxiIdStr);
            return {taxiUuid, inputEvent.GetPickupCellWE(), inputEvent.GetPickUpCellNS(), inputEvent.GetDropOffCellWE(),
                    inputEvent.GetDropOffCellNS(), inputEvent.GetFareAmount(), inputEvent.GetTipAmount()};
        }
    };

    class NYTEmptyTaxiCountFunction : public enjima::api::KeyedAggregateFunction<NYTEmptyReportT, NYTEmptyCountT> {
    private:
        // key - dropOffCell
        // value - count
        NYTFunctionsHashMap<std::pair<int, int>, int, boost::hash<std::pair<int, int>>> count_;
        std::vector<NYTEmptyCountT> results_;

    public:
        void operator()(const NYTEmptyReportT& inputEvent) override
        {
            std::pair<int, int> key = std::make_pair(inputEvent.GetDropOffCellWE(), inputEvent.GetDropOffCellNS());
            auto emplaceResult = count_.emplace(key, 0);
            emplaceResult.first->second++;
        }

        std::vector<NYTEmptyCountT>& GetResult() override
        {
            results_.clear();
            results_.reserve(15000);
            for (auto& entry: count_) {
                int cellIdWE = entry.first.first;
                int cellIdNS = entry.first.second;
                int count = entry.second;
                if (count == 0) {
                    continue;
                }
                results_.emplace_back(cellIdWE, cellIdNS, count);
            }
            count_.clear();
            count_.reserve(600 * 600);
            return results_;
        }
    };

    class NYTEmptyTaxiCountMultiKeyExtractFunction
        : public enjima::api::KeyExtractionFunction<NYTEmptyCountT, std::pair<int, int>> {
    public:
        std::pair<int, int> operator()(const NYTEmptyCountT& inputEvent) override
        {
            return std::make_pair(inputEvent.GetCellIdWE(), inputEvent.GetCellIdNS());
        }
    };

    class NYTProfitMultiKeyExtractFunction
        : public enjima::api::KeyExtractionFunction<NYTProfitReportT, std::pair<int, int>> {
    public:
        std::pair<int, int> operator()(const NYTProfitReportT& inputEvent) override
        {
            return std::make_pair(inputEvent.GetCellIdWE(), inputEvent.GetCellIdNS());
        }
    };

    class NYTProfitJoinFunction
        : public enjima::api::JoinFunction<NYTEmptyCountT, NYTProfitReportT, NYTProfitabilityT> {
    public:
        NYTProfitabilityT operator()(const NYTEmptyCountT& leftInputEvent,
                const NYTProfitReportT& rightInputEvent) override
        {
            return {leftInputEvent.GetCellIdWE(), leftInputEvent.GetCellIdNS(),
                    rightInputEvent.GetProfit() / leftInputEvent.GetCount()};
        }
    };

    class NYTProfitSinkFunction : public enjima::api::SinkFunction<NYTProfitabilityT> {
    private:
        size_t rankingLength_{10};
        std::vector<NYTProfitabilityT> records_;// Sorted in decreasing order based on profitability
    public:
        void Execute(uint64_t timestamp, NYTProfitabilityT inputEvent) override
        {
            bool changed = false;
            int insertPos = 0;
            // Finding correct place to insert
            for (auto& x: records_) {
                if (inputEvent.GetCellIdWE() == x.GetCellIdWE() && inputEvent.GetCellIdNS() == x.GetCellIdNS() &&
                        inputEvent.GetProfitability() == x.GetProfitability()) {
                    // Duplicate, hence discard
                    return;
                }
                if (inputEvent.GetProfitability() > x.GetProfitability()) {
                    changed = true;
                    break;
                }
                insertPos++;
            }
            // insertPos is the index that new entry should be in.
            // if records_ has less than 10 entries and new entry is smallest, then changed could be false.
            // hence, checking size of records_ will handle above case
            if (changed || records_.size() < rankingLength_) {
                changed = true;
                records_.emplace(records_.begin() + insertPos, inputEvent);
            }

            if (records_.size() > rankingLength_) {
                records_.pop_back();
            }

            if (changed) {
                for (const auto& r: records_) {
                    spdlog::info("{}, {},: {}", r.GetCellIdWE(), r.GetCellIdNS(), r.GetProfitability());
                }
            }
        }
    };

}//namespace enjima::workload::functions

template<>
class enjima::api::MergeableKeyedAggregateFunction<NYTEventProjT, NYTEmptyReportT>
    : public enjima::api::KeyedAggregateFunction<NYTEventProjT, NYTEmptyReportT> {
    using NYTMergeableFunction = enjima::api::MergeableKeyedAggregateFunction<NYTEventProjT, NYTEmptyReportT>;
    using NYTEmptyHashMapT = NYTFunctionsHashMap<UUIDTypeT, std::pair<int, int>>;

private:
    // key - taxiId (medallion)
    // value - (dropOffCellIdWE, dropOffCellIdNS, dropOffTime)
    NYTEmptyHashMapT empty_;
    std::vector<NYTEmptyReportT> results_;
    uint32_t paneId_{0};
    uint32_t currentNumPanes_{1};
    uint32_t currentPaneId_{0};

public:
    MergeableKeyedAggregateFunction()
    {
        // empty_.reserve(15000);
        results_.reserve(15000);
    }

    void operator()(const NYTEventProjT& inputEvent) override
    {
        int dropOffCellWE = inputEvent.GetDropOffCellWE();
        int dropOffCellNS = inputEvent.GetDropOffCellNS();
        auto taxiUuid = inputEvent.GetMedallionUuid();
        empty_[taxiUuid] = std::make_pair(dropOffCellWE, dropOffCellNS);
    }

    void Merge(const std::vector<NYTMergeableFunction>& vecOfCompletedPanes)
    {
        auto numPanes = vecOfCompletedPanes.size();
        // paneId_ counting starts from 1. So if we use (currentPaneIdForAgg_ % numPanes) as our starting index,
        // we refer to the first pane after the currently active pane. Since we go in a circular fashion, this means
        // that we start from the first pane and iterate until the currently active pane.
        for (uint32_t i = currentPaneId_ % numPanes, count = 0; count < numPanes; count++, i = (i + 1) % numPanes) {
            const auto& funcToRef = vecOfCompletedPanes[i];
            assert(funcToRef.paneId_ > 0 && funcToRef.paneId_ <= numPanes);
            for (const auto& emptyEntryFromPane: funcToRef.empty_) {
                auto taxiId = emptyEntryFromPane.first;
                auto dropOffData = emptyEntryFromPane.second;
                // assert(!empty_.contains(taxiId) || std::get<2>(empty_.at(taxiId)) <= std::get<2>(dropOffData));
                empty_[taxiId] = dropOffData;
            }
        }
    }

    std::vector<NYTEmptyReportT>& GetResult() override
    {
        for (const auto& entry: empty_) {
            results_.emplace_back(entry.first, entry.second.first, entry.second.second);
        }
        return results_;
    }

    void Reset()
    {
        empty_.clear();
        results_.clear();
    }

    void InitializeActivePane(NYTMergeableFunction& activePaneFunctor)
    {
        if (activePaneFunctor.paneId_ == 0) {
            activePaneFunctor.paneId_ = currentNumPanes_++;
        }
        currentPaneId_ = activePaneFunctor.paneId_;
    }
};

template<>
class enjima::api::MergeableKeyedAggregateFunction<NYTEventProjT, NYTProfitReportT>
    : public enjima::api::KeyedAggregateFunction<NYTEventProjT, NYTProfitReportT> {
    using NYTProfitMergeableFunction = enjima::api::MergeableKeyedAggregateFunction<NYTEventProjT, NYTProfitReportT>;
    using NYTProfitMapT = NYTFunctionsHashMap<std::pair<int, int>, NYTFunctionsHashMap<int, std::vector<double>>,
            boost::hash<std::pair<int, int>>>;

private:
    // key - pickupCell
    // value - vector of profits (fare + tip)
    NYTProfitMapT profits_;
    NYTProfitMapT* profitsMapPtr_{nullptr};
    std::vector<NYTProfitReportT> results_;
    int paneId_{-1};
    int currentNumPanes_{0};
    int currentPaneId_{-1};

public:
    void operator()(const NYTEventProjT& inputEvent) override
    {
        int pickupCellIdWE = inputEvent.GetPickupCellWE();
        int pickupCellIdNS = inputEvent.GetPickupCellNS();
        double profit = inputEvent.GetFareAmount() + inputEvent.GetTipAmount();
        auto cellId = std::make_pair(pickupCellIdWE, pickupCellIdNS);
        if (!profitsMapPtr_->contains(cellId)) {
            profitsMapPtr_->emplace(cellId, NYTFunctionsHashMap<int, std::vector<double>>{});
        }
        auto& profitPaneMap = profitsMapPtr_->at(cellId);
        if (!profitPaneMap.contains(paneId_)) {
            profitPaneMap.emplace(paneId_, std::vector<double>{});
        }
        profitPaneMap.at(paneId_).emplace_back(profit);
    }

    void Merge(const std::vector<NYTProfitMergeableFunction>& vecOfCompletedPanes)
    {
        /*
        for (const auto& funcToRef: vecOfCompletedPanes) {
            for (const auto& profitsEntryFromPane: funcToRef.profits_) {
                const auto& key = profitsEntryFromPane.first;
                auto& vec = profitsEntryFromPane.second;
                std::copy(vec.begin(), vec.end(), std::back_inserter(profits_[key]));
            }
        }
         */
    }

    std::vector<NYTProfitReportT>& GetResult() override
    {
        results_.reserve(600 * 600);
        // profits_.reserve(600 * 600);
        results_.clear();
        // auto numPanes = currentNumPanes_ - 1;
        // auto firstPaneId = currentPaneId_ + 1 % numPanes;
        auto firstPaneId = (currentPaneId_ + 1) % currentNumPanes_;
        for (const auto& entry: profits_) {
            auto vec = std::vector<double>{};
            // vec.reserve(1'000'000);
            auto mapOfVecs = entry.second;
            std::for_each(mapOfVecs.cbegin(), mapOfVecs.cend(), [&vec, &firstPaneId](const auto& mapOfVecsEntry) {
                auto paneId = mapOfVecsEntry.first;
                std::vector<double> innerVec = mapOfVecsEntry.second;
                std::copy_n(innerVec.begin(), innerVec.size(), std::back_inserter(vec));
                if (paneId == firstPaneId) {
                    innerVec.clear();
                    // innerVec.reserve(1'000'000);
                }
            });
            auto size = static_cast<long>(vec.size());
            double median = 0.0;
            if (size % 2 == 1) {
                std::nth_element(vec.begin(), vec.begin() + size / 2, vec.end());
                median = vec[size / 2];
            }
            else {
                std::nth_element(vec.begin(), vec.begin() + size / 2, vec.end());
                double val1 = vec[size / 2];
                std::nth_element(vec.begin(), vec.begin() + (size / 2 - 1), vec.begin() + (size / 2));
                double val2 = vec[size / 2 - 1];
                median = (val1 + val2) / 2;
            }
            results_.emplace_back(entry.first.first, entry.first.second, median);
        }
        return results_;
    }

    void Reset()
    {
        results_.clear();
    }

    void InitializeActivePane(NYTProfitMergeableFunction& activePaneFunctor)
    {
        if (activePaneFunctor.paneId_ < 0) {
            activePaneFunctor.paneId_ = currentNumPanes_++;
            activePaneFunctor.profitsMapPtr_ = &profits_;
        }
        currentPaneId_ = activePaneFunctor.paneId_;
    }
};

#endif//ENJIMA_NYT_FUNCTIONS_H