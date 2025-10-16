//
// Created by t86kim on 04/10/24.
//

#ifndef ENJIMA_LRB_FUNCTIONS_H
#define ENJIMA_LRB_FUNCTIONS_H

#include "ankerl/unordered_dense.h"
#include "enjima/api/CoGroupFunction.h"
#include "enjima/api/JoinFunction.h"
#include "enjima/api/KeyExtractionFunction.h"
#include "enjima/api/KeyedAggregateFunction.h"
#include "enjima/api/MapFunction.h"
#include "enjima/api/MergeableKeyedAggregateFunction.h"
#include "enjima/api/SinkFunction.h"
#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/api/data_types/LinearRoadInterimEventTypes.h"
#include "enjima/benchmarks/workload/functions/LRBUtilities.h"
#include "spdlog/spdlog.h"

#include <cmath>
#include <iostream>
#include <queue>
#include <sstream>

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using LRBProjEventT = enjima::api::data_types::LRBProjectedEvent;
using LRBAcdReportT = enjima::api::data_types::LRBAccidentReport;
using LRBCntReportT = enjima::api::data_types::LRBCountReport;
using LRBSpdReportT = enjima::api::data_types::LRBSpeedReport;
using LRBTollReportT = enjima::api::data_types::LRBTollReport;
using LRBTollFinalReportT = enjima::api::data_types::LRBTollFinalReport;

using IntTuple3 = std::tuple<int, int, int>;
using IntTuple4 = std::tuple<int, int, int, int>;

template<typename Key, typename T, typename HashFunc = ankerl::unordered_dense::hash<Key>>
using LRBFunctionsHashMap = ankerl::unordered_dense::map<Key, T, HashFunc>;

//template<typename Key, typename T, typename HashFunc = std::hash<Key>>
//using LRBFunctionsHashMap = UnorderedHashMapST<Key, T, HashFunc>;

namespace enjima::workload::functions {
    inline uint64_t truncateToMin(uint64_t x)
    {
        return (x / 60'000) * 60'000 + 60'000;
    }

    class LRBIdentMapFunction : public enjima::api::MapFunction<LRBProjEventT, LRBProjEventT> {
    public:
        LRBProjEventT operator()(const LRBProjEventT& inputEvent) override
        {
            return inputEvent;
        }
    };

    class LRBEventProjectFunction : public enjima::api::MapFunction<LinearRoadT, LRBProjEventT> {
    public:
        LRBProjEventT operator()(const LinearRoadT& inputEvent) override
        {
            return {inputEvent.GetTimestamp(), inputEvent.GetVid(), inputEvent.GetSpeed(), inputEvent.GetXway(),
                    inputEvent.GetLane(), inputEvent.GetDir(), inputEvent.GetSeg(), inputEvent.GetPos()};
        }
    };

    class LRBCountAggFunction : public enjima::api::KeyedAggregateFunction<LRBProjEventT, LRBCntReportT> {
    private:
        // key - (xway, dir, seg)
        // value - count
        LRBFunctionsHashMap<IntTuple3, int> visited;
        std::vector<LRBCntReportT> results;
        uint64_t curTime_{0};

    public:
        void operator()(const LRBProjEventT& inputEvent) override
        {
            int xway = inputEvent.GetXway();
            int dir = inputEvent.GetDir();
            int seg = inputEvent.GetSeg();
            if (curTime_ == 0) {
                // Do we output the result with the window start time?
                // Usually, the convention is you output results with the window end time.
                curTime_ = inputEvent.GetTimestamp();
            }
            std::tuple<int, int, int> key = std::make_tuple(xway, dir, seg);
            auto emplaceResult = visited.emplace(key, 0);
            emplaceResult.first->second++;
        }

        std::vector<LRBCntReportT>& GetResult() override
        {
            // TODO It would be nice if we can not do clear and allocate every time for results as well. Need to think...
            results.clear();
            results.reserve(visited.size());
            for (auto& entry: visited) {
                int xway = std::get<0>(entry.first);
                int dir = std::get<1>(entry.first);
                int seg = std::get<2>(entry.first);
                int vcount = entry.second;
                // Do we output the result with the window start time?
                // Usually, the convention is you output results with the window end time.
                results.emplace_back(xway, dir, seg, vcount, truncateToMin(curTime_));
                entry.second = 0;
            }
            curTime_ = 0;
            return results;
        }
    };

    class LRBSpeedAggFunction : public enjima::api::KeyedAggregateFunction<LRBProjEventT, LRBSpdReportT> {
    private:
        // key - xway, dir, seg
        // value - Map<vid, pair <count, speedSum>>
        LRBFunctionsHashMap<IntTuple3, LRBFunctionsHashMap<int, std::pair<int, int>>> avgSV_;

        // key - xway, dir, seg
        // value - vector of spd
        LRBFunctionsHashMap<IntTuple3, std::vector<int>> lastFiveLav_;
        std::vector<LRBSpdReportT> results_;
        uint64_t curTime_{0};

    public:
        void operator()(const LRBProjEventT& inputEvent) override
        {
            std::tuple<int, int, int> key =
                    std::make_tuple(inputEvent.GetXway(), inputEvent.GetDir(), inputEvent.GetSeg());
            int vid = inputEvent.GetVid();
            if (!avgSV_[key].contains(vid)) {
                avgSV_[key][vid] = std::make_pair(0, 0);
            }
            auto& countSpeedPair = avgSV_[key][vid];
            countSpeedPair.first++;
            countSpeedPair.second += inputEvent.GetSpeed();
            if (curTime_ == 0) {
                curTime_ = inputEvent.GetTimestamp();
            }
        }

        std::vector<LRBSpdReportT>& GetResult() override
        {
            results_.clear();
            results_.reserve(lastFiveLav_.size());
            // Put current minute into the last five minute based on all entries in avgsv
            for (auto& entry: avgSV_) {
                auto& xWaySegDirKey = entry.first;
                auto& vidData = entry.second;

                int totalSpeed = 0;
                int vCount = static_cast<int>(vidData.size());
                for (const auto& vidEntry: vidData) {
                    totalSpeed += vidEntry.second.second / vidEntry.second.first;
                }

                int avgSpeed = (vCount > 0) ? totalSpeed / vCount : 0;
                auto& speedHistory = lastFiveLav_[xWaySegDirKey];
                speedHistory.emplace_back(avgSpeed);
                if (speedHistory.size() > 5) {
                    speedHistory.erase(speedHistory.begin());
                }
                avgSV_[xWaySegDirKey].clear();
            }

            // Then materialize based on lastFiveLav - only need to keep 5 entries in the vector
            for (const auto& entry: lastFiveLav_) {
                const auto& xWaySegDirKey = entry.first;
                const auto& speedHistory = entry.second;

                int xway = std::get<0>(xWaySegDirKey);
                int dir = std::get<1>(xWaySegDirKey);
                int seg = std::get<2>(xWaySegDirKey);

                // Is avgSpeed required as an int?
                int avgSpeed = 0;
                if (!speedHistory.empty()) {
                    avgSpeed = std::reduce(speedHistory.begin(), speedHistory.end()) /
                               static_cast<int>(speedHistory.size());
                }
                results_.emplace_back(xway, dir, seg, avgSpeed, truncateToMin(curTime_));
            }
            curTime_ = 0;
            return results_;
        }
    };

    class LRBCntReportMultiKeyExtractFunction
        : public enjima::api::KeyExtractionFunction<LRBCntReportT, std::tuple<int, int, int, uint64_t>> {
    public:
        std::tuple<int, int, int, uint64_t> operator()(const LRBCntReportT& inputEvent) override
        {
            int xway = inputEvent.GetXway();
            int dir = inputEvent.GetDir();
            int seg = inputEvent.GetSeg();
            uint64_t time = inputEvent.GetTimestamp();

            return std::make_tuple(xway, dir, seg, time);
        }
    };

    class LRBSpdReportMultiKeyExtractFunction
        : public enjima::api::KeyExtractionFunction<LRBSpdReportT, std::tuple<int, int, int, uint64_t>> {
    public:
        std::tuple<int, int, int, uint64_t> operator()(const LRBSpdReportT& inputEvent) override
        {
            int xway = inputEvent.GetXway();
            int dir = inputEvent.GetDir();
            int seg = inputEvent.GetSeg();
            uint64_t time = inputEvent.GetTimestamp();

            return std::make_tuple(xway, dir, seg, time);
        }
    };

    class LRBSpdCntJoinFunction : public enjima::api::JoinFunction<LRBSpdReportT, LRBCntReportT, LRBTollReportT> {
    public:
        LRBTollReportT operator()(const LRBSpdReportT& leftInputEvent, const LRBCntReportT& rightInputEvent) override
        {
            uint64_t time = leftInputEvent.GetTimestamp();
            int lav = leftInputEvent.GetSpeed();
            int numCars = rightInputEvent.GetCount();

            int xway = leftInputEvent.GetXway();
            int seg = leftInputEvent.GetSeg();
            int dir = leftInputEvent.GetDir();
            long toll = 0l;

            if (lav < 40 && numCars > 50) {
                long tollBase = numCars - 50;
                toll = 2 * tollBase * tollBase;
            }
            return LRBTollReportT(xway, dir, seg, toll, time);
        }
    };

    class LRBTollAcdCoGroupFunction
        : public enjima::api::CoGroupFunction<LRBTollReportT, LRBAcdReportT, LRBTollFinalReportT> {
    private:
        // Key - xway, dir, seg
        // Value - time
        LRBFunctionsHashMap<IntTuple3, uint64_t> reportedAcds;

    public:
        void operator()(const std::vector<core::Record<LRBTollReportT>>& leftInputRecords,
                const std::vector<core::Record<LRBAcdReportT>>& rightInputRecords,
                std::queue<LRBTollFinalReportT>& outputEvents) override
        {
            //  Storing accident information
            for (const auto& acdInputRecord: rightInputRecords) {
                auto acd = acdInputRecord.GetData();
                int type = acd.GetType();
                int xway = acd.GetXway();
                int dir = acd.GetDir();
                int seg = acd.GetSeg();
                std::tuple<int, int, int> curKey = std::make_tuple(xway, dir, seg);
                if (type == 0) {
                    uint64_t time = acd.GetTimestamp();
                    reportedAcds[curKey] = time;
                }
                else if (type == 1) {
                    reportedAcds.erase(curKey);
                }
            }

            for (const auto& tollInputRecord: leftInputRecords) {
                auto toll = tollInputRecord.GetData();
                int xwayToll = toll.GetXway();
                int dirToll = toll.GetDir();
                int segToll = toll.GetSeg();
                long finalToll = toll.GetToll();
                std::tuple<int, int, int> curKey = std::make_tuple(xwayToll, dirToll, segToll);
                if (dirToll == 0) {
                    auto& segKey = std::get<2>(curKey);
                    auto lastSegKey = std::min(segKey + 4, 99);
                    for (int i = segKey; i <= lastSegKey; ++i) {
                        segKey = i;
                        if (reportedAcds.contains(curKey)) {
                            finalToll = 0;
                            break;
                        }
                    }
                }
                else if (dirToll == 1) {
                    auto& segKey = std::get<2>(curKey);
                    auto lastSegKey = std::max(0, segKey - 4);
                    for (int i = segKey; i >= lastSegKey; --i) {
                        segKey = i;
                        if (reportedAcds.contains(curKey)) {
                            finalToll = 0;
                            break;
                        }
                    }
                }
                outputEvents.emplace(xwayToll, dirToll, segToll, finalToll);
            }
        }
    };
}// namespace enjima::workload::functions

template<>
class enjima::api::MergeableKeyedAggregateFunction<LRBProjEventT, LRBAcdReportT>
    : public enjima::api::KeyedAggregateFunction<LRBProjEventT, LRBAcdReportT> {
    using LRBMergeableFunction = enjima::api::MergeableKeyedAggregateFunction<LRBProjEventT, LRBAcdReportT>;

private:
    // key - xway, lane, pos, dir
    // value - Map<vid, Pair<count, time>>
    LRBFunctionsHashMap<IntTuple4, LRBFunctionsHashMap<int, std::pair<int, uint64_t>>> stopped_;
    // key - vid
    // value - xway, dir, pos, time
    UnorderedHashMapST<int, std::tuple<int, int, int, uint64_t>> reported_;
    UnorderedHashMapST<int, std::tuple<int, int, int, uint64_t>>* ptrToReported_{nullptr};
    std::vector<LRBAcdReportT> results_;
    std::vector<LRBAcdReportT>* ptrToResults_{nullptr};

public:
    void operator()(const LRBProjEventT& inputEvent) override
    {
        int xWay = inputEvent.GetXway();
        int lane = inputEvent.GetLane();
        int curPos = inputEvent.GetPos();
        int dir = inputEvent.GetDir();
        uint64_t time = inputEvent.GetTimestamp();
        int vid = inputEvent.GetVid();
        std::tuple<int, int, int, int> key = std::make_tuple(xWay, lane, curPos, dir);
        if (inputEvent.GetSpeed() == 0) {
            auto emplaceResult = stopped_[key].emplace(vid, std::make_pair(0, time));
            emplaceResult.first++;
        }
        else {
            if (ptrToReported_ != nullptr && ptrToResults_ != nullptr) {
                if (ptrToReported_->contains(vid)) {
                    auto data = ptrToReported_->at(vid);
                    int pos = std::get<2>(data);
                    int seg = static_cast<int>(std::floor(pos / 5280));
                    uint64_t clearedTime = inputEvent.GetTimestamp();
                    ptrToResults_->emplace_back(1, std::get<0>(data), std::get<1>(data), seg, clearedTime, pos);
                    ptrToReported_->erase(vid);
                }
            }
        }
    }

    void Merge(const std::vector<LRBMergeableFunction>& vecOfCompletedPanes)
    {
        stopped_.reserve(700000);
        for (const auto& funcToRef: vecOfCompletedPanes) {
            for (const auto& stoppedEntryFromPane: funcToRef.stopped_) {
                const auto key = stoppedEntryFromPane.first;
                const auto& paneVidMap = stoppedEntryFromPane.second;
                for (const auto& vEntry: paneVidMap) {
                    int vid = vEntry.first;
                    int count = vEntry.second.first;
                    uint64_t time = vEntry.second.second;
                    if (stopped_[key].find(vid) != stopped_[key].end()) {
                        stopped_[key][vid].first += count;
                    }
                    else {
                        stopped_[key][vid] = std::make_pair(count, time);
                    }
                }
            }
        }
    }

    std::vector<LRBAcdReportT>& GetResult() override
    {
        reported_.reserve(stopped_.size());
        for (const auto& entry: stopped_) {
            const auto& xWayLanePosDir = entry.first;
            const auto& vidMap = entry.second;

            // vector of pair(vid, initial time observed that this vehicle was observed to stop)
            std::vector<std::pair<int, int>> possibleAcdVids;
            for (const auto& vEntry: vidMap) {
                int vid = vEntry.first;
                int count = vEntry.second.first;
                uint64_t timeObserved = vEntry.second.second;
                if (!reported_.contains(vid) && count >= 4) {
                    possibleAcdVids.emplace_back(vid, timeObserved);
                }
            }
            if (possibleAcdVids.size() >= 2) {
                int xway = std::get<0>(xWayLanePosDir);
                int pos = std::get<2>(xWayLanePosDir);
                int dir = std::get<3>(xWayLanePosDir);
                int seg = pos / 5280;
                uint64_t time = possibleAcdVids[0].second;
                results_.emplace_back(0, xway, dir, seg, time, pos);
                for (const auto& vidTimePair: possibleAcdVids) {
                    int vid = vidTimePair.first;
                    reported_[vid] = std::make_tuple(xway, dir, pos, time);
                }
            }
        }
        return results_;
    }

    void Reset()
    {
        stopped_.clear();
        results_.clear();
    }

    void InitializeActivePane(LRBMergeableFunction& activePaneFunctor)
    {
        results_.clear();
        activePaneFunctor.ptrToReported_ = &reported_;
        activePaneFunctor.ptrToResults_ = &results_;
    }
};
#endif//ENJIMA_LRB_FUNCTIONS_H