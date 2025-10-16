//
// Created by t86kim on 04/10/24.
//

#ifndef ENJIMA_LRBVB_FUNCTIONS_H
#define ENJIMA_LRBVB_FUNCTIONS_H

#include "enjima/api/JoinFunction.h"
#include "enjima/api/KeyExtractionFunction.h"
#include "enjima/api/KeyedAggregateFunction.h"
#include "enjima/api/MapFunction.h"
#include "enjima/api/SinkFunction.h"
#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/api/data_types/LRBValidateEvent.h"
#include "enjima/api/MergeableKeyedAggregateFunction.h"
#include "enjima/benchmarks/workload/functions/LRBUtilities.h"
#include "spdlog/spdlog.h"
#include <iostream>
#include <cmath>
#include <sstream>
#include <exception>
#include <string>
#include <atomic>

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using LRBVBProjEventT = enjima::api::data_types::LRBVBProjectedEvent;
using LRBVBAcdReportT = enjima::api::data_types::LRBVBAccidentReport;
using LRBVBCntReportT = enjima::api::data_types::LRBVBCountReport;
using LRBVBSpdReportT = enjima::api::data_types::LRBVBSpeedReport;
using LRBVBTollReportT = enjima::api::data_types::LRBVBTollReport;
using LRBVBTollFinalReportT = enjima::api::data_types::LRBVBTollFinalReport;

template<typename Key, typename T, typename HashFunc>
using CustomKeyUnorderedHashST = std::unordered_map<Key, T, HashFunc>;
using IntTuple3 = std::tuple<int, int, int>;
using IntTuple4 = std::tuple<int, int, int, int>;

namespace enjima::workload::functions {
    inline uint64_t truncToMin(uint64_t x) {
        return (x / 60'000) * 60'000 + 60'000;
    }

    class LRBVBIdentMapFunction : public enjima::api::MapFunction<LRBVBProjEventT, LRBVBProjEventT> {
        private:
            uint64_t lastSeenId_{0};
        public:
        LRBVBProjEventT operator()(const LRBVBProjEventT& inputEvent) override
        {
            try
            {
                uint64_t incomingId = inputEvent.GetId();
                if (lastSeenId_ > incomingId) {
                    throw incomingId;
                }
                else  {
                    lastSeenId_ = incomingId;
                }
                
            }
            catch (uint64_t e) 
            {
                std::cerr << "LRBVB Identity Map Function - incoming id is smaller than last seen id" << std::endl;
                std::cerr << "Last seen Id: " << lastSeenId_ << std::endl;
                std::cerr << "Incoming Id: " << e << std::endl;
            }
            return inputEvent;
        }
    };

    class LRBVBEventProjectFunction : public enjima::api::MapFunction<LinearRoadT, LRBVBProjEventT> {
        private:
            uint64_t id{1};
        public:
            LRBVBProjEventT operator()(const LinearRoadT& inputEvent) override {
                return LRBVBProjEventT(inputEvent.GetTimestamp(), inputEvent.GetVid(), inputEvent.GetSpeed(), inputEvent.GetXway(), inputEvent.GetLane(), inputEvent.GetDir(), inputEvent.GetSeg(), inputEvent.GetPos(), ++id);
            }
    };

    class LRBVBCountAggFunction : public enjima::api::KeyedAggregateFunction<LRBVBProjEventT, LRBVBCntReportT> {
        private:
            // key - (xway, dir, seg)
            // value - count
            CustomKeyUnorderedHashST<IntTuple3, int, HashTuple3> visited;
            std::vector<LRBVBCntReportT> results;
            uint64_t curTime_ {0};
            uint64_t lastSeenId_{0};
            uint64_t countId_{1};
        public:
            void operator()(const LRBVBProjEventT& inputEvent) override {
                try
                {
                    uint64_t incomingId = inputEvent.GetId();
                    if (lastSeenId_ > incomingId) {
                        throw incomingId;
                    }
                    else  {
                        lastSeenId_ = incomingId;
                    }
                }
                catch (uint64_t e) 
                {
                    std::cerr << "LRBVB Count Agg Function - incoming id is smaller than last seen id" << std::endl;
                    std::cerr << "Last seen Id: " << lastSeenId_ << std::endl;
                    std::cerr << "Incoming Id: " << e << std::endl;
                }   
                int xway = inputEvent.GetXway();
                int seg = inputEvent.GetSeg();
                int dir = inputEvent.GetDir();
                if (curTime_ == 0) {
                    curTime_ = inputEvent.GetTimestamp();
                }
                std::tuple<int, int, int> key = std::make_tuple(xway, dir, seg);
                auto emplaceResult = visited.emplace(key, 0);
                emplaceResult.first->second++;
            }

            std::vector<LRBVBCntReportT>& GetResult() override {
                results.clear();
                results.reserve(visited.size());
                for (auto& entry : visited) {
                    int xway = std::get<0>(entry.first);
                    int dir = std::get<1>(entry.first);
                    int seg = std::get<2>(entry.first);
                    int vcount = entry.second;
                    results.emplace_back(xway, dir, seg, vcount, truncToMin(curTime_), countId_);
                    entry.second = 0;
                }
                countId_++;
                std::cerr << "Count: materialized for minute " << truncToMin(curTime_) << std::endl;
                curTime_ = 0;
                return results;
            }
    };

    class LRBVBSpeedAggFunction : public enjima::api::KeyedAggregateFunction<LRBVBProjEventT, LRBVBSpdReportT> {
        private:
            // key - xway, dir, seg
            // value - Map<vid, pair <count, speedSum>>
            CustomKeyUnorderedHashST<IntTuple3, UnorderedHashMapST<int, std::pair<int, int>>, HashTuple3> avgsv;
            
            // key - xway, dir, seg
            // value - vector of spd
            CustomKeyUnorderedHashST<IntTuple3, std::vector<int>, HashTuple3> lastFiveLav;

            std::vector<LRBVBSpdReportT> results;

            uint64_t curTime_{0};
            uint64_t lastSeenId_{0};
            uint64_t speedId_{1};
        public:
            void operator()(const LRBVBProjEventT& inputEvent) override  {
                try
                {
                    uint64_t incomingId = inputEvent.GetId();
                    if (lastSeenId_ > incomingId) {
                        throw incomingId;
                    }
                    else  {
                        lastSeenId_ = incomingId;
                    }
                }
                catch (uint64_t e) 
                {
                    std::cerr << "LRBVB Speed Agg Function - incoming id is smaller than last seen id" << std::endl;
                    std::cerr << "Last seen Id: " << lastSeenId_ << std::endl;
                    std::cerr << "Incoming Id: " << e << std::endl;
                }   
                int xway = inputEvent.GetXway();
                int vid = inputEvent.GetVid();
                int speed = inputEvent.GetSpeed();
                int seg = inputEvent.GetSeg();
                int dir = inputEvent.GetDir();
                if (curTime_ == 0) {
                    curTime_ = inputEvent.GetTimestamp();
                }
                std::tuple<int, int, int> key = std::make_tuple(xway, dir, seg);
                auto emplaceResult = avgsv[key].emplace(vid, std::make_pair(0, 0));
                auto& countSpeedPair = emplaceResult.first->second;
                countSpeedPair.first++;
                countSpeedPair.second += speed;
            }

            std::vector<LRBVBSpdReportT>& GetResult() override {
                results.clear();
                results.reserve(lastFiveLav.size());
                // Put current minute into the last five minute based on all entries in avgsv
                for (auto& entry : avgsv) {
                    auto& key = entry.first;
                    auto& vidData = entry.second;

                    int totalSpeed = 0;
                    int vCount = vidData.size();

                    for (const auto& vidEntry : vidData) {
                        totalSpeed += vidEntry.second.second / vidEntry.second.first;
                    }

                    int avgSpeed = (vCount > 0) ? totalSpeed / vCount : 0;
                    auto& speedHistory = lastFiveLav[key];
                    speedHistory.emplace_back(avgSpeed);
                    if (speedHistory.size() > 5) {
                        speedHistory.erase(speedHistory.begin());
                    }
                    avgsv[key].clear();
                }
                // Then materialize based on lastFiveLav - only need to keep 5 entries in the vector
                for (const auto& entry : lastFiveLav) {
                    const auto& key = entry.first;
                    const auto& speedHistory = entry.second;

                    int xway = std::get<0>(key);
                    int dir = std::get<1>(key);
                    int seg = std::get<2>(key);

                    int avgSpeed = 0;
                    if (!speedHistory.empty()) {
                        avgSpeed = std::reduce(speedHistory.begin(), speedHistory.end()) /
                                static_cast<int>(speedHistory.size());
                    }
                    results.emplace_back(xway, dir, seg, avgSpeed, truncToMin(curTime_), speedId_);
                }
                speedId_++;
                std::cerr << "Speed: materialized for minute " << truncToMin(curTime_) << std::endl;
                curTime_ = 0;
                return results;
            }
    };

    class LRBVBCntReportMultiKeyExtractFunction : public enjima::api::KeyExtractionFunction<LRBVBCntReportT, std::tuple<int, int, int, uint64_t>> {
        public:
            std::tuple<int, int, int, uint64_t> operator()(const LRBVBCntReportT& inputEvent) override {
                int xway = inputEvent.GetXway();
                int dir = inputEvent.GetDir();
                int seg = inputEvent.GetSeg();
                uint64_t time = inputEvent.GetTimestamp();

                return std::make_tuple(xway, dir, seg, time);
            }
    };

    class LRBVBSpdReportMultiKeyExtractFunction : public enjima::api::KeyExtractionFunction<LRBVBSpdReportT, std::tuple<int, int, int, uint64_t>> {
        public:
            std::tuple<int, int, int, uint64_t> operator()(const LRBVBSpdReportT& inputEvent) override {
                int xway = inputEvent.GetXway();
                int dir = inputEvent.GetDir();
                int seg = inputEvent.GetSeg();
                uint64_t time = inputEvent.GetTimestamp();

                return std::make_tuple(xway, dir, seg, time);
            }
    };

    class LRBVBSpdCntJoinFunction : public enjima::api::JoinFunction<LRBVBSpdReportT, LRBVBCntReportT, LRBVBTollReportT> {
        private:
            uint64_t lastSeenSpeedId_{0};
            uint64_t lastSeenCountId_{0};
            uint64_t tollId_{1};
        public:
            LRBVBTollReportT operator()(const LRBVBSpdReportT& leftInputEvent, const LRBVBCntReportT& rightInputEvent) override
            {
                
                try {
                    uint64_t incomingSpeedId = leftInputEvent.GetId();
                    if (lastSeenSpeedId_ > incomingSpeedId) {
                        throw incomingSpeedId;
                    } else {
                        lastSeenSpeedId_ = incomingSpeedId;    
                    }
                }
                catch (uint64_t e) {   
                    std::cerr << "LRBVB Speed and Count Join Function - Speed's incoming id is smaller than last seen id" << std::endl;
                    std::cerr << "Last seen Speed Id: " << lastSeenSpeedId_ << std::endl;
                    std::cerr << "Incoming Id: " << e << std::endl;
                    std::cerr << std::endl;
                }
                
                try {
                    uint64_t incomingCountId = rightInputEvent.GetId();
                    if (lastSeenCountId_ > incomingCountId) {
                        throw incomingCountId;
                    } else {
                        lastSeenCountId_ = incomingCountId;
                    }
                }
                catch (uint64_t e) {
                    std::cerr << "LRBVB Speed and Count Join Function - Count's incoming id is smaller than last seen id" << std::endl;
                    std::cerr << "Last seen Count Id: " << lastSeenCountId_ << std::endl;
                    std::cerr << "Incoming Id: " << e << std::endl;
                    std::cerr << std::endl;
                }
                
                uint64_t time = leftInputEvent.GetTimestamp();
                int lav = leftInputEvent.GetSpeed();
                int numCars = rightInputEvent.GetCount();

                int xway = leftInputEvent.GetXway();
                int seg = leftInputEvent.GetSeg();
                int dir = leftInputEvent.GetDir();
                int toll = 0;

                if (lav < 40 && numCars > 50) {
                    auto tollBase = numCars - 50;
                    toll = 2 * tollBase * tollBase;
                }
                return LRBVBTollReportT(xway, dir, seg, toll, time, tollId_++);
            }
    };

    class LRBVBTollAcdCoGroupFunction : public enjima::api::CoGroupFunction<LRBVBTollReportT, LRBVBAcdReportT, LRBVBTollFinalReportT> {
        private:
            // Key - xway, dir, seg
            // Value - time
            CustomKeyUnorderedHashST<IntTuple3, uint64_t, HashTuple3> reportedAcds;
            uint64_t lastSeenTollId_{0};
            uint64_t lastSeenAcdId_{0};
            uint64_t nOut_{0};
        public:
            void operator()(const std::vector<core::Record<LRBVBTollReportT>>& leftInputRecords,
                const std::vector<core::Record<LRBVBAcdReportT>>& rightInputRecords, std::queue<LRBVBTollFinalReportT>& outputEvents) override
            {
                //  Storing accident information
                for (const auto& acdInputRecord : rightInputRecords) {
                    try
                    {
                        uint64_t incomingAccidentId = acdInputRecord.GetData().GetId();
                        if (lastSeenAcdId_ > incomingAccidentId) {
                            throw incomingAccidentId;
                        } else {
                            lastSeenAcdId_ = incomingAccidentId;
                        }
                    }
                    catch (uint64_t e) 
                    {
                        std::cerr << "LRBVB Accident events in CoGroup Function - incoming id is smaller than last seen id" << std::endl;
                        std::cerr << "Last seen Id: " << lastSeenAcdId_ << std::endl;
                        std::cerr << "Incoming Id: " << e << std::endl;
                    }
                    auto acd = acdInputRecord.GetData();
                    int type = acd.GetType();
                    int xway = acd.GetXway();
                    int dir = acd.GetDir();
                    int seg = acd.GetSeg();
                    std::tuple<int, int, int> curkey = std::make_tuple(xway, dir, seg);
                    if (type == 0) {
                        uint64_t time = acd.GetTimestamp();
                        reportedAcds[curkey] = time;
                    } else if (type == 1) {
                        reportedAcds.erase(curkey);
                    }
                }

                for (const auto& tollInputRecord : leftInputRecords) {
                    try
                    {
                        uint64_t incomingTolltId = tollInputRecord.GetData().GetId();
                        if (lastSeenTollId_ > incomingTolltId) {
                            throw incomingTolltId;
                        } else {
                            lastSeenTollId_ = incomingTolltId;
                        }
                    }
                    catch (uint64_t e) 
                    {
                        std::cerr << "LRBVB Toll events in CoGroup Function - incoming id is smaller than last seen id" << std::endl;
                        std::cerr << "Last seen Id: " << lastSeenTollId_ << std::endl;
                        std::cerr << "Incoming Id: " << e << std::endl;
                    }
                    auto toll = tollInputRecord.GetData();
                    int xwayToll = toll.GetXway();
                    int dirToll = toll.GetDir();
                    int segToll = toll.GetSeg();
                    int finalToll = toll.GetToll();
                    std::tuple<int, int, int> curkey = std::make_tuple(xwayToll, dirToll, segToll);
                    if (dirToll == 0) {
                        auto& segKey = std::get<2>(curkey);
                        auto lastSegKey = std::min(segKey + 4, 99);
                        for (int i = segKey; i <= lastSegKey; ++i) {
                            segKey = i;
                            if (reportedAcds.contains(curkey)) {
                                finalToll = 0;
                                break;
                            }
                        }
                    } else if (dirToll == 1) {
                        auto& segKey = std::get<2>(curkey);
                        auto lastSegKey = std::max(0, segKey - 4);
                        for (int i = segKey; i >= lastSegKey; --i) {
                            segKey = i;
                            if (reportedAcds.contains(curkey)) {
                                finalToll = 0;
                                break;
                            }
                        }
                    }
                    outputEvents.emplace(xwayToll, dirToll, segToll, finalToll);
                }
            }
    };
} // namespace enjima::workload::functions

template<>
class enjima::api::MergeableKeyedAggregateFunction<LRBVBProjEventT, LRBVBAcdReportT> : public enjima::api::KeyedAggregateFunction<LRBVBProjEventT, LRBVBAcdReportT> {
    using LRBVBMergeableFunction = enjima::api::MergeableKeyedAggregateFunction<LRBVBProjEventT, LRBVBAcdReportT>;
    private:
    // key - xway, lane, pos, dir
    // value - Map<vid, Pair<count, time>>
    CustomKeyUnorderedHashST<IntTuple4, UnorderedHashMapST<int, std::pair<int, uint64_t>>, HashTuple4> stopped_;
    // key - vid
    // value - xway, dir, pos, time
    UnorderedHashMapST<int, std::tuple<int, int, int, uint64_t>> reported_;
    UnorderedHashMapST<int, std::tuple<int, int, int, uint64_t>>* ptrToReported_ {nullptr};
    std::vector<LRBVBAcdReportT> results_;
    std::vector<LRBVBAcdReportT>* ptrToResults_ {nullptr};

public:
    void operator()(const LRBVBProjEventT& inputEvent) override
    {
        int xway = inputEvent.GetXway();
        int lane = inputEvent.GetLane();
        int curpos = inputEvent.GetPos();
        int dir = inputEvent.GetDir();
        uint64_t time = inputEvent.GetTimestamp();
        int vid = inputEvent.GetVid();
        std::tuple<int, int, int, int> key = std::make_tuple(xway, lane, curpos, dir);
        if (inputEvent.GetSpeed() == 0) {
            auto emplaceResult = stopped_[key].emplace(vid, std::make_pair(0, time));
            emplaceResult.first->second.first++;
        }
        else {
            if (ptrToReported_ != nullptr && ptrToResults_ != nullptr) {
                if (ptrToReported_->find(vid) != ptrToReported_->end()) {
                    auto data = ptrToReported_->at(vid);
                    int pos = std::get<2>(data);
                    int seg = pos / 5280;
                    uint64_t clearedTime = inputEvent.GetTimestamp();
                    ptrToResults_->emplace_back(1, std::get<0>(data), std::get<1>(data), seg, clearedTime, pos, 0);
                    ptrToReported_->erase(vid);
                }
            }
        }
    }

    void Merge(const std::vector<LRBVBMergeableFunction>& vecToMerge)
    {        
        for (const auto& funcToRef: vecToMerge) {
            for (const auto& entry: funcToRef.stopped_) {
                const auto& key = entry.first;
                const auto& vidMap = entry.second;
                for (const auto& vEntry: vidMap) {
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

    std::vector<LRBVBAcdReportT>& GetResult() override
    {
        for (const auto& entry: stopped_) {
            const auto& key = entry.first;
            const auto& vidMap = entry.second;

            // vector of pair(vid, initial time observed that this vehicle was observed to stop)
            std::vector<std::pair<int, int>> possibleAcdVids;
            for (const auto& vEntry: vidMap) {
                int vid = vEntry.first;
                int count = vEntry.second.first;
                uint64_t timeObserved = vEntry.second.second;
                if (reported_.find(vid) == reported_.end() && count >= 4) {
                    possibleAcdVids.emplace_back(vid, timeObserved);
                }
            }
            if (possibleAcdVids.size() >= 2) {
                int xway = std::get<0>(key);
                int pos = std::get<2>(key);
                int dir = std::get<3>(key);
                int seg = pos / 5280;
                uint64_t time = possibleAcdVids[0].second;
                results_.emplace_back(0, xway, dir, seg, time, pos, 0);
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

    void InitializeActivePane(LRBVBMergeableFunction& activePaneFunctor)
    {
        results_.clear();
        activePaneFunctor.ptrToReported_ = &reported_;
        activePaneFunctor.ptrToResults_ = &results_;
    }
};

#endif//ENJIMA_LRBVB_FUNCTIONS_H