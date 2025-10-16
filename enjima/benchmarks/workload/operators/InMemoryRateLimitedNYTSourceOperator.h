//
// Created by t86kim on 21/11/24
//

#ifndef ENJIMA_BENCHMARKS_IN_MEMORY_RATE_LIMITED_NYT_SOURCE_OPERATOR_H
#define ENJIMA_BENCHMARKS_IN_MEMORY_RATE_LIMITED_NYT_SOURCE_OPERATOR_H

#include "csv2/reader.hpp"
#include "enjima/api/data_types/NewYorkTaxiEvent.h"
#include "enjima/api/data_types/NewYorkTaxiEventCondensed.h"
#include "enjima/benchmarks/workload/WorkloadException.h"
#include "enjima/benchmarks/workload/functions/NYTUtilities.h"
#include "enjima/operators/LatencyTrackingSourceOperator.h"
#include <chrono>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

using NewYorkTaxiT = enjima::api::data_types::NewYorkTaxiEventCondensed;

namespace enjima::workload::operators {
    template<typename Duration>
    class InMemoryRateLimitedNYTSourceOperator
        : public enjima::operators::LatencyTrackingSourceOperator<NewYorkTaxiT, Duration> {
    public:
        explicit InMemoryRateLimitedNYTSourceOperator(enjima::operators::OperatorID opId, const std::string& opName,
                uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate)
            : enjima::operators::LatencyTrackingSourceOperator<NewYorkTaxiT, Duration>(opId, opName,
                      latencyRecEmitPeriodMs),
              maxEmitRatePerMs_(maxInputRate / 1000)
        {
            nextEmitStartUs_ = (enjima::runtime::GetSystemTimeMillis() + 1) * 1000;
        }

        bool EmitEvent(enjima::core::OutputCollector* collector) override
        {
            SleepIfNeeded();
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
                auto latencyRecord =
                        enjima::core::Record<NewYorkTaxiT>(enjima::core::Record<NewYorkTaxiT>::RecordType::kLatency,
                                enjima::runtime::GetSystemTime<Duration>());
                collector->Collect(latencyRecord);
                this->latencyRecordLastEmittedAt_ = currentTime;
            }
            else {
                if (++cacheIterator_ == eventCache_.cend()) {
                    cacheIterator_ = eventCache_.cbegin();
                }
                auto nytEvent = *cacheIterator_.base();
                collector->CollectWithTimestamp<NewYorkTaxiT>(currentTime, nytEvent);
            }
            currentEmittedInMs_++;
            return true;
        }

        uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer,
                enjima::core::OutputCollector* collector) override
        {
            SleepIfNeeded();
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            void* outBufferWritePtr = outputBuffer;
            uint32_t numLatencyRecordsOut = 0;
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
                new (outBufferWritePtr)
                        enjima::core::Record<NewYorkTaxiT>(enjima::core::Record<NewYorkTaxiT>::RecordType::kLatency,
                                enjima::runtime::GetSystemTime<Duration>());
                outBufferWritePtr = static_cast<enjima::core::Record<NewYorkTaxiT>*>(outBufferWritePtr) + 1;
                this->latencyRecordLastEmittedAt_ = currentTime;
                numLatencyRecordsOut = 1;
            }
            for (auto i = (maxRecordsToWrite - numLatencyRecordsOut); i > 0; i--) {
                if (++cacheIterator_ == eventCache_.cend()) {
                    cacheIterator_ = eventCache_.cbegin();
                }
                auto nytEvent = *cacheIterator_.base();
                new (outBufferWritePtr) enjima::core::Record<NewYorkTaxiT>(currentTime, nytEvent);
                outBufferWritePtr = static_cast<enjima::core::Record<NewYorkTaxiT>*>(outBufferWritePtr) + 1;
            }
            collector->CollectBatch<NewYorkTaxiT>(outputBuffer, maxRecordsToWrite);
            currentEmittedInMs_ += maxRecordsToWrite;
            return maxRecordsToWrite;
        }

        void PopulateEventCache(const std::string& nytDataPath)
        {
            csv2::Reader<csv2::delimiter<','>, csv2::quote_character<'"'>, csv2::first_row_is_header<false>> csv;
            benchmarks::workload::AreaMapper areaMapper(0.25);
            std::string medallion;
            std::string puDateTime;
            std::string doDateTime;
            std::string temp;
            double puLat = 0;
            double puLon = 0;
            double doLat = 0;
            double doLon = 0;
            double fareAmount = 0;
            double tipAmount = 0;
            eventCache_.reserve(100'000);
            if (csv.mmap(nytDataPath)) {
                for (const auto& row: csv) {
                    int index = 0;
                    medallion.clear();
                    puDateTime.clear();
                    doDateTime.clear();
                    for (const auto& cell: row) {
                        switch (index) {
                            case 0:
                                cell.read_value(medallion);
                                break;
                            case 2:
                                cell.read_value(puDateTime);
                                break;
                            case 3:
                                cell.read_value(doDateTime);
                                break;
                            case 6:
                                cell.read_value(temp);
                                puLon = CustomStrToDouble(temp.data(), temp.data() + temp.size());
                                temp.clear();
                                break;
                            case 7:
                                cell.read_value(temp);
                                puLat = CustomStrToDouble(temp.data(), temp.data() + temp.size());
                                temp.clear();
                                break;
                            case 8:
                                cell.read_value(temp);
                                doLon = CustomStrToDouble(temp.data(), temp.data() + temp.size());
                                temp.clear();
                                break;
                            case 9:
                                cell.read_value(temp);
                                doLat = CustomStrToDouble(temp.data(), temp.data() + temp.size());
                                temp.clear();
                                break;
                            case 11:
                                cell.read_value(temp);
                                fareAmount = CustomStrToDouble(temp.data(), temp.data() + temp.size());
                                temp.clear();
                                break;
                            case 14:
                                cell.read_value(temp);
                                tipAmount = CustomStrToDouble(temp.data(), temp.data() + temp.size());
                                temp.clear();
                                break;
                            default:
                                break;
                        }
                        index++;
                    }
                    auto puId = areaMapper.GetCellID(puLat, puLon);
                    auto doId = areaMapper.GetCellID(doLat, doLon);
                    if (puId.first == -1 || puId.second == -1 || doId.first == -1 || doId.second == -1) {
                        continue;
                    }
                    auto currentTime = enjima::runtime::GetSystemTimeMillis();
                    assert(doDateTime.size() == 19);
                    eventCache_.emplace_back(medallion, puDateTime, doDateTime, puId.first, puId.second, doId.first,
                            doId.second, fareAmount, tipAmount, currentTime);
                }
            }
            else {
                throw enjima::benchmarks::workload::WorkloadException{"Cannot read file " + nytDataPath};
            }
            cacheIterator_ = eventCache_.cbegin();
        }

        NewYorkTaxiT GenerateQueueEvent() override
        {
            throw enjima::benchmarks::workload::WorkloadException{"Unsupported method called!"};
        }

        bool GenerateQueueRecord(core::Record<NewYorkTaxiT>& outputRecord) override
        {
            throw enjima::benchmarks::workload::WorkloadException{"Unsupported method called!"};
        }

    private:
        double CustomStrToDouble(const char* begin, const char* end)
        {
            double result = 0.0;
            double factor = 1.0;
            bool negative = false;
            if (begin == end) return result;
            if (*begin == '-') {
                negative = true;
                ++begin;
            }
            else if (*begin == '+') {
                ++begin;
            }
            while (begin < end && *begin >= '0' && *begin <= '9') {
                result = result * 10.0 + (*begin - '0');
                ++begin;
            }
            if (begin < end && *begin == '.') {
                ++begin;
                while (begin < end && *begin >= '0' && *begin <= '9') {
                    factor *= 0.1;
                    result += (*begin - '0') * factor;
                    ++begin;
                }
            }
            return negative ? -result : result;
        }

        void SleepIfNeeded()
        {
            uint64_t currentTimeUs = enjima::runtime::GetSystemTimeMicros();
            if (currentEmittedInMs_ >= maxEmitRatePerMs_) {
                auto sleepTimeUs = nextEmitStartUs_ - currentTimeUs;
                if (sleepTimeUs > 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(sleepTimeUs));
                    currentTimeUs += (sleepTimeUs + 1);
                }
            }
            if (currentTimeUs >= nextEmitStartUs_) {
                nextEmitStartUs_ += 1000;
                currentEmittedInMs_ = 0;
            }
        }

        std::vector<NewYorkTaxiT> eventCache_;
        std::vector<NewYorkTaxiT>::const_iterator cacheIterator_;
        uint64_t maxEmitRatePerMs_;
        uint64_t currentEmittedInMs_{0};
        uint64_t nextEmitStartUs_{0};
    };
}//namespace enjima::workload::operators

#endif//ENJIMA_BENCHMARKS_IN_MEMORY_RATE_LIMITED_NYT_SOURCE_OPERATOR_H