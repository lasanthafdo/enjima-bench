//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_IN_MEMORY_RATE_LIMITED_LRB_SOURCE_OPERATOR_H
#define ENJIMA_BENCHMARKS_IN_MEMORY_RATE_LIMITED_LRB_SOURCE_OPERATOR_H


#include "csv2/reader.hpp"
#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/benchmarks/workload/WorkloadException.h"
#include "enjima/operators/LatencyTrackingSourceOperator.h"
#include <fstream>
#include <random>
#include <sstream>

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;

namespace enjima::workload::operators {
    template<typename Duration>
    class InMemoryRateLimitedLRBSourceOperator
        : public enjima::operators::LatencyTrackingSourceOperator<LinearRoadT, Duration> {
    public:
        explicit InMemoryRateLimitedLRBSourceOperator(enjima::operators::OperatorID opId, const std::string& opName,
                uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate)
            : enjima::operators::LatencyTrackingSourceOperator<LinearRoadT, Duration>(opId, opName,
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
                        enjima::core::Record<LinearRoadT>(enjima::core::Record<LinearRoadT>::RecordType::kLatency,
                                enjima::runtime::GetSystemTime<Duration>());
                collector->Collect(latencyRecord);
                this->latencyRecordLastEmittedAt_ = currentTime;
            }
            else {
                if (++cacheIterator_ == eventCache_.cend()) {
                    cacheIterator_ = eventCache_.cbegin();
                    baseTimestamp_ += maxTimestampFromFile_;
                }
                auto lrEvent = *cacheIterator_.base();
                // Following collects based on timestamp from the file
                // uint64_t oldTs = lrEvent.GetTimestamp();
                // lrEvent.SetTimestamp(oldTs + baseTimestamp_);
                // collector->CollectWithTimestamp<LinearRoadT>(oldTs + baseTimestamp_, lrEvent);
                lrEvent.SetTimestamp(currentTime);
                std::cerr << "Source Operator" << currentTime << std::endl;
                collector->CollectWithTimestamp<LinearRoadT>(currentTime, lrEvent);
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
                        enjima::core::Record<LinearRoadT>(enjima::core::Record<LinearRoadT>::RecordType::kLatency,
                                enjima::runtime::GetSystemTime<Duration>());
                outBufferWritePtr = static_cast<enjima::core::Record<LinearRoadT>*>(outBufferWritePtr) + 1;
                this->latencyRecordLastEmittedAt_ = currentTime;
                numLatencyRecordsOut = 1;
            }
            for (auto i = (maxRecordsToWrite - numLatencyRecordsOut); i > 0; i--) {
                if (++cacheIterator_ == eventCache_.cend()) {
                    cacheIterator_ = eventCache_.cbegin();
                    baseTimestamp_ += maxTimestampFromFile_;
                }
                auto lrEvent = *cacheIterator_.base();
                // Following collects based on timestamp from the file
                // uint64_t oldTs = lrEvent.GetTimestamp();
                // lrEvent.SetTimestamp(oldTs + baseTimestamp_);
                // new (outBufferWritePtr) enjima::core::Record<LinearRoadT>(oldTs + baseTimestamp_, lrEvent);
                lrEvent.SetTimestamp(currentTime);
                new (outBufferWritePtr) enjima::core::Record<LinearRoadT>(currentTime, lrEvent);
                outBufferWritePtr = static_cast<enjima::core::Record<LinearRoadT>*>(outBufferWritePtr) + 1;
            }
            collector->CollectBatch<LinearRoadT>(outputBuffer, maxRecordsToWrite);
            currentEmittedInMs_ += maxRecordsToWrite;
            return maxRecordsToWrite;
        }

        void PopulateEventCache(const std::string& lrbDataPath)
        {
            csv2::Reader<csv2::delimiter<','>, csv2::quote_character<'"'>, csv2::first_row_is_header<false>> csv;
            if (csv.mmap(lrbDataPath)) {
                for (const auto row: csv) {
                    int fields[15];
                    int i = 0;
                    for (const auto cell: row) {
                        std::string value;
                        cell.read_value(value);
                        fields[i] = QuickAtoi(value.c_str());
                        i++;
                    }
                    eventCache_.emplace_back(fields[0], fields[1] * 1000, fields[2], fields[3], fields[4], fields[5],
                            fields[6], fields[7], fields[8], fields[9], fields[10], fields[11], fields[12], fields[13],
                            fields[14]);
                }
            }
            else {
                throw enjima::benchmarks::workload::WorkloadException{"Cannot read file " + lrbDataPath};
            }

            maxTimestampFromFile_ = eventCache_.back().GetTimestamp() + 1000;
            cacheIterator_ = eventCache_.cbegin();
        }

        LinearRoadT GenerateQueueEvent() override
        {
            throw enjima::benchmarks::workload::WorkloadException{"Unsupported method called!"};
        }

        bool GenerateQueueRecord(core::Record<LinearRoadT>& outputRecord) override
        {
            SleepIfNeeded();
            currentEmittedInMs_++;
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
                outputRecord =
                        enjima::core::Record<LinearRoadT>(enjima::core::Record<LinearRoadT>::RecordType::kLatency,
                                enjima::runtime::GetSystemTime<Duration>());
                this->latencyRecordLastEmittedAt_ = currentTime;
            }
            else {
                if (++cacheIterator_ == eventCache_.cend()) {
                    cacheIterator_ = eventCache_.cbegin();
                    baseTimestamp_ += maxTimestampFromFile_;
                }
                auto lrEvent = *cacheIterator_.base();
                lrEvent.SetTimestamp(currentTime);
                // std::cerr << "Source Operator: " << currentTime << std::endl;
                outputRecord = enjima::core::Record<LinearRoadT>(currentTime, lrEvent);
            }
            return true;
        }

    private:
        int QuickAtoi(const char* str)
        {
            int val = 0;
            bool isNegative = false;

            if (*str == '-') {
                isNegative = true;
                ++str;
            }
            uint8_t x;
            while ((x = uint8_t(*str++ - '0')) <= 9 && x >= 0) {
                val = val * 10 + x;
            }
            return isNegative ? -val : val;
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

        std::vector<LinearRoadT> eventCache_;
        std::vector<LinearRoadT>::const_iterator cacheIterator_;
        uint64_t maxEmitRatePerMs_;
        uint64_t currentEmittedInMs_{0};
        uint64_t nextEmitStartUs_{0};
        uint64_t baseTimestamp_{0};
        uint64_t maxTimestampFromFile_{0};
        uint64_t nOut_{0};
    };
}// namespace enjima::workload::operators

#endif//ENJIMA_BENCHMARKS_IN_MEMORY_RATE_LIMITED_LRB_SOURCE_OPERATOR_H