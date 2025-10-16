//
// Created by t86kim on 21/11/24
//

#ifndef ENJIMA_BENCHMARKS_IN_MEMORY_RATE_LIMITED_NYT_DQ_SOURCE_OPERATOR_H
#define ENJIMA_BENCHMARKS_IN_MEMORY_RATE_LIMITED_NYT_DQ_SOURCE_OPERATOR_H

#include "csv2/reader.hpp"
#include "enjima/api/data_types/NewYorkTaxiEvent.h"
#include "enjima/benchmarks/workload/WorkloadException.h"
#include "enjima/benchmarks/workload/functions/NYTUtilities.h"
#include "enjima/operators/LatencyTrackingSourceOperator.h"

#include <chrono>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace enjima::workload::operators {
    template<typename Duration>
    class InMemoryRateLimitedNYTDQSourceOperator
        : public enjima::operators::LatencyTrackingSourceOperator<NYTFullEventT, Duration> {
    public:
        explicit InMemoryRateLimitedNYTDQSourceOperator(enjima::operators::OperatorID opId, const std::string& opName,
                uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate)
            : enjima::operators::LatencyTrackingSourceOperator<NYTFullEventT, Duration>(opId, opName,
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
                        enjima::core::Record<NYTFullEventT>(enjima::core::Record<NYTFullEventT>::RecordType::kLatency,
                                enjima::runtime::GetSystemTime<Duration>());
                collector->Collect(latencyRecord);
                this->latencyRecordLastEmittedAt_ = currentTime;
            }
            else {
                if (++cacheIterator_ == eventCachePtr_->cend()) {
                    cacheIterator_ = eventCachePtr_->cbegin();
                }
                auto nytEvent = *cacheIterator_.base();
                collector->CollectWithTimestamp<NYTFullEventT>(currentTime, nytEvent);
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
                        enjima::core::Record<NYTFullEventT>(enjima::core::Record<NYTFullEventT>::RecordType::kLatency,
                                enjima::runtime::GetSystemTime<Duration>());
                outBufferWritePtr = static_cast<enjima::core::Record<NYTFullEventT>*>(outBufferWritePtr) + 1;
                this->latencyRecordLastEmittedAt_ = currentTime;
                numLatencyRecordsOut = 1;
            }
            for (auto i = (maxRecordsToWrite - numLatencyRecordsOut); i > 0; i--) {
                if (++cacheIterator_ == eventCachePtr_->cend()) {
                    cacheIterator_ = eventCachePtr_->cbegin();
                }
                auto nytEvent = *cacheIterator_.base();
                new (outBufferWritePtr) enjima::core::Record<NYTFullEventT>(currentTime, nytEvent);
                outBufferWritePtr = static_cast<enjima::core::Record<NYTFullEventT>*>(outBufferWritePtr) + 1;
            }
            collector->CollectBatch<NYTFullEventT>(outputBuffer, maxRecordsToWrite);
            currentEmittedInMs_ += maxRecordsToWrite;
            return maxRecordsToWrite;
        }

        void PopulateEventCache(const std::string& nytDataPath, std::vector<NYTFullEventT>* eventCachePtr)
        {
            if (eventCachePtr == nullptr) {
                throw enjima::benchmarks::workload::WorkloadException{"Event cache ptr cannot be a nullptr!"};
            }
            eventCachePtr_ = eventCachePtr;
            if (eventCachePtr_->empty()) {
                auto startTime = runtime::GetSystemTimeMillis();
                NYTCsvReaderT csv;
                if (csv.mmap(nytDataPath)) {
                    // std::cout << "Num rows in file : " << csv.rows() << std::endl;
                    // std::cout << "Num rows in file (ignore_empty_lines) : " << csv.rows(true) << std::endl;
                    // assert(csv.rows(true) == csv.rows());
                    eventCachePtr_->reserve(131'072);
                    NYTRowReader<NYTFullEventT> cellReader{eventCachePtr_};
                    for (const auto& row: csv) {
                        if (row.length() > 0) {
                            cellReader.ReadFromRow(row);
                        }
                    }
                }
                else {
                    throw enjima::benchmarks::workload::WorkloadException{"Cannot read file " + nytDataPath};
                }
                spdlog::info("Completed reading file {} in {} milliseconds", nytDataPath,
                        runtime::GetSystemTimeMillis() - startTime);
            }
            cacheIterator_ = eventCachePtr_->cbegin();
        }

        NYTFullEventT GenerateQueueEvent() override
        {
            throw enjima::benchmarks::workload::WorkloadException{"Unsupported method called!"};
        }

        bool GenerateQueueRecord(core::Record<NYTFullEventT>& outputRecord) override
        {
            throw enjima::benchmarks::workload::WorkloadException{"Unsupported method called!"};
        }

    private:
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

        std::vector<NYTFullEventT>* eventCachePtr_{nullptr};
        std::vector<NYTFullEventT>::const_iterator cacheIterator_;
        uint64_t maxEmitRatePerMs_;
        uint64_t currentEmittedInMs_{0};
        uint64_t nextEmitStartUs_{0};
    };
}//namespace enjima::workload::operators

#endif//ENJIMA_BENCHMARKS_IN_MEMORY_RATE_LIMITED_NYT_DQ_SOURCE_OPERATOR_H