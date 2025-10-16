#ifndef ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_LRB_SOURCE_OPERATOR_WITH_PROC_LATENCY_H
#define ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_LRB_SOURCE_OPERATOR_WITH_PROC_LATENCY_H

#include "csv2/reader.hpp"
#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/benchmarks/workload/WorkloadException.h"
#include "enjima/operators/LatencyTrackingSourceOperator.h"
#include "enjima/runtime/RuntimeTypes.h"
#include "spdlog/spdlog.h"

#include <fstream>
#include <sstream>
#include <string>

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;
using LRBRecordT = enjima::core::Record<LinearRoadT>;


namespace enjima::workload::operators {
    template<typename Duration>
    class InMemoryFixedRateLRBSourceOperatorWithProcLatency
        : public enjima::operators::LatencyTrackingSourceOperator<LinearRoadT, Duration> {
    public:
        InMemoryFixedRateLRBSourceOperatorWithProcLatency(enjima::operators::OperatorID opId, const std::string& opName,
                uint64_t latencyRecordEmitPeriodMs, uint64_t maxInputRate, uint64_t srcReservoirCapacity)
            : enjima::operators::LatencyTrackingSourceOperator<LinearRoadT, Duration>(opId, opName,
                      latencyRecordEmitPeriodMs),
              maxEmitRatePerMs_(maxInputRate / 1000), srcReservoirCapacity_(srcReservoirCapacity),
              eventReservoir_(static_cast<LRBRecordT*>(
                      aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, sizeof(LRBRecordT) * srcReservoirCapacity)))
        {
            nextEmitStartUs_ = enjima::runtime::GetSteadyClockMicros() + 1000;
        }

        ~InMemoryFixedRateLRBSourceOperatorWithProcLatency() override
        {
            genTaskRunning_.store(false, std::memory_order::release);
            eventGenThread_.join();
            free(static_cast<void*>(eventReservoir_));
        }

        bool EmitEvent(enjima::core::OutputCollector* collector) override
        {
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
#if ENJIMA_METRICS_LEVEL >= 3
                auto metricsVec = new std::vector<uint64_t>;
                metricsVec->emplace_back(enjima::runtime::GetSystemTimeMicros());
                collector->Collect(LRBRecordT(LRBRecordT ::RecordType::kLatency,
                        enjima::runtime::GetSystemTime<Duration>(), metricsVec));
#else
                collector->Collect(
                        LRBRecordT(LRBRecordT::RecordType::kLatency, enjima::runtime::GetSystemTime<Duration>()));
#endif
                this->latencyRecordLastEmittedAt_ = currentTime;
                return true;
            }
            else if (writeIdx_.load(std::memory_order::acquire) > readIdx_.load(std::memory_order::acquire)) {
                auto readBeginIdx = readIdx_.load(std::memory_order::acquire) % srcReservoirCapacity_;
                auto nextRecord = eventReservoir_[readBeginIdx];
                readIdx_.fetch_add(1, std::memory_order::acq_rel);
                collector->Collect(nextRecord);
                return true;
            }
            return false;
        }

        uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer,
                enjima::core::OutputCollector* collector) override
        {
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            void* outBufferWritePtr = outputBuffer;
            uint32_t numLatencyRecordsOut = 0;
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
#if ENJIMA_METRICS_LEVEL >= 3
                auto metricsVec = new std::vector<uint64_t>;
                metricsVec->emplace_back(enjima::runtime::GetSystemTimeMicros());
                new (outBufferWritePtr) LRBRecordT(LRBRecordT ::RecordType::kLatency,
                        enjima::runtime::GetSystemTime<Duration>(), metricsVec);
#else
                new (outBufferWritePtr)
                        LRBRecordT(LRBRecordT ::RecordType::kLatency, enjima::runtime::GetSystemTime<Duration>());
#endif
                outBufferWritePtr = static_cast<LRBRecordT*>(outBufferWritePtr) + 1;
                this->latencyRecordLastEmittedAt_ = currentTime;
                numLatencyRecordsOut = 1;
            }

            auto numRecordsToCopy = std::min(maxRecordsToWrite - numLatencyRecordsOut,
                    static_cast<uint32_t>(
                            writeIdx_.load(std::memory_order::acquire) - readIdx_.load(std::memory_order::acquire)));
            if (numRecordsToCopy > 0) {
                auto recordSize = sizeof(LRBRecordT);
                auto currentCachedReadIdx = readIdx_.load(std::memory_order::acquire);
                auto readBeginIdx = currentCachedReadIdx % srcReservoirCapacity_;
                auto readEndIdx = readBeginIdx + numRecordsToCopy;
                auto srcBeginPtr = static_cast<void*>(&eventReservoir_[readBeginIdx]);
                if (readEndIdx > srcReservoirCapacity_) {
                    auto numRecordsToReadFromBeginning = readEndIdx - srcReservoirCapacity_;
                    auto numRecordsToReadFromEnd = numRecordsToCopy - numRecordsToReadFromBeginning;
                    std::memcpy(outBufferWritePtr, srcBeginPtr, numRecordsToReadFromEnd * recordSize);
                    // We have to circle back to read the rest of the events
                    srcBeginPtr = static_cast<void*>(eventReservoir_);
                    auto tempOutputBuffer =
                            static_cast<void*>(static_cast<LRBRecordT*>(outBufferWritePtr) + numRecordsToReadFromEnd);
                    std::memcpy(tempOutputBuffer, srcBeginPtr, numRecordsToReadFromBeginning * recordSize);
                }
                else {
                    std::memcpy(outBufferWritePtr, srcBeginPtr, numRecordsToCopy * recordSize);
                }
                // TODO Fix the extra memcpy
                readIdx_.store(currentCachedReadIdx + numRecordsToCopy, std::memory_order::release);
                collector->CollectBatch<LinearRoadT>(outputBuffer, numRecordsToCopy);
            }
            return numRecordsToCopy;
        }

        LinearRoadT GenerateQueueEvent() override
        {
            throw enjima::benchmarks::workload::WorkloadException{"Unsupported method called!"};
        }

        bool GenerateQueueRecord(core::Record<LinearRoadT>& outputRecord) override
        {
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
#if ENJIMA_METRICS_LEVEL >= 3
                auto metricsVec = new std::vector<uint64_t>;
                metricsVec->emplace_back(enjima::runtime::GetSystemTimeMicros());
                outputRecord = LRBRecordT(LRBRecordT ::RecordType::kLatency, enjima::runtime::GetSystemTime<Duration>(),
                        metricsVec);
#else
                outputRecord = LRBRecordT(LRBRecordT::RecordType::kLatency, enjima::runtime::GetSystemTime<Duration>());
#endif
                this->latencyRecordLastEmittedAt_ = currentTime;
                return true;
            }
            else if (writeIdx_.load(std::memory_order::acquire) > readIdx_.load(std::memory_order::acquire)) {
                auto readBeginIdx = readIdx_.load(std::memory_order::acquire) % srcReservoirCapacity_;
                outputRecord = eventReservoir_[readBeginIdx];
                readIdx_.fetch_add(1, std::memory_order::acq_rel);
                return true;
            }
            return false;
        }

        void PopulateEventCache(const std::string& lrbDataPath, std::vector<LinearRoadT>* eventCachePtr)
        {
            if (eventCachePtr == nullptr) {
                throw enjima::benchmarks::workload::WorkloadException{"Event cache ptr cannot be a nullptr!"};
            }
            eventCachePtr_ = eventCachePtr;
            csv2::Reader<csv2::delimiter<','>, csv2::quote_character<'"'>, csv2::first_row_is_header<false>> csv;
            if (eventCachePtr_->empty()) {
                if (csv.mmap(lrbDataPath)) {
                    auto startTime = runtime::GetSystemTimeMillis();
                    for (const auto row: csv) {
                        int fields[15];
                        int i = 0;
                        for (const auto cell: row) {
                            std::string value;
                            cell.read_value(value);
                            fields[i] = QuickAtoi(value.c_str());
                            i++;
                        }
                        eventCachePtr_->emplace_back(fields[0], fields[1] * 1000, fields[2], fields[3], fields[4],
                                fields[5], fields[6], fields[7], fields[8], fields[9], fields[10], fields[11],
                                fields[12], fields[13], fields[14]);
                    }
                    spdlog::info("Completed reading file {} in {} milliseconds", lrbDataPath,
                            runtime::GetSystemTimeMillis() - startTime);
                }
                else {
                    throw enjima::benchmarks::workload::WorkloadException{"Cannot read file " + lrbDataPath};
                }
            }
            maxTimestampFromFile_ = eventCachePtr_->back().GetTimestamp() + 1000;
            cacheIterator_ = eventCachePtr_->cbegin();
        }

        void GenerateEvents()
        {
            try {
                readyPromise_.get_future().wait();
                // Environment initialization
                this->pExecutionEngine_->PinThreadToCpuListFromConfig(enjima::runtime::SupportedThreadType::kEventGen);
                pthread_setname_np(pthread_self(),
                        std::string("event_gen_").append(std::to_string(this->GetOperatorId())).c_str());
                // End of environment initialization

                cacheIterator_ = eventCachePtr_->cbegin();
                auto blockSize = this->pMemoryManager_->GetDefaultNumEventsPerBlock();
                auto recordSize = sizeof(LRBRecordT);
                genWriteBufferBeginPtr_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, blockSize * recordSize);
                while (genTaskRunning_.load(std::memory_order::acquire)) {
                    SleepIfNeeded();
                    auto leftToEmitInMs = maxEmitRatePerMs_ - currentEmittedInMs_;
                    auto eventReservoirSize =
                            srcReservoirCapacity_ -
                            (writeIdx_.load(std::memory_order::acquire) - readIdx_.load(std::memory_order::acquire));
                    size_t numRecordsToWrite = std::min(leftToEmitInMs, std::min(eventReservoirSize, blockSize));
                    if (numRecordsToWrite > 0) {
                        auto* writeBuffer = static_cast<LRBRecordT*>(genWriteBufferBeginPtr_);
                        auto currentTime = enjima::runtime::GetSystemTimeMillis();
                        for (auto i = numRecordsToWrite; i > 0; i--) {
                            if (++cacheIterator_ == eventCachePtr_->cend()) {
                                cacheIterator_ = eventCachePtr_->cbegin();
                                baseTimestamp_ += maxTimestampFromFile_;
                            }
                            auto lrbEvent = *cacheIterator_.base();
                            lrbEvent.SetTimestamp(currentTime);
                            new (writeBuffer++) LRBRecordT(currentTime, lrbEvent);
                        }

                        auto currentCachedWriteIdx = writeIdx_.load(std::memory_order::acquire);
                        auto writeBeginIndex = currentCachedWriteIdx % srcReservoirCapacity_;
                        auto writeEndIdx = writeBeginIndex + numRecordsToWrite;
                        void* destBeginPtr = static_cast<void*>(&eventReservoir_[writeBeginIndex]);
                        if (writeEndIdx > srcReservoirCapacity_) {
                            auto numRecordsToWriteAtBeginning = writeEndIdx - srcReservoirCapacity_;
                            auto numRecordsToWriteAtEnd = numRecordsToWrite - numRecordsToWriteAtBeginning;
                            std::memcpy(destBeginPtr, genWriteBufferBeginPtr_, numRecordsToWriteAtEnd * recordSize);
                            // We have to circle back to write the rest of the events
                            auto currentGenWriteBufferPtr = static_cast<void*>(
                                    static_cast<LRBRecordT*>(genWriteBufferBeginPtr_) + numRecordsToWriteAtEnd);
                            destBeginPtr = static_cast<void*>(eventReservoir_);
                            std::memcpy(destBeginPtr, currentGenWriteBufferPtr,
                                    numRecordsToWriteAtBeginning * recordSize);
                        }
                        else {
                            std::memcpy(destBeginPtr, genWriteBufferBeginPtr_, numRecordsToWrite * recordSize);
                        }
                        currentEmittedInMs_ += numRecordsToWrite;
                        writeIdx_.store(currentCachedWriteIdx + numRecordsToWrite, std::memory_order::release);
                    }
                }
                free(genWriteBufferBeginPtr_);
            }
            catch (const std::exception& e) {
                spdlog::error("Exception raised by generator thread with operator ID {} : {}", this->GetOperatorId(),
                        e.what());
            }
        }

        void Initialize(enjima::runtime::ExecutionEngine* executionEngine, enjima::memory::MemoryManager* memoryManager,
                enjima::metrics::Profiler* profiler) override
        {
            enjima::operators::SourceOperator<LinearRoadT>::Initialize(executionEngine, memoryManager, profiler);
            readyPromise_.set_value();
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
            uint64_t currentTimeUs = enjima::runtime::GetSteadyClockMicros();
            if (currentEmittedInMs_ >= maxEmitRatePerMs_) {
                if (nextEmitStartUs_ > currentTimeUs) {
                    std::this_thread::sleep_until(
                            enjima::runtime::GetSteadyClockTimePoint<std::chrono::microseconds>(nextEmitStartUs_));
                    currentTimeUs = enjima::runtime::GetSteadyClockMicros();
                }
            }
            while (currentTimeUs >= nextEmitStartUs_) {
                nextEmitStartUs_ += 1000;
                currentEmittedInMs_ = 0;
            }
        }

        std::vector<LinearRoadT>* eventCachePtr_{nullptr};
        std::vector<LinearRoadT>::const_iterator cacheIterator_;
        uint64_t maxEmitRatePerMs_;
        uint64_t currentEmittedInMs_{0};
        uint64_t nextEmitStartUs_{0};

        // Generator member variables
        std::promise<void> readyPromise_;
        std::thread eventGenThread_{&InMemoryFixedRateLRBSourceOperatorWithProcLatency::GenerateEvents, this};
        std::atomic<bool> genTaskRunning_{true};
        std::atomic<size_t> readIdx_{0};
        std::atomic<size_t> writeIdx_{0};
        uint64_t srcReservoirCapacity_;
        LRBRecordT* eventReservoir_;
        void* genWriteBufferBeginPtr_{nullptr};
        uint64_t baseTimestamp_{0};
        uint64_t maxTimestampFromFile_{0};
    };
}// namespace enjima::workload::operators

#endif//ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_LRB_SOURCE_OPERATOR_WITH_PROC_LATENCY_H