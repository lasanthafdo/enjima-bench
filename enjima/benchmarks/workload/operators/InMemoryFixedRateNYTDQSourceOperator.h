#ifndef ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_NYT_DQ_SOURCE_OPERATOR_H
#define ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_NYT_DQ_SOURCE_OPERATOR_H

#include "csv2/reader.hpp"
#include "enjima/api/data_types/NewYorkTaxiEvent.h"
#include "enjima/benchmarks/workload/WorkloadException.h"
#include "enjima/benchmarks/workload/functions/NYTUtilities.h"
#include "enjima/operators/LatencyTrackingSourceOperator.h"
#include "enjima/runtime/RuntimeTypes.h"
#include "spdlog/spdlog.h"

#include <fstream>
#include <sstream>
#include <string>

using NYTFullEventT = enjima::api::data_types::NewYorkTaxiEvent;
using NYTFullRecordT = enjima::core::Record<NYTFullEventT>;
using NYTCsvReaderT = csv2::Reader<csv2::delimiter<','>, csv2::quote_character<'"'>, csv2::first_row_is_header<false>>;
using namespace enjima::benchmarks::workload;

namespace enjima::workload::operators {
    template<typename Duration>
    class InMemoryFixedRateNYTDQSourceOperator
        : public enjima::operators::LatencyTrackingSourceOperator<NYTFullEventT, Duration> {
    public:
        InMemoryFixedRateNYTDQSourceOperator(enjima::operators::OperatorID opId, const std::string& opName,
                uint64_t latencyRecordEmitPeriodMs, uint64_t maxInputRate, uint64_t srcReservoirCapacity)
            : enjima::operators::LatencyTrackingSourceOperator<NYTFullEventT, Duration>(opId, opName,
                      latencyRecordEmitPeriodMs),
              maxEmitRatePerMs_(maxInputRate / 1000), srcReservoirCapacity_(srcReservoirCapacity),
              eventReservoir_(static_cast<NYTFullRecordT*>(
                      aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, sizeof(NYTFullRecordT) * srcReservoirCapacity)))
        {
            nextEmitStartUs_ = enjima::runtime::GetSteadyClockMicros() + 1000;
        }

        ~InMemoryFixedRateNYTDQSourceOperator() override
        {
            genTaskRunning_.store(false, std::memory_order::release);
            eventGenThread_.join();
            free(static_cast<void*>(eventReservoir_));
        }

        bool EmitEvent(enjima::core::OutputCollector* collector) override
        {
            if (writeIdx_.load(std::memory_order::acquire) > readIdx_.load(std::memory_order::acquire)) {
                auto readBeginIdx = readIdx_.load(std::memory_order::acquire) % srcReservoirCapacity_;
                auto nextRecord = eventReservoir_[readBeginIdx];
                readIdx_.fetch_add(1, std::memory_order::acq_rel);
                collector->Collect(nextRecord);
                return true;
            }
            return false;
        }

        uint32_t EmitBatch(uint32_t maxRecordsToWrite, [[maybe_unused]] void* outputBuffer,
                enjima::core::OutputCollector* collector) override
        {
            // Note: Changing the memory order from acquire to relaxed has a significant CPU usage difference.
            // Acquire/release puts a memory fence that most likely waits for the store buffer to drain,
            // but apparently better for performance
            auto eventReservoirSize = cachedWriteIdx_ - readIdx_.load(std::memory_order::acquire);
            if (eventReservoirSize == 0) {
                cachedWriteIdx_ = writeIdx_.load(std::memory_order::acquire);
                eventReservoirSize = cachedWriteIdx_ - readIdx_.load(std::memory_order::relaxed);
            }
            auto numRecordsToCopy = std::min(maxRecordsToWrite, static_cast<uint32_t>(eventReservoirSize));
            if (numRecordsToCopy > 0) {
                auto currentCachedReadIdx = readIdx_.load(std::memory_order::relaxed);
                auto readBeginIdx = currentCachedReadIdx % srcReservoirCapacity_;
                auto readEndIdx = readBeginIdx + numRecordsToCopy;
                auto srcBeginPtr = static_cast<void*>(&eventReservoir_[readBeginIdx]);
                if (readEndIdx > srcReservoirCapacity_) {
                    auto numRecordsToReadFromBeginning = readEndIdx - srcReservoirCapacity_;
                    auto numRecordsToReadFromEnd = numRecordsToCopy - numRecordsToReadFromBeginning;
                    collector->CollectBatch<NYTFullEventT>(srcBeginPtr, numRecordsToReadFromEnd);
                    // We have to circle back to read the rest of the events
                    srcBeginPtr = static_cast<void*>(eventReservoir_);
                    collector->CollectBatch<NYTFullEventT>(srcBeginPtr, numRecordsToReadFromBeginning);
                }
                else {
                    collector->CollectBatch<NYTFullEventT>(srcBeginPtr, numRecordsToCopy);
                }
                readIdx_.store(currentCachedReadIdx + numRecordsToCopy, std::memory_order::release);
            }
            return numRecordsToCopy;
        }

        NYTFullEventT GenerateQueueEvent() override
        {
            throw enjima::benchmarks::workload::WorkloadException{"Unsupported method called!"};
        }

        bool GenerateQueueRecord([[maybe_unused]] core::Record<NYTFullEventT>& outputRecord) override
        {
            throw enjima::benchmarks::workload::WorkloadException{"Unsupported method called!"};
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
                auto recordSize = sizeof(NYTFullRecordT);
                genWriteBufferBeginPtr_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, blockSize * recordSize);
                size_t numRecordsToWrite;
                while (genTaskRunning_.load(std::memory_order::acquire)) {
                    SleepIfNeeded();
                    auto currentTime = enjima::runtime::GetSystemTimeMillis();
                    auto leftToEmitInMs = maxEmitRatePerMs_ - currentEmittedInMs_;
                    if (leftToEmitInMs > 0 && !this->emitLatencyMarker_ &&
                            (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_))) {
                        this->latencyMarkerTime_ = enjima::runtime::GetSystemTime<Duration>();
                        this->emitLatencyMarker_ = true;
                    }
                    auto eventReservoirRemaining =
                            srcReservoirCapacity_ - (writeIdx_.load(std::memory_order::relaxed) - cachedReadIdx_);
                    if (eventReservoirRemaining == 0) {
                        cachedReadIdx_ = readIdx_.load(std::memory_order::acquire);
                        eventReservoirRemaining =
                                srcReservoirCapacity_ - (writeIdx_.load(std::memory_order::relaxed) - cachedReadIdx_);
                    }
                    numRecordsToWrite = std::min(leftToEmitInMs, std::min(eventReservoirRemaining, blockSize));
                    if (numRecordsToWrite > 0) {
                        auto* writeBuffer = static_cast<NYTFullRecordT*>(genWriteBufferBeginPtr_);
                        size_t numLatencyRecordsWritten = 0;
                        if (this->emitLatencyMarker_) {
#if ENJIMA_METRICS_LEVEL >= 3
                            auto metricsVec = new std::vector<uint64_t>;
                            metricsVec->emplace_back(enjima::runtime::GetSystemTimeMicros());
                            new (writeBuffer++) NYTFullRecordT(NYTFullRecordT ::RecordType::kLatency,
                                    this->latencyMarkerTime_, metricsVec);
#else
                            new (writeBuffer++)
                                    NYTFullRecordT(NYTFullRecordT::RecordType::kLatency, this->latencyMarkerTime_);
#endif
                            this->latencyRecordLastEmittedAt_ = currentTime;
                            this->emitLatencyMarker_ = false;
                            numLatencyRecordsWritten = 1;
                        }
                        for (auto i = (numRecordsToWrite - numLatencyRecordsWritten); i > 0; i--) {
                            if (++cacheIterator_ == eventCachePtr_->cend()) {
                                cacheIterator_ = eventCachePtr_->cbegin();
                            }
                            auto nytEvent = *cacheIterator_.base();
                            new (writeBuffer++) NYTFullRecordT(currentTime, nytEvent);
                        }

                        auto currentCachedWriteIdx = writeIdx_.load(std::memory_order::relaxed);
                        auto writeBeginIndex = currentCachedWriteIdx % srcReservoirCapacity_;
                        auto writeEndIdx = writeBeginIndex + numRecordsToWrite;
                        void* destBeginPtr = static_cast<void*>(&eventReservoir_[writeBeginIndex]);
                        if (writeEndIdx > srcReservoirCapacity_) {
                            auto numRecordsToWriteAtBeginning = writeEndIdx - srcReservoirCapacity_;
                            auto numRecordsToWriteAtEnd = numRecordsToWrite - numRecordsToWriteAtBeginning;
                            std::memcpy(destBeginPtr, genWriteBufferBeginPtr_, numRecordsToWriteAtEnd * recordSize);
                            // We have to circle back to write the rest of the events
                            auto currentGenWriteBufferPtr = static_cast<void*>(
                                    static_cast<NYTFullRecordT*>(genWriteBufferBeginPtr_) + numRecordsToWriteAtEnd);
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
            enjima::operators::SourceOperator<NYTFullEventT>::Initialize(executionEngine, memoryManager, profiler);
            readyPromise_.set_value();
        }

    private:
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

        std::vector<NYTFullEventT>* eventCachePtr_{nullptr};
        std::vector<NYTFullEventT>::const_iterator cacheIterator_;
        uint64_t maxEmitRatePerMs_;
        uint64_t currentEmittedInMs_{0};
        uint64_t nextEmitStartUs_{0};

        // Generator member variables
        std::promise<void> readyPromise_;
        std::thread eventGenThread_{&InMemoryFixedRateNYTDQSourceOperator::GenerateEvents, this};
        std::atomic<bool> genTaskRunning_{true};
        std::atomic<size_t> readIdx_{0};
        size_t cachedReadIdx_{0};
        std::atomic<size_t> writeIdx_{0};
        size_t cachedWriteIdx_{0};
        uint64_t srcReservoirCapacity_;
        NYTFullRecordT* eventReservoir_;
        void* genWriteBufferBeginPtr_{nullptr};
    };
}// namespace enjima::workload::operators
#endif//ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_NYT_SOURCE_OPERATOR_H