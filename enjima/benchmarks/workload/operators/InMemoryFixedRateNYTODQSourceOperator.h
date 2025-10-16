#ifndef ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_NYT_ODQ_SOURCE_OPERATOR_H
#define ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_NYT_ODQ_SOURCE_OPERATOR_H

#include "csv2/reader.hpp"
#include "enjima/api/data_types/NewYorkTaxiEventODQ.h"
#include "enjima/benchmarks/workload/WorkloadException.h"
#include "enjima/benchmarks/workload/functions/NYTUtilities.h"
#include "enjima/operators/LatencyTrackingSourceOperator.h"
#include "enjima/runtime/RuntimeTypes.h"
#include "spdlog/spdlog.h"

#include <fstream>
#include <sstream>
#include <string>

using NYTODQEventT = enjima::api::data_types::NewYorkTaxiEventODQ;
using NYTODQRecordT = enjima::core::Record<NYTODQEventT>;
using NYTCsvReaderT = csv2::Reader<csv2::delimiter<','>, csv2::quote_character<'"'>, csv2::first_row_is_header<false>>;

namespace enjima::workload::operators {
    template<typename Duration>
    class InMemoryFixedRateNYTODQSourceOperator
        : public enjima::operators::LatencyTrackingSourceOperator<NYTODQEventT, Duration> {
    public:
        InMemoryFixedRateNYTODQSourceOperator(enjima::operators::OperatorID opId, const std::string& opName,
                uint64_t latencyRecordEmitPeriodMs, uint64_t maxInputRate, uint64_t srcReservoirCapacity,
                uint64_t minInputRate = 0, const std::string& stepSizeStr = "0", uint64_t shiftInterval = 0)
            : enjima::operators::LatencyTrackingSourceOperator<NYTODQEventT, Duration>(opId, opName,
                      latencyRecordEmitPeriodMs),
              minEmitRatePerMs_(minInputRate / 1000), maxEmitRatePerMs_(maxInputRate / 1000),
              shiftIntervalMs_(shiftInterval), currentMaxEmitRatePerMs_(maxInputRate / 1000),
              srcReservoirCapacity_(srcReservoirCapacity),
              eventReservoir_(static_cast<NYTODQRecordT*>(
                      aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, sizeof(NYTODQRecordT) * srcReservoirCapacity)))
        {
            if (size_t tokEnd = stepSizeStr.find(delim_); tokEnd != std::string::npos) {
                size_t tokStart = 0;
                stepSize_ = 0;
                while (tokEnd != std::string::npos) {
                    const auto currentStepSize = std::stol(stepSizeStr.substr(tokStart, tokEnd - tokStart));
                    stepSizeQ_.push(currentStepSize / 1000);
                    tokStart = tokEnd + 1;
                    tokEnd = stepSizeStr.find(delim_, tokStart);
                }
                auto lastStepSize = std::stol(stepSizeStr.substr(tokStart));
                stepSizeQ_.push(lastStepSize / 1000);
            }
            else {
                stepSize_ = std::stoul(stepSizeStr) / 1000;
            }
            nextEmitStartUs_ = enjima::runtime::GetSteadyClockMicros() + 1000;
        }

        ~InMemoryFixedRateNYTODQSourceOperator() override
        {
            genTaskRunning_.store(false, std::memory_order::release);
            if (eventGenThread_.joinable()) {
                eventGenThread_.join();
            }
            free(static_cast<void*>(eventReservoir_));
        }

        void CancelEventGeneration()
        {
            genTaskRunning_.store(false, std::memory_order::release);
            if (eventGenThread_.joinable()) {
                eventGenThread_.join();
            }
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
            auto eventReservoirSize =
                    writeIdx_.load(std::memory_order::acquire) - readIdx_.load(std::memory_order::acquire);
            auto numRecordsToCopy = std::min(maxRecordsToWrite, static_cast<uint32_t>(eventReservoirSize));
            if (numRecordsToCopy > 0) {
                auto currentCachedReadIdx = readIdx_.load(std::memory_order::relaxed);
                auto readBeginIdx = currentCachedReadIdx % srcReservoirCapacity_;
                auto readEndIdx = readBeginIdx + numRecordsToCopy;
                auto srcBeginPtr = static_cast<void*>(&eventReservoir_[readBeginIdx]);
                if (readEndIdx > srcReservoirCapacity_) {
                    auto numRecordsToReadFromBeginning = readEndIdx - srcReservoirCapacity_;
                    auto numRecordsToReadFromEnd = numRecordsToCopy - numRecordsToReadFromBeginning;
                    collector->CollectBatch<NYTODQEventT>(srcBeginPtr, numRecordsToReadFromEnd);
                    // We have to circle back to read the rest of the events
                    srcBeginPtr = static_cast<void*>(eventReservoir_);
                    collector->CollectBatch<NYTODQEventT>(srcBeginPtr, numRecordsToReadFromBeginning);
                }
                else {
                    collector->CollectBatch<NYTODQEventT>(srcBeginPtr, numRecordsToCopy);
                }
                readIdx_.store(currentCachedReadIdx + numRecordsToCopy, std::memory_order::release);
            }
            return numRecordsToCopy;
        }

        NYTODQEventT GenerateQueueEvent() override
        {
            throw enjima::benchmarks::workload::WorkloadException{"Unsupported method called!"};
        }

        bool GenerateQueueRecord(core::Record<NYTODQEventT>& outputRecord) override
        {
            if (writeIdx_.load(std::memory_order::acquire) > readIdx_.load(std::memory_order::acquire)) {
                auto readBeginIdx = readIdx_.load(std::memory_order::acquire) % srcReservoirCapacity_;
                outputRecord = eventReservoir_[readBeginIdx];
                readIdx_.fetch_add(1, std::memory_order::acq_rel);
                return true;
            }
            return false;
        }

        void PopulateEventCache(const std::string& nytDataPath, std::vector<NYTODQEventT>* eventCachePtr)
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
                    benchmarks::workload::NYTOptimizedDQRowReader cellReader{eventCachePtr_};
                    for (const auto& row: csv) {
                        if (row.length() > 0) {
                            cellReader.ReadFromRow(row);
                        }
                    }
                    spdlog::info("Read {} records with invalid location.", cellReader.GetInvalidLocRecordsCount());
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
                SetWorkloadType();
                lastInputRateChangeAtMs_ = enjima::runtime::GetSystemTimeMillis();
                // End of environment initialization

                cacheIterator_ = eventCachePtr_->cbegin();
                auto blockSize = this->pMemoryManager_->GetDefaultNumEventsPerBlock();
                auto recordSize = sizeof(NYTODQRecordT);
                genWriteBufferBeginPtr_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, blockSize * recordSize);
                size_t numRecordsToWrite;
                while (genTaskRunning_.load(std::memory_order::acquire)) {
                    SleepIfNeeded();
                    auto currentTimeMillis = enjima::runtime::GetSystemTimeMillis();
                    auto leftToEmitInMs = maxEmitRatePerMs_ - currentEmittedInMs_;
                    if (leftToEmitInMs > 0 && !this->emitLatencyMarker_ &&
                            (currentTimeMillis >=
                                    (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_))) {
                        this->latencyMarkerTime_ = enjima::runtime::GetSystemTime<Duration>();
                        this->emitLatencyMarker_ = true;
                    }
                    auto eventReservoirRemaining =
                            srcReservoirCapacity_ -
                            (writeIdx_.load(std::memory_order::relaxed) - readIdx_.load(std::memory_order::acquire));
                    numRecordsToWrite = std::min(leftToEmitInMs, std::min(eventReservoirRemaining, blockSize));
                    if (numRecordsToWrite > 0) {
                        auto* writeBuffer = static_cast<NYTODQRecordT*>(genWriteBufferBeginPtr_);
                        size_t numLatencyRecordsWritten = 0;
                        if (this->emitLatencyMarker_) {
#if ENJIMA_METRICS_LEVEL >= 3
                            auto metricsVec = new std::vector<uint64_t>;
                            metricsVec->emplace_back(enjima::runtime::GetSystemTimeMicros());
                            new (writeBuffer++) NYTODQRecordT(NYTODQRecordT ::RecordType::kLatency,
                                    this->latencyMarkerTime_, metricsVec);
#else
                            new (writeBuffer++)
                                    NYTODQRecordT(NYTODQRecordT::RecordType::kLatency, this->latencyMarkerTime_);
#endif
                            this->latencyRecordLastEmittedAt_ = currentTimeMillis;
                            this->emitLatencyMarker_ = false;
                            numLatencyRecordsWritten = 1;
                        }
                        for (auto i = (numRecordsToWrite - numLatencyRecordsWritten); i > 0; i--) {
                            if (++cacheIterator_ == eventCachePtr_->cend()) {
                                cacheIterator_ = eventCachePtr_->cbegin();
                            }
                            auto nytEvent = *cacheIterator_.base();
                            new (writeBuffer++) NYTODQRecordT(currentTimeMillis, nytEvent);
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
                                    static_cast<NYTODQRecordT*>(genWriteBufferBeginPtr_) + numRecordsToWriteAtEnd);
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
                    if ((workloadType_ != InMemoryFixedRateNYTODQSourceOperator<Duration>::kConstantRate_) &&
                            (currentMaxEmitRatePerMs_ < maxEmitRatePerMs_)) {
                        UpdateInputRateIfNeeded(currentTimeMillis);
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
            enjima::operators::SourceOperator<NYTODQEventT>::Initialize(executionEngine, memoryManager, profiler);
            readyPromise_.set_value();
        }

    private:
        void SleepIfNeeded()
        {
            uint64_t currentTimeUs = enjima::runtime::GetSteadyClockMicros();
            if (currentEmittedInMs_ >= currentMaxEmitRatePerMs_) {
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

        void SetWorkloadType()
        {
            if ((minEmitRatePerMs_ > 0) && (minEmitRatePerMs_ < maxEmitRatePerMs_) && (shiftIntervalMs_ > 0)) {
                if (stepSize_ > 0) {
                    workloadType_ = InMemoryFixedRateNYTODQSourceOperator<Duration>::kStepIncrease_;
                    currentMaxEmitRatePerMs_ = minEmitRatePerMs_;
                    spdlog::info("Generating events with a step increase of {} e/ms every {} ms starting from {} e/ms "
                                 "upto {} e/ms",
                            stepSize_, shiftIntervalMs_, minEmitRatePerMs_, maxEmitRatePerMs_);
                }
                else if (!stepSizeQ_.empty()) {
                    workloadType_ = InMemoryFixedRateNYTODQSourceOperator<Duration>::kUserDefinedRateChange_;
                    currentMaxEmitRatePerMs_ = minEmitRatePerMs_;
                    spdlog::info("Generating events with a user-defined rate change every {} ms starting from {} e/ms "
                                 "upto {} e/ms",
                            shiftIntervalMs_, minEmitRatePerMs_, maxEmitRatePerMs_);
                }
                else {
                    workloadType_ =
                            InMemoryFixedRateNYTODQSourceOperator<Duration>::kUnpredictableRateVariationsInRange_;
                    spdlog::info("Generating events with random rate variations every {} ms upto a rate of {} e/ms",
                            shiftIntervalMs_, maxEmitRatePerMs_);
                }
                return;
            }
            spdlog::info("Generating events with a constant rate of {} e/ms", maxEmitRatePerMs_);
        }

        void UpdateInputRateIfNeeded(uint64_t currentTimeMillis)
        {
            if (currentTimeMillis > (lastInputRateChangeAtMs_ + shiftIntervalMs_)) {
                if (workloadType_ == InMemoryFixedRateNYTODQSourceOperator<Duration>::kStepIncrease_) {
                    currentMaxEmitRatePerMs_ = std::min(currentMaxEmitRatePerMs_ + stepSize_, maxEmitRatePerMs_);
                    spdlog::debug("Setting input rate to {} e/ms", currentMaxEmitRatePerMs_);
                }
                else if (workloadType_ == InMemoryFixedRateNYTODQSourceOperator<Duration>::kUserDefinedRateChange_) {
                    if (!stepSizeQ_.empty()) {
                        auto nextChangeInRate = stepSizeQ_.front();
                        stepSizeQ_.pop();
                        currentMaxEmitRatePerMs_ =
                                std::min(currentMaxEmitRatePerMs_ + nextChangeInRate, maxEmitRatePerMs_);
                        spdlog::debug("Setting input rate to {} e/ms", currentMaxEmitRatePerMs_);
                    }
                }
                else {
                    // We do not support kUnpredictableRateVariationsInRange_. Ignore
                }
                lastInputRateChangeAtMs_ = currentTimeMillis;
            }
        }

        const char delim_ = '_';

        const uint64_t minEmitRatePerMs_;
        const uint64_t maxEmitRatePerMs_;
        const uint64_t shiftIntervalMs_;

        std::vector<NYTODQEventT>* eventCachePtr_{nullptr};
        std::vector<NYTODQEventT>::const_iterator cacheIterator_;
        uint64_t currentMaxEmitRatePerMs_;
        uint64_t stepSize_;
        std::queue<int64_t> stepSizeQ_;
        uint64_t lastInputRateChangeAtMs_{0};
        uint64_t currentEmittedInMs_{0};
        uint64_t nextEmitStartUs_{0};
        uint8_t workloadType_{InMemoryFixedRateNYTODQSourceOperator<Duration>::kConstantRate_};

        // Generator member variables
        std::promise<void> readyPromise_;
        std::thread eventGenThread_{&InMemoryFixedRateNYTODQSourceOperator::GenerateEvents, this};
        std::atomic<bool> genTaskRunning_{true};
        std::atomic<size_t> readIdx_{0};
        std::atomic<size_t> writeIdx_{0};
        uint64_t srcReservoirCapacity_;
        NYTODQRecordT* eventReservoir_;
        void* genWriteBufferBeginPtr_{nullptr};
    };
}// namespace enjima::workload::operators

#endif//ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_NYT_ODQ_SOURCE_OPERATOR_H