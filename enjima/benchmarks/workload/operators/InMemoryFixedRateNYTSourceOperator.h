#ifndef ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_NYT_SOURCE_OPERATOR_H
#define ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_NYT_SOURCE_OPERATOR_H

#include "csv2/reader.hpp"
#include "enjima/api/data_types/NewYorkTaxiEvent.h"
#include "enjima/api/data_types/NewYorkTaxiEventCondensed.h"
#include "enjima/benchmarks/workload/WorkloadException.h"
#include "enjima/benchmarks/workload/functions/NYTUtilities.h"
#include "enjima/operators/LatencyTrackingSourceOperator.h"
#include "enjima/runtime/RuntimeTypes.h"
#include "spdlog/spdlog.h"

#include <fstream>
#include <sstream>
#include <string>

using NYTEventT = enjima::api::data_types::NewYorkTaxiEventCondensed;
using NYTRecordT = enjima::core::Record<NYTEventT>;
using NYTCsvReaderT = csv2::Reader<csv2::delimiter<','>, csv2::quote_character<'"'>, csv2::first_row_is_header<false>>;

namespace enjima::workload::operators {
    template<typename Duration>
    class InMemoryFixedRateNYTSourceOperator
        : public enjima::operators::LatencyTrackingSourceOperator<NYTEventT, Duration> {
    public:
        InMemoryFixedRateNYTSourceOperator(enjima::operators::OperatorID opId, const std::string& opName,
                uint64_t latencyRecordEmitPeriodMs, uint64_t maxInputRate, uint64_t srcReservoirCapacity)
            : enjima::operators::LatencyTrackingSourceOperator<NYTEventT, Duration>(opId, opName,
                      latencyRecordEmitPeriodMs),
              maxEmitRatePerMs_(maxInputRate / 1000), srcReservoirCapacity_(srcReservoirCapacity),
              eventReservoir_(static_cast<NYTRecordT*>(
                      aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, sizeof(NYTRecordT) * srcReservoirCapacity)))
        {
            nextEmitStartUs_ = enjima::runtime::GetSteadyClockMicros() + 1000;
        }

        ~InMemoryFixedRateNYTSourceOperator() override
        {
            genTaskRunning_.store(false, std::memory_order::release);
            eventGenThread_.join();
            free(static_cast<void*>(eventReservoir_));
        }

        bool EmitEvent(enjima::core::OutputCollector* collector) override
        {
            if (cachedWriteIdx_.load(std::memory_order::acquire) > cachedReadIdx_.load(std::memory_order::acquire)) {
                auto readBeginIdx = cachedReadIdx_.load(std::memory_order::acquire) % srcReservoirCapacity_;
                auto nextRecord = eventReservoir_[readBeginIdx];
                cachedReadIdx_.fetch_add(1, std::memory_order::acq_rel);
                collector->Collect(nextRecord);
                return true;
            }
            return false;
        }

        uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer,
                enjima::core::OutputCollector* collector) override
        {
            auto numRecordsToCopy =
                    std::min(maxRecordsToWrite, static_cast<uint32_t>(cachedWriteIdx_.load(std::memory_order::acquire) -
                                                                      cachedReadIdx_.load(std::memory_order::acquire)));
            if (numRecordsToCopy > 0) {
                auto recordSize = sizeof(NYTRecordT);
                auto currentCachedReadIdx = cachedReadIdx_.load(std::memory_order::acquire);
                auto readBeginIdx = currentCachedReadIdx % srcReservoirCapacity_;
                auto readEndIdx = readBeginIdx + numRecordsToCopy;
                auto srcBeginPtr = static_cast<void*>(&eventReservoir_[readBeginIdx]);
                if (readEndIdx > srcReservoirCapacity_) {
                    auto numRecordsToReadFromBeginning = readEndIdx - srcReservoirCapacity_;
                    auto numRecordsToReadFromEnd = numRecordsToCopy - numRecordsToReadFromBeginning;
                    std::memcpy(outputBuffer, srcBeginPtr, numRecordsToReadFromEnd * recordSize);
                    // We have to circle back to read the rest of the events
                    srcBeginPtr = static_cast<void*>(eventReservoir_);
                    auto tempOutputBuffer =
                            static_cast<void*>(static_cast<NYTRecordT*>(outputBuffer) + numRecordsToReadFromEnd);
                    std::memcpy(tempOutputBuffer, srcBeginPtr, numRecordsToReadFromBeginning * recordSize);
                }
                else {
                    std::memcpy(outputBuffer, srcBeginPtr, numRecordsToCopy * recordSize);
                }
                cachedReadIdx_.store(currentCachedReadIdx + numRecordsToCopy, std::memory_order::release);
                // TODO Fix the extra memcpy
                collector->CollectBatch<NYTEventT>(outputBuffer, numRecordsToCopy);
            }
            return numRecordsToCopy;
        }

        NYTEventT GenerateQueueEvent() override
        {
            throw enjima::benchmarks::workload::WorkloadException{"Unsupported method called!"};
        }

        bool GenerateQueueRecord([[maybe_unused]] core::Record<NYTEventT>& outputRecord) override
        {
            throw enjima::benchmarks::workload::WorkloadException{"Unsupported method called!"};
        }

        void PopulateEventCache(const std::string& nytDataPath, std::vector<NYTEventT>* eventCachePtr)
        {
            if (eventCachePtr == nullptr) {
                throw enjima::benchmarks::workload::WorkloadException{"Event cache ptr cannot be a nullptr!"};
            }
            eventCachePtr_ = eventCachePtr;
            eventCachePtr_->reserve(131'072);
            if (eventCachePtr_->empty()) {
                auto startTime = runtime::GetSystemTimeMillis();
                NYTCsvReaderT csv;
                if (csv.mmap(nytDataPath)) {
                    // std::cout << "Num rows in file : " << csv.rows() << std::endl;
                    // std::cout << "Num rows in file (ignore_empty_lines) : " << csv.rows(true) << std::endl;
                    // assert(csv.rows(true) == csv.rows());
                    NYTRowReader cellReader{eventCachePtr_};
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
                auto recordSize = sizeof(NYTRecordT);
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
                    auto eventReservoirSize =
                            srcReservoirCapacity_ - (cachedWriteIdx_.load(std::memory_order::acquire) -
                                                            cachedReadIdx_.load(std::memory_order::acquire));
                    numRecordsToWrite = std::min(leftToEmitInMs, std::min(eventReservoirSize, blockSize));
                    if (numRecordsToWrite > 0) {
                        auto* writeBuffer = static_cast<NYTRecordT*>(genWriteBufferBeginPtr_);
                        size_t numLatencyRecordsWritten = 0;
                        if (this->emitLatencyMarker_) {
#if ENJIMA_METRICS_LEVEL >= 3
                            auto metricsVec = new std::vector<uint64_t>;
                            metricsVec->emplace_back(enjima::runtime::GetSystemTimeMicros());
                            new (writeBuffer++)
                                    NYTRecordT(NYTRecordT ::RecordType::kLatency, this->latencyMarkerTime_, metricsVec);
#else
                            new (writeBuffer++) NYTRecordT(NYTRecordT::RecordType::kLatency, this->latencyMarkerTime_);
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
                            new (writeBuffer++) NYTRecordT(currentTime, nytEvent);
                        }

                        auto currentCachedWriteIdx = cachedWriteIdx_.load(std::memory_order::acquire);
                        auto writeBeginIndex = currentCachedWriteIdx % srcReservoirCapacity_;
                        auto writeEndIdx = writeBeginIndex + numRecordsToWrite;
                        void* destBeginPtr = static_cast<void*>(&eventReservoir_[writeBeginIndex]);
                        if (writeEndIdx > srcReservoirCapacity_) {
                            auto numRecordsToWriteAtBeginning = writeEndIdx - srcReservoirCapacity_;
                            auto numRecordsToWriteAtEnd = numRecordsToWrite - numRecordsToWriteAtBeginning;
                            std::memcpy(destBeginPtr, genWriteBufferBeginPtr_, numRecordsToWriteAtEnd * recordSize);
                            // We have to circle back to write the rest of the events
                            auto currentGenWriteBufferPtr = static_cast<void*>(
                                    static_cast<NYTRecordT*>(genWriteBufferBeginPtr_) + numRecordsToWriteAtEnd);
                            destBeginPtr = static_cast<void*>(eventReservoir_);
                            std::memcpy(destBeginPtr, currentGenWriteBufferPtr,
                                    numRecordsToWriteAtBeginning * recordSize);
                        }
                        else {
                            std::memcpy(destBeginPtr, genWriteBufferBeginPtr_, numRecordsToWrite * recordSize);
                        }
                        currentEmittedInMs_ += numRecordsToWrite;
                        cachedWriteIdx_.store(currentCachedWriteIdx + numRecordsToWrite, std::memory_order::release);
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
            enjima::operators::SourceOperator<NYTEventT>::Initialize(executionEngine, memoryManager, profiler);
            readyPromise_.set_value();
        }

    private:
        class NYTRowReader {
        public:
            explicit NYTRowReader(std::vector<NYTEventT>* eventCachePtr, char delim = ',')
                : delim_(delim), rowReaderEventCachePtr_(eventCachePtr)
            {
            }

            void ReadFromRow(const NYTCsvReaderT::Row& row)
            {
                assert(row.length() > 104 && row.length() < 300);
                rowStr_.clear();
                row.read_raw_value(rowStr_);
                auto rowIter = rowStr_.begin();
                CustomReadCell(rowStr_, medallion_, rowIter);
                CustomReadCell(rowStr_, ignore_, rowIter);
                CustomReadCell(rowStr_, puDateTime_, rowIter);
                CustomReadCell(rowStr_, doDateTime_, rowIter);
                CustomReadCell(rowStr_, ignore_, rowIter);
                CustomReadCell(rowStr_, ignore_, rowIter);
                CustomReadCell(rowStr_, puLonStr_, rowIter);
                CustomReadCell(rowStr_, puLatStr_, rowIter);
                CustomReadCell(rowStr_, doLonStr_, rowIter);
                CustomReadCell(rowStr_, doLatStr_, rowIter);
                CustomReadCell(rowStr_, ignore_, rowIter);
                CustomReadCell(rowStr_, fareAmountStr_, rowIter);
                CustomReadCell(rowStr_, ignore_, rowIter);
                CustomReadCell(rowStr_, ignore_, rowIter);
                CustomReadCell(rowStr_, tipAmountStr_, rowIter);
                assert(medallion_.size() == 32);
                assert(puDateTime_.size() == 19);
                assert(doDateTime_.size() == 19);
                auto puId = areaMapper_.GetCellID(CustomStrToDouble(puLatStr_), CustomStrToDouble(puLonStr_));
                auto doId = areaMapper_.GetCellID(CustomStrToDouble(doLatStr_), CustomStrToDouble(doLonStr_));
                if (puId.first == -1 || puId.second == -1 || doId.first == -1 || doId.second == -1) {
                    return;
                }
                auto currentTime = enjima::runtime::GetSystemTimeMillis();
                rowReaderEventCachePtr_->emplace_back(medallion_, puDateTime_, doDateTime_, puId.first, puId.second,
                        doId.first, doId.second, CustomStrToDouble(fareAmountStr_), CustomStrToDouble(tipAmountStr_),
                        currentTime);
            }

            void CustomReadCell(const std::string& rowStr, std::string& outStr, std::string::iterator& iter)
            {
                outStr.clear();
                auto rowEnd = rowStr.end();
                const auto begin = iter;
                while (iter != rowEnd && *iter != delim_) {
                    ++iter;
                }
                outStr.append(begin, iter);
                if (iter != rowEnd) {
                    ++iter;
                }
            }

        private:
            const char delim_;
            benchmarks::workload::AreaMapper areaMapper_{0.25};
            std::vector<NYTEventT>* rowReaderEventCachePtr_{nullptr};
            std::string rowStr_;
            std::string medallion_;
            std::string puDateTime_;
            std::string doDateTime_;
            std::string puLatStr_;
            std::string puLonStr_;
            std::string doLatStr_;
            std::string doLonStr_;
            std::string fareAmountStr_;
            std::string tipAmountStr_;
            std::string ignore_;
        };

        static double CustomStrToDouble(const std::string& doubleStr)
        {
            const char* begin = doubleStr.data();
            const char* end = begin + doubleStr.size();
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

        std::vector<NYTEventT>* eventCachePtr_{nullptr};
        std::vector<NYTEventT>::const_iterator cacheIterator_;
        uint64_t maxEmitRatePerMs_;
        uint64_t currentEmittedInMs_{0};
        uint64_t nextEmitStartUs_{0};

        // Generator member variables
        std::promise<void> readyPromise_;
        std::thread eventGenThread_{&InMemoryFixedRateNYTSourceOperator::GenerateEvents, this};
        std::atomic<bool> genTaskRunning_{true};
        std::atomic<size_t> cachedReadIdx_{0};
        std::atomic<size_t> cachedWriteIdx_{0};
        uint64_t srcReservoirCapacity_;
        NYTRecordT* eventReservoir_;
        void* genWriteBufferBeginPtr_{nullptr};
    };
}// namespace enjima::workload::operators
#endif//ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_NYT_SOURCE_OPERATOR_H