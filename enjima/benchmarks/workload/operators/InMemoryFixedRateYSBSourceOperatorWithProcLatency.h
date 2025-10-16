//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_YSB_SOURCE_OPERATOR_WITH_PROC_LATENCY_H
#define ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_YSB_SOURCE_OPERATOR_WITH_PROC_LATENCY_H


#include "enjima/api/data_types/YSBAdEvent.h"
#include "enjima/benchmarks/workload/WorkloadException.h"
#include "enjima/operators/LatencyTrackingSourceOperator.h"
#include "enjima/runtime/RuntimeTypes.h"
#include "spdlog/spdlog.h"

#include <random>

using YSBAdT = enjima::api::data_types::YSBAdEvent;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;
using YSBAdRecordT = enjima::core::Record<YSBAdT>;

namespace enjima::workload::operators {
    template<typename Duration>
    class InMemoryFixedRateYSBSourceOperatorWithProcLatency
        : public enjima::operators::LatencyTrackingSourceOperator<YSBAdT, Duration> {
    public:
        InMemoryFixedRateYSBSourceOperatorWithProcLatency(enjima::operators::OperatorID opId, const std::string& opName,
                uint64_t latencyRecordEmitPeriodMs, uint64_t maxInputRate, uint64_t srcReservoirCapacity)
            : enjima::operators::LatencyTrackingSourceOperator<YSBAdT, Duration>(opId, opName,
                      latencyRecordEmitPeriodMs),
              maxEmitRatePerMs_(maxInputRate / 1000), srcReservoirCapacity_(srcReservoirCapacity),
              eventReservoir_(static_cast<YSBAdRecordT*>(
                      aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, sizeof(YSBAdRecordT) * srcReservoirCapacity)))
        {
            nextEmitStartUs_ = runtime::GetSteadyClockMicros() + 1000;
        }

        ~InMemoryFixedRateYSBSourceOperatorWithProcLatency() override
        {
            genTaskRunning_.store(false, std::memory_order::release);
            eventGenThread_.join();
            free(static_cast<void*>(eventReservoir_));
        }

        bool EmitEvent(core::OutputCollector* collector) override
        {
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
#if ENJIMA_METRICS_LEVEL >= 3
                auto metricsVec = new std::vector<uint64_t>;
                metricsVec->emplace_back(enjima::runtime::GetSystemTimeMicros());
                collector->Collect(YSBAdRecordT(YSBAdRecordT::RecordType::kLatency,
                        enjima::runtime::GetSystemTime<Duration>(), metricsVec));
#else
                collector->Collect(
                        YSBAdRecordT(YSBAdRecordT ::RecordType::kLatency, enjima::runtime::GetSystemTime<Duration>()));
#endif
                this->latencyRecordLastEmittedAt_ = currentTime;
                return true;
            }
            else if (writeIdx_.load(std::memory_order::acquire) > readIdx_.load(std::memory_order::acquire)) {
                auto readBeginIdx = readIdx_.load(std::memory_order::relaxed) % srcReservoirCapacity_;
                auto nextRecord = eventReservoir_[readBeginIdx];
                readIdx_.fetch_add(1, std::memory_order::acq_rel);
                collector->Collect(nextRecord);
                return true;
            }
            return false;
        }

        uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer, core::OutputCollector* collector) override
        {
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            uint32_t numLatencyRecordsOut = 0;
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
#if ENJIMA_METRICS_LEVEL >= 3
                auto metricsVec = new std::vector<uint64_t>;
                metricsVec->emplace_back(enjima::runtime::GetSystemTimeMicros());
                collector->Collect(YSBAdRecordT(YSBAdRecordT::RecordType::kLatency,
                        enjima::runtime::GetSystemTime<Duration>(), metricsVec));
#else
                collector->Collect(
                        YSBAdRecordT(YSBAdRecordT ::RecordType::kLatency, enjima::runtime::GetSystemTime<Duration>()));
#endif
                this->latencyRecordLastEmittedAt_ = currentTime;
                numLatencyRecordsOut = 1;
            }
            auto numRecordsToCopy = std::min(maxRecordsToWrite - numLatencyRecordsOut,
                    static_cast<uint32_t>(static_cast<uint32_t>(
                            writeIdx_.load(std::memory_order::acquire) - readIdx_.load(std::memory_order::acquire))));
            if (numRecordsToCopy > 0) {
                auto currentCachedReadIdx = readIdx_.load(std::memory_order::relaxed);
                auto readBeginIdx = currentCachedReadIdx % srcReservoirCapacity_;
                auto readEndIdx = readBeginIdx + numRecordsToCopy;
                auto srcBeginPtr = static_cast<void*>(&eventReservoir_[readBeginIdx]);
                if (readEndIdx > srcReservoirCapacity_) {
                    auto numRecordsToReadFromBeginning = readEndIdx - srcReservoirCapacity_;
                    auto numRecordsToReadFromEnd = numRecordsToCopy - numRecordsToReadFromBeginning;
                    collector->CollectBatch<YSBAdT>(srcBeginPtr, numRecordsToReadFromEnd);
                    // We have to circle back to read the rest of the events
                    srcBeginPtr = static_cast<void*>(eventReservoir_);
                    collector->CollectBatch<YSBAdT>(srcBeginPtr, numRecordsToReadFromBeginning);
                }
                else {
                    collector->CollectBatch<YSBAdT>(srcBeginPtr, numRecordsToCopy);
                }
                readIdx_.store(currentCachedReadIdx + numRecordsToCopy, std::memory_order::release);
            }
            return numRecordsToCopy;
        }

        YSBAdT GenerateQueueEvent() override
        {
            throw enjima::benchmarks::workload::WorkloadException{"Unsupported method called!"};
        }

        bool GenerateQueueRecord(core::Record<YSBAdT>& outputRecord) override
        {
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
#if ENJIMA_METRICS_LEVEL >= 3
                auto metricsVec = new std::vector<uint64_t>;
                metricsVec->emplace_back(enjima::runtime::GetSystemTimeMicros());
                outputRecord = YSBAdRecordT(YSBAdRecordT::RecordType::kLatency,
                        enjima::runtime::GetSystemTime<Duration>(), metricsVec);
#else
                outputRecord =
                        YSBAdRecordT(YSBAdRecordT ::RecordType::kLatency, enjima::runtime::GetSystemTime<Duration>());
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

        void PopulateEventCache(uint32_t numEvents, uint32_t numCampaigns, uint32_t numAdsPerCampaign)
        {
            UniformIntDistParamT pt(1, 1000000);
            uniformIntDistribution.param(pt);
            std::unordered_set<uint64_t> campaignIds(numCampaigns);
            auto adId = 0;
            auto numAds = numCampaigns * numAdsPerCampaign;
            while (campaignIds.size() < numCampaigns) {
                auto campaignId = uniformIntDistribution(mtEng);
                if (!campaignIds.contains(campaignId)) {
                    campaignIds.emplace(campaignId);
                    for (auto j = numAdsPerCampaign; j > 0; j--) {
                        adIdToCampaignIdMap_.emplace(++adId, campaignId);
                    }
                }
            }

            eventCache_.reserve(numEvents);
            for (auto i = numEvents; i > 0; i--) {
                auto timestamp = enjima::runtime::GetSystemTimeMillis();
                auto userId = uniformIntDistribution(mtEng);
                auto pageId = uniformIntDistribution(mtEng);
                auto adType = i % 5;
                auto eventType = i % 3;
                YSBAdT lrEvent = YSBAdT(timestamp, userId, pageId, (i % numAds) + 1, adType, eventType, -1);
                eventCache_.emplace_back(lrEvent);
            }
        }

        const UnorderedHashMapST<uint64_t, uint64_t>& GetAdIdToCampaignIdMap() const
        {
            return adIdToCampaignIdMap_;
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

                cacheIterator_ = eventCache_.cbegin();
                auto blockSize = this->pMemoryManager_->GetDefaultNumEventsPerBlock();
                auto recordSize = sizeof(YSBAdRecordT);
                genWriteBufferBeginPtr_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, blockSize * recordSize);
                while (genTaskRunning_.load(std::memory_order::acquire)) {
                    SleepIfNeeded();
                    auto leftToEmitInMs = maxEmitRatePerMs_ - currentEmittedInMs_;
                    auto eventReservoirSize =
                            srcReservoirCapacity_ -
                            (writeIdx_.load(std::memory_order::acquire) - readIdx_.load(std::memory_order::acquire));
                    size_t numRecordsToWrite = std::min(leftToEmitInMs, std::min(eventReservoirSize, blockSize));
                    if (numRecordsToWrite > 0) {
                        auto* writeBuffer = static_cast<YSBAdRecordT*>(genWriteBufferBeginPtr_);
                        auto currentTime = enjima::runtime::GetSystemTimeMillis();
                        for (auto i = numRecordsToWrite; i > 0; i--) {
                            if (++cacheIterator_ == eventCache_.cend()) {
                                cacheIterator_ = eventCache_.cbegin();
                            }
                            auto ysbEvent = *cacheIterator_.base();
                            new (writeBuffer++) YSBAdRecordT(currentTime, ysbEvent);
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
                                    static_cast<YSBAdRecordT*>(genWriteBufferBeginPtr_) + numRecordsToWriteAtEnd);
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
            enjima::operators::SourceOperator<YSBAdT>::Initialize(executionEngine, memoryManager, profiler);
            readyPromise_.set_value();
        }

    private:
        void SleepIfNeeded()
        {
            uint64_t currentTimeUs = runtime::GetSteadyClockMicros();
            if (currentEmittedInMs_ >= maxEmitRatePerMs_) {
                if (nextEmitStartUs_ > currentTimeUs) {
                    std::this_thread::sleep_until(
                            runtime::GetSteadyClockTimePoint<std::chrono::microseconds>(nextEmitStartUs_));
                    currentTimeUs = runtime::GetSteadyClockMicros();
                }
            }
            while (currentTimeUs >= nextEmitStartUs_) {
                nextEmitStartUs_ += 1000;
                currentEmittedInMs_ = 0;
            }
        }

        std::random_device rd;
        std::mt19937 mtEng{rd()};
        std::uniform_int_distribution<int> uniformIntDistribution;
        std::vector<YSBAdT> eventCache_;
        std::vector<YSBAdT>::const_iterator cacheIterator_;
        UnorderedHashMapST<uint64_t, uint64_t> adIdToCampaignIdMap_;
        uint64_t maxEmitRatePerMs_;
        uint64_t currentEmittedInMs_{0};
        uint64_t nextEmitStartUs_{0};

        // Generator member variables
        std::promise<void> readyPromise_;
        std::thread eventGenThread_{&InMemoryFixedRateYSBSourceOperatorWithProcLatency::GenerateEvents, this};
        std::atomic<bool> genTaskRunning_{true};
        std::atomic<size_t> readIdx_{0};
        std::atomic<size_t> writeIdx_{0};
        uint64_t srcReservoirCapacity_;
        YSBAdRecordT* eventReservoir_;
        void* genWriteBufferBeginPtr_{nullptr};
    };
}// namespace enjima::workload::operators

#endif//ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_YSB_SOURCE_OPERATOR_H
