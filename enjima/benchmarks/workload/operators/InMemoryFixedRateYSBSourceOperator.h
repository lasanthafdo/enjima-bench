//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_YSB_SOURCE_OPERATOR_H
#define ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_YSB_SOURCE_OPERATOR_H


#include "enjima/api/data_types/YSBAdEvent.h"
#include "enjima/benchmarks/workload/WorkloadException.h"
#include "enjima/operators/LatencyTrackingSourceOperator.h"
#include "enjima/runtime/RuntimeTypes.h"
#include <random>

using YSBAdT = enjima::api::data_types::YSBAdEvent;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;
using YSBAdRecordT = enjima::core::Record<YSBAdT>;

namespace enjima::workload::operators {
    template<typename Duration>
    class InMemoryFixedRateYSBSourceOperator
        : public enjima::operators::LatencyTrackingSourceOperator<YSBAdT, Duration> {
    public:
        InMemoryFixedRateYSBSourceOperator(enjima::operators::OperatorID opId, const std::string& opName,
                uint64_t latencyRecordEmitPeriodMs, uint64_t maxInputRate, uint64_t srcReservoirCapacity,
                uint64_t minInputRate = 0, const std::string& stepSizeStr = "0", uint64_t shiftInterval = 0)
            : enjima::operators::LatencyTrackingSourceOperator<YSBAdT, Duration>(opId, opName,
                      latencyRecordEmitPeriodMs),
              currentMaxEmitRatePerMs_(maxInputRate / 1000), minEmitRatePerMs_(minInputRate / 1000),
              maxEmitRatePerMs_(maxInputRate / 1000), shiftIntervalMs_(shiftInterval),
              srcReservoirCapacity_(srcReservoirCapacity),
              eventReservoir_(static_cast<YSBAdRecordT*>(
                      aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, sizeof(YSBAdRecordT) * srcReservoirCapacity)))
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
            nextEmitStartUs_ = runtime::GetSteadyClockMicros() + 1000;
        }

        ~InMemoryFixedRateYSBSourceOperator() override
        {
            genTaskRunning_.store(false, std::memory_order::release);
            eventGenThread_.join();
            free(static_cast<void*>(eventReservoir_));
        }

        bool EmitEvent(core::OutputCollector* collector) override
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

        uint32_t EmitBatch(uint32_t maxRecordsToWrite, [[maybe_unused]] void* outputBuffer,
                core::OutputCollector* collector) override
        {
            auto numRecordsToCopy =
                    std::min(maxRecordsToWrite, static_cast<uint32_t>(cachedWriteIdx_.load(std::memory_order::acquire) -
                                                                      cachedReadIdx_.load(std::memory_order::acquire)));
            if (numRecordsToCopy > 0) {
                auto currentCachedReadIdx = cachedReadIdx_.load(std::memory_order::acquire);
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
                cachedReadIdx_.store(currentCachedReadIdx + numRecordsToCopy, std::memory_order::release);
            }
            return numRecordsToCopy;
        }

        YSBAdT GenerateQueueEvent() override
        {
            throw enjima::benchmarks::workload::WorkloadException{"Unsupported method called!"};
        }

        bool GenerateQueueRecord(core::Record<YSBAdT>& outputRecord) override
        {
            if (cachedWriteIdx_.load(std::memory_order::acquire) > cachedReadIdx_.load(std::memory_order::acquire)) {
                auto readBeginIdx = cachedReadIdx_.load(std::memory_order::acquire) % srcReservoirCapacity_;
                outputRecord = eventReservoir_[readBeginIdx];
                cachedReadIdx_.fetch_add(1, std::memory_order::acq_rel);
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
                SetWorkloadType();
                lastInputRateChangeAtMs_ = enjima::runtime::GetSystemTimeMillis();
                // End of environment initialization

                cacheIterator_ = eventCache_.cbegin();
                auto blockSize = this->pMemoryManager_->GetDefaultNumEventsPerBlock();
                auto recordSize = sizeof(YSBAdRecordT);
                genWriteBufferBeginPtr_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, blockSize * recordSize);
                size_t numRecordsToWrite;
                while (genTaskRunning_.load(std::memory_order::acquire)) {
                    SleepIfNeeded();
                    auto currentTimeMillis = enjima::runtime::GetSystemTimeMillis();
                    auto leftToEmitInMs = currentMaxEmitRatePerMs_ - currentEmittedInMs_;
                    if (leftToEmitInMs > 0 && !this->emitLatencyMarker_ &&
                            (currentTimeMillis >=
                                    (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_))) {
                        this->latencyMarkerTime_ = enjima::runtime::GetSystemTime<Duration>();
                        this->emitLatencyMarker_ = true;
                    }
                    auto eventReservoirSize =
                            srcReservoirCapacity_ - (cachedWriteIdx_.load(std::memory_order::acquire) -
                                                            cachedReadIdx_.load(std::memory_order::acquire));
                    numRecordsToWrite = std::min(leftToEmitInMs, std::min(eventReservoirSize, blockSize));
                    if (numRecordsToWrite > 0) {
                        auto* writeBuffer = static_cast<YSBAdRecordT*>(genWriteBufferBeginPtr_);
                        size_t numLatencyRecordsWritten = 0;
                        if (this->emitLatencyMarker_) {
#if ENJIMA_METRICS_LEVEL >= 3
                            auto metricsVec = new std::vector<uint64_t>;
                            metricsVec->emplace_back(enjima::runtime::GetSystemTimeMicros());
                            new (writeBuffer++) YSBAdRecordT(YSBAdRecordT ::RecordType::kLatency,
                                    this->latencyMarkerTime_, metricsVec);
#else
                            new (writeBuffer++)
                                    YSBAdRecordT(YSBAdRecordT::RecordType::kLatency, this->latencyMarkerTime_);
#endif
                            this->latencyRecordLastEmittedAt_ = currentTimeMillis;
                            this->emitLatencyMarker_ = false;
                            numLatencyRecordsWritten = 1;
                        }
                        for (auto i = (numRecordsToWrite - numLatencyRecordsWritten); i > 0; i--) {
                            if (++cacheIterator_ == eventCache_.cend()) {
                                cacheIterator_ = eventCache_.cbegin();
                            }
                            auto ysbEvent = *cacheIterator_.base();
                            new (writeBuffer++) YSBAdRecordT(currentTimeMillis, ysbEvent);
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
                                    static_cast<YSBAdRecordT*>(genWriteBufferBeginPtr_) + numRecordsToWriteAtEnd);
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
                    if ((workloadType_ != InMemoryFixedRateYSBSourceOperator<Duration>::kConstantRate_) &&
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
            enjima::operators::SourceOperator<YSBAdT>::Initialize(executionEngine, memoryManager, profiler);
            readyPromise_.set_value();
        }

    private:
        void SleepIfNeeded()
        {
            uint64_t currentTimeUs = runtime::GetSteadyClockMicros();
            if (currentEmittedInMs_ >= currentMaxEmitRatePerMs_) {
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

        void SetWorkloadType()
        {
            if ((minEmitRatePerMs_ > 0) && (minEmitRatePerMs_ < maxEmitRatePerMs_) && (shiftIntervalMs_ > 0)) {
                if (stepSize_ > 0) {
                    workloadType_ = InMemoryFixedRateYSBSourceOperator<Duration>::kStepIncrease_;
                    currentMaxEmitRatePerMs_ = minEmitRatePerMs_;
                    spdlog::info("Generating events with a step increase of {} e/ms every {} ms starting from {} e/ms "
                                 "upto {} e/ms",
                            stepSize_, shiftIntervalMs_, minEmitRatePerMs_, maxEmitRatePerMs_);
                }
                else if (!stepSizeQ_.empty()) {
                    workloadType_ = InMemoryFixedRateYSBSourceOperator<Duration>::kUserDefinedRateChange_;
                    currentMaxEmitRatePerMs_ = minEmitRatePerMs_;
                    spdlog::info("Generating events with a user-defined rate change every {} ms starting from {} e/ms "
                                 "upto {} e/ms",
                            shiftIntervalMs_, minEmitRatePerMs_, maxEmitRatePerMs_);
                }
                else {
                    workloadType_ = InMemoryFixedRateYSBSourceOperator<Duration>::kUnpredictableRateVariationsInRange_;
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
                if (workloadType_ == InMemoryFixedRateYSBSourceOperator<Duration>::kStepIncrease_) {
                    currentMaxEmitRatePerMs_ = std::min(currentMaxEmitRatePerMs_ + stepSize_, maxEmitRatePerMs_);
                    spdlog::debug("Setting input rate to {} e/ms", currentMaxEmitRatePerMs_);
                }
                else if (workloadType_ == InMemoryFixedRateYSBSourceOperator<Duration>::kUserDefinedRateChange_) {
                    if (!stepSizeQ_.empty()) {
                        auto nextChangeInRate = stepSizeQ_.front();
                        stepSizeQ_.pop();
                        currentMaxEmitRatePerMs_ =
                                std::min(currentMaxEmitRatePerMs_ + nextChangeInRate, maxEmitRatePerMs_);
                        spdlog::debug("Setting input rate to {} e/ms", currentMaxEmitRatePerMs_);
                    }
                }
                else {
                    assert(workloadType_ ==
                            InMemoryFixedRateYSBSourceOperator<Duration>::kUnpredictableRateVariationsInRange_);
                    FlipCoinAndChangeInputRate();
                    spdlog::debug("Setting input rate to {} e/ms", currentMaxEmitRatePerMs_);
                }
                lastInputRateChangeAtMs_ = currentTimeMillis;
            }
        }

        void FlipCoinAndChangeInputRate()
        {
            static std::uniform_int_distribution<> boolDistrib(0, 1);// Distribution 0 or 1
            static std::uniform_int_distribution<> rateDistrib(minEmitRatePerMs_,
                    maxEmitRatePerMs_ - 1);// Distribution between minEmitRatePerMs_ and maxEmitRatePerMs_
            if (boolDistrib(mtEng) == 1) {
                currentMaxEmitRatePerMs_ = rateDistrib(mtEng);
            }
        }

        const char delim_ = '_';

        std::random_device rd;
        std::mt19937 mtEng{rd()};
        std::uniform_int_distribution<int> uniformIntDistribution;
        std::vector<YSBAdT> eventCache_;
        std::vector<YSBAdT>::const_iterator cacheIterator_;
        UnorderedHashMapST<uint64_t, uint64_t> adIdToCampaignIdMap_;
        uint64_t currentMaxEmitRatePerMs_;
        const uint64_t minEmitRatePerMs_;
        const uint64_t maxEmitRatePerMs_;
        const uint64_t shiftIntervalMs_;
        uint64_t stepSize_;
        std::queue<int64_t> stepSizeQ_;
        uint64_t lastInputRateChangeAtMs_{0};
        uint64_t currentEmittedInMs_{0};
        uint64_t nextEmitStartUs_{0};
        uint8_t workloadType_{InMemoryFixedRateYSBSourceOperator<Duration>::kConstantRate_};

        // Generator member variables
        std::promise<void> readyPromise_;
        std::thread eventGenThread_{&InMemoryFixedRateYSBSourceOperator::GenerateEvents, this};
        std::atomic<bool> genTaskRunning_{true};
        std::atomic<size_t> cachedReadIdx_{0};
        std::atomic<size_t> cachedWriteIdx_{0};
        uint64_t srcReservoirCapacity_;
        YSBAdRecordT* eventReservoir_;
        void* genWriteBufferBeginPtr_{nullptr};
    };
}// namespace enjima::workload::operators

#endif//ENJIMA_BENCHMARKS_IN_MEMORY_FIXED_RATE_YSB_SOURCE_OPERATOR_H
