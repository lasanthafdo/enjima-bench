//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_IN_MEMORY_RATE_LIMITED_YSB_SOURCE_OPERATOR_H
#define ENJIMA_BENCHMARKS_IN_MEMORY_RATE_LIMITED_YSB_SOURCE_OPERATOR_H


#include "enjima/api/data_types/YSBAdEvent.h"
#include "enjima/operators/LatencyTrackingSourceOperator.h"
#include <random>

using YSBAdT = enjima::api::data_types::YSBAdEvent;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;

namespace enjima::workload::operators {
    template<typename Duration>
    class InMemoryRateLimitedYSBSourceOperator
        : public enjima::operators::LatencyTrackingSourceOperator<YSBAdT, Duration> {
    public:
        explicit InMemoryRateLimitedYSBSourceOperator(enjima::operators::OperatorID opId, const std::string& opName,
                uint64_t latencyRecordEmitPeriodMs, uint64_t maxInputRate)
            : enjima::operators::LatencyTrackingSourceOperator<YSBAdT, Duration>(opId, opName,
                      latencyRecordEmitPeriodMs),
              maxEmitRatePerMs_(maxInputRate / 1000)
        {
            nextEmitStartUs_ = (enjima::runtime::GetSystemTimeMillis() + 1) * 1000;
        }

        bool EmitEvent(core::OutputCollector* collector) override
        {
            SleepIfNeeded();
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
                auto latencyRecord = enjima::core::Record<YSBAdT>(enjima::core::Record<YSBAdT>::RecordType::kLatency,
                        enjima::runtime::GetSystemTime<Duration>());
                collector->Collect(latencyRecord);
                this->latencyRecordLastEmittedAt_ = currentTime;
            }
            else {
                if (++cacheIterator_ == eventCache_.cend()) {
                    cacheIterator_ = eventCache_.cbegin();
                }
                auto ysbEvent = *cacheIterator_.base();
                collector->Collect<YSBAdT>(ysbEvent);
            }
            currentEmittedInMs_++;
            return true;
        }

        uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer, core::OutputCollector* collector) override
        {
            SleepIfNeeded();
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            void* outBufferWritePtr = outputBuffer;
            uint32_t numLatencyRecordsOut = 0;
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
#if ENJIMA_METRICS_LEVEL >= 3
                auto metricsVec = new std::vector<uint64_t>;
                metricsVec->emplace_back(enjima::runtime::GetSystemTimeMicros());
                new (outBufferWritePtr) enjima::core::Record<YSBAdT>(enjima::core::Record<YSBAdT>::RecordType::kLatency,
                        enjima::runtime::GetSystemTime<Duration>(), metricsVec);
#else
                new (outBufferWritePtr) enjima::core::Record<YSBAdT>(enjima::core::Record<YSBAdT>::RecordType::kLatency,
                        enjima::runtime::GetSystemTime<Duration>());
#endif
                outBufferWritePtr = static_cast<enjima::core::Record<YSBAdT>*>(outBufferWritePtr) + 1;
                this->latencyRecordLastEmittedAt_ = currentTime;
                numLatencyRecordsOut = 1;
            }
            for (auto i = (maxRecordsToWrite - numLatencyRecordsOut); i > 0; i--) {
                if (++cacheIterator_ == eventCache_.cend()) {
                    cacheIterator_ = eventCache_.cbegin();
                }
                auto ysbEvent = *cacheIterator_.base();
                new (outBufferWritePtr) enjima::core::Record<YSBAdT>(currentTime, ysbEvent);
                outBufferWritePtr = static_cast<enjima::core::Record<YSBAdT>*>(outBufferWritePtr) + 1;
            }
            collector->CollectBatch<YSBAdT>(outputBuffer, maxRecordsToWrite);
            currentEmittedInMs_ += maxRecordsToWrite;
            return maxRecordsToWrite;
        }

        YSBAdT GenerateQueueEvent() override
        {
            if (++cacheIterator_ == eventCache_.cend()) {
                cacheIterator_ = eventCache_.cbegin();
            }
            auto ysbEvent = *cacheIterator_.base();
            return ysbEvent;
        }

        bool GenerateQueueRecord(core::Record<YSBAdT>& outputRecord) override
        {
            SleepIfNeeded();
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            currentEmittedInMs_++;
            if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
                outputRecord = enjima::core::Record<YSBAdT>(enjima::core::Record<YSBAdT>::RecordType::kLatency,
                        enjima::runtime::GetSystemTime<Duration>());
                this->latencyRecordLastEmittedAt_ = currentTime;
            }
            else {
                auto event = GenerateQueueEvent();
                outputRecord = enjima::core::Record<YSBAdT>(currentTime, event);
            }
            return true;
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
            cacheIterator_ = eventCache_.cbegin();
        }

        const UnorderedHashMapST<uint64_t, uint64_t>& GetAdIdToCampaignIdMap() const
        {
            return adIdToCampaignIdMap_;
        }

    private:
        void SleepIfNeeded()
        {
            uint64_t currentTimeUs = runtime::GetSystemTimeMicros();
            if (currentEmittedInMs_ >= maxEmitRatePerMs_) {
                if (nextEmitStartUs_ > currentTimeUs) {
                    std::this_thread::sleep_until(
                            runtime::GetSystemTimePoint<std::chrono::microseconds>(nextEmitStartUs_));
                    currentTimeUs = runtime::GetSystemTimeMicros();
                }
            }
            if (currentTimeUs >= nextEmitStartUs_) {
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
    };
}// namespace enjima::workload::operators

#endif//ENJIMA_BENCHMARKS_IN_MEMORY_RATE_LIMITED_YSB_SOURCE_OPERATOR_H
