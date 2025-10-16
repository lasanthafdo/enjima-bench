//
// Created by m34ferna on 05/03/24.
//

#ifndef ENJIMA_YSB_FUNCTIONS_H
#define ENJIMA_YSB_FUNCTIONS_H

#include "enjima/api/JoinFunction.h"
#include "enjima/api/KeyExtractionFunction.h"
#include "enjima/api/KeyedAggregateFunction.h"
#include "enjima/api/MapFunction.h"
#include "enjima/api/SinkFunction.h"
#include "enjima/api/data_types/YSBAdEvent.h"
#include "enjima/api/data_types/YSBInterimEventTypes.h"
#include "spdlog/spdlog.h"

using YSBAdT = enjima::api::data_types::YSBAdEvent;
using YSBProjT = enjima::api::data_types::YSBProjectedEvent;
using YSBWinT = enjima::api::data_types::YSBWinEmitEvent;
using YSBCampT = enjima::api::data_types::YSBCampaignAdEvent;

namespace enjima::workload::functions {
    class YSBProjectFunction : public enjima::api::MapFunction<YSBAdT, YSBProjT> {
    public:
        YSBProjT operator()(const YSBAdT& inputEvent) override
        {
            return YSBProjT(inputEvent.GetTimestamp(), inputEvent.GetAdId());
        }
    };

    class YSBCampaignAggFunction : public enjima::api::KeyedAggregateFunction<YSBCampT, YSBWinT> {
    private:
        std::vector<YSBWinT> resultVec_;
        UnorderedHashMapST<uint64_t, uint64_t> countMap_;

    public:
        void operator()(const YSBCampT& inputEvent) override
        {
            auto campaignId = inputEvent.GetCampaignId();
            if (!countMap_.contains(campaignId)) {
                countMap_.emplace(campaignId, 0);
            }
            countMap_.at(campaignId)++;
        }

        std::vector<YSBWinT>& GetResult() override
        {
            if (resultVec_.size() < countMap_.size()) {
                resultVec_.reserve(countMap_.size());
            }
            resultVec_.clear();
            for (const auto& countPair: countMap_) {
                auto campaignId = countPair.first;
                resultVec_.emplace_back(enjima::runtime::GetSystemTimeMillis(), campaignId, countPair.second);
                countMap_.insert_or_assign(campaignId, 0);
            }
            return resultVec_;
        }
    };

    class YSBLoggingSinkFunction : public enjima::api::SinkFunction<YSBWinT> {
    public:
        void Execute(uint64_t timestamp, YSBWinT inputEvent) override
        {
            spdlog::info("[{}] Received window event {}", timestamp, inputEvent);
        }
    };

    class YSBNoOpSinkFunction : public enjima::api::SinkFunction<YSBWinT> {
    public:
        void Execute([[maybe_unused]] uint64_t timestamp, [[maybe_unused]] YSBWinT inputEvent) override
        {
            assert(timestamp > 0 && inputEvent.GetCampaignId() >= 0);
        }
    };

    template<typename TInput>
    class YSBGenericNoOpSinkFunction : public enjima::api::SinkFunction<TInput> {
    public:
        void Execute([[maybe_unused]] uint64_t timestamp, [[maybe_unused]] TInput inputEvent) override
        {
            assert(timestamp > 0);
        }
    };

    class YSBKeyExtractFunction : public enjima::api::KeyExtractionFunction<YSBProjT, uint64_t> {
    public:
        uint64_t operator()(const YSBProjT& inputEvent) override
        {
            // assert(inputEvent.GetAdId() > 0 && inputEvent.GetAdId() <= 1000);
            return inputEvent.GetAdId();
        }
    };

    class YSBEquiJoinFunction : public enjima::api::JoinFunction<YSBProjT, uint64_t, YSBCampT> {
    public:
        YSBCampT operator()(const YSBProjT& leftInputEvent, const uint64_t& rightInputEvent) override
        {
            return YSBCampT(leftInputEvent.GetTimestamp(), leftInputEvent.GetAdId(), rightInputEvent);
        }
    };
}// namespace enjima::workload::functions

#endif//ENJIMA_YSB_FUNCTIONS_H
