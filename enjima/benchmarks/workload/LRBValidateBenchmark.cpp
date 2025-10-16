//
// Created by m34ferna on 21/02/24.
//

#include "LRBValidateBenchmark.h"
#include <iostream>

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;

template<typename Duration>
using LRBSrcOpT  = enjima::workload::operators::InMemoryRateLimitedLRBSourceOperator<Duration>;

using LRBVBMapIdentFuncT = enjima::workload::functions::LRBVBIdentMapFunction;
using LRBVBProjFuncT = enjima::workload::functions::LRBVBEventProjectFunction;
using LRBVBCountAggFuncT = enjima::workload::functions::LRBVBCountAggFunction;
using LRBVBSpeedAggFuncT = enjima::workload::functions::LRBVBSpeedAggFunction;
using LRBVBCntMultiKeyExtractFuncT = enjima::workload::functions::LRBVBCntReportMultiKeyExtractFunction;
using LRBVBSpdMultiKeyExtractFuncT = enjima::workload::functions::LRBVBSpdReportMultiKeyExtractFunction;
using LRBVBSpdCntJoinFuncT = enjima::workload::functions::LRBVBSpdCntJoinFunction;
using LRBVBTollAcdCoGroupFuncT = enjima::workload::functions::LRBVBTollAcdCoGroupFunction;

void enjima::benchmarks::workload::LRBValidateBenchmark::SetUpPipeline(uint64_t latencyRecEmitPeriodMs,
        uint64_t maxInputRate, bool generateWithEmit, bool useProcessingLatency)
{
    auto jobIdSuffix = 0;
    for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
        auto jobIdSuffixStr = std::to_string(++jobIdSuffix);
        enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
        auto uPtrSrcOp = std::make_unique<LRBSrcOpT<std::chrono::milliseconds>>(
                srcOpId, GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate);
        uPtrSrcOp->PopulateEventCache(lrbDataPath_);
        benchmarkStreamingJob->AddOperator(std::move(uPtrSrcOp));

        // Filtering position reports
        enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
        auto filterFn = [](const LinearRoadT& lrTypeEvent) { return lrTypeEvent.GetType() == 0; };
        auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<LinearRoadT, decltype(filterFn)>>(
                filterOpId, GetSuffixedOperatorName(kFilterOpName_, jobIdSuffixStr), filterFn);
        benchmarkStreamingJob->AddOperator(std::move(uPtrFilterOp), srcOpId);

        // Project
        enjima::operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
        auto uPtrProjOp = std::make_unique<enjima::operators::MapOperator<LinearRoadT, LRBVBProjEventT, LRBVBProjFuncT>>(
            projectOpId, GetSuffixedOperatorName(projOpName_, jobIdSuffixStr), LRBVBProjFuncT{});
        benchmarkStreamingJob->AddOperator(std::move(uPtrProjOp), filterOpId);

        // Split into 3
        enjima::operators::OperatorID multiMapOpId = executionEngine_->GetNextOperatorId();
        auto uPtrMultiMapOp = std::make_unique<enjima::operators::MapOperator<LRBVBProjEventT, LRBVBProjEventT, LRBVBMapIdentFuncT, 3>>(
            multiMapOpId, GetSuffixedOperatorName(multiMapOpName_, jobIdSuffixStr), LRBVBMapIdentFuncT{});
        benchmarkStreamingJob->AddOperator(std::move(uPtrMultiMapOp), projectOpId);

        // Accident tracking: upstream at multiMapOpId
        enjima::operators::OperatorID windowOpAcdId = executionEngine_->GetNextOperatorId();
        auto acdAggfunc = enjima::api::MergeableKeyedAggregateFunction<LRBVBProjEventT, LRBVBAcdReportT>{};
        auto uPtrWindowOpAcd = std::make_unique<enjima::operators::SlidingEventTimeWindowOperator<LRBVBProjEventT, LRBVBAcdReportT, decltype(acdAggfunc)>>(
            windowOpAcdId, GetSuffixedOperatorName(acdWindowOpName_, jobIdSuffixStr), acdAggfunc, std::chrono::seconds(120), std::chrono::seconds(30));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOpAcd), multiMapOpId);

        // Speed monitor: upstream at multiMapOpId
        enjima::operators::OperatorID windowOpSpdId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowOpSpd = std::make_unique<enjima::operators::FixedEventTimeWindowOperator<LRBVBProjEventT, LRBVBSpdReportT, true, LRBVBSpeedAggFuncT>>(
            windowOpSpdId, GetSuffixedOperatorName(spdWindowOpName_, jobIdSuffixStr), LRBVBSpeedAggFuncT{}, std::chrono::seconds(60));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOpSpd), multiMapOpId);

        // Count vehicles: upstream at multiMapOpId
        enjima::operators::OperatorID windowOpCntId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowOpCnt = std::make_unique<enjima::operators::FixedEventTimeWindowOperator<LRBVBProjEventT, LRBVBCntReportT, true, LRBVBCountAggFuncT>>(
            windowOpCntId, GetSuffixedOperatorName(cntWindowOpName_, jobIdSuffixStr), LRBVBCountAggFuncT{}, std::chrono::seconds(60));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOpCnt), multiMapOpId);

        // Join Count and Speed
        enjima::operators::OperatorID windowJoinCntSpdId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowJoinCntSpd = std::make_unique<enjima::operators::FixedEventTimeWindowJoinOperator<
            LRBVBSpdReportT, LRBVBCntReportT, std::tuple<int, int, int, uint64_t>, LRBVBSpdMultiKeyExtractFuncT, LRBVBCntMultiKeyExtractFuncT, LRBVBTollReportT, LRBVBSpdCntJoinFuncT>>(
                windowJoinCntSpdId,  GetSuffixedOperatorName(cntSpdWindowJoinOpName_, jobIdSuffixStr), 
                LRBVBSpdMultiKeyExtractFuncT{}, LRBVBCntMultiKeyExtractFuncT{}, LRBVBSpdCntJoinFuncT{}, std::chrono::seconds(60));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowJoinCntSpd), std::make_pair(windowOpSpdId, windowOpCntId));

        // Join above with accident
        enjima::operators::OperatorID windowCoGroupOpId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowJoinTollAcd = std::make_unique<enjima::operators::FixedEventTimeWindowCoGroupOperator<LRBVBTollReportT, LRBVBAcdReportT, LRBVBTollFinalReportT, LRBVBTollAcdCoGroupFuncT>>(
            windowCoGroupOpId, GetSuffixedOperatorName(acdTollCoGroupOpName_, jobIdSuffixStr), LRBVBTollAcdCoGroupFuncT{}, std::chrono::seconds(60));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowJoinTollAcd), std::make_pair(windowJoinCntSpdId, windowOpAcdId));

        // Sink
        enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
        auto uPtrSinkOp = std::make_unique<enjima::operators::NoOpSinkOperator<LRBVBTollFinalReportT>>(sinkOpId,
                GetSuffixedOperatorName(sinkOpName_, jobIdSuffixStr));
        benchmarkStreamingJob->AddOperator(std::move(uPtrSinkOp), windowCoGroupOpId);

        if (schedulingMode_ == runtime::SchedulingMode::kThreadBased) {
            benchmarkStreamingJob->SetProcessingMode(processingMode_);
        }
    }
}

void enjima::benchmarks::workload::LRBValidateBenchmark::RunBenchmark(uint64_t durationInSec)
{
    spdlog::info("Starting benchmark : {}", this->GetBenchmarkName());
    startTime_ = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
    for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
        queryIds_.emplace_back(executionEngine_->Submit(*benchmarkStreamingJob));
    }
    std::this_thread::sleep_for(std::chrono::seconds(durationInSec));
    for (const auto& jobId: queryIds_) {
        executionEngine_->Cancel(jobId);
    }
    endTime_ = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
    spdlog::info("Stopped benchmark : {}", this->GetBenchmarkName());
}

enjima::benchmarks::workload::LRBValidateBenchmark::LRBValidateBenchmark() : StreamingBenchmark("lrb_validate") {}

void enjima::benchmarks::workload::LRBValidateBenchmark::SetDataPath(std::string path)
{
    lrbDataPath_ = path;
}