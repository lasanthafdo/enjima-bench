//
// Created by m34ferna on 04/06/25.
//

using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;

template<typename Duration>
using YSBSrcOpT = enjima::workload::operators::InMemoryRateLimitedYSBSourceOperator<Duration>;
template<typename Duration>
using FixedRateYSBSrcOpT = enjima::workload::operators::InMemoryFixedRateYSBSourceOperator<Duration>;
template<typename Duration>
using FixedRateYSBSrcOpWithProcLatencyT =
        enjima::workload::operators::InMemoryFixedRateYSBSourceOperatorWithProcLatency<Duration>;
template<typename Duration>
using NYTODQSrcOpT = enjima::workload::operators::InMemoryRateLimitedNYTODQSourceOperator<Duration>;
template<typename Duration>
using FixedRateNYTODQSrcOpT = enjima::workload::operators::InMemoryFixedRateNYTODQSourceOperator<Duration>;
template<typename Duration>
using FixedRateNYTODQSrcOpWithProcLatencyT =
        enjima::workload::operators::InMemoryFixedRateNYTODQSourceOperatorWithProcLatency<Duration>;

template<typename TInput, typename TFunc, typename Duration>
using YSBSinkOpT = enjima::operators::GenericSinkOperator<TInput, TFunc, Duration>;

using YSBProjFuncT = enjima::workload::functions::YSBProjectFunction;
using YSBKeyExtractFuncT = enjima::workload::functions::YSBKeyExtractFunction;
using YSBEquiJoinFuncT = enjima::workload::functions::YSBEquiJoinFunction;
using YSBAggFuncT = enjima::workload::functions::YSBCampaignAggFunction;
using YSBSinkFuncT = enjima::workload::functions::YSBNoOpSinkFunction;

using NYTODQProjectFuncT = enjima::workload::functions::NYTODQMapProjectFunction;
using NYTDQSinkFuncT = enjima::workload::functions::NYTDQNoOpSinkFunction;
using NYTDQWindowAggFuncT = enjima::workload::functions::NYTDQTripAggFunction;

namespace enjima::benchmarks::workload {

    template<typename Duration>
    void MixedWorkloadYSBNYT<Duration>::SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate,
            bool generateWithEmit, bool useProcessingLatency)
    {
        throw std::runtime_error("Please call SetUpMixedWorkloadPipeline() instead.");
    }

    template<typename Duration>
    void MixedWorkloadYSBNYT<Duration>::SetUpMixedWorkloadPipeline(uint64_t latencyRecEmitPeriodMs,
            uint64_t maxInputRateA, uint64_t maxInputRateB, bool generateWithEmit, bool useProcessingLatency)
    {
        auto jobIdSuffix = 0;
        for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
            auto jobIdSuffixStr = std::to_string(++jobIdSuffix);
            if (jobIdSuffix % 2 == 1) {
                if (generateWithEmit) {
                    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                    auto uPtrSrcOp = std::make_unique<NYTODQSrcOpT<Duration>>(srcOpId,
                            GetSuffixedOperatorName(kNYTSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                            maxInputRateA);
                    uPtrSrcOp->PopulateEventCache(nytDataPath_, &nytEventCache_);
                    SetupNYTPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                }
                else {
                    if (useProcessingLatency) {
                        enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                        auto uPtrSrcOp = std::make_unique<FixedRateNYTODQSrcOpWithProcLatencyT<Duration>>(srcOpId,
                                GetSuffixedOperatorName(kNYTSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                                maxInputRateA, srcReservoirCapacity_);
                        uPtrSrcOp->PopulateEventCache(nytDataPath_, &nytEventCache_);
                        SetupNYTPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                    }
                    else {
                        enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                        auto uPtrSrcOp = std::make_unique<FixedRateNYTODQSrcOpT<Duration>>(srcOpId,
                                GetSuffixedOperatorName(kNYTSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                                maxInputRateA, srcReservoirCapacity_);
                        uPtrSrcOp->PopulateEventCache(nytDataPath_, &nytEventCache_);
                        SetupNYTPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                    }
                }
            }
            else {
                if (generateWithEmit) {
                    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                    auto uPtrSrcOp = std::make_unique<YSBSrcOpT<Duration>>(srcOpId,
                            GetSuffixedOperatorName(kYSBSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                            maxInputRateB);
                    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
                    SetupYSBPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                }
                else {
                    if (useProcessingLatency) {
                        enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                        auto uPtrSrcOp = std::make_unique<FixedRateYSBSrcOpWithProcLatencyT<Duration>>(srcOpId,
                                GetSuffixedOperatorName(kYSBSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                                maxInputRateB, srcReservoirCapacity_);
                        uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
                        SetupYSBPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                    }
                    else {
                        enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                        auto uPtrSrcOp = std::make_unique<FixedRateYSBSrcOpT<Duration>>(srcOpId,
                                GetSuffixedOperatorName(kYSBSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                                maxInputRateB, srcReservoirCapacity_);
                        uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
                        SetupYSBPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                    }
                }
            }
            if (schedulingMode_ == runtime::SchedulingMode::kThreadBased) {
                benchmarkStreamingJob->SetProcessingMode(processingMode_);
            }
        }
    }

    template<typename Duration>
    template<typename T>
        requires std::is_base_of_v<LatencyTrackingNYTODQSrcOpT<Duration>, T>
    void MixedWorkloadYSBNYT<Duration>::SetupNYTPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
            runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr)
    {
        auto srcOpId = uPtrSrcOp->GetOperatorId();
        benchmarkStreamingJob->AddOperator(std::move(uPtrSrcOp));

        // Project
        operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
        auto uPtrProjectOp =
                std::make_unique<operators::MapOperator<NYTODQEventT, NYTDQProjEventT, NYTODQProjectFuncT>>(projectOpId,
                        GetSuffixedOperatorName(kNYTProjectOpName_, jobIdSuffixStr), NYTODQProjectFuncT{});
        benchmarkStreamingJob->AddOperator(std::move(uPtrProjectOp), srcOpId);

        // Filter by vendor id and trip distance
        operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
        auto nytFilterFn = [](const NYTDQProjEventT& nytEvent) {
            return nytEvent.GetDropOffCellId() > 0 && nytEvent.GetPickupCellId() > 0 &&
                   nytEvent.GetVendorId() == "VTS" && nytEvent.GetTripDistance() > 1;
        };
        auto uPtrFilterOp = std::make_unique<operators::FilterOperator<NYTDQProjEventT, decltype(nytFilterFn)>>(
                filterOpId, GetSuffixedOperatorName(kNYTFilterOpName_, jobIdSuffixStr), nytFilterFn);
        benchmarkStreamingJob->AddOperator(std::move(uPtrFilterOp), projectOpId);

        // Window
        operators::OperatorID windowOpId = this->executionEngine_->GetNextOperatorId();
        auto uPtrWindowOp = std::make_unique<
                operators::FixedEventTimeWindowOperator<NYTDQProjEventT, NYTDQWinT, true, NYTDQWindowAggFuncT>>(
                windowOpId, GetSuffixedOperatorName(kNYTAggWindowOpName_, jobIdSuffixStr), NYTDQWindowAggFuncT{},
                std::chrono::seconds(kWindowDurationBase_));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOp), filterOpId);


        // Sink
        operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
        auto uPtrSinkOp = std::make_unique<operators::GenericSinkOperator<NYTDQWinT, NYTDQSinkFuncT, Duration>>(
                sinkOpId, GetSuffixedOperatorName(kNYTSinkOpName_, jobIdSuffixStr), NYTDQSinkFuncT{});
        benchmarkStreamingJob->AddOperator(std::move(uPtrSinkOp), windowOpId);
    }

    template<typename Duration>
    template<typename T>
        requires std::is_base_of_v<LatencyTrackingYSBSrcOpT<Duration>, T>
    void MixedWorkloadYSBNYT<Duration>::SetupYSBPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
            runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr)
    {
        operators::OperatorID filterOpId = this->executionEngine_->GetNextOperatorId();
        auto filterFn = [](const YSBAdT& ysbTypeEvent) { return ysbTypeEvent.GetEventType() == 0; };
        auto uPtrFilterOp = std::make_unique<operators::FilterOperator<YSBAdT, decltype(filterFn)>>(filterOpId,
                GetSuffixedOperatorName(this->kYSBFilterOpName_, jobIdSuffixStr), filterFn);

        operators::OperatorID projectOpId = this->executionEngine_->GetNextOperatorId();
        auto uPtrProjectOp = std::make_unique<operators::MapOperator<YSBAdT, YSBProjT, YSBProjFuncT>>(projectOpId,
                GetSuffixedOperatorName(this->kYSBProjectOpName_, jobIdSuffixStr), YSBProjFuncT{});

        operators::OperatorID statJoinOpId = this->executionEngine_->GetNextOperatorId();
        auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();
        auto uPtrStaticEqJoinOp = std::make_unique<operators::StaticEquiJoinOperator<YSBProjT, uint64_t, YSBCampT,
                uint64_t, YSBKeyExtractFuncT, YSBEquiJoinFuncT>>(statJoinOpId,
                GetSuffixedOperatorName(this->kYSBStaticEqJoinOpName_, jobIdSuffixStr), YSBKeyExtractFuncT{},
                YSBEquiJoinFuncT{}, adIdCampaignIdMap);

        operators::OperatorID windowOpId = this->executionEngine_->GetNextOperatorId();
        auto uPtrWindowOp =
                std::make_unique<operators::FixedEventTimeWindowOperator<YSBCampT, YSBWinT, true, YSBAggFuncT>>(
                        windowOpId, GetSuffixedOperatorName(this->kYSBWindowOpName_, jobIdSuffixStr), YSBAggFuncT{},
                        std::chrono::seconds(10));

        operators::OperatorID sinkOpId = this->executionEngine_->GetNextOperatorId();
        auto uPtrSinkOp = std::make_unique<YSBSinkOpT<YSBWinT, YSBSinkFuncT, Duration>>(sinkOpId,
                GetSuffixedOperatorName(this->kYSBSinkOpName_, jobIdSuffixStr), YSBSinkFuncT{});

        auto srcOpId = uPtrSrcOp->GetOperatorId();
        benchmarkStreamingJob->AddOperator(std::move(uPtrSrcOp));
        benchmarkStreamingJob->AddOperator(std::move(uPtrFilterOp), srcOpId);
        benchmarkStreamingJob->AddOperator(std::move(uPtrProjectOp), filterOpId);
        benchmarkStreamingJob->AddOperator(std::move(uPtrStaticEqJoinOp), projectOpId);
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOp), statJoinOpId);
        benchmarkStreamingJob->AddOperator(std::move(uPtrSinkOp), windowOpId);
    }

    template<typename Duration>
    void MixedWorkloadYSBNYT<Duration>::RunBenchmark(uint64_t durationInSec)
    {
        spdlog::info("Starting benchmark : {}", this->GetBenchmarkName());
        startTime_ = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
        for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
            queryIds_.emplace_back(executionEngine_->Submit(*benchmarkStreamingJob));
        }
        std::this_thread::sleep_for(std::chrono::seconds(durationInSec));
        try {
            for (const auto& jobId: queryIds_) {
                executionEngine_->Cancel(jobId);
            }
        }
        catch (const enjima::runtime::CancellationException& e) {
            spdlog::error("Could not gracefully cancel job: {}", e.what());
        }

        endTime_ = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
        spdlog::info("Stopped benchmark : {}", this->GetBenchmarkName());
    }

    template<typename Duration>
    MixedWorkloadYSBNYT<Duration>::MixedWorkloadYSBNYT() : StreamingBenchmark("mixedysbnyt")
    {
    }

    template<typename Duration>
    void MixedWorkloadYSBNYT<Duration>::SetDataPath(const std::string& path)
    {
        nytDataPath_ = path;
    }

    template<typename Duration>
    constexpr bool MixedWorkloadYSBNYT<Duration>::IsMixedWorkload() const
    {
        return true;
    }

    template<typename Duration>
    std::string MixedWorkloadYSBNYT<Duration>::GetSourceOperatorName(const std::string& jobIdSuffix)
    {
        if (std::stoi(jobIdSuffix) % 2 == 1) {
            return GetSuffixedOperatorName(kNYTSrcOpName_, jobIdSuffix);
        }
        else {
            return GetSuffixedOperatorName(kYSBSrcOpName_, jobIdSuffix);
        }
    }

    template<typename Duration>
    std::string MixedWorkloadYSBNYT<Duration>::GetSinkOperatorName(const std::string& jobIdSuffix)
    {
        if (std::stoi(jobIdSuffix) % 2 == 1) {
            return GetSuffixedOperatorName(kNYTSinkOpName_, jobIdSuffix);
        }
        else {
            return GetSuffixedOperatorName(kYSBSinkOpName_, jobIdSuffix);
        }
    }
}// namespace enjima::benchmarks::workload