//
// Created by m34ferna on 21/02/24.
//

template<typename Duration>
using YSBSrcOpT = enjima::workload::operators::InMemoryRateLimitedYSBSourceOperator<Duration>;
template<typename Duration>
using FixedRateYSBSrcOpT = enjima::workload::operators::InMemoryFixedRateYSBSourceOperator<Duration>;
template<typename Duration>
using FixedRateYSBSrcOpWithConceptDriftT =
        enjima::workload::operators::InMemoryFixedRateYSBSourceOperatorWithConceptDrift<Duration>;
template<typename Duration>
using FixedRateYSBSrcOpWithProcLatencyT =
        enjima::workload::operators::InMemoryFixedRateYSBSourceOperatorWithProcLatency<Duration>;

template<typename TInput, typename TFunc, typename Duration>
using YSBSinkOpT = enjima::operators::GenericSinkOperator<TInput, TFunc, Duration>;

using YSBProjFuncT = enjima::workload::functions::YSBProjectFunction;
using YSBKeyExtractFuncT = enjima::workload::functions::YSBKeyExtractFunction;
using YSBEquiJoinFuncT = enjima::workload::functions::YSBEquiJoinFunction;
using YSBAggFuncT = enjima::workload::functions::YSBCampaignAggFunction;
using YSBSinkFuncT = enjima::workload::functions::YSBNoOpSinkFunction;

namespace enjima::benchmarks::workload {
    template<typename Duration>
    void YahooStreamingBenchmark<Duration>::SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate,
            bool generateWithEmit, bool useProcessingLatency)
    {
        auto jobIdSuffix = 0;
        auto useConceptDrift = shiftIntervalMs_ > 0 && minInputRate_ == 0 && stepSize_ == "0";
        for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
            auto jobIdSuffixStr = std::to_string(++jobIdSuffix);
            if (generateWithEmit) {
                enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                auto uPtrSrcOp = std::make_unique<YSBSrcOpT<Duration>>(srcOpId,
                        GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate);
                uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
                SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
            }
            else if (useProcessingLatency) {
                enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                auto uPtrSrcOp = std::make_unique<FixedRateYSBSrcOpWithProcLatencyT<Duration>>(srcOpId,
                        GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate,
                        srcReservoirCapacity_);
                uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
                SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
            }
            else if (useConceptDrift) {
                enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                auto uPtrSrcOp = std::make_unique<FixedRateYSBSrcOpWithConceptDriftT<Duration>>(srcOpId,
                        GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate,
                        srcReservoirCapacity_, minInputRate_, stepSize_, shiftIntervalMs_);
                uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
                SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
            }
            else {
                enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                auto uPtrSrcOp = std::make_unique<FixedRateYSBSrcOpT<Duration>>(srcOpId,
                        GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate,
                        srcReservoirCapacity_, minInputRate_, stepSize_, shiftIntervalMs_);
                uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
                SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
            }
            if (schedulingMode_ == runtime::SchedulingMode::kThreadBased) {
                benchmarkStreamingJob->SetProcessingMode(processingMode_);
            }
        }
    }

    template<typename Duration>
    template<typename T>
        requires std::is_base_of_v<LatencyTrackingYSBSrcOpT<Duration>, T>
    void YahooStreamingBenchmark<Duration>::SetupPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
            runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr)
    {
        operators::OperatorID filterOpId = this->executionEngine_->GetNextOperatorId();
        auto filterFn = [](const YSBAdT& ysbTypeEvent) { return ysbTypeEvent.GetEventType() == 0; };
        auto uPtrFilterOp = std::make_unique<operators::FilterOperator<YSBAdT, decltype(filterFn)>>(filterOpId,
                GetSuffixedOperatorName(this->kFilterOpName_, jobIdSuffixStr), filterFn);

        operators::OperatorID projectOpId = this->executionEngine_->GetNextOperatorId();
        auto uPtrProjectOp = std::make_unique<operators::MapOperator<YSBAdT, YSBProjT, YSBProjFuncT>>(projectOpId,
                GetSuffixedOperatorName(this->kProjectOpName_, jobIdSuffixStr), YSBProjFuncT{});

        operators::OperatorID statJoinOpId = this->executionEngine_->GetNextOperatorId();
        auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();
        auto uPtrStaticEqJoinOp = std::make_unique<operators::StaticEquiJoinOperator<YSBProjT, uint64_t, YSBCampT,
                uint64_t, YSBKeyExtractFuncT, YSBEquiJoinFuncT>>(statJoinOpId,
                GetSuffixedOperatorName(this->kStaticEqJoinOpName_, jobIdSuffixStr), YSBKeyExtractFuncT{},
                YSBEquiJoinFuncT{}, adIdCampaignIdMap);

        operators::OperatorID windowOpId = this->executionEngine_->GetNextOperatorId();
        auto uPtrWindowOp =
                std::make_unique<operators::FixedEventTimeWindowOperator<YSBCampT, YSBWinT, true, YSBAggFuncT>>(
                        windowOpId, GetSuffixedOperatorName(this->kWindowOpName_, jobIdSuffixStr), YSBAggFuncT{},
                        std::chrono::seconds(10));

        operators::OperatorID sinkOpId = this->executionEngine_->GetNextOperatorId();
        auto uPtrSinkOp = std::make_unique<YSBSinkOpT<YSBWinT, YSBSinkFuncT, Duration>>(sinkOpId,
                GetSuffixedOperatorName(this->sinkOpName_, jobIdSuffixStr), YSBSinkFuncT{});

        auto srcOpId = uPtrSrcOp->GetOperatorId();
        benchmarkStreamingJob->AddOperator(std::move(uPtrSrcOp));
        benchmarkStreamingJob->AddOperator(std::move(uPtrFilterOp), srcOpId);
        benchmarkStreamingJob->AddOperator(std::move(uPtrProjectOp), filterOpId);
        benchmarkStreamingJob->AddOperator(std::move(uPtrStaticEqJoinOp), projectOpId);
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOp), statJoinOpId);
        benchmarkStreamingJob->AddOperator(std::move(uPtrSinkOp), windowOpId);
    }

    template<typename Duration>
    void YahooStreamingBenchmark<Duration>::RunBenchmark(uint64_t durationInSec)
    {
        spdlog::info("Starting benchmark : {}", this->GetBenchmarkName());
        for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
            queryIds_.emplace_back(executionEngine_->Submit(*benchmarkStreamingJob));
        }
        startTime_ = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
        std::this_thread::sleep_for(std::chrono::seconds(durationInSec));
        try {
            for (const auto& jobId: queryIds_) {
                executionEngine_->Cancel(jobId, std::chrono::seconds{1});
            }
        }
        catch (const enjima::runtime::CancellationException& e) {
            spdlog::error("Could not gracefully cancel job: {}", e.what());
        }
        endTime_ = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
        spdlog::info("Stopped benchmark : {}", this->GetBenchmarkName());
    }

    template<typename Duration>
    YahooStreamingBenchmark<Duration>::YahooStreamingBenchmark() : StreamingBenchmark("ysb")
    {
    }
}// namespace enjima::benchmarks::workload
