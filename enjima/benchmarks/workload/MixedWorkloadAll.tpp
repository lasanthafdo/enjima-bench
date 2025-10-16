//
// Created by m34ferna on 21/02/24.
//

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;

template<typename Duration>
using LRBSrcOpT = enjima::workload::operators::InMemoryRateLimitedLRBSourceOperator<Duration>;
template<typename Duration>
using FixedRateLRBSrcOpT = enjima::workload::operators::InMemoryFixedRateLRBSourceOperator<Duration>;
template<typename Duration>
using FixedRateLRBSrcOpWithProcLatencyT =
        enjima::workload::operators::InMemoryFixedRateLRBSourceOperatorWithProcLatency<Duration>;
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

using LRBProjFuncT = enjima::workload::functions::LRBEventProjectFunction;
using LRBCountAggFuncT = enjima::workload::functions::LRBCountAggFunction;
using LRBSpeedAggFuncT = enjima::workload::functions::LRBSpeedAggFunction;
using LRBCntMultiKeyExtractFuncT = enjima::workload::functions::LRBCntReportMultiKeyExtractFunction;
using LRBSpdMultiKeyExtractFuncT = enjima::workload::functions::LRBSpdReportMultiKeyExtractFunction;
using LRBSpdCntJoinFuncT = enjima::workload::functions::LRBSpdCntJoinFunction;
using LRBTollAcdCoGroupFuncT = enjima::workload::functions::LRBTollAcdCoGroupFunction;

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
    void MixedWorkloadAll<Duration>::SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate,
            bool generateWithEmit, bool useProcessingLatency)
    {
        throw std::runtime_error("Please call SetUpMixedWorkloadPipeline() instead.");
    }

    template<typename Duration>
    void MixedWorkloadAll<Duration>::SetUpMixedWorkloadPipeline(uint64_t latencyRecEmitPeriodMs,
            uint64_t maxInputRateLRB, uint64_t maxInputRateYSB, uint64_t maxInputRateNYT, bool generateWithEmit,
            bool useProcessingLatency)
    {
        auto jobIdSuffix = 0;
        // TODO Introduce stirng utils
        auto delim = '_';
        std::vector<int> queryDistVec;
        if (size_t tokEnd = queryDistributionStr_.find(delim); tokEnd != std::string::npos) {
            size_t tokStart = 0;
            auto cumulativeCount = 0;
            while (tokEnd != std::string::npos) {
                const auto currentQueryTypeCount = std::stoi(queryDistributionStr_.substr(tokStart, tokEnd - tokStart));
                queryDistVec.emplace_back(currentQueryTypeCount + cumulativeCount);
                cumulativeCount += currentQueryTypeCount;
                tokStart = tokEnd + 1;
                tokEnd = queryDistributionStr_.find(delim, tokStart);
            }
        }

        for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
            auto jobIdSuffixStr = std::to_string(++jobIdSuffix);
            int workloadIdForQuery = 0;
            if (queryDistVec.empty()) {
                workloadIdForQuery = jobIdSuffix % 3;
            }
            else {
                if (jobIdSuffix > queryDistVec[1]) {
                    workloadIdForQuery = 0;
                }
                else if (jobIdSuffix > queryDistVec[0]) {
                    workloadIdForQuery = 2;
                }
                else {
                    workloadIdForQuery = 1;
                }
            }
            if (workloadIdForQuery == 1) {
                if (generateWithEmit) {
                    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                    auto uPtrSrcOp = std::make_unique<LRBSrcOpT<Duration>>(srcOpId,
                            GetSuffixedOperatorName(kLRBSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                            maxInputRateLRB);
                    uPtrSrcOp->PopulateEventCache(lrbDataPath_);
                    SetupLRBPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                }
                else {
                    if (useProcessingLatency) {
                        enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                        auto uPtrSrcOp = std::make_unique<FixedRateLRBSrcOpWithProcLatencyT<Duration>>(srcOpId,
                                GetSuffixedOperatorName(kLRBSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                                maxInputRateLRB, srcReservoirCapacity_);
                        uPtrSrcOp->PopulateEventCache(lrbDataPath_, &lrbEventCache_);
                        SetupLRBPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                    }
                    else {
                        enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                        auto uPtrSrcOp = std::make_unique<FixedRateLRBSrcOpT<Duration>>(srcOpId,
                                GetSuffixedOperatorName(kLRBSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                                maxInputRateLRB, srcReservoirCapacity_);
                        uPtrSrcOp->PopulateEventCache(lrbDataPath_, &lrbEventCache_);
                        SetupLRBPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                    }
                }
            }
            else if (workloadIdForQuery == 2) {
                if (generateWithEmit) {
                    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                    auto uPtrSrcOp = std::make_unique<YSBSrcOpT<Duration>>(srcOpId,
                            GetSuffixedOperatorName(kYSBSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                            maxInputRateYSB);
                    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
                    SetupYSBPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                }
                else {
                    if (useProcessingLatency) {
                        enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                        auto uPtrSrcOp = std::make_unique<FixedRateYSBSrcOpWithProcLatencyT<Duration>>(srcOpId,
                                GetSuffixedOperatorName(kYSBSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                                maxInputRateYSB, srcReservoirCapacity_);
                        uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
                        SetupYSBPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                    }
                    else {
                        enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                        auto uPtrSrcOp = std::make_unique<FixedRateYSBSrcOpT<Duration>>(srcOpId,
                                GetSuffixedOperatorName(kYSBSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                                maxInputRateYSB, srcReservoirCapacity_);
                        uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
                        SetupYSBPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                    }
                }
            }
            else {
                if (generateWithEmit) {
                    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                    auto uPtrSrcOp = std::make_unique<NYTODQSrcOpT<Duration>>(srcOpId,
                            GetSuffixedOperatorName(kNYTSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                            maxInputRateNYT);
                    uPtrSrcOp->PopulateEventCache(nytDataPath_, &nytEventCache_);
                    SetupNYTPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                }
                else {
                    if (useProcessingLatency) {
                        enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                        auto uPtrSrcOp = std::make_unique<FixedRateNYTODQSrcOpWithProcLatencyT<Duration>>(srcOpId,
                                GetSuffixedOperatorName(kNYTSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                                maxInputRateNYT, srcReservoirCapacity_);
                        uPtrSrcOp->PopulateEventCache(nytDataPath_, &nytEventCache_);
                        SetupNYTPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                    }
                    else {
                        enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                        auto uPtrSrcOp = std::make_unique<FixedRateNYTODQSrcOpT<Duration>>(srcOpId,
                                GetSuffixedOperatorName(kNYTSrcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs,
                                maxInputRateNYT, srcReservoirCapacity_);
                        uPtrSrcOp->PopulateEventCache(nytDataPath_, &nytEventCache_);
                        SetupNYTPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
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
        requires std::is_base_of_v<LatencyTrackingLRBSrcOpT<Duration>, T>
    void MixedWorkloadAll<Duration>::SetupLRBPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
            runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr)
    {
        auto srcOpId = uPtrSrcOp->GetOperatorId();
        benchmarkStreamingJob->AddOperator(std::move(uPtrSrcOp));

        // Filtering position reports
        enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
        auto filterFn = [](const LinearRoadT& lrTypeEvent) { return lrTypeEvent.GetType() == 0; };
        auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<LinearRoadT, decltype(filterFn)>>(
                filterOpId, GetSuffixedOperatorName(kLRBFilterOpName_, jobIdSuffixStr), filterFn);
        benchmarkStreamingJob->AddOperator(std::move(uPtrFilterOp), srcOpId);

        // Project
        // You can split into 3 here and avoid an extra "Identity" operator
        enjima::operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
        auto uPtrProjOp = std::make_unique<enjima::operators::MapOperator<LinearRoadT, LRBProjEventT, LRBProjFuncT, 3>>(
                projectOpId, GetSuffixedOperatorName(kLRBProjOpName_, jobIdSuffixStr), LRBProjFuncT{});
        benchmarkStreamingJob->AddOperator(std::move(uPtrProjOp), filterOpId);

        // Accident tracking: upstream at multiMapOpId
        enjima::operators::OperatorID acdFilterOpId = executionEngine_->GetNextOperatorId();
        auto acdFilterFn = [](const LRBProjEventT& lrInputEvent) {
            return (1 <= lrInputEvent.GetLane()) && (lrInputEvent.GetLane() <= 3);
        };
        auto uPtrAcdFilterOp =
                std::make_unique<enjima::operators::FilterOperator<LRBProjEventT, decltype(acdFilterFn)>>(acdFilterOpId,
                        GetSuffixedOperatorName(kLRBAcdFilterOpName_, jobIdSuffixStr), acdFilterFn);
        benchmarkStreamingJob->AddOperator(std::move(uPtrAcdFilterOp), projectOpId);

        enjima::operators::OperatorID windowOpAcdId = executionEngine_->GetNextOperatorId();
        auto acdAggfunc = enjima::api::MergeableKeyedAggregateFunction<LRBProjEventT, LRBAcdReportT>{};
        auto uPtrWindowOpAcd = std::make_unique<
                enjima::operators::SlidingEventTimeWindowOperator<LRBProjEventT, LRBAcdReportT, decltype(acdAggfunc)>>(
                windowOpAcdId, GetSuffixedOperatorName(kLRBAcdWindowOpName_, jobIdSuffixStr), acdAggfunc,
                std::chrono::seconds(120), std::chrono::seconds(30));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOpAcd), acdFilterOpId);

        // Speed monitor: upstream at multiMapOpId
        enjima::operators::OperatorID windowOpSpdId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowOpSpd = std::make_unique<
                enjima::operators::FixedEventTimeWindowOperator<LRBProjEventT, LRBSpdReportT, true, LRBSpeedAggFuncT>>(
                windowOpSpdId, GetSuffixedOperatorName(kLRBSpdWindowOpName_, jobIdSuffixStr), LRBSpeedAggFuncT{},
                std::chrono::seconds(60));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOpSpd), projectOpId);

        // Count vehicles: upstream at multiMapOpId
        enjima::operators::OperatorID windowOpCntId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowOpCnt = std::make_unique<
                enjima::operators::FixedEventTimeWindowOperator<LRBProjEventT, LRBCntReportT, true, LRBCountAggFuncT>>(
                windowOpCntId, GetSuffixedOperatorName(kLRBCntWindowOpName_, jobIdSuffixStr), LRBCountAggFuncT{},
                std::chrono::seconds(60));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOpCnt), projectOpId);

        // Join Count and Speed
        enjima::operators::OperatorID windowJoinCntSpdId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowJoinCntSpd = std::make_unique<enjima::operators::FixedEventTimeWindowJoinOperator<LRBSpdReportT,
                LRBCntReportT, std::tuple<int, int, int, uint64_t>, LRBSpdMultiKeyExtractFuncT,
                LRBCntMultiKeyExtractFuncT, LRBTollReportT, LRBSpdCntJoinFuncT>>(windowJoinCntSpdId,
                GetSuffixedOperatorName(kLRBCntSpdWindowJoinOpName_, jobIdSuffixStr), LRBSpdMultiKeyExtractFuncT{},
                LRBCntMultiKeyExtractFuncT{}, LRBSpdCntJoinFuncT{}, std::chrono::seconds(60));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowJoinCntSpd),
                std::make_pair(windowOpSpdId, windowOpCntId));

        // Join above with accident
        enjima::operators::OperatorID windowCoGroupOpId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowJoinTollAcd =
                std::make_unique<enjima::operators::FixedEventTimeWindowCoGroupOperator<LRBTollReportT, LRBAcdReportT,
                        LRBTollFinalReportT, LRBTollAcdCoGroupFuncT>>(windowCoGroupOpId,
                        GetSuffixedOperatorName(kLRBAcdTollCoGroupOpName_, jobIdSuffixStr), LRBTollAcdCoGroupFuncT{},
                        std::chrono::seconds(60));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowJoinTollAcd),
                std::make_pair(windowJoinCntSpdId, windowOpAcdId));

        // Sink
        enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
        auto uPtrSinkOp = std::make_unique<enjima::operators::NoOpSinkOperator<LRBTollFinalReportT, Duration>>(sinkOpId,
                GetSuffixedOperatorName(kLRBSinkOpName_, jobIdSuffixStr));
        benchmarkStreamingJob->AddOperator(std::move(uPtrSinkOp), windowCoGroupOpId);
    }

    template<typename Duration>
    template<typename T>
        requires std::is_base_of_v<LatencyTrackingYSBSrcOpT<Duration>, T>
    void MixedWorkloadAll<Duration>::SetupYSBPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
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
    template<typename T>
        requires std::is_base_of_v<LatencyTrackingNYTODQSrcOpT<Duration>, T>
    void MixedWorkloadAll<Duration>::SetupNYTPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
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
    void MixedWorkloadAll<Duration>::RunBenchmark(uint64_t durationInSec)
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
    MixedWorkloadAll<Duration>::MixedWorkloadAll() : StreamingBenchmark("mixedall")
    {
    }

    template<typename Duration>
    void MixedWorkloadAll<Duration>::SetLRBDataPath(const std::string& path)
    {
        lrbDataPath_ = path;
    }

    template<typename Duration>
    void MixedWorkloadAll<Duration>::SetNYTDataPath(const std::string& path)
    {
        nytDataPath_ = path;
    }

    template<typename Duration>
    constexpr bool MixedWorkloadAll<Duration>::IsMixedWorkload() const
    {
        return true;
    }

    template<typename Duration>
    std::string MixedWorkloadAll<Duration>::GetSourceOperatorName(const std::string& jobIdSuffix)
    {
        auto workloadTypeId = std::stoi(jobIdSuffix) % 3;
        if (workloadTypeId == 1) {
            return GetSuffixedOperatorName(kLRBSrcOpName_, jobIdSuffix);
        }
        if (workloadTypeId == 2) {
            return GetSuffixedOperatorName(kYSBSrcOpName_, jobIdSuffix);
        }
        return GetSuffixedOperatorName(kNYTSrcOpName_, jobIdSuffix);
    }

    template<typename Duration>
    std::string MixedWorkloadAll<Duration>::GetSinkOperatorName(const std::string& jobIdSuffix)
    {
        auto workloadTypeId = std::stoi(jobIdSuffix) % 3;
        if (workloadTypeId == 1) {
            return GetSuffixedOperatorName(kLRBSinkOpName_, jobIdSuffix);
        }
        if (workloadTypeId == 2) {
            return GetSuffixedOperatorName(kYSBSinkOpName_, jobIdSuffix);
        }
        return GetSuffixedOperatorName(kNYTSinkOpName_, jobIdSuffix);
    }
}// namespace enjima::benchmarks::workload