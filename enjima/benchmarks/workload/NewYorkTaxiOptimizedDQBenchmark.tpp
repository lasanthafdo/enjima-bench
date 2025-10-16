//
// Created by t85kim on 28//11/24.
//

using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;

using NYTODQProjectFuncT = enjima::workload::functions::NYTODQMapProjectFunction;
using NYTDQSinkFuncT = enjima::workload::functions::NYTDQNoOpSinkFunction;
using NYTDQWindowAggFuncT = enjima::workload::functions::NYTDQTripAggFunction;

namespace enjima::benchmarks::workload {
    template<typename Duration>
    void NewYorkTaxiOptimizedDQBenchmark<Duration>::SetUpPipeline(uint64_t latencyRecEmitPeriodMs,
            uint64_t maxInputRate, bool generateWithEmit, bool useProcessingLatency)
    {
        useProcLatency_ = useProcessingLatency;
        auto jobIdSuffix = 0;
        for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
            auto jobIdSuffixStr = std::to_string(++jobIdSuffix);
            if (generateWithEmit) {
                operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                auto uPtrSrcOp = std::make_unique<NYTODQSrcOpT<Duration>>(srcOpId,
                        GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate);
                uPtrSrcOp->PopulateEventCache(nytDataPath_, &eventCache_);
                SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
            }
            else {
                if (useProcessingLatency) {
                    operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                    auto uPtrSrcOp = std::make_unique<FixedRateNYTODQSrcOpWithProcLatencyT<Duration>>(srcOpId,
                            GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate,
                            srcReservoirCapacity_);
                    uPtrSrcOp->PopulateEventCache(nytDataPath_, &eventCache_);
                    fixedRateSrcOpWithProcLatencyPtrMap_.emplace(jobIdSuffix, uPtrSrcOp.get());
                    SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
                }
                else {
                    operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                    auto uPtrSrcOp = std::make_unique<FixedRateNYTODQSrcOpT<Duration>>(srcOpId,
                            GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate,
                            srcReservoirCapacity_, minInputRate_, stepSize_, shiftIntervalMs_);
                    uPtrSrcOp->PopulateEventCache(nytDataPath_, &eventCache_);
                    fixedRateSrcOpPtrMap_.emplace(jobIdSuffix, uPtrSrcOp.get());
                    SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
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
    void NewYorkTaxiOptimizedDQBenchmark<Duration>::SetupPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
            runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr)
    {
        auto srcOpId = uPtrSrcOp->GetOperatorId();
        benchmarkStreamingJob->AddOperator(std::move(uPtrSrcOp));

        // Project
        operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
        auto uPtrProjectOp =
                std::make_unique<operators::MapOperator<NYTODQEventT, NYTDQProjEventT, NYTODQProjectFuncT>>(projectOpId,
                        GetSuffixedOperatorName(projectOpName_, jobIdSuffixStr), NYTODQProjectFuncT{});
        benchmarkStreamingJob->AddOperator(std::move(uPtrProjectOp), srcOpId);

        // Filter by vendor id and trip distance
        operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
        auto nytFilterFn = [](const NYTDQProjEventT& nytEvent) {
            return nytEvent.GetDropOffCellId() > 0 && nytEvent.GetPickupCellId() > 0 &&
                   nytEvent.GetVendorId() == "VTS" && nytEvent.GetTripDistance() > 1;
        };
        auto uPtrFilterOp = std::make_unique<operators::FilterOperator<NYTDQProjEventT, decltype(nytFilterFn)>>(
                filterOpId, GetSuffixedOperatorName(filterOpName_, jobIdSuffixStr), nytFilterFn);
        benchmarkStreamingJob->AddOperator(std::move(uPtrFilterOp), projectOpId);

        // Window
        operators::OperatorID windowOpId = this->executionEngine_->GetNextOperatorId();
        auto uPtrWindowOp = std::make_unique<
                operators::FixedEventTimeWindowOperator<NYTDQProjEventT, NYTDQWinT, true, NYTDQWindowAggFuncT>>(
                windowOpId, GetSuffixedOperatorName(this->aggWindowOpName_, jobIdSuffixStr), NYTDQWindowAggFuncT{},
                std::chrono::seconds(kWindowDurationBase_));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOp), filterOpId);


        // Sink
        operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
        auto uPtrSinkOp = std::make_unique<operators::GenericSinkOperator<NYTDQWinT, NYTDQSinkFuncT, Duration>>(
                sinkOpId, GetSuffixedOperatorName(this->sinkOpName_, jobIdSuffixStr), NYTDQSinkFuncT{});
        benchmarkStreamingJob->AddOperator(std::move(uPtrSinkOp), windowOpId);
    }

    template<typename Duration>
    void NewYorkTaxiOptimizedDQBenchmark<Duration>::RunBenchmark(uint64_t durationInSec)
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
        catch (const runtime::CancellationException& e) {
            spdlog::error("Could not gracefully cancel job: {}", e.what());
            if (useProcLatency_) {
                for (auto queryIdPtrEntry: fixedRateSrcOpWithProcLatencyPtrMap_) {
                    queryIdPtrEntry.second->CancelEventGeneration();
                }
            }
            else {
                for (auto queryIdPtrEntry: fixedRateSrcOpPtrMap_) {
                    queryIdPtrEntry.second->CancelEventGeneration();
                }
            }
        }

        endTime_ = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
        spdlog::info("Stopped benchmark : {}", this->GetBenchmarkName());
    }

    template<typename Duration>
    NewYorkTaxiOptimizedDQBenchmark<Duration>::NewYorkTaxiOptimizedDQBenchmark() : StreamingBenchmark("nytodq")
    {
    }

    template<typename Duration>
    void NewYorkTaxiOptimizedDQBenchmark<Duration>::SetDataPath(const std::string& path)
    {
        nytDataPath_ = path;
    }
}// namespace enjima::benchmarks::workload