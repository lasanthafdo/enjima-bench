//
// Created by t85kim on 28//11/24.
//

using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;

template<typename Duration>
using NYTDQSrcOpT = enjima::workload::operators::InMemoryRateLimitedNYTDQSourceOperator<Duration>;
template<typename Duration>
using FixedRateNYTDQSrcOpT = enjima::workload::operators::InMemoryFixedRateNYTDQSourceOperator<Duration>;

using NYTDQProjectFuncT = enjima::workload::functions::NYTDQMapProjectFunction;
using NYTDQSinkFuncT = enjima::workload::functions::NYTDQNoOpSinkFunction;
using NYTDQWindowAggFuncT = enjima::workload::functions::NYTDQTripAggFunction;

namespace enjima::benchmarks::workload {
    template<typename Duration>
    void NewYorkTaxiDQBenchmark<Duration>::SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate,
            bool generateWithEmit, bool useProcessingLatency)
    {
        if (useProcessingLatency) {
            throw enjima::runtime::IllegalArgumentException{
                    "NYTDQ benchmark does not support processing-latency-based source operators"};
        }
        auto jobIdSuffix = 0;
        for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
            auto jobIdSuffixStr = std::to_string(++jobIdSuffix);
            if (generateWithEmit) {
                operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                auto uPtrSrcOp = std::make_unique<NYTDQSrcOpT<Duration>>(srcOpId,
                        GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate);
                uPtrSrcOp->PopulateEventCache(nytDataPath_, &eventCache_);
                SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
            }
            else {
                operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                auto uPtrSrcOp = std::make_unique<FixedRateNYTDQSrcOpT<Duration>>(srcOpId,
                        GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate,
                        srcReservoirCapacity_);
                uPtrSrcOp->PopulateEventCache(nytDataPath_, &eventCache_);
                SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
            }
            if (schedulingMode_ == runtime::SchedulingMode::kThreadBased) {
                benchmarkStreamingJob->SetProcessingMode(processingMode_);
            }
        }
    }

    template<typename Duration>
    template<typename T>
        requires std::is_base_of_v<LatencyTrackingNYTDQSrcOpT<Duration>, T>
    void NewYorkTaxiDQBenchmark<Duration>::SetupPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
            runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr)
    {
        auto srcOpId = uPtrSrcOp->GetOperatorId();
        benchmarkStreamingJob->AddOperator(std::move(uPtrSrcOp));

        // Project
        operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
        auto uPtrProjectOp =
                std::make_unique<operators::MapOperator<NYTFullEventT, NYTDQProjEventT, NYTDQProjectFuncT>>(projectOpId,
                        GetSuffixedOperatorName(projectOpName_, jobIdSuffixStr), NYTDQProjectFuncT{});
        benchmarkStreamingJob->AddOperator(std::move(uPtrProjectOp), srcOpId);

        // Filter by vendor id and trip distance
        operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
        auto nytFilterFn = [](const NYTDQProjEventT& nytEvent) {
            return nytEvent.GetDropOffCellId() > 0 && nytEvent.GetPickupCellId() > 0 &&
                   nytEvent.GetVendorId() == "VTS" && nytEvent.GetTripDistance() > 5;
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
    void NewYorkTaxiDQBenchmark<Duration>::RunBenchmark(uint64_t durationInSec)
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
        }

        endTime_ = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
        spdlog::info("Stopped benchmark : {}", this->GetBenchmarkName());
    }

    template<typename Duration>
    NewYorkTaxiDQBenchmark<Duration>::NewYorkTaxiDQBenchmark() : StreamingBenchmark("nytdq")
    {
    }

    template<typename Duration>
    void NewYorkTaxiDQBenchmark<Duration>::SetDataPath(const std::string& path)
    {
        nytDataPath_ = path;
    }
}// namespace enjima::benchmarks::workload