//
// Created by t85kim on 28//11/24.
//

using NYTEventT = enjima::api::data_types::NewYorkTaxiEventCondensed;
using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;

template<typename Duration>
using NYTSrcOpT = enjima::workload::operators::InMemoryRateLimitedNYTSourceOperator<Duration>;
template<typename Duration>
using FixedRateNYTSrcOpT = enjima::workload::operators::InMemoryFixedRateNYTSourceOperator<Duration>;

using NYTProjectFuncT = enjima::workload::functions::NYTMapProjectFunction;
using NYTEmptyTaxiCountFuncT = enjima::workload::functions::NYTEmptyTaxiCountFunction;
using NYTEmptyCountMultiKeyExtractFuncT = enjima::workload::functions::NYTEmptyTaxiCountMultiKeyExtractFunction;
using NYTProfitMultiKeyExtractFuncT = enjima::workload::functions::NYTProfitMultiKeyExtractFunction;
using NYTProfitJoinFuncT = enjima::workload::functions::NYTProfitJoinFunction;
using NYTProfitSinkFuncT = enjima::workload::functions::NYTProfitSinkFunction;

namespace enjima::benchmarks::workload {
    template<typename Duration>
    void NewYorkTaxiBenchmark<Duration>::SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate,
            bool generateWithEmit, bool useProcessingLatency)
    {
        if (useProcessingLatency) {
            throw enjima::runtime::IllegalArgumentException{
                    "NYT benchmark does not support processing-latency-based source operators"};
        }
        auto jobIdSuffix = 0;
        for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
            auto jobIdSuffixStr = std::to_string(++jobIdSuffix);
            if (generateWithEmit) {
                enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                auto uPtrSrcOp = std::make_unique<NYTSrcOpT<Duration>>(srcOpId,
                        GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate);
                uPtrSrcOp->PopulateEventCache(nytDataPath_);
                SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
            }
            else {
                enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                auto uPtrSrcOp = std::make_unique<FixedRateNYTSrcOpT<Duration>>(srcOpId,
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
        requires std::is_base_of_v<LatencyTrackingNYTSrcOpT<Duration>, T>
    void NewYorkTaxiBenchmark<Duration>::SetupPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
            runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr)
    {
        auto srcOpId = uPtrSrcOp->GetOperatorId();
        benchmarkStreamingJob->AddOperator(std::move(uPtrSrcOp));

        // Split into 2
        enjima::operators::OperatorID multiMapOpId = executionEngine_->GetNextOperatorId();
        auto uPtrMultiMapOp =
                std::make_unique<enjima::operators::MapOperator<NYTEventT, NYTEventProjT, NYTProjectFuncT, 2>>(
                        multiMapOpId, GetSuffixedOperatorName(multiMapOpName_, jobIdSuffixStr), NYTProjectFuncT{});
        benchmarkStreamingJob->AddOperator(std::move(uPtrMultiMapOp), srcOpId);

        // Empty Taxi
        /// Gets last drop-off locations for each taxi at the time of materialization
        enjima::operators::OperatorID emptyTaxiOpId = executionEngine_->GetNextOperatorId();
        auto emptyAggFunc = enjima::api::MergeableKeyedAggregateFunction<NYTEventProjT, NYTEmptyReportT>{};
        auto uPtrWindowOpEmptyTaxi = std::make_unique<enjima::operators::SlidingEventTimeWindowOperator<NYTEventProjT,
                NYTEmptyReportT, decltype(emptyAggFunc)>>(emptyTaxiOpId,
                GetSuffixedOperatorName(emptyTaxiSlidingWindowOpName_, jobIdSuffixStr), emptyAggFunc,
                std::chrono::seconds(kWindowDurationBase_ * 15), std::chrono::seconds(kWindowDurationBase_));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOpEmptyTaxi), multiMapOpId);
        /// Counts number of taxis per cell
        enjima::operators::OperatorID emptyTaxiCountOpId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowOpEmptyTaxiCount =
                std::make_unique<enjima::operators::FixedEventTimeWindowOperator<NYTEmptyReportT, NYTEmptyCountT, true,
                        NYTEmptyTaxiCountFuncT>>(emptyTaxiCountOpId,
                        GetSuffixedOperatorName(emptyTaxiCountWindowOpName_, jobIdSuffixStr), NYTEmptyTaxiCountFuncT{},
                        std::chrono::seconds(kWindowDurationBase_));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOpEmptyTaxiCount), emptyTaxiOpId);

        // Profit
        /// Computes median of fare+tip amount for each cell at the time of materialization
        enjima::operators::OperatorID profitOpId = executionEngine_->GetNextOperatorId();
        auto profitAggFunc = enjima::api::MergeableKeyedAggregateFunction<NYTEventProjT, NYTProfitReportT>{};
        auto uPtrWindowOpProfit = std::make_unique<enjima::operators::SlidingEventTimeWindowOperator<NYTEventProjT,
                NYTProfitReportT, decltype(profitAggFunc)>>(profitOpId,
                GetSuffixedOperatorName(profitWindowOpName_, jobIdSuffixStr), profitAggFunc,
                std::chrono::seconds(kWindowDurationBase_ * 15), std::chrono::seconds(kWindowDurationBase_));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowOpProfit), multiMapOpId);

        // Profitability - joining empty taxi counts and profit
        enjima::operators::OperatorID profitabilityOpId = executionEngine_->GetNextOperatorId();
        auto uPtrWindowJoinProfit = std::make_unique<enjima::operators::FixedEventTimeWindowJoinOperator<NYTEmptyCountT,
                NYTProfitReportT, std::pair<int, int>, NYTEmptyCountMultiKeyExtractFuncT, NYTProfitMultiKeyExtractFuncT,
                NYTProfitabilityT, NYTProfitJoinFuncT>>(profitabilityOpId,
                GetSuffixedOperatorName(profitabilityWindowJoinOpName_, jobIdSuffixStr),
                NYTEmptyCountMultiKeyExtractFuncT{}, NYTProfitMultiKeyExtractFuncT{}, NYTProfitJoinFuncT{},
                std::chrono::seconds(kWindowDurationBase_));
        benchmarkStreamingJob->AddOperator(std::move(uPtrWindowJoinProfit),
                std::make_pair(emptyTaxiCountOpId, profitOpId));

        // Sink
        enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
        auto sinkFn = NYTProfitSinkFuncT{};
        auto uPtrSinkOp =
                std::make_unique<enjima::operators::GenericSinkOperator<NYTProfitabilityT, NYTProfitSinkFuncT>>(
                        sinkOpId, GetSuffixedOperatorName(sinkOpName_, jobIdSuffixStr), sinkFn);
        benchmarkStreamingJob->AddOperator(std::move(uPtrSinkOp), profitabilityOpId);
    }

    template<typename Duration>
    void NewYorkTaxiBenchmark<Duration>::RunBenchmark(uint64_t durationInSec)
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
    NewYorkTaxiBenchmark<Duration>::NewYorkTaxiBenchmark() : StreamingBenchmark("nyt")
    {
    }

    template<typename Duration>
    void NewYorkTaxiBenchmark<Duration>::SetDataPath(const std::string& path)
    {
        nytDataPath_ = path;
    }
}// namespace enjima::benchmarks::workload