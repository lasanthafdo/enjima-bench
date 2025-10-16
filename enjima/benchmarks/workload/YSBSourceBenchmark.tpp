//
// Created by m34ferna on 21/02/24.
//

template<typename Duration>
using YSBSrcOpT = enjima::workload::operators::InMemoryRateLimitedYSBSourceOperator<Duration>;
template<typename Duration>
using FixedRateYSBSrcOpT = enjima::workload::operators::InMemoryFixedRateYSBSourceOperator<Duration>;
template<typename TInput, typename TFunc, typename Duration>
using YSBSinkOpT = enjima::operators::GenericSinkOperator<TInput, TFunc, Duration>;

using YSBSourceBenchSinkFuncT = enjima::workload::functions::YSBGenericNoOpSinkFunction<YSBAdT>;

namespace enjima::benchmarks::workload {
    template<typename Duration>
    void YSBSourceBenchmark<Duration>::SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate,
            bool generateWithEmit, bool useProcessingLatency)
    {
        auto jobIdSuffix = 0;
        for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
            auto jobIdSuffixStr = std::to_string(++jobIdSuffix);
            if (generateWithEmit) {
                enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                auto uPtrSrcOp = std::make_unique<YSBSrcOpT<Duration>>(srcOpId,
                        GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate);
                uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
                SetupPipelineWithSourceOp(uPtrSrcOp, benchmarkStreamingJob, jobIdSuffixStr);
            }
            else {
                enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
                auto uPtrSrcOp = std::make_unique<FixedRateYSBSrcOpT<Duration>>(srcOpId,
                        GetSuffixedOperatorName(srcOpName_, jobIdSuffixStr), latencyRecEmitPeriodMs, maxInputRate,
                        srcReservoirCapacity_);
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
    void YSBSourceBenchmark<Duration>::SetupPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
            runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr)
    {
        operators::OperatorID sinkOpId = this->executionEngine_->GetNextOperatorId();
        auto uPtrSinkOp = std::make_unique<YSBSinkOpT<YSBAdT, YSBSourceBenchSinkFuncT, Duration>>(sinkOpId,
                GetSuffixedOperatorName(this->sinkOpName_, jobIdSuffixStr), YSBSourceBenchSinkFuncT{});

        auto srcOpId = uPtrSrcOp->GetOperatorId();
        benchmarkStreamingJob->AddOperator(std::move(uPtrSrcOp));
        benchmarkStreamingJob->AddOperator(std::move(uPtrSinkOp), srcOpId);
    }

    template<typename Duration>
    void YSBSourceBenchmark<Duration>::RunBenchmark(uint64_t durationInSec)
    {
        spdlog::info("Starting benchmark : {}", this->GetBenchmarkName());
        for (const auto& benchmarkStreamingJob: benchmarkStreamingJobs_) {
            queryIds_.emplace_back(executionEngine_->Submit(*benchmarkStreamingJob));
        }
        startTime_ = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
        std::this_thread::sleep_for(std::chrono::seconds(durationInSec));
        for (const auto& jobId: queryIds_) {
            executionEngine_->Cancel(jobId, std::chrono::seconds{1});
        }
        endTime_ = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
        spdlog::info("Stopped benchmark : {}", this->GetBenchmarkName());
    }

    template<typename Duration>
    YSBSourceBenchmark<Duration>::YSBSourceBenchmark() : StreamingBenchmark("ysbsb")
    {
    }
}// namespace enjima::benchmarks::workload
