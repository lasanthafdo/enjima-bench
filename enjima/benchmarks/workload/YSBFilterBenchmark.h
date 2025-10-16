//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_YSB_FILTER_BENCHMARK_H
#define ENJIMA_BENCHMARKS_YSB_FILTER_BENCHMARK_H

#include "StreamingBenchmark.h"
#include "enjima/benchmarks/workload/functions/YSBFunctions.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateYSBSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryRateLimitedYSBSourceOperator.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/operators/GenericSinkOperator.h"
#include "enjima/runtime/StreamingJob.h"
#include "enjima/runtime/DataStream.h"

namespace enjima::benchmarks::workload {
    template<typename Duration>
    using LatencyTrackingYSBSrcOpT = enjima::operators::LatencyTrackingSourceOperator<YSBAdT, Duration>;

    template<typename Duration = std::chrono::milliseconds>
    class YSBFilterBenchmark : public StreamingBenchmark {
    public:
        YSBFilterBenchmark();
        void SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate, bool generateWithEmit,
                bool useProcessingLatency) override;
        void RunBenchmark(uint64_t durationInSec) override;

    private:
        template<typename T>
            requires std::is_base_of_v<LatencyTrackingYSBSrcOpT<Duration>, T>
        void SetupPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
                runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr);

        const std::string kFilterOpName_ = "eventFilter";

    };

}// namespace enjima::benchmarks::workload

#include "YSBFilterBenchmark.tpp"

#endif//ENJIMA_BENCHMARKS_YSB_FILTER_BENCHMARK_H
