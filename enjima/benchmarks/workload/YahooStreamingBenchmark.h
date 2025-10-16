//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_YAHOO_STREAMING_BENCHMARK_H
#define ENJIMA_BENCHMARKS_YAHOO_STREAMING_BENCHMARK_H

#include "StreamingBenchmark.h"
#include "enjima/benchmarks/workload/functions/YSBFunctions.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/operators/FixedEventTimeWindowOperator.h"
#include "enjima/operators/GenericSinkOperator.h"
#include "enjima/operators/MapOperator.h"
#include "enjima/operators/StaticEquiJoinOperator.h"
#include "enjima/runtime/StreamingJob.h"
#include "operators/InMemoryFixedRateYSBSourceOperator.h"
#include "operators/InMemoryFixedRateYSBSourceOperatorWithConceptDrift.h"
#include "operators/InMemoryFixedRateYSBSourceOperatorWithProcLatency.h"
#include "operators/InMemoryRateLimitedYSBSourceOperator.h"

namespace enjima::benchmarks::workload {
    template<typename Duration>
    using LatencyTrackingYSBSrcOpT = enjima::operators::LatencyTrackingSourceOperator<YSBAdT, Duration>;

    template<typename Duration = std::chrono::milliseconds>
    class YahooStreamingBenchmark : public StreamingBenchmark {
    public:
        YahooStreamingBenchmark();
        void SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate, bool generateWithEmit,
                bool useProcessingLatency) override;
        void RunBenchmark(uint64_t durationInSec) override;

    private:
        const std::string kFilterOpName_ = "eventFilter";
        const std::string kProjectOpName_ = "project";
        const std::string kStaticEqJoinOpName_ = "staticEqJoin";
        const std::string kWindowOpName_ = "timeWindow";

        template<typename T>
            requires std::is_base_of_v<LatencyTrackingYSBSrcOpT<Duration>, T>
        void SetupPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
                runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr);
    };

}// namespace enjima::benchmarks::workload

#include "YahooStreamingBenchmark.tpp"

#endif//ENJIMA_BENCHMARKS_YAHOO_STREAMING_BENCHMARK_H
