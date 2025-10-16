//
// Created by t86kim on 20/11/24.
//

#ifndef ENJIMA_BENCHMARKS_NEWYORK_TAXI_BENCHMARK_H
#define ENJIMA_BENCHMARKS_NEWYORK_TAXI_BENCHMARK_H

#include "StreamingBenchmark.h"
#include "enjima/benchmarks/workload/functions/NYTFunctions.h"
#include "enjima/benchmarks/workload/functions/NYTUtilities.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateNYTSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryRateLimitedNYTSourceOperator.h"
#include "enjima/operators/FixedEventTimeWindowJoinOperator.h"
#include "enjima/operators/FixedEventTimeWindowOperator.h"
#include "enjima/operators/GenericSinkOperator.h"
#include "enjima/operators/MapOperator.h"
#include "enjima/operators/NoOpSinkOperator.h"
#include "enjima/operators/SlidingEventTimeWindowOperator.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/StreamingJob.h"

namespace enjima::benchmarks::workload {
    template<typename Duration>
    using LatencyTrackingNYTSrcOpT = enjima::operators::LatencyTrackingSourceOperator<NYTEventT, Duration>;

    template<typename Duration = std::chrono::milliseconds>
    class NewYorkTaxiBenchmark : public StreamingBenchmark {
    public:
        NewYorkTaxiBenchmark();
        void SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate, bool generateWithEmit,
                bool useProcessingLatency) override;
        void RunBenchmark(uint64_t durationInSec) override;
        void SetDataPath(const std::string& path);

    private:
        template<typename T>
            requires std::is_base_of_v<LatencyTrackingNYTSrcOpT<Duration>, T>
        void SetupPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
                runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr);

        inline static const long kWindowDurationBase_{10};
        std::string multiMapOpName_ = "splitMap";
        std::string emptyTaxiSlidingWindowOpName_ = "emptyTaxiSlidingWindow";
        std::string emptyTaxiCountWindowOpName_ = "emptyTaxiCountWindow";
        std::string profitWindowOpName_ = "profitWindow";
        std::string profitabilityWindowJoinOpName_ = "profitabilityWindowJoin";
        std::string nytDataPath_;
        std::vector<NewYorkTaxiT> eventCache_;
    };
}// namespace enjima::benchmarks::workload

#include "NewYorkTaxiBenchmark.tpp"

#endif//ENJIMA_BENCHMARKS_NEWYORK_TAXI_BENCHMARK_H