//
// Created by t86kim on 20/11/24.
//

#ifndef ENJIMA_BENCHMARKS_NEW_YORK_TAXI_ODQ_BENCHMARK_H
#define ENJIMA_BENCHMARKS_NEW_YORK_TAXI_ODQ_BENCHMARK_H

#include "StreamingBenchmark.h"
#include "enjima/benchmarks/workload/functions/NYTDQFunctions.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateNYTODQSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateNYTODQSourceOperatorWithProcLatency.h"
#include "enjima/benchmarks/workload/operators/InMemoryRateLimitedNYTODQSourceOperator.h"
#include "enjima/operators/FixedEventTimeWindowOperator.h"
#include "enjima/operators/GenericSinkOperator.h"
#include "enjima/operators/MapOperator.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/StreamingJob.h"

namespace enjima::benchmarks::workload {
    template<typename Duration>
    using LatencyTrackingNYTODQSrcOpT = enjima::operators::LatencyTrackingSourceOperator<NYTODQEventT, Duration>;
    template<typename Duration>
    using NYTODQSrcOpT = enjima::workload::operators::InMemoryRateLimitedNYTODQSourceOperator<Duration>;
    template<typename Duration>
    using FixedRateNYTODQSrcOpT = enjima::workload::operators::InMemoryFixedRateNYTODQSourceOperator<Duration>;
    template<typename Duration>
    using FixedRateNYTODQSrcOpWithProcLatencyT =
            enjima::workload::operators::InMemoryFixedRateNYTODQSourceOperatorWithProcLatency<Duration>;

    template<typename Duration = std::chrono::milliseconds>
    class NewYorkTaxiOptimizedDQBenchmark : public StreamingBenchmark {
    public:
        NewYorkTaxiOptimizedDQBenchmark();
        void SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate, bool generateWithEmit,
                bool useProcessingLatency) override;
        void RunBenchmark(uint64_t durationInSec) override;
        void SetDataPath(const std::string& path);

    private:
        template<typename T>
            requires std::is_base_of_v<LatencyTrackingNYTODQSrcOpT<Duration>, T>
        void SetupPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
                runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr);

        inline static const long kWindowDurationBase_{2};
        std::string projectOpName_ = "project";
        std::string filterOpName_ = "filter";
        std::string aggWindowOpName_ = "aggWindow";
        std::string nytDataPath_;
        std::vector<NYTODQEventT> eventCache_;
        bool useProcLatency_{false};
        UnorderedHashMapST<int, FixedRateNYTODQSrcOpT<Duration>*> fixedRateSrcOpPtrMap_;
        UnorderedHashMapST<int, FixedRateNYTODQSrcOpWithProcLatencyT<Duration>*> fixedRateSrcOpWithProcLatencyPtrMap_;
    };
}// namespace enjima::benchmarks::workload

#include "NewYorkTaxiOptimizedDQBenchmark.tpp"

#endif//ENJIMA_BENCHMARKS_NEW_YORK_TAXI_ODQ_BENCHMARK_H