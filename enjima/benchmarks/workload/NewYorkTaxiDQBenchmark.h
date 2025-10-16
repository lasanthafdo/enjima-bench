//
// Created by t86kim on 20/11/24.
//

#ifndef ENJIMA_BENCHMARKS_NEW_YORK_TAXI_DQ_BENCHMARK_H
#define ENJIMA_BENCHMARKS_NEW_YORK_TAXI_DQ_BENCHMARK_H

#include "StreamingBenchmark.h"
#include "enjima/benchmarks/workload/functions/NYTDQFunctions.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateNYTDQSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryRateLimitedNYTDQSourceOperator.h"
#include "enjima/operators/FixedEventTimeWindowOperator.h"
#include "enjima/operators/GenericSinkOperator.h"
#include "enjima/operators/MapOperator.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/StreamingJob.h"

namespace enjima::benchmarks::workload {
    template<typename Duration>
    using LatencyTrackingNYTDQSrcOpT = enjima::operators::LatencyTrackingSourceOperator<NYTFullEventT, Duration>;

    template<typename Duration = std::chrono::milliseconds>
    class NewYorkTaxiDQBenchmark : public StreamingBenchmark {
    public:
        NewYorkTaxiDQBenchmark();
        void SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate, bool generateWithEmit,
                bool useProcessingLatency) override;
        void RunBenchmark(uint64_t durationInSec) override;
        void SetDataPath(const std::string& path);

    private:
        template<typename T>
            requires std::is_base_of_v<LatencyTrackingNYTDQSrcOpT<Duration>, T>
        void SetupPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
                runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr);

        inline static const long kWindowDurationBase_{2};
        std::string filterOpName_ = "filter";
        std::string projectOpName_ = "project";
        std::string cellFilterOpName_ = "cellFilter";
        std::string aggWindowOpName_ = "aggWindow";
        std::string nytDataPath_;
        std::vector<NYTFullEventT> eventCache_;
    };
}// namespace enjima::benchmarks::workload

#include "NewYorkTaxiDQBenchmark.tpp"

#endif//ENJIMA_BENCHMARKS_NEW_YORK_TAXI_DQ_BENCHMARK_H