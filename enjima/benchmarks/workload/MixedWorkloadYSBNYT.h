//
// Created by m34ferna on 04/06/25.
//

#ifndef ENJIMA_BENCHMARKS_YSB_NYT_H
#define ENJIMA_BENCHMARKS_YSB_NYT_H

#include "StreamingBenchmark.h"
#include "enjima/benchmarks/workload/functions/NYTDQFunctions.h"
#include "enjima/benchmarks/workload/functions/YSBFunctions.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateNYTODQSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateNYTODQSourceOperatorWithProcLatency.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateYSBSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateYSBSourceOperatorWithProcLatency.h"
#include "enjima/benchmarks/workload/operators/InMemoryRateLimitedNYTODQSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryRateLimitedYSBSourceOperator.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/operators/FixedEventTimeWindowOperator.h"
#include "enjima/operators/GenericSinkOperator.h"
#include "enjima/operators/MapOperator.h"
#include "enjima/operators/NoOpSinkOperator.h"
#include "enjima/operators/StaticEquiJoinOperator.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/StreamingJob.h"

namespace enjima::benchmarks::workload {
    template<typename Duration>
    using LatencyTrackingYSBSrcOpT = enjima::operators::LatencyTrackingSourceOperator<YSBAdT, Duration>;
    template<typename Duration>
    using LatencyTrackingNYTODQSrcOpT = enjima::operators::LatencyTrackingSourceOperator<NYTODQEventT, Duration>;

    template<typename Duration = std::chrono::milliseconds>
    class MixedWorkloadYSBNYT : public StreamingBenchmark {
    public:
        MixedWorkloadYSBNYT();
        void SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate, bool generateWithEmit,
                bool useProcessingLatency) override;
        void SetUpMixedWorkloadPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRateA,
                uint64_t maxInputRateB, bool generateWithEmit, bool useProcessingLatency);
        void RunBenchmark(uint64_t durationInSec) override;
        void SetDataPath(const std::string& path);
        [[nodiscard]] constexpr bool IsMixedWorkload() const override;

    private:
        std::string GetSourceOperatorName(const std::string& jobIdSuffix) override;
        std::string GetSinkOperatorName(const std::string& jobIdSuffix) override;
        template<typename T>
            requires std::is_base_of_v<LatencyTrackingYSBSrcOpT<Duration>, T>
        void SetupYSBPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
                runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr);
        template<typename T>
            requires std::is_base_of_v<LatencyTrackingNYTODQSrcOpT<Duration>, T>
        void SetupNYTPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
                runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr);

        const std::string kYSBSrcOpName_ = "ysbSrc";
        const std::string kYSBFilterOpName_ = "ysbEventFilter";
        const std::string kYSBProjectOpName_ = "ysbProject";
        const std::string kYSBStaticEqJoinOpName_ = "ysbStaticEqJoin";
        const std::string kYSBWindowOpName_ = "ysbTimeWindow";
        const std::string kYSBSinkOpName_ = "ysbSink";

        inline static const long kWindowDurationBase_{2};

        const std::string kNYTSrcOpName_ = "nytSrc";
        const std::string kNYTProjectOpName_ = "nytProject";
        const std::string kNYTFilterOpName_ = "nytFilter";
        const std::string kNYTAggWindowOpName_ = "nytAggWindow";
        const std::string kNYTSinkOpName_ = "nytSink";

        std::string nytDataPath_;
        std::vector<NYTODQEventT> nytEventCache_;
    };
}// namespace enjima::benchmarks::workload

#include "MixedWorkloadYSBNYT.tpp"

#endif//ENJIMA_BENCHMARKS_YSB_NYT_H
