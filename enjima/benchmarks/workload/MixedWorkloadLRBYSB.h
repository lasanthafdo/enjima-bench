//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_LRB_YSB_H
#define ENJIMA_BENCHMARKS_LRB_YSB_H

#include "StreamingBenchmark.h"
#include "enjima/benchmarks/workload/functions/LRBFunctions.h"
#include "enjima/benchmarks/workload/functions/YSBFunctions.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateLRBSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateLRBSourceOperatorWithProcLatency.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateYSBSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateYSBSourceOperatorWithProcLatency.h"
#include "enjima/benchmarks/workload/operators/InMemoryRateLimitedLRBSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryRateLimitedYSBSourceOperator.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/operators/FixedEventTimeWindowCoGroupOperator.h"
#include "enjima/operators/FixedEventTimeWindowJoinOperator.h"
#include "enjima/operators/FixedEventTimeWindowOperator.h"
#include "enjima/operators/GenericSinkOperator.h"
#include "enjima/operators/MapOperator.h"
#include "enjima/operators/NoOpSinkOperator.h"
#include "enjima/operators/SlidingEventTimeWindowOperator.h"
#include "enjima/operators/StaticEquiJoinOperator.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/StreamingJob.h"

namespace enjima::benchmarks::workload {
    template<typename Duration>
    using LatencyTrackingLRBSrcOpT = enjima::operators::LatencyTrackingSourceOperator<LinearRoadT, Duration>;
    template<typename Duration>
    using LatencyTrackingYSBSrcOpT = enjima::operators::LatencyTrackingSourceOperator<YSBAdT, Duration>;

    template<typename Duration = std::chrono::milliseconds>
    class MixedWorkloadLRBYSB : public StreamingBenchmark {
    public:
        MixedWorkloadLRBYSB();
        void SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate, bool generateWithEmit,
                bool useProcessingLatency) override;
        void SetUpMixedWorkloadPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRateLRB,
                uint64_t maxInputRateYSB, bool generateWithEmit, bool useProcessingLatency);
        void RunBenchmark(uint64_t durationInSec) override;
        void SetDataPath(const std::string& path);
        [[nodiscard]] constexpr bool IsMixedWorkload() const override;

    private:
        std::string GetSourceOperatorName(const std::string& jobIdSuffix) override;
        std::string GetSinkOperatorName(const std::string& jobIdSuffix) override;
        template<typename T>
            requires std::is_base_of_v<LatencyTrackingLRBSrcOpT<Duration>, T>
        void SetupLRBPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
                runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr);
        template<typename T>
            requires std::is_base_of_v<LatencyTrackingYSBSrcOpT<Duration>, T>
        void SetupYSBPipelineWithSourceOp(std::unique_ptr<T>& uPtrSrcOp,
                runtime::StreamingJob* const& benchmarkStreamingJob, const std::string& jobIdSuffixStr);

        const std::string kYSBSrcOpName_ = "ysbSrc";
        const std::string kYSBFilterOpName_ = "ysbEventFilter";
        const std::string kYSBProjectOpName_ = "ysbProject";
        const std::string kYSBStaticEqJoinOpName_ = "ysbStaticEqJoin";
        const std::string kYSBWindowOpName_ = "ysbTimeWindow";
        const std::string kYSBSinkOpName_ = "ysbSink";

        const std::string kLRBSrcOpName_ = "lrbSrc";
        const std::string kLRBFilterOpName_ = "lrbTypeFilter";
        const std::string kLRBProjOpName_ = "lrbProject";
        const std::string kLRBMultiMapOpName_ = "lrbSplitMap";
        const std::string kLRBAcdFilterOpName_ = "lrbTravelFilter";
        const std::string kLRBAcdWindowOpName_ = "lrbAccidentWindow";
        const std::string kLRBSpdWindowOpName_ = "lrbSpeedWindow";
        const std::string kLRBCntWindowOpName_ = "lrbCountWindow";
        const std::string kLRBCntSpdWindowJoinOpName_ = "lrbCountAndSpeedJoin";
        const std::string kLRBAcdTollCoGroupOpName_ = "lrbTollAndAccidentCoGroup";
        const std::string kLRBSinkOpName_ = "lrbSink";

        std::string lrbDataPath_;
        std::vector<LinearRoadT> lrbEventCache_;
    };
}// namespace enjima::benchmarks::workload

#include "MixedWorkloadLRBYSB.tpp"

#endif//ENJIMA_BENCHMARKS_LRB_YSB_H
