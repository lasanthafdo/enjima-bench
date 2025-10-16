//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_MIXED_H
#define ENJIMA_BENCHMARKS_MIXED_H

#include "StreamingBenchmark.h"
#include "enjima/benchmarks/workload/functions/LRBFunctions.h"
#include "enjima/benchmarks/workload/functions/NYTDQFunctions.h"
#include "enjima/benchmarks/workload/functions/YSBFunctions.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateLRBSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateLRBSourceOperatorWithProcLatency.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateNYTODQSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateNYTODQSourceOperatorWithProcLatency.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateYSBSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateYSBSourceOperatorWithProcLatency.h"
#include "enjima/benchmarks/workload/operators/InMemoryRateLimitedLRBSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryRateLimitedNYTODQSourceOperator.h"
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
    template<typename Duration>
    using LatencyTrackingNYTODQSrcOpT = enjima::operators::LatencyTrackingSourceOperator<NYTODQEventT, Duration>;

    template<typename Duration = std::chrono::milliseconds>
    class MixedWorkloadAll : public StreamingBenchmark {
    public:
        MixedWorkloadAll();
        ~MixedWorkloadAll() override = default;

        void SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate, bool generateWithEmit,
                bool useProcessingLatency) override;
        void SetUpMixedWorkloadPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRateLRB,
                uint64_t maxInputRateYSB, uint64_t maxInputRateNYT, bool generateWithEmit, bool useProcessingLatency);
        void RunBenchmark(uint64_t durationInSec) override;
        void SetLRBDataPath(const std::string& path);
        void SetNYTDataPath(const std::string& path);
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

        const std::string kNYTSrcOpName_ = "nytSrc";
        const std::string kNYTProjectOpName_ = "nytProject";
        const std::string kNYTFilterOpName_ = "nytFilter";
        const std::string kNYTAggWindowOpName_ = "nytAggWindow";
        const std::string kNYTSinkOpName_ = "nytSink";

        inline static const long kWindowDurationBase_{2};

        std::string nytDataPath_;
        std::string lrbDataPath_;
        std::vector<LinearRoadT> lrbEventCache_;
        std::vector<NYTODQEventT> nytEventCache_;
    };
}// namespace enjima::benchmarks::workload

#include "MixedWorkloadAll.tpp"

#endif//ENJIMA_BENCHMARKS_MIXED_H
