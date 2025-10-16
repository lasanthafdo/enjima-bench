//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_LINEAR_ROAD_VALIDATE_H
#define ENJIMA_BENCHMARKS_LINEAR_ROAD_VALIDATE_H

#include "StreamingBenchmark.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/benchmarks/workload/operators/InMemoryRateLimitedLRBSourceOperator.h"
#include "enjima/benchmarks/workload/operators/InMemoryFixedRateLRBSourceOperator.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/operators/NoOpSinkOperator.h"
#include "enjima/operators/MapOperator.h"
#include "enjima/operators/FixedEventTimeWindowOperator.h"
#include "enjima/operators/FixedEventTimeWindowJoinOperator.h"
#include "enjima/operators/FixedEventTimeWindowCoGroupOperator.h"
#include "enjima/operators/SlidingEventTimeWindowOperator.h"
#include "enjima/benchmarks/workload/functions/LRBVBFunctions.h"
#include "enjima/benchmarks/workload/functions/LRBUtilities.h"
#include "enjima/runtime/StreamingJob.h"

namespace enjima::benchmarks::workload {
    class LRBValidateBenchmark : public StreamingBenchmark {
    public:
        LRBValidateBenchmark();
        void SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate, bool generateWithEmit,
                bool useProcessingLatency) override;
        void RunBenchmark(uint64_t durationInSec) override;
        void SetDataPath(std::string path);

    private:
        const std::string kFilterOpName_ = "typeFilter";
        const std::string projOpName_ = "project";
        const std::string multiMapOpName_ = "splitMap";
        const std::string acdFilterOpName_ = "travelFilter";
        const std::string acdWindowOpName_ = "accidentWindow";
        const std::string spdWindowOpName_ = "speedWindow";
        const std::string cntWindowOpName_ = "countWindow";
        const std::string cntSpdWindowJoinOpName_ = "countAndSpeedJoin";
        const std::string acdTollCoGroupOpName_ = "tollAndAccidentCoGroup";
        std::string lrbDataPath_;
    };
}// namespace enjima::benchmarks::workload

#endif//ENJIMA_BENCHMARKS_LINEAR_ROAD_VALIDATE_H
