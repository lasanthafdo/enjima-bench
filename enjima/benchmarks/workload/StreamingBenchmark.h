//
// Created by m34ferna on 21/02/24.
//

#ifndef ENJIMA_BENCHMARKS_STREAMING_BENCHMARK_H
#define ENJIMA_BENCHMARKS_STREAMING_BENCHMARK_H

#include "enjima/operators/StreamingOperator.h"
#include "enjima/runtime/CancellationException.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/scheduling/SchedulingTypes.h"

#include <cstdint>
#include <string>

namespace enjima::benchmarks::workload {
    using TimestampT = std::chrono::time_point<std::chrono::system_clock, std::chrono::microseconds>;

    class StreamingBenchmark {
    public:
        explicit StreamingBenchmark(std::string benchmarkName);
        virtual ~StreamingBenchmark();
        void Initialize(unsigned int iteration, size_t maxMemory, int32_t numEventsPerBlock, size_t numBlocksPerChunk,
                int32_t backPressureThreshold, const std::string& schedulingModeStr, std::string systemIDString,
                uint64_t schedulingPeriodMs = 0, uint32_t numWorkers = 8,
                const std::string& processingModeStr = "BlockBasedSingle", int numQueries = 1, uint64_t loggingPeriodMs = 10);
        void StopBenchmark();
        virtual void SetUpPipeline(uint64_t latencyRecEmitPeriodMs, uint64_t maxInputRate, bool generateWithEmit,
                bool useProcessingLatency) = 0;
        virtual void RunBenchmark(uint64_t durationInSec) = 0;
        [[nodiscard]] const std::string& GetBenchmarkName() const;

        void SetPreemptMode(const std::string& preemptModeStr);
        void SetPriorityType(const std::string& priorityTypeStr);
        void SetIdleThresholdMs(uint64_t idleThresholdMs);
        void SetEventThreshold(uint64_t eventThreshold);
        void SetSourceReservoirCapacity(uint64_t srcReservoirCapacity);
        void SetMinInputRate(uint64_t minInputRate);
        void SetStepSize(const std::string& stepSize);
        void SetShiftIntervalMillis(uint64_t shiftIntervalMs);
        void SetQueryDistributionStr(const std::string& queryDistributionStr);
        [[nodiscard]] constexpr virtual bool IsMixedWorkload() const;

    protected:
        static runtime::StreamingTask::ProcessingMode GetProcessingMode(const std::string& processingModeStr);
        static runtime::SchedulingMode GetSchedulingMode(const std::string& schedulingModeStr);
        static runtime::PriorityType GetPriorityType(const std::string& priorityTypeStr);
        static runtime::PreemptMode GetPreemptMode(const std::string& preemptModeStr);
        static std::string GetSuffixedOperatorName(const std::string& opName, const std::string& jobIdSuffix);

        const std::string srcOpName_;
        const std::string sinkOpName_;

        enjima::runtime::ExecutionEngine* executionEngine_{nullptr};
        enjima::memory::MemoryManager* pMemoryManager_{nullptr};
        enjima::metrics::Profiler* profilerPtr_{nullptr};
        std::vector<enjima::runtime::StreamingJob*> benchmarkStreamingJobs_;
        std::vector<enjima::core::JobID> queryIds_;
        enjima::runtime::StreamingTask::ProcessingMode processingMode_{
                runtime::StreamingTask::ProcessingMode::kUnassigned};
        enjima::runtime::SchedulingMode schedulingMode_{runtime::SchedulingMode::kThreadBased};
        enjima::runtime::PriorityType priorityType_{runtime::PriorityType::kInputQueueSize};
        enjima::runtime::PreemptMode preemptMode_{runtime::PreemptMode::kNonPreemptive};
        uint64_t idleThresholdMs_{1};
        uint64_t eventThreshold_{1000};
        uint64_t srcReservoirCapacity_{1'000'000};
        uint64_t minInputRate_{0};
        std::string stepSize_;
        uint64_t shiftIntervalMs_{0};
        TimestampT startTime_;
        TimestampT endTime_;
        unsigned int iter_{0};
        std::string queryDistributionStr_;

    private:
        static std::vector<std::string> GetTargetValuesAsTokenizedVectorFromFile(const std::string& filename,
                enjima::metrics::Profiler* pProf, const std::string& targetVal, size_t targetIdx, size_t metricNameIdx);
        void PrintThroughputResults();
        void PrintLatencyResults();
        void writeResultToFile(uint64_t srcOutCount, const std::chrono::seconds& timeDurationSec,
                double throughput) const;
        void PrintAndWriteSummaryResults();
        virtual std::string GetSourceOperatorName(const std::string& jobIdSuffix);
        virtual std::string GetSinkOperatorName(const std::string& jobIdSuffix);

        const std::string benchmarkName_;
    };

    constexpr bool StreamingBenchmark::IsMixedWorkload() const
    {
        return false;
    }
}// namespace enjima::benchmarks::workload

#endif//ENJIMA_BENCHMARKS_STREAMING_BENCHMARK_H
