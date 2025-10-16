//
// Created by m34ferna on 21/02/24.
//

#include "StreamingBenchmark.h"
#include "WorkloadException.h"
#include "enjima/memory/MemoryManager.h"
#include "enjima/metrics/MetricSuffixes.h"
#include "enjima/runtime/StreamingJob.h"
#include <fstream>
#include <iomanip>
#include <iostream>
#include <utility>

using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;

namespace enjima::benchmarks::workload {
    static const std::string kPerfResultFilename = "perf_results.csv";
    static const char kPerfResultFileDelim = ',';

    StreamingBenchmark::StreamingBenchmark(std::string benchmarkName)
        : srcOpName_(std::string(benchmarkName).append("_src")),
          sinkOpName_(std::string(benchmarkName).append("_sink")), benchmarkName_(std::move(benchmarkName))
    {
    }

    StreamingBenchmark::~StreamingBenchmark()
    {
        for (const auto& streamingJob: benchmarkStreamingJobs_) {
            delete streamingJob;
        }
        delete executionEngine_;
    }

    void StreamingBenchmark::Initialize(unsigned int iteration, size_t maxMemory, int32_t numEventsPerBlock,
            size_t numBlocksPerChunk, int32_t backPressureThreshold, const std::string& schedulingModeStr,
            std::string systemIDString, uint64_t schedulingPeriodMs, uint32_t numWorkers,
            const std::string& processingModeStr, int numQueries, uint64_t loggingPeriodMs)
    {
        iter_ = iteration;
        processingMode_ = GetProcessingMode(processingModeStr);
        schedulingMode_ = GetSchedulingMode(schedulingModeStr);
        pMemoryManager_ = new MemManT(maxMemory, numBlocksPerChunk, numEventsPerBlock, MemManT::AllocatorType::kBasic);
        pMemoryManager_->SetMaxActiveChunksPerOperator(backPressureThreshold);
        profilerPtr_ = new ProflierT(loggingPeriodMs, true, std::move(systemIDString));
        executionEngine_ = new EngineT;
        executionEngine_->SetSchedulingPeriodMs(schedulingPeriodMs);
        executionEngine_->SetPriorityType(priorityType_);
        executionEngine_->SetEventThreshold(eventThreshold_);
        executionEngine_->SetIdleThresholdMs(idleThresholdMs_);
        executionEngine_->Init(pMemoryManager_, profilerPtr_, schedulingMode_, numWorkers, processingMode_,
                preemptMode_);
        executionEngine_->Start();
        for (int i = 0; i < numQueries; i++) {
            benchmarkStreamingJobs_.emplace_back(new runtime::StreamingJob);
        }
    }

    std::vector<std::string> StreamingBenchmark::GetTargetValuesAsTokenizedVectorFromFile(const std::string& filename,
            enjima::metrics::Profiler* pProf, const std::string& targetVal, size_t targetIdx, size_t metricNameIdx)
    {
        pProf->FlushMetricsLogger();
        auto metricsLoggerThreadId = pProf->GetMetricsLoggerThreadId();
        auto threadIdStr = std::to_string(metricsLoggerThreadId);
        std::ifstream inputFileStream(filename);
        std::vector<std::string> matchedLines;
        std::string line;
        while (std::getline(inputFileStream, line)) {
            if (line.find(threadIdStr) != std::string::npos && line.find(targetVal) != std::string::npos) {
                matchedLines.push_back(line);
            }
        }
        std::vector<std::string> targetValueVec;
        std::vector<std::string> tokenizedStrVec;
        tokenizedStrVec.reserve(10);
        std::string delim = ",";
        size_t pos;
        std::string token;
        for (auto& matchedLine: matchedLines) {
            tokenizedStrVec.clear();
            while ((pos = matchedLine.find(delim)) != std::string::npos) {
                token = matchedLine.substr(0, pos);
                tokenizedStrVec.push_back(token);
                matchedLine.erase(0, pos + delim.length());
            }
            tokenizedStrVec.push_back(matchedLine);
            if (tokenizedStrVec.at(metricNameIdx) == targetVal) {
                targetValueVec.push_back(tokenizedStrVec.at(targetIdx));
            }
        }
        return targetValueVec;
    }

    void StreamingBenchmark::PrintLatencyResults()
    {
        double overallAvgLatencySum = 0;
        double overallAvgLatencySumA = 0;
        double overallAvgLatencySumB = 0;
        double overallAvgLatencySumC = 0;
        for (const auto& queryId_: queryIds_) {
            auto queryIdSuffix = std::to_string(queryId_.GetId());
            auto targetMetricName = GetSinkOperatorName(queryIdSuffix).append("_latency_histogram");
            auto avgLatencyValues = GetTargetValuesAsTokenizedVectorFromFile("metrics/latency.csv", profilerPtr_,
                    targetMetricName, 4, 3);
            if (!avgLatencyValues.empty()) {
                uint64_t avgLatencySumForQ = 0;
                uint64_t avgLatencyCountForQ = 0;
                for (size_t i = 1; i < (avgLatencyValues.size() - 1); i++) {
                    avgLatencySumForQ += std::stoul(avgLatencyValues.at(i));
                    avgLatencyCountForQ++;
                }
                double overallAvgLatencyForQ =
                        static_cast<double>(avgLatencySumForQ) / static_cast<double>(avgLatencyCountForQ);
                overallAvgLatencySum += overallAvgLatencyForQ;
                if (IsMixedWorkload()) {
                    if (GetBenchmarkName() == "mixedall") {
                        if (queryId_.GetId() % 3 == 1) {
                            overallAvgLatencySumA += overallAvgLatencyForQ;
                        }
                        else if (queryId_.GetId() % 3 == 2) {
                            overallAvgLatencySumB += overallAvgLatencyForQ;
                        }
                        else {
                            overallAvgLatencySumC += overallAvgLatencyForQ;
                        }
                    }
                    else {
                        if (queryId_.GetId() % 2 == 1) {
                            overallAvgLatencySumA += overallAvgLatencyForQ;
                        }
                        else {
                            overallAvgLatencySumB += overallAvgLatencyForQ;
                        }
                    }
                }
                std::cout << "Avg. latency average for " << targetMetricName << " : " << overallAvgLatencyForQ
                          << std::endl;
            }
        }
        if (IsMixedWorkload()) {
            if (GetBenchmarkName() == "mixedall") {
                auto queriesPerWorkload = queryIds_.size() / 3;
                std::cout << std::endl
                          << "Avg. latency average for " << queriesPerWorkload << " queries of workload A : "
                          << static_cast<double>(overallAvgLatencySumA) / static_cast<double>(queriesPerWorkload)
                          << std::endl;
                std::cout << "Avg. latency average for " << queriesPerWorkload << " queries of workload B : "
                          << static_cast<double>(overallAvgLatencySumB) / static_cast<double>(queriesPerWorkload)
                          << std::endl;
                std::cout << "Avg. latency average for " << queriesPerWorkload << " queries of workload C : "
                          << static_cast<double>(overallAvgLatencySumC) / static_cast<double>(queriesPerWorkload)
                          << std::endl;
            }
            else {
                auto queriesPerWorkload = queryIds_.size() / 2;
                std::cout << std::endl
                          << "Avg. latency average for " << queriesPerWorkload << " queries of workload A : "
                          << static_cast<double>(overallAvgLatencySumA) / static_cast<double>(queriesPerWorkload)
                          << std::endl;
                std::cout << "Avg. latency average for " << queriesPerWorkload << " queries of workload B : "
                          << static_cast<double>(overallAvgLatencySumB) / static_cast<double>(queriesPerWorkload)
                          << std::endl;
            }
        }
        std::cout << std::endl
                  << "Avg. latency average for " << queryIds_.size()
                  << " queries : " << static_cast<double>(overallAvgLatencySum) / static_cast<double>(queryIds_.size())
                  << std::endl;
        std::cout << std::endl;
    }

    void StreamingBenchmark::PrintThroughputResults()
    {
        std::cout << std::fixed << std::setprecision(2) << "Setting 'cout' format to 'fixed' and precision to '2'"
                  << std::endl;
        double overallTpSum = 0;
        double overallTpSumA = 0;
        double overallTpSumB = 0;
        double overallTpSumC = 0;
        for (const auto& queryId_: queryIds_) {
            auto queryIdSuffix = std::to_string(queryId_.GetId());
            auto targetMetricName = GetSourceOperatorName(queryIdSuffix).append("_outThroughput_gauge");
            auto tpValues = GetTargetValuesAsTokenizedVectorFromFile("metrics/throughput.csv", profilerPtr_,
                    targetMetricName, 4, 3);
            if (!tpValues.empty()) {
                double tpSumForQ = 0;
                uint64_t tpCountForQ = 0;
                // We discard the first and the last value
                for (size_t i = 1; i < (tpValues.size() - 1); i++) {
                    tpSumForQ += std::stod(tpValues.at(i));
                    tpCountForQ++;
                }
                double avgTpForQ = tpSumForQ / static_cast<double>(tpCountForQ);
                overallTpSum += avgTpForQ;
                if (IsMixedWorkload()) {
                    if (GetBenchmarkName() == "mixedall") {
                        if (queryId_.GetId() % 3 == 1) {
                            overallTpSumA += avgTpForQ;
                        }
                        else if (queryId_.GetId() % 3 == 2) {
                            overallTpSumB += avgTpForQ;
                        }
                        else {
                            overallTpSumC += avgTpForQ;
                        }
                    }
                    else {
                        if (queryId_.GetId() % 2 == 1) {
                            overallTpSumA += avgTpForQ;
                        }
                        else {
                            overallTpSumB += avgTpForQ;
                        }
                    }
                }
                std::cout << "Avg. throughput for " << targetMetricName << " : " << avgTpForQ << std::endl;
            }
        }
        if (IsMixedWorkload()) {
            if (GetBenchmarkName() == "mixedall") {
                std::cout << std::endl
                          << "Sum throughput for " << queryIds_.size() / 3
                          << " queries of workload A : " << overallTpSumA << std::endl;
                std::cout << "Sum throughput for " << queryIds_.size() / 3
                          << " queries of workload B : " << overallTpSumB << std::endl;
                std::cout << "Sum throughput for " << queryIds_.size() / 3
                          << " queries of workload C : " << overallTpSumC << std::endl;
            }
            else {
                std::cout << std::endl
                          << "Sum throughput for " << queryIds_.size() / 2
                          << " queries of workload A : " << overallTpSumA << std::endl;
                std::cout << "Sum throughput for " << queryIds_.size() / 2
                          << " queries of workload B : " << overallTpSumB << std::endl;
            }
        }
        std::cout << std::endl
                  << "Sum throughput for " << queryIds_.size() << " queries : " << overallTpSum << std::endl;
        std::cout << std::endl;
    }

    void StreamingBenchmark::StopBenchmark()
    {
        try {
            if (!IsMixedWorkload() || queryDistributionStr_ == "0") {
                PrintAndWriteSummaryResults();
                PrintThroughputResults();
                PrintLatencyResults();
            }
        }
        catch (std::exception& e) {
            std::cout << "Exception when summarizing results: " << e.what() << std::endl;
        }
        executionEngine_->Shutdown();
    }

    void StreamingBenchmark::PrintAndWriteSummaryResults()
    {
        uint64_t totSrcOutCount = 0;
        double sumLastLatencyVals = 0;
        uint64_t totSrcOutCountA = 0;
        uint64_t totSrcOutCountB = 0;
        uint64_t totSrcOutCountC = 0;
        double sumLastLatencyValsA = 0;
        double sumLastLatencyValsB = 0;
        double sumLastLatencyValsC = 0;
        for (const auto& queryId_: queryIds_) {
            auto queryIdSuffix = std::to_string(queryId_.GetId());
            auto suffixedSrcOpName = GetSourceOperatorName(queryIdSuffix);
            auto srcOutCounterMetricName = suffixedSrcOpName + metrics::kOutCounterSuffix;
            std::unsigned_integral auto srcOutCount = profilerPtr_->GetCounter(srcOutCounterMetricName)->GetCount();
            std::cout << "Output count for " << suffixedSrcOpName << " : " << srcOutCount << std::endl;
            totSrcOutCount += srcOutCount;
            auto suffixedSinkOpName = GetSinkOperatorName(queryIdSuffix);
            auto sinkLastAvgLatency = profilerPtr_->GetLatencyHistogram(suffixedSinkOpName)->GetAverage();
            sumLastLatencyVals += sinkLastAvgLatency;
            if (IsMixedWorkload()) {
                if (GetBenchmarkName() == "mixedall") {
                    if (queryId_.GetId() % 3 == 1) {
                        totSrcOutCountA += srcOutCount;
                        sumLastLatencyValsA += sinkLastAvgLatency;
                    }
                    else if (queryId_.GetId() % 3 == 2) {
                        totSrcOutCountB += srcOutCount;
                        sumLastLatencyValsB += sinkLastAvgLatency;
                    }
                    else {
                        totSrcOutCountC += srcOutCount;
                        sumLastLatencyValsC += sinkLastAvgLatency;
                    }
                }
                else {
                    if (queryId_.GetId() % 2 == 1) {
                        totSrcOutCountA += srcOutCount;
                        sumLastLatencyValsA += sinkLastAvgLatency;
                    }
                    else {
                        totSrcOutCountB += srcOutCount;
                        sumLastLatencyValsB += sinkLastAvgLatency;
                    }
                }
            }
        }

        auto timeDurationSec = std::chrono::duration_cast<std::chrono::seconds>(endTime_ - startTime_);
        if (IsMixedWorkload()) {
            auto throughputA = (double) totSrcOutCountA / (double) timeDurationSec.count();
            auto throughputB = (double) totSrcOutCountB / (double) timeDurationSec.count();
            if (GetBenchmarkName() == "mixedall") {
                auto throughputC = (double) totSrcOutCountC / (double) timeDurationSec.count();
                std::cout << "Benchmark (" << benchmarkName_ << " workload A) ran for " << timeDurationSec.count()
                          << " seconds [Overall Throughput: " << std::fixed << std::setprecision(2) << throughputA
                          << " events/sec, Last Avg. Latency Measurement: "
                          << sumLastLatencyValsA / static_cast<double>(queryIds_.size()) / 3 << " units]" << std::endl;
                std::cout << "Benchmark (" << benchmarkName_ << " workload B) ran for " << timeDurationSec.count()
                          << " seconds [Overall Throughput: " << std::fixed << std::setprecision(2) << throughputB
                          << " events/sec, Last Avg. Latency Measurement: "
                          << sumLastLatencyValsB / static_cast<double>(queryIds_.size()) / 3 << " units]" << std::endl;
                std::cout << "Benchmark (" << benchmarkName_ << " workload C) ran for " << timeDurationSec.count()
                          << " seconds [Overall Throughput: " << std::fixed << std::setprecision(2) << throughputC
                          << " events/sec, Last Avg. Latency Measurement: "
                          << sumLastLatencyValsC / static_cast<double>(queryIds_.size()) / 3 << " units]" << std::endl;
            }
            else {
                std::cout << "Benchmark (" << benchmarkName_ << " workload A) ran for " << timeDurationSec.count()
                          << " seconds [Overall Throughput: " << std::fixed << std::setprecision(2) << throughputA
                          << " events/sec, Last Avg. Latency Measurement: "
                          << sumLastLatencyValsA / static_cast<double>(queryIds_.size()) / 2 << " units]" << std::endl;
                std::cout << "Benchmark (" << benchmarkName_ << " workload B) ran for " << timeDurationSec.count()
                          << " seconds [Overall Throughput: " << std::fixed << std::setprecision(2) << throughputB
                          << " events/sec, Last Avg. Latency Measurement: "
                          << sumLastLatencyValsB / static_cast<double>(queryIds_.size()) / 2 << " units]" << std::endl;
            }
        }
        auto throughput = (double) totSrcOutCount / (double) timeDurationSec.count();
        std::cout << "Benchmark (" << benchmarkName_ << ") ran for " << timeDurationSec.count()
                  << " seconds [Overall Throughput: " << std::fixed << std::setprecision(2) << throughput
                  << " events/sec, Last Avg. Latency Measurement: "
                  << sumLastLatencyVals / static_cast<double>(queryIds_.size()) << " units]" << std::endl;
        writeResultToFile(totSrcOutCount, timeDurationSec, throughput);
        std::cout << "Wrote results to " << kPerfResultFilename << std::endl;
        std::cout << std::endl;
    }

    void StreamingBenchmark::writeResultToFile(uint64_t srcOutCount, const std::chrono::seconds& timeDurationSec,
            double throughput) const
    {
        std::ofstream perfResultFile;
        perfResultFile.open("metrics/" + kPerfResultFilename, std::ios::app);
        perfResultFile << benchmarkName_ << kPerfResultFileDelim << iter_ << kPerfResultFileDelim
                       << startTime_.time_since_epoch().count() << kPerfResultFileDelim << timeDurationSec.count()
                       << kPerfResultFileDelim << srcOutCount << kPerfResultFileDelim << std::fixed
                       << std::setprecision(2) << throughput << std::endl;
        perfResultFile.close();
    }

    const std::string& StreamingBenchmark::GetBenchmarkName() const
    {
        return benchmarkName_;
    }

    runtime::StreamingTask::ProcessingMode StreamingBenchmark::GetProcessingMode(const std::string& processingModeStr)
    {
        if (processingModeStr == "QueueBasedSingle") {
            return runtime::StreamingTask::ProcessingMode::kQueueBasedSingle;
        }
        else if (processingModeStr == "BlockBasedSingle") {
            return runtime::StreamingTask::ProcessingMode::kBlockBasedSingle;
        }
        else if (processingModeStr == "BlockBasedBatch") {
            return runtime::StreamingTask::ProcessingMode::kBlockBasedBatch;
        }
        throw WorkloadException(std::string("Invalid processing mode : ").append(processingModeStr));
    }

    runtime::SchedulingMode StreamingBenchmark::GetSchedulingMode(const std::string& schedulingModeStr)
    {
        if (schedulingModeStr == "SBPriority") {
            return runtime::SchedulingMode::kStateBasedPriority;
        }
        else if (schedulingModeStr == "TB") {
            return runtime::SchedulingMode::kThreadBased;
        }
        throw WorkloadException(std::string("Invalid scheduling mode : ").append(schedulingModeStr));
    }

    runtime::PriorityType StreamingBenchmark::GetPriorityType(const std::string& priorityTypeStr)
    {
        if (priorityTypeStr == "InputQueueSize") {
            return runtime::PriorityType::kInputQueueSize;
        }
        if (priorityTypeStr == "Adaptive") {
            return runtime::PriorityType::kAdaptive;
        }
        if (priorityTypeStr == "LatencyOptimized") {
            return runtime::PriorityType::kLatencyOptimized;
        }
        if (priorityTypeStr == "SPLatencyOptimized") {
            return runtime::PriorityType::kSPLatencyOptimized;
        }
        if (priorityTypeStr == "HighestRate") {
            return runtime::PriorityType::kHighestRate;
        }
        if (priorityTypeStr == "MinLatency") {
            return runtime::PriorityType::kMinLatency;
        }
        if (priorityTypeStr == "MinCost") {
            return runtime::PriorityType::kMinCost;
        }
        if (priorityTypeStr == "LeastRecent") {
            return runtime::PriorityType::kLeastRecent;
        }
        if (priorityTypeStr == "ThroughputOptimized") {
            return runtime::PriorityType::kThroughputOptimized;
        }
        if (priorityTypeStr == "SimpleThroughput") {
            return runtime::PriorityType::kSimpleThroughput;
        }
        if (priorityTypeStr == "RoundRobin") {
            return runtime::PriorityType::kRoundRobin;
        }
        if (priorityTypeStr == "FCFS") {
            return runtime::PriorityType::kFirstComeFirstServed;
        }
        throw WorkloadException(std::string("Invalid priority type : ").append(priorityTypeStr));
    }

    runtime::PreemptMode StreamingBenchmark::GetPreemptMode(const std::string& preemptModeStr)
    {
        if (preemptModeStr == "Preemptive") {
            return runtime::PreemptMode::kPreemptive;
        }
        else if (preemptModeStr == "NonPreemptive") {
            return runtime::PreemptMode::kNonPreemptive;
        }
        throw WorkloadException(std::string("Invalid preempt mode : ").append(preemptModeStr));
    }

    void StreamingBenchmark::SetPreemptMode(const std::string& preemptModeStr)
    {
        preemptMode_ = GetPreemptMode(preemptModeStr);
    }

    void StreamingBenchmark::SetPriorityType(const std::string& priorityTypeStr)
    {
        priorityType_ = GetPriorityType(priorityTypeStr);
    }

    std::string StreamingBenchmark::GetSuffixedOperatorName(const std::string& opName, const std::string& jobIdSuffix)
    {
        return std::string(opName).append("_").append(jobIdSuffix);
    }

    void StreamingBenchmark::SetIdleThresholdMs(uint64_t idleThresholdMs)
    {
        idleThresholdMs_ = idleThresholdMs;
    }

    void StreamingBenchmark::SetEventThreshold(uint64_t eventThreshold)
    {
        eventThreshold_ = eventThreshold;
    }

    void StreamingBenchmark::SetSourceReservoirCapacity(uint64_t srcReservoirCapacity)
    {
        srcReservoirCapacity_ = srcReservoirCapacity;
    }

    std::string StreamingBenchmark::GetSourceOperatorName(const std::string& jobIdSuffix)
    {
        return GetSuffixedOperatorName(srcOpName_, jobIdSuffix);
    }

    std::string StreamingBenchmark::GetSinkOperatorName(const std::string& jobIdSuffix)
    {
        return GetSuffixedOperatorName(sinkOpName_, jobIdSuffix);
    }

    void StreamingBenchmark::SetMinInputRate(uint64_t minInputRate)
    {
        minInputRate_ = minInputRate;
    }

    void StreamingBenchmark::SetStepSize(const std::string& stepSize)
    {
        stepSize_ = stepSize;
    }

    void StreamingBenchmark::SetShiftIntervalMillis(uint64_t shiftIntervalMs)
    {
        shiftIntervalMs_ = shiftIntervalMs;
    }

    void StreamingBenchmark::SetQueryDistributionStr(const std::string& queryDistributionStr)
    {
        queryDistributionStr_ = queryDistributionStr;
    }

}// namespace enjima::benchmarks::workload