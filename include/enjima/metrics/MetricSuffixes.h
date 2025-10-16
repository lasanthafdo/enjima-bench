//
// Created by m34ferna on 10/02/24.
//

#ifndef ENJIMA_METRIC_SUFFIXES_H
#define ENJIMA_METRIC_SUFFIXES_H

#pragma once

namespace enjima::metrics {
    inline constexpr auto kInCounterSuffix = "_in_counter";
    inline constexpr auto kOutCounterSuffix = "_out_counter";
    inline constexpr auto kInThroughputGaugeSuffix = "_in_throughput_gauge";
    inline constexpr auto kOutThroughputGaugeSuffix = "_out_throughput_gauge";
    inline constexpr auto kLatencyHistogramSuffix = "_latency_histogram";
    inline constexpr auto kPendingEventsGaugeSuffix = "_pendingEvents_gauge";
    inline constexpr auto kOperatorCostGaugeSuffix = "_cost_gauge";
    inline constexpr auto kOperatorSelectivityGaugeSuffix = "_selectivity_gauge";
}// namespace enjima::metrics

#endif//ENJIMA_METRIC_SUFFIXES_H
