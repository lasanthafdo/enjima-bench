#!/bin/bash

TOPLEV_DIR="/hdd1/tools/pmu-tools"
TOPLEV_COMMAND_INVARIANT="--thread --cpu 2,3,4,5,8,9,10,11 --csv ; sleep 30"
RUN_COMMAND_BASE="cmake-release-build/enjima_benchmarks -b YSB -t 120 -m 8096 -r 1 --backpressure_threshold 100 --latency_res Micros -l 50 -p BlockBasedBatch -s SBPriority -w 8 --preempt_mode NonPreemptive --priority_type LatencyOptimized --scheduling_period 2 --max_idle_threshold_ms 2"

CALC_MODE=$1
echo "Calc mode: $CALC_MODE"

if [[ -z $CALC_MODE || $CALC_MODE == "ifixed" ]]; then
	BASE_INPUT_RATE=12000000
	BASE_INPUT_RATE_M=12
	NUM_QUERIES_ARR="1 2 4 6 8 10 12"
	for NUM_QUERIES in $NUM_QUERIES_ARR
	do
		RUN_COMMAND="$RUN_COMMAND_BASE -i $BASE_INPUT_RATE -q $NUM_QUERIES"
		echo "Running $RUN_COMMAND"
		$RUN_COMMAND 2>&1 >> ifixed_out.txt &
		PID=$!
		echo "Enjima running with PID $PID"
		sleep 10
		TOPLEV_L2="$TOPLEV_DIR/toplev -l2 -o toplev_ifixed_l2_i${BASE_INPUT_RATE_M}_q${NUM_QUERIES}.csv $TOPLEV_COMMAND_INVARIANT"
		echo "Runnint toplev L2: $TOPLEV_L2"
		$TOPLEV_L2
		sleep 10
		TOPLEV_L3="$TOPLEV_DIR/toplev -l3 -o toplev_ifixed_l3_i${BASE_INPUT_RATE_M}_q${NUM_QUERIES}.csv $TOPLEV_COMMAND_INVARIANT"
		echo "Runnint toplev L3: $TOPLEV_L3"
		$TOPLEV_L3
		wait $PID
		sleep 5
	done
fi

if [[ -z $CALC_MODE || $CALC_MODE == "qfixed" ]]; then
	INPUT_RATES="1 2 4 8 12 16 20 24"
	BASE_NUM_QUERIES=4
	for INPUT_RATE in $INPUT_RATES
	do
		CALC_INPUT_RATE=$(( INPUT_RATE * 1000000 ))
		echo "Calculated input rate: $CALC_INPUR_RATE"
		RUN_COMMAND="$RUN_COMMAND_BASE -i $CALC_INPUT_RATE -q $BASE_NUM_QUERIES"
		echo "Running $RUN_COMMAND"
		$RUN_COMMAND 2>&1 >> qfixed_out.txt &	
		PID=$!
		echo "Enjima running with PID $PID"
		sleep 10
		TOPLEV_L2="$TOPLEV_DIR/toplev -l2 -o toplev_qfixed_l2_i${INPUT_RATE}_q${BASE_NUM_QUERIES}.csv $TOPLEV_COMMAND_INVARIANT"
		echo "Runnint toplev L2: $TOPLEV_L2"
		$TOPLEV_L2
		sleep 10
		TOPLEV_L3="$TOPLEV_DIR/toplev -l3 -o toplev_qfixed_l3_i${INPUT_RATE}_q${BASE_NUM_QUERIES}.csv $TOPLEV_COMMAND_INVARIANT"
		echo "Runnint toplev L3: $TOPLEV_L3"
		$TOPLEV_L3
		wait $PID
		sleep 5
	done
fi
