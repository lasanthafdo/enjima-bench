#!/bin/bash

SETUP_FILE="bench.yaml"
CONF_FILE="conf/enjima-config.yaml"
ENV_FILE=".env"
METRICS_TARGET_DIR=$HOME/enjima/metrics
EXP_NAME=$1
DURATION_SEC=$2
NUM_REPETITIONS=$3
DIRECTIVES=$4

BENCHMARK=$(yq e .benchmarkConfig.${EXP_NAME}.benchmark < $SETUP_FILE)
TARGET_DIR=$(yq e .benchmarkConfig.${EXP_NAME}.targetDir < $SETUP_FILE)
PROCESSING_MODES=$(yq e .benchmarkConfig.${EXP_NAME}.processingModes < $SETUP_FILE)
SCHEDULING_MODES=$(yq e .benchmarkConfig.${EXP_NAME}.schedulingModes < $SETUP_FILE)
LATENCY_VALUES=$(yq e .benchmarkConfig.${EXP_NAME}.latencyValues < $SETUP_FILE)
INPUT_RATES=$(yq e .benchmarkConfig.${EXP_NAME}.inputRates < $SETUP_FILE)
MIN_INPUT_RATES=$(yq e .benchmarkConfig.${EXP_NAME}.minInputRates < $SETUP_FILE)
STEP_SIZE=$(yq e .benchmarkConfig.${EXP_NAME}.stepSize < $SETUP_FILE)
SHIFT_INTERVAL_MS=$(yq e .benchmarkConfig.${EXP_NAME}.shiftIntervalMs < $SETUP_FILE)
NUM_WORKERS=$(yq e .benchmarkConfig.${EXP_NAME}.numWorkers < $SETUP_FILE)
PREEMPT_MODES=$(yq e .benchmarkConfig.${EXP_NAME}.preemptModes < $SETUP_FILE)
PRIORITY_TYPES=$(yq e .benchmarkConfig.${EXP_NAME}.priorityTypes < $SETUP_FILE)
NUM_QUERIES_ARR=$(yq e .benchmarkConfig.${EXP_NAME}.numQueriesArr < $SETUP_FILE)
MAX_MEMORY=$(yq e .benchmarkConfig.${EXP_NAME}.maxMemory < $SETUP_FILE)
BACKPRESSURE_THRESHOLD=$(yq e .benchmarkConfig.${EXP_NAME}.backpressureThreshold < $SETUP_FILE)
EVENTS_PER_BLOCK=$(yq e .benchmarkConfig.${EXP_NAME}.eventsPerBlock < $SETUP_FILE)
BLOCKS_PER_CHUNK=$(yq e .benchmarkConfig.${EXP_NAME}.blocksPerChunk < $SETUP_FILE)
SCHEDULING_PERIOD=$(yq e .benchmarkConfig.${EXP_NAME}.schedulingPeriod < $SETUP_FILE)
LATENCY_RES=$(yq e .benchmarkConfig.${EXP_NAME}.latencyRes < $SETUP_FILE)
LATENCY_TYPE=$(yq e .benchmarkConfig.${EXP_NAME}.latencyType < $SETUP_FILE)
IDLE_THRESHOLD_MS=$(yq e .benchmarkConfig.${EXP_NAME}.maxIdleThresholdMs < $SETUP_FILE)
EVENT_THRESHOLD=$(yq e .benchmarkConfig.${EXP_NAME}.eventThreshold < $SETUP_FILE)
PATH_TO_DATA=$(yq e .benchmarkConfig.${EXP_NAME}.dataPath < $SETUP_FILE)
SRC_RESERVOIR_SIZE=$(yq e .benchmarkConfig.${EXP_NAME}.srcReservoirSize < $SETUP_FILE)
ADAPT_MAX_IDLE_THRESHOLD=$(yq e .benchmarkConfig.${EXP_NAME}.adaptMaxIdleThreshold < $SETUP_FILE)
ADAPT_MAX_EVENT_THRESHOLD=$(yq e .benchmarkConfig.${EXP_NAME}.adaptMaxEventThreshold < $SETUP_FILE)
ADAPT_STEP_IDLE_THRESHOLD=$(yq e .benchmarkConfig.${EXP_NAME}.adaptStepIdleThreshold < $SETUP_FILE)
ADAPT_STEP_EVENT_THRESHOLD=$(yq e .benchmarkConfig.${EXP_NAME}.adaptStepEventThreshold < $SETUP_FILE)
OPT_VAL_TYPE=$(yq e .benchmarkConfig.${EXP_NAME}.optValType < $SETUP_FILE)
MIXED_WORKLOAD=$(yq e .benchmarkConfig.${EXP_NAME}.mixedWorkload < $SETUP_FILE)
VARY_RATE=$(yq e .benchmarkConfig.${EXP_NAME}.varyRate < $SETUP_FILE)

WORKER_CPU_LIST=$(yq e .runtime.workerCpuList < $CONF_FILE)

TB_PROCESSING_MODES=
SB_PROCESSING_MODES=
TB_BACKPRESSURE_THRESHOLD=
SB_BACKPRESSURE_THRESHOLD=
QUERY_SPECIFIC_NUM_WORKERS=
INPUT_RATES_A=
INPUT_RATES_B=
INPUT_RATES_C=
ADDITIONAL_DATA_PATH=
PRIORITY_TYPE="Unassigned"

EXP_TYPE=$(yq e .benchmarkConfig.${EXP_NAME}.expType < $SETUP_FILE)
USE_TIMEOUT=$(yq e .benchmarkConfig.${EXP_NAME}.useTimeout < $SETUP_FILE)

if [[ $EXP_TYPE == "config_specific" ]]; then
	TB_PROCESSING_MODES=$(yq e .benchmarkConfig.${EXP_NAME}.specific.TB.processingModes < $SETUP_FILE)
	SB_PROCESSING_MODES=$(yq e .benchmarkConfig.${EXP_NAME}.specific.SB.processingModes < $SETUP_FILE)
	TB_BACKPRESSURE_THRESHOLD=$(yq e .benchmarkConfig.${EXP_NAME}.specific.TB.backpressureThreshold < $SETUP_FILE)
	SB_BACKPRESSURE_THRESHOLD=$(yq e .benchmarkConfig.${EXP_NAME}.specific.SB.backpressureThreshold < $SETUP_FILE)
	QUERY_SPECIFIC_NUM_WORKERS=$(yq e .benchmarkConfig.${EXP_NAME}.specific.querySpecificNumWorkers < $SETUP_FILE)
fi

if [[ $MIXED_WORKLOAD == "true" ]]; then
	INPUT_RATES_A=$(yq e .benchmarkConfig.${EXP_NAME}.mixed.workloadA.inputRates < $SETUP_FILE)
	INPUT_RATES_B=$(yq e .benchmarkConfig.${EXP_NAME}.mixed.workloadB.inputRates < $SETUP_FILE)
	INPUT_RATES_C=$(yq e .benchmarkConfig.${EXP_NAME}.mixed.workloadC.inputRates < $SETUP_FILE)
	ADDITIONAL_DATA_PATH=$(yq e .benchmarkConfig.${EXP_NAME}.additionalDataPath < $SETUP_FILE)
fi

archive_metrics() 
{
	DIR_NAME="${START_DATE_TIME}_${HOSTNAME}"
	ARCHIVE_NAME="${DIR_NAME}.tar.gz"
	echo "Creating metrics archive $ARCHIVE_NAME"
	mkdir $DIR_NAME
	cp metrics/*.csv $DIR_NAME/
	cp metrics/*.txt $DIR_NAME/
	tar cvzf $ARCHIVE_NAME $DIR_NAME
	mv $ARCHIVE_NAME $METRICS_TARGET_DIR
	rm -r $DIR_NAME 
}

execute_run_command()
{
	if [[ $BENCHMARK == "LRB" || $BENCHMARK == "LRBVB" || $BENCHMARK == "NYT" || $BENCHMARK == "NYTDQ" || $BENCHMARK == "NYTODQ" || $BENCHMARK == "MIXEDLRBYSB" || $BENCHMARK == "MIXEDYSBNYT" ]]; then
		RUN_COMMAND="$RUN_COMMAND --data_path="${PATH_TO_DATA}""
	fi
	if [[ $BENCHMARK == "MIXEDALL" ]]; then
		RUN_COMMAND="$RUN_COMMAND --data_path="${PATH_TO_DATA}" --additional_data_path="${ADDITIONAL_DATA_PATH}""
	fi
	if [[ $LATENCY_TYPE == "Processing" ]]; then
		RUN_COMMAND="$RUN_COMMAND --use_processing_latency"
	fi

	EXP_DATE_TIME=$(date +"%Y_%m_%d_%H%M")	
	if [[ $PERF_ENABLED == "true" ]]; then
		RUN_COMMAND="perf stat -I 100 --cpu $WORKER_CPU_LIST -d -x ; -o metrics/perf_stat_${SCHEDULING_MODE}_${PROCESSING_MODE}_${PRIORITY_TYPE}_${NUM_QUERIES}_${EXP_DATE_TIME}.txt $RUN_COMMAND"
	fi
	if [[ $USE_TIMEOUT == "true" ]]; then
		PER_QUERY_TIMEOUT=$((DURATION_SEC + 30))
		TIMEOUT_DURATION=$((PER_QUERY_TIMEOUT * NUM_REPETITIONS))
		RUN_COMMAND="timeout $TIMEOUT_DURATION $RUN_COMMAND"
	fi
	if [[ $SRC_RESERVOIR_SIZE != "null" ]]; then
		RUN_COMMAND="$RUN_COMMAND --src_reservoir_size $SRC_RESERVOIR_SIZE"
	fi
	RUN_COMMAND_FMT=`echo $RUN_COMMAND | sed -E 's,\\t|\\r|\\n,,g'`
	echo "Running command '$RUN_COMMAND_FMT' at $EXP_DATE_TIME"
	$RUN_COMMAND
	EXIT_CODE=$?
	echo "Command ran with exit code: $EXIT_CODE"
	echo "=============================="
	if [ ${EXIT_CODE} -ne 0 ] && [ ${EXIT_CODE} -ne 124 ]; then
		echo "Exiting program with exit code ${EXIT_CODE}"
		exit $EXIT_CODE
	fi
	sleep $SLEEP_DELAY
	echo ""
	echo ""
}

if [ ! -f $ENV_FILE ]; then
	echo "Detected environment not set. Setting environment"
	sudo cpupower frequency-set -g ondemand
	echo
	sleep 1
	sudo cpupower frequency-info
	LAST_BOOT_TIME=$(who -b | awk '{print $3,$4}' )
	CURR_DATE=$(date +"%Y-%m-%d %H:%M")
	echo "CPU governor set at: ${CURR_DATE}" > $ENV_FILE
	echo "Boot time detected: ${LAST_BOOT_TIME}" > $ENV_FILE
	echo "Finished setting up the environment."
	echo
	sleep 1
else
	LAST_BOOT_TIME=$(who -b | awk '{print $3,$4}' )
	PREV_BOOT_TIME=$( cat ${ENV_FILE} | grep "Boot time" | awk '{print $4,$5}' )
	if [[ "$PREV_BOOT_TIME" == "$LAST_BOOT_TIME" ]]; then
		echo "Last boot at : $PREV_BOOT_TIME"
	else
		echo "Detected new boot after setting environment. Resetting environment ..."
		sudo cpupower frequency-set -g ondemand
		echo
		sleep 1
		sudo cpupower frequency-info
		CURR_DATE=$(date +"%Y-%m-%d %H:%M")
		echo "CPU governor set at: ${CURR_DATE}" > $ENV_FILE
		echo "Boot time detected: ${LAST_BOOT_TIME}" > $ENV_FILE
		echo "Finished setting up the environment."
		echo
		sleep 1
	fi
fi

echo "Running benchmark $BENCHMARK $NUM_REPETITIONS times using the program enjima_benchmarks in $TARGET_DIR"
echo "Selected scheduling modes : $SCHEDULING_MODES"

if [[ $EXP_TYPE == "config_specific" ]]; then
	echo "Selected processing modes : [TB: $TB_PROCESSING_MODES, SB: $SB_PROCESSING_MODES]"
	echo "General parameters: [Backpressure Threshold: (TB: $TB_BACKPRESSURE_THRESHOLD, SB: $SB_BACKPRESSURE_THRESHOLD), Input Rates: $INPUT_RATES, Max Memory: $MAX_MEMORY]"
	echo "Use query specific number of workers : $QUERY_SPECIFIC_NUM_WORKERS"
else
	echo "Selected processing modes : $PROCESSING_MODES"
	echo "General parameters: [Backpressure Threshold: $BACKPRESSURE_THRESHOLD, Input Rates: $INPUT_RATES, Max Memory: $MAX_MEMORY]"
fi
echo "Main SB parameters: [Scheduling Period: $SCHEDULING_PERIOD ms, Preempt Modes: $PREEMPT_MODES, Priority Types: $PRIORITY_TYPES]"
echo "Other SB parameters: [Max Idle Threshold: $IDLE_THRESHOLD_MS ms]"
echo "Additional parameters: [Num Workers: $NUM_WORKERS, Num Queries: $NUM_QUERIES_ARR, Latency Reporting Resolution: $LATENCY_RES]"
echo "Press [Ctrl + C] now or forever hold your peace! ..."
sleep 5
echo ""

if [[ $DIRECTIVES == *"clean"* ]]; then
	echo "Cleaning 'metrics' and 'logs' directories..."
	CURRENT_DATE_TIME=$(date +"%Y_%m_%d_%H%M")
	(cd metrics; tar czf metrics_${CURRENT_DATE_TIME}.tar.gz *.csv *.txt; rm *.csv *.txt)
	echo "Archived metrics to file metrics/metrics_${CURRENT_DATE_TIME}.tar.gz"
	(cp conf/*.yaml *.yaml logs; cd logs; tar czf logs_${CURRENT_DATE_TIME}.tar.gz *.log* *.yaml; rm *.log* *.yaml)
	echo "Archived logs and configurations to file logs/logs_${CURRENT_DATE_TIME}.tar.gz"
	echo ""
fi

PERF_ENABLED="false"
if [[ $DIRECTIVES == *"perf"* ]]; then
	PERF_ENABLED="true"
fi

START_DATE_TIME=$(date +"%Y_%m_%d_%H%M")
SLEEP_DELAY=10

if [[ $EXP_TYPE == "config_specific" ]]; then
	RUN_COMMAND_INVARIANT="$TARGET_DIR/enjima_benchmarks -b ${BENCHMARK} -t $DURATION_SEC -m $MAX_MEMORY -r $NUM_REPETITIONS --latency_res $LATENCY_RES --events_per_block $EVENTS_PER_BLOCK --blocks_per_chunk $BLOCKS_PER_CHUNK"
	for INPUT_RATE in $INPUT_RATES; do
		for LATENCY_EMIT_PERIOD_MS in $LATENCY_VALUES; do
			for SCHEDULING_MODE in $SCHEDULING_MODES; do	
				RUN_COMMAND_BASE="$RUN_COMMAND_INVARIANT -l $LATENCY_EMIT_PERIOD_MS -s $SCHEDULING_MODE"
				if [[ $MIXED_WORKLOAD == "true" ]]; then
					RUN_COMMAND_BASE="$RUN_COMMAND_BASE --rate_a $INPUT_RATES_A --rate_b $INPUT_RATES_B"
					if [[ $BENCHMARK == "MIXEDALL" ]]; then
						RUN_COMMAND_BASE="$RUN_COMMAND_BASE --rate_c $INPUT_RATES_C"
					fi

				else
					RUN_COMMAND_BASE="$RUN_COMMAND_BASE  -i $INPUT_RATE"
				fi
				if [[ $VARY_RATE == "true" ]]; then
					RUN_COMMAND_BASE="$RUN_COMMAND_BASE --min_rate $MIN_INPUT_RATES --step $STEP_SIZE --shift_interval $SHIFT_INTERVAL_MS"
				fi
				if [[ $SCHEDULING_MODE == "TB" ]]; then
					for PROCESSING_MODE in $TB_PROCESSING_MODES; do
						for NUM_QUERIES in $NUM_QUERIES_ARR; do
							sleep $SLEEP_DELAY
							if [[ $QUERY_SPECIFIC_NUM_WORKERS == "true" ]]; then
								EVENT_GEN_CPU_LIST=$(yq e .benchmarkConfig.${EXP_NAME}.specific.q${NUM_QUERIES}.eventGenCpuList < $SETUP_FILE)
								sed -i "s/eventGenCpuList:.*/eventGenCpuList: \"${EVENT_GEN_CPU_LIST}\"/g" $CONF_FILE
								WORKER_CPU_LIST=$(yq e .benchmarkConfig.${EXP_NAME}.specific.q${NUM_QUERIES}.workerCpuList < $SETUP_FILE)
								sed -i "s/workerCpuList:.*/workerCpuList: \"${WORKER_CPU_LIST}\"/g" $CONF_FILE
								NUM_WORKERS=$(yq e .benchmarkConfig.${EXP_NAME}.specific.q${NUM_QUERIES}.numWorkers < $SETUP_FILE)
							fi
							RUN_COMMAND="$RUN_COMMAND_BASE -p $PROCESSING_MODE --backpressure_threshold $TB_BACKPRESSURE_THRESHOLD -q $NUM_QUERIES -w $NUM_WORKERS"
							execute_run_command
						done
					done
				else		
					for PROCESSING_MODE in $SB_PROCESSING_MODES; do
						for PREEMPT_MODE in $PREEMPT_MODES; do
							for PRIORITY_TYPE in $PRIORITY_TYPES; do
								for NUM_QUERIES in $NUM_QUERIES_ARR; do
									sleep $SLEEP_DELAY
									if [[ $QUERY_SPECIFIC_NUM_WORKERS == "true" ]]; then
										EVENT_GEN_CPU_LIST=$(yq e .benchmarkConfig.${EXP_NAME}.specific.q${NUM_QUERIES}.eventGenCpuList < $SETUP_FILE)
										sed -i "s/eventGenCpuList:.*/eventGenCpuList: \"${EVENT_GEN_CPU_LIST}\"/g" $CONF_FILE
										WORKER_CPU_LIST=$(yq e .benchmarkConfig.${EXP_NAME}.specific.q${NUM_QUERIES}.workerCpuList < $SETUP_FILE)
										sed -i "s/workerCpuList:.*/workerCpuList: \"${WORKER_CPU_LIST}\"/g" $CONF_FILE
										NUM_WORKERS=$(yq e .benchmarkConfig.${EXP_NAME}.specific.q${NUM_QUERIES}.numWorkers < $SETUP_FILE)
									fi
									RUN_COMMAND="$RUN_COMMAND_BASE --preempt_mode $PREEMPT_MODE --priority_type $PRIORITY_TYPE \
--scheduling_period $SCHEDULING_PERIOD --max_idle_threshold_ms $IDLE_THRESHOLD_MS -p $PROCESSING_MODE --backpressure_threshold $SB_BACKPRESSURE_THRESHOLD -q $NUM_QUERIES -w $NUM_WORKERS"
									execute_run_command
								done
							done
						done
					done
				fi
			done
			echo "Finished benchmark for latency emit period of $LATENCY_EMIT_PERIOD_MS ms"
			echo ""
		done
	done
elif [[ $EXP_TYPE == "sensitivity_analysis" ]]; then
	RUN_COMMAND_INVARIANT="$TARGET_DIR/enjima_benchmarks -b ${BENCHMARK} -t $DURATION_SEC -m $MAX_MEMORY -r $NUM_REPETITIONS --latency_res $LATENCY_RES -w $NUM_WORKERS"
	LATENCY_EMIT_PERIOD_MS=$LATENCY_VALUES
	OPT_VAL="0"
	for INPUT_RATE in $INPUT_RATES; do		
		for IT_MAX in $ADAPT_MAX_IDLE_THRESHOLD; do
			if [[ $IT_MAX != "null" ]]; then
				sed -i "s/maxIdleThreshold:.*/maxIdleThreshold: \"${IT_MAX}\"/g" $CONF_FILE
			fi
			if [[ $OPT_VAL_TYPE == "MAX_IT" ]]; then
				OPT_VAL=$IT_MAX
			fi
			for ET_MAX in $ADAPT_MAX_EVENT_THRESHOLD; do
				if [[ $ET_MAX != "null" ]]; then
					sed -i "s/maxEventThreshold:.*/maxEventThreshold: \"${ET_MAX}\"/g" $CONF_FILE
				fi
				if [[ $OPT_VAL_TYPE == "MAX_ET" ]]; then
					OPT_VAL=$ET_MAX
				fi
				for IT_STEP in $ADAPT_STEP_IDLE_THRESHOLD; do
					if [[ $IT_STEP != "null" ]]; then
						sed -i "s/idleThresholdMaxAdjustment:.*/idleThresholdMaxAdjustment: \"${IT_STEP}\"/g" $CONF_FILE
					fi
					if [[ $OPT_VAL_TYPE == "STEP_IT" ]]; then
						OPT_VAL=$IT_STEP
					fi
					for ET_STEP in $ADAPT_STEP_EVENT_THRESHOLD; do
						if [[ $ET_STEP != "null" ]]; then
							sed -i "s/eventThresholdMaxAdjustment:.*/eventThresholdMaxAdjustment: \"${ET_STEP}\"/g" $CONF_FILE
						fi
						if [[ $OPT_VAL_TYPE == "STEP_ET" ]]; then
							OPT_VAL=$ET_STEP
						fi
						for IT in $IDLE_THRESHOLD_MS; do
							for ET in $EVENT_THRESHOLD; do
								for EBP in $EVENTS_PER_BLOCK; do
									for BPC in $BLOCKS_PER_CHUNK; do
										for SCHEDULING_MODE in $SCHEDULING_MODES; do	
											RUN_COMMAND_BASE="$RUN_COMMAND_INVARIANT -i $INPUT_RATE -l $LATENCY_EMIT_PERIOD_MS -s $SCHEDULING_MODE \
												--backpressure_threshold $BACKPRESSURE_THRESHOLD --event_threshold $ET --opt_val $OPT_VAL"
											if [[ $SCHEDULING_MODE == "TB" ]]; then
												for PROCESSING_MODE in $PROCESSING_MODES; do
													for NUM_QUERIES in $NUM_QUERIES_ARR; do
														sleep $SLEEP_DELAY
														RUN_COMMAND="$RUN_COMMAND_BASE -p $PROCESSING_MODE -q $NUM_QUERIES --events_per_block $EBP --blocks_per_chunk $BPC"
														execute_run_command
													done
												done
											else		
												for PROCESSING_MODE in $PROCESSING_MODES; do
													for PREEMPT_MODE in $PREEMPT_MODES; do
														for PRIORITY_TYPE in $PRIORITY_TYPES; do
															for SP in $SCHEDULING_PERIOD; do
																for NUM_QUERIES in $NUM_QUERIES_ARR; do
																	sleep $SLEEP_DELAY
																	RUN_COMMAND="$RUN_COMMAND_BASE --preempt_mode $PREEMPT_MODE --priority_type $PRIORITY_TYPE \
																		--scheduling_period $SP --max_idle_threshold_ms $IT -p $PROCESSING_MODE \
																		-q $NUM_QUERIES --events_per_block $EBP --blocks_per_chunk $BPC"
																	execute_run_command
																done
															done
														done
													done
												done
											fi
										done
										echo "Finished benchmark for latency emit period of $LATENCY_EMIT_PERIOD_MS ms"
										echo ""
									done
								done
							done
						done
					done
				done
			done
		done
	done
else
	RUN_COMMAND_INVARIANT="$TARGET_DIR/enjima_benchmarks -b ${BENCHMARK} -t $DURATION_SEC -m $MAX_MEMORY -r $NUM_REPETITIONS \
--backpressure_threshold $BACKPRESSURE_THRESHOLD --latency_res $LATENCY_RES -w $NUM_WORKERS --events_per_block $EVENTS_PER_BLOCK --blocks_per_chunk $BLOCKS_PER_CHUNK"
	for INPUT_RATE in $INPUT_RATES; do
		for LATENCY_EMIT_PERIOD_MS in $LATENCY_VALUES; do
			for PROCESSING_MODE in $PROCESSING_MODES; do
				for SCHEDULING_MODE in $SCHEDULING_MODES; do
					for NUM_QUERIES in $NUM_QUERIES_ARR; do
						RUN_COMMAND_BASE="$RUN_COMMAND_INVARIANT -i $INPUT_RATE  -l $LATENCY_EMIT_PERIOD_MS \
-p $PROCESSING_MODE -s $SCHEDULING_MODE -q $NUM_QUERIES"
						if [[ $SCHEDULING_MODE == "TB" ]]; then
							sleep $SLEEP_DELAY
							RUN_COMMAND=$RUN_COMMAND_BASE
							execute_run_command
						else	
							for PREEMPT_MODE in $PREEMPT_MODES; do
								for PRIORITY_TYPE in $PRIORITY_TYPES; do
									sleep $SLEEP_DELAY
									RUN_COMMAND="$RUN_COMMAND_BASE \
--preempt_mode $PREEMPT_MODE --priority_type $PRIORITY_TYPE --scheduling_period $SCHEDULING_PERIOD --max_idle_threshold_ms $IDLE_THRESHOLD_MS"
									execute_run_command
								done
							done
						fi
					done
				done
			done
			echo "Finished benchmark for latency emit period of $LATENCY_EMIT_PERIOD_MS ms"
			echo ""
		done
	done
fi

if [[ $DIRECTIVES == *"metrics"* ]]; then
	archive_metrics
fi
echo "Done."
