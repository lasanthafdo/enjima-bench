#!/bin/bash

RUN_SCRIPT_PID=`ps -ef | grep -m 1 run_benchmark | grep -v grep | awk '{print $2}'`
while [ ! -z "$RUN_SCRIPT_PID" ]
do
	ENJIMA_PID=`ps -ef | grep -m 1 enjima_benchmarks | grep -v grep | awk '{print $2}'`
	while [ ! -z "$ENJIMA_PID" ]
	do
		CMD_NAME=`ps -p ${ENJIMA_PID} -o comm | tail -1`
		ENJIMA_CPU_USAGE=`top -b -n 2 -p $ENJIMA_PID | tail -1 | awk '{print $9}' | cut -d "." -f 1`

		echo "CPU usage of program ${CMD_NAME} with PID ${ENJIMA_PID} is ${ENJIMA_CPU_USAGE}"

		UPPER_THRESHOLD_CPU=1000
		LOWER_THRESHOLD_CPU=500
		if (( ENJIMA_CPU_USAGE < LOWER_THRESHOLD_CPU )); then
			sleep 10
			CMD_NAME=`ps -p ${ENJIMA_PID} -o comm | tail -1`
			ENJIMA_CPU_USAGE=`top -b -n 2 -p $ENJIMA_PID | tail -1 | awk '{print $9}' | cut -d "." -f 1`
			echo "CPU usage of program ${CMD_NAME} with PID ${ENJIMA_PID} is ${ENJIMA_CPU_USAGE}"
		fi
		if (( ENJIMA_CPU_USAGE < UPPER_THRESHOLD_CPU )); then
			echo "CPU usage less than ${UPPER_THRESHOLD_CPU} detected. Entering GDB..."
			gdb attach $ENJIMA_PID
		fi
		sleep 5
		ENJIMA_PID=`ps -ef | grep -m 1 enjima_benchmarks | grep -v grep | awk '{print $2}'`
	done
	sleep 30
	RUN_SCRIPT_PID=`ps -ef | grep -m 1 run_benchmark | grep -v grep | awk '{print $2}'`
done
