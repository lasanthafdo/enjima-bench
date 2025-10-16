# Enjima Benchmarks
A benchmark suite for Enjima: A Resource-Adaptive Stream Processing System
<!--
```
Usage:
  enjima_benchmarks [OPTION...]

 default options:
  -b, --bench arg               Benchmark name (default: LRB)
  -s, --scheduling_mode arg     Operator scheduling mode (default: TB)
      --scheduling_period arg   Scheduling period (in milliseconds) 
                                (default: 50)
  -t, --duration arg            Benchmark duration (in seconds) (default: 
                                30)
  -i, --rate arg                Input event rate (in events/seconds) 
                                (default: 1000000)
      --rate_a arg              Input event rate (in events/seconds) for 
                                workload A (default: 0)
      --rate_b arg              Input event rate (in events/seconds) for 
                                workload B (default: 0)
      --rate_c arg              Input event rate (in events/seconds) for 
                                workload C (default: 0)
      --query_distribution arg  Comma-separated query distribution for 
                                mixed workloads (default: 0)
      --min_rate arg            Minimum input event rate (in 
                                events/seconds) (default: 0)
      --step arg                Step size(s) to vary input event rate (in 
                                events/seconds) (default: 0)
      --shift_interval arg      Interval between varying input event rate 
                                (in milliseconds) (default: 0)
  -r, --repeat arg              Number of times to repeat (default: 1)
  -q, --num_queries arg         Number of queries to execute (default: 1)
  -w, --num_workers arg         Number of worker threads to spawn for 
                                state-based scheduling (default: 8)
  -l, --latency_record_period arg
                                A latency record will be emitted from the 
                                source operator this number of milliseconds 
                                after the previous (default: 50)
      --latency_res arg         Resolution of the latency timestamp (e.g., 
                                milliseconds, microseconds etc.) (default: 
                                Millis)
  -m, --memory arg              Amount of memory for data exchange (in 
                                megabytes) (default: 512)
      --events_per_block arg    Number of events per memory block (default: 
                                384)
      --blocks_per_chunk arg    Number of blocks per memory chunk (default: 
                                4)
      --backpressure_threshold arg
                                Number of chunks that can be active per 
                                operator before backpressure is triggered 
                                (default: 50)
      --max_idle_threshold_ms arg
                                Minimum time (ms) before rescheduling an 
                                idle operator below event threshold 
                                (default: 1)
      --event_threshold arg     Minimum pending events required for an 
                                operator to be considered for scheduling 
                                (default: 1000)
      --src_reservoir_size arg  Capacity of the event reservoir at the 
                                generator (default: 10000000)
  -p, --processing_mode arg     Processing mode (default: BlockBasedBatch)
      --par_level arg           Parallelism level (default: 1)
      --preempt_mode arg        Preempt mode for scheduling (default: 
                                NonPreemptive)
      --priority_type arg       Priority calculation type for scheduling 
                                (default: InputQueueSize)
      --generate_with_emit      Enable generating events at the time of 
                                emission by the source operator
      --use_processing_latency  Latency markers are generated at the time 
                                of ingestion by the source operator
      --data_path arg           Path to source files generating events 
                                (default: ./car.dat)
      --additional_data_path arg
                                Path to source files generating events for 
                                other benchmark (default: "")
      --opt_val arg             Optional system identification value 
                                (default: 0)
  -h, --help                    Print usage
```

For example, if you want to run the YSB benchmark for 30 seconds with 512 MiB of memory, with other parameters as the default, run the following command against the built binary in a shell.

```
./enjima_benchmarks -b ysb -t 30 -m 512
```
-->

### Preparing Build
There are some files to prepare before executing benchmarks.

## Prerequisite libraries
Install the necessary dependencies as shown.
```sh
sudo snap install yq --classic
```

### Creating <code>lib</code> folder
If the <code>lib</code> folder does not exist, create one. Then, copy following files generated after building <code>Enjima</code>. Refer to <a href="https://github.com/lasanthafdo/enjima-lib">this link</a> for details on how to build <code>Enjima</code>
```
libEnjimaDebug.a      (if you built enjima with Debug mode)
libEnjimaRelease.a    (if you build enjima with Release mode)
```

### Creating <code>.yaml</code> files
Copy the content within <code>bench.yaml.sample</code> into <code>bench.yaml</code>:
```sh
cat bench.yaml.sample > bench.yaml
```
Do the same for <code>enjima-config.yaml</code> within <code>conf</code> directory:
```sh
cat ./conf/enjima-config.yaml.sample > ./conf/enjima-config.yaml
```

Please note these configuration files have machine specific configurations. 

The `eventGenCpuList` and the `workerCpuList` in `conf/enjima-config.yaml` contains a comma-separated list of the IDs of CPU cores used as event-generation CPUs and worker CPUs. 

The number of worker CPUs listed in `conf/enjima-config.yaml` should match the `numWorkers` parameter in the relevant configuration of the `bench.yaml` file. Similarly ensure that the `maxMemory` of the workload configuration in `bench.yaml` does not exceed the maximum memory available within the machine.

### Creating other folders
Create <code>logs</code> and <code>metrics</code> folders:
```sh
mkdir logs && mkdir metrics
```


## Build using CMake
First, create a directory for the build files. Ensure the directory name matches the <code>targetDir</code> specified in <code>bench.yaml</code>:
```sh
mkdir cmake-release-build && cd cmake-release-build
```

Then,configure the build by specifying the build type (either <code>Release</code> or <code>Build</code>):
```sh
cmake -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) ..
```

Finally, compile <code>enjima-benchmark</code> executable:
```sh
cmake --build . -j
```

## Executing Benchmarks
Use the following command to execute benchmakrs:
```sh
./run_benchmark.sh BENCHMARK DURATION_SEC NUM_REPETITIONS DIRECTIVES
```

| Arguments | Description |
| :--------- | :----------- |
| <code>BENCHMARK</code> | The type of benchmark to run (e.g. YSB) |
| <code>DURATION_SEC</code> | The duration of bechmark in seconds |
| <code>NUM_REPETITIONS</code> | The number of times to repeat the benchmark |
| <code>DIRECTIVES</code> | Parameters for specific configuration |
| | <code>perf</code> : enabling performance monitoring |
| | <code>clean</code> : cleaning the <code>logs</code> and <code>metrics</code> folders|
| | <code>metrics</code> : archives the metrics collected to the <code>METRICS_TARGET_DIR</code> directory specified in the <code>run_benchmark.sh</code> script|

For example
```sh
./run_benchmark.sh SCALE_YSB 60 2 clean
```
This command will run the configuration defined under SCALE_YSB in `bench.yaml` to run <code>Enjima</code> for 60 seconds and repeat this experiment twice. It will also clean the metrics directory before starting the run. 

The SCALE_YSB configuration runs the YSB benchmark while scaling the number of queries from 1 to 10. Similarly, you can run SCALE_LRB, SINGLE_YSB, INPUT_VAR_LRB and other configurations available in the `bench.yaml.sample' file. Please note that in order to run the NYT benchmark, you have to specify NYTODQ as the benchmark name (e.g., SCALE_NYTODQ etc).
