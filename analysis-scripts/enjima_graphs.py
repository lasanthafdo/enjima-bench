import argparse
import json

from matplotlib import colors as mcolors

from time_series_common import *

pd.set_option('display.max_columns', 10)


def load_exp_sel():
    try:
        with open("experiment_selection.json", "r") as file:
            return json.load(file)
    except FileNotFoundError:
        raise Exception(
            "Configuration file 'experiment_selection.json' not found. Please create it based on 'config.example.json'.")
    except json.JSONDecodeError:
        raise Exception("Error parsing 'experiment_selection.json'. Please ensure it's correctly formatted.")


def load_config(config_file_name="config.json"):
    try:
        with open(config_file_name, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        raise Exception(
            "Configuration file '" + config_file_name + "' not found. Please create it based on 'config.example.json'.")
    except json.JSONDecodeError:
        raise Exception("Error parsing '" + config_file_name + "'. Please ensure it's correctly formatted.")


def load_labels():
    try:
        with open("labels.json", "r") as file:
            return json.load(file)
    except FileNotFoundError:
        raise Exception("Configuration file 'labels.json' not found. Please create it based on 'labels.example.json'.")
    except json.JSONDecodeError:
        raise Exception("Error parsing 'labels.json'. Please ensure it's correctly formatted.")


def process_target_metrics(config, labels, data_dir, results_dir, csv_columns, target_filename, independent_var,
                           target_var_filter, exp_id, column_dtypes, group_by_types, non_group_types, sci_labels=False,
                           unit_scale=1.0, iter_num="1", target_metric="", agg_type="avg", graph_type="bar",
                           y_scale="linear"):
    time_lower = config['time_lower']
    time_upper = config['time_upper']

    if target_metric == "":
        target_metric = target_filename
    if config['multistage_processing'] and config['processing_stage'] == 4:
        combined_data_ids = config['combined_data_ids'].split("|")
        all_files = []
        for combined_data_id in combined_data_ids:
            all_files.append(config['data_dir'] + "/" + combined_data_id + "/" + target_filename + ".csv")
        df_list = []
        for filename in all_files:
            metric_df_per_run = pd.read_csv(filename, names=csv_columns, dtype=column_dtypes)
            df_list.append(metric_df_per_run)
        metric_df = pd.concat(df_list, ignore_index=True)
    else:
        metric_df = pd.read_csv(data_dir + "/" + exp_id + "/" + target_filename + ".csv", names=csv_columns,
                                dtype=column_dtypes)

    thread_ids = metric_df['thread_id'].unique()
    print(thread_ids)
    sys_ids = metric_df['sys_id'].unique()
    benchmarks = []
    for sys_id in sys_ids:
        benchmark_name = sys_id.split("_")[1]
        if benchmark_name not in benchmarks:
            benchmarks.append(benchmark_name)

    assert len(benchmarks) == 1
    for benchmark_name in benchmarks:
        agg_metrics = {}
        x_var_keys = {}
        non_group_text = ""
        for thread_id in thread_ids:
            filtered_metric_df = metric_df[metric_df['thread_id'] == thread_id].copy()

            filtered_metric_df = filtered_metric_df[
                filtered_metric_df['metric_type'].str.contains(target_var_filter, regex=True)]
            if filtered_metric_df.empty:
                continue

            assert (len(filtered_metric_df['sys_id'].unique()) == 1)
            sys_id_parts = filtered_metric_df['sys_id'].unique()[0].split("_")

            if non_group_text == "" and sys_id_parts[config['part_indices']["scheduling_mode"]] != "TB":
                non_group_text = get_non_group_text(config, labels, non_group_types, sys_id_parts)
            group_id = get_group_id(config, group_by_types, sys_id_parts)
            x_var_key = int(sys_id_parts[config['part_indices'][independent_var]])

            if x_var_key not in x_var_keys:
                x_var_keys[x_var_key] = []
            if group_id not in agg_metrics:
                agg_metrics[group_id] = []
                x_var_keys[x_var_key].append(group_id)

            if group_id not in agg_metrics:
                agg_metrics[group_id] = []

            old_target_metric = target_metric
            if target_metric.endswith('schedTime_counter') or target_metric.endswith('schedCount_counter'):
                target_metric = re.sub(r"(schedCount|schedTime)", r"\1Diff", target_metric)
                target_var = config['target_vars'][target_metric]
                filtered_metric_df[target_var] = (filtered_metric_df[target_var] -
                                                  filtered_metric_df[target_var].shift(1, fill_value=0))
            else:
                target_var = config['target_vars'][target_metric]

            filtered_metric_df['rel_time'] = filtered_metric_df['timestamp'].subtract(
                filtered_metric_df['timestamp'].min()).div(1_000)
            filtered_metric_df = filtered_metric_df[
                (filtered_metric_df['rel_time'] > time_lower) & (filtered_metric_df['rel_time'] <= time_upper)]

            if agg_type != "none":
                if agg_type == "sum":
                    agg_metric_df = filtered_metric_df.groupby(["thread_id", "timestamp", "rel_time"]).sum()
                elif agg_type == "avg":
                    agg_metric_df = filtered_metric_df.groupby(["thread_id", "timestamp", "rel_time"]).mean(
                        numeric_only=True)
                else:
                    raise "Unknown aggregation type '" + agg_type + "'"
                agg_metric = agg_metric_df.loc[:, target_var].mean(numeric_only=True) / unit_scale
            else:
                agg_metric = filtered_metric_df.loc[:, target_var].mean(numeric_only=True) / unit_scale
            agg_metrics[group_id].append(agg_metric)
            target_metric = old_target_metric

        if target_metric.endswith('schedTime_counter') or target_metric.endswith('schedCount_counter'):
            target_metric = re.sub(r"(schedCount|schedTime)", r"\1Diff", target_metric)

        x_var_keys = dict(sorted(x_var_keys.items()))
        sorted_agg_metrics = {}
        sorted_unfiltered_agg_metrics = {}
        for x_var_key, group_ids in x_var_keys.items():
            for group_id in group_ids:
                if config.get('reject_outliers', True):
                    filtered_data = reject_outliers(agg_metrics[group_id], 200)
                    if len(filtered_data) < 5:
                        raise Exception("Cannot find 5 data points within IQR")
                    # print(group_id + ":" + str(len(filtered_data)))
                    # print(filtered_data)
                    sorted_agg_metrics[group_id] = filtered_data[:5]
                sorted_unfiltered_agg_metrics[group_id] = agg_metrics[group_id]
        if config.get('reject_outliers', True):
            agg_metrics = sorted_agg_metrics
        else:
            agg_metrics = sorted_unfiltered_agg_metrics

        if graph_type == "bar":
            plot_bar_graph_for_metric(config, labels, results_dir, agg_metrics, independent_var, target_metric,
                                      non_group_text,
                                      derive_plot_title(config, labels, benchmark_name, target_metric),
                                      agg_type + "_" + target_metric + "_" + benchmark_name,
                                      exp_id, iter_num,
                                      sci_labels=sci_labels, show_plot=True, y_scale=y_scale)
        else:
            plot_line_graph_for_metric(config, labels, target_metric, results_dir, agg_metrics, independent_var,
                                       target_metric, non_group_text,
                                       derive_plot_title(config, labels, benchmark_name, target_metric),
                                       agg_type + "_" + target_metric + "_" + benchmark_name,
                                       exp_id, iter_num,
                                       sci_labels=sci_labels, show_plot=True, y_scale=y_scale)
        return sorted_unfiltered_agg_metrics


def process_target_metrics_extended(config, labels, data_dir, results_dir, csv_columns, target_filename,
                                    independent_var, target_var_filter, exp_id, column_dtypes,
                                    group_by_types, non_group_types, sci_labels=False, unit_scale=1.0, iter_num="1",
                                    target_metric="", agg_type="avg", graph_type="bar", y_scale="linear"):
    time_lower = config['time_lower']
    time_upper = config['time_upper']

    if target_metric == "":
        target_metric = target_filename
    metric_df = pd.read_csv(data_dir + "/" + exp_id + "/" + target_filename + ".csv", names=csv_columns,
                            dtype=column_dtypes)
    thread_ids = metric_df['thread_id'].unique()
    print(thread_ids)
    sys_ids = metric_df['sys_id'].unique()
    benchmarks = []
    for sys_id in sys_ids:
        benchmark_name = sys_id.split("_")[1]
        if benchmark_name not in benchmarks:
            benchmarks.append(benchmark_name)

    assert len(benchmarks) == 1
    for benchmark_name in benchmarks:
        agg_metrics = {}
        non_group_text = ""
        for thread_id in thread_ids:
            filtered_metric_df = metric_df[metric_df['thread_id'] == thread_id].copy()

            filtered_metric_df = filtered_metric_df[
                filtered_metric_df['metric_type'].str.contains(target_var_filter, regex=True)]
            if filtered_metric_df.empty:
                continue

            assert (len(filtered_metric_df['sys_id'].unique()) == 1)
            sys_id_parts = filtered_metric_df['sys_id'].unique()[0].split("_")

            if non_group_text == "" and sys_id_parts[config['part_indices']["scheduling_mode"]] != "TB":
                non_group_text = get_non_group_text(config, labels, non_group_types, sys_id_parts)
            group_id = get_group_id(config, group_by_types, sys_id_parts)

            if group_id not in agg_metrics:
                agg_metrics[group_id] = []

            old_target_metric = target_metric
            if target_metric.endswith('schedTime_counter') or target_metric.endswith('schedCount_counter'):
                target_metric = re.sub(r"(schedCount|schedTime)", r"\1Diff", target_metric)
                target_var = config['target_vars'][target_metric]
                filtered_metric_df[target_var] = (filtered_metric_df[target_var] -
                                                  filtered_metric_df[target_var].shift(1, fill_value=0))
            else:
                target_var = config['target_vars'][target_metric]

            filtered_metric_df['rel_time'] = filtered_metric_df['timestamp'].subtract(
                filtered_metric_df['timestamp'].min()).div(1_000)
            filtered_metric_df = filtered_metric_df[
                (filtered_metric_df['rel_time'] > time_lower) & (filtered_metric_df['rel_time'] <= time_upper)]

            if agg_type != "none":
                if agg_type == "sum":
                    agg_metric_df = filtered_metric_df.groupby(["thread_id", "timestamp", "rel_time"]).sum()
                elif agg_type == "avg":
                    agg_metric_df = filtered_metric_df.groupby(["thread_id", "timestamp", "rel_time"]).mean()
                else:
                    raise "Unknown aggregation type '" + agg_type + "'"
                agg_metric = agg_metric_df.loc[:, target_var].mean() / unit_scale
            else:
                agg_metric = filtered_metric_df.loc[:, target_var].mean() / unit_scale
            agg_metrics[group_id].append(agg_metric)
            target_metric = old_target_metric

        if target_metric.endswith('schedTime_counter') or target_metric.endswith('schedCount_counter'):
            target_metric = re.sub(r"(schedCount|schedTime)", r"\1Diff", target_metric)
        if graph_type == "bar":
            plot_bar_graph_for_metric(config, labels, results_dir, agg_metrics, independent_var, target_metric,
                                      non_group_text,
                                      derive_plot_title(config, labels, benchmark_name, target_metric),
                                      agg_type + "_" + target_metric + "_" + benchmark_name,
                                      exp_id, iter_num,
                                      sci_labels=sci_labels, show_plot=True, y_scale=y_scale)
        else:
            plot_line_graph_for_metric(config, labels, target_metric, results_dir, agg_metrics, independent_var,
                                       target_metric, non_group_text,
                                       derive_plot_title(config, labels, benchmark_name, target_metric),
                                       agg_type + "_" + target_metric + "_" + benchmark_name,
                                       exp_id, iter_num,
                                       sci_labels=sci_labels, show_plot=True, y_scale=y_scale)
        return agg_metrics


def process_and_flush_target_metrics(config, labels, data_dir, intermediate_data_dir, csv_columns, target_filename,
                                     independent_var, target_var_filter, experiment_ids, column_dtypes,
                                     group_by_types, non_group_types, target_metric="", agg_type="avg"):
    time_lower = config['time_lower']
    time_upper = config['time_upper']

    if target_metric == "":
        target_metric = target_filename
    experiment_id_li = experiment_ids.split("|")
    experiment_ids_with_tid_skips = config.get("thread_ids_to_skip", [])
    experiment_ids_with_sid_skips = config.get("sys_ids_to_skip", [])
    for experiment_id in experiment_id_li:
        metric_df = pd.read_csv(data_dir + "/" + experiment_id + "/" + target_filename + ".csv", names=csv_columns,
                                dtype=column_dtypes)
        thread_ids = metric_df['thread_id'].unique()
        print(thread_ids)
        sys_ids = metric_df['sys_id'].unique()
        benchmarks = []
        for sys_id in sys_ids:
            benchmark_name = sys_id.split("_")[1]
            if benchmark_name not in benchmarks:
                benchmarks.append(benchmark_name)

        thread_ids_to_skip = []
        if experiment_id in experiment_ids_with_tid_skips:
            thread_ids_to_skip = experiment_ids_with_tid_skips[experiment_id].split(",")
        sys_ids_to_skip = []
        if experiment_id in experiment_ids_with_sid_skips:
            sys_ids_to_skip = experiment_ids_with_sid_skips[experiment_id].split(",")

        df_to_flush = None
        if sys_ids_to_skip:
            pattern_str = ""
            for sys_id in sys_ids_to_skip:
                pattern_str += sys_id + "|"
            pattern_str = pattern_str[:-1]
            pattern = re.compile(pattern_str)
        for _ in benchmarks:
            agg_metrics = {}
            non_group_text = ""
            for thread_id in thread_ids:
                if str(thread_id) in thread_ids_to_skip:
                    print("Skipping thread ID: " + str(thread_id))
                    continue

                filtered_metric_df = metric_df[metric_df['thread_id'] == thread_id].copy()
                filtered_metric_df = filtered_metric_df[
                    filtered_metric_df['metric_type'].str.contains(target_var_filter, regex=True)]
                if filtered_metric_df.empty:
                    continue

                assert (len(filtered_metric_df['sys_id'].unique()) == 1)
                curr_sys_id = filtered_metric_df['sys_id'].unique()[0]
                if sys_ids_to_skip and pattern.match(curr_sys_id):
                    print("Skipping sys ID: " + curr_sys_id)
                    continue

                sys_id_parts = curr_sys_id.split("_")
                if non_group_text == "" and sys_id_parts[config['part_indices']["scheduling_mode"]] != "TB":
                    non_group_text = get_non_group_text(config, labels, non_group_types, sys_id_parts)
                group_id = get_group_id(config, group_by_types, sys_id_parts)

                if group_id not in agg_metrics:
                    agg_metrics[group_id] = []

                old_target_metric = target_metric
                if target_metric.endswith('schedTime_counter') or target_metric.endswith('schedCount_counter'):
                    target_metric = re.sub(r"(schedCount|schedTime)", r"\1Diff", target_metric)
                    target_var = config['target_vars'][target_metric]
                    filtered_metric_df[target_var] = (filtered_metric_df[target_var] -
                                                      filtered_metric_df[target_var].shift(1, fill_value=0))

                filtered_metric_df['rel_time'] = filtered_metric_df['timestamp'].subtract(
                    filtered_metric_df['timestamp'].min()).div(1_000)
                filtered_metric_df = filtered_metric_df[
                    (filtered_metric_df['rel_time'] > time_lower) & (filtered_metric_df['rel_time'] <= time_upper)]

                filtered_metric_df['thread_id'] = experiment_id + "_" + filtered_metric_df['thread_id'].astype(str)
                filtered_metric_df['agg_metric_type'] = filtered_metric_df['metric_type'].str.split("_", n=1).str[0]
                if independent_var == "num_queries":
                    if agg_type == "sum":
                        agg_metric_df = filtered_metric_df.groupby(
                            ["thread_id", "sys_id", "agg_metric_type", "timestamp", "rel_time"]).sum(numeric_only=True)
                    else:
                        agg_metric_df = filtered_metric_df.groupby(
                            ["thread_id", "sys_id", "agg_metric_type", "timestamp", "rel_time"]).mean(numeric_only=True)
                    agg_metric_df = agg_metric_df.reset_index()
                    if df_to_flush is None:
                        df_to_flush = agg_metric_df
                    else:
                        df_to_flush = pd.concat([df_to_flush, agg_metric_df], ignore_index=True)
                else:
                    if df_to_flush is None:
                        df_to_flush = filtered_metric_df
                    else:
                        df_to_flush = pd.concat([df_to_flush, filtered_metric_df], ignore_index=True)

                target_metric = old_target_metric

            print(df_to_flush)
            df_to_flush.to_csv(intermediate_data_dir + "/" + target_filename + "_" + experiment_id + ".csv",
                               index=False)


def process_target_metrics_from_intermediate(config, labels, data_dir, results_dir, target_filename, independent_var,
                                             intermediate_input_id, group_by_types,
                                             non_group_types, sci_labels=False, unit_scale=1.0, graph_type_list=["bar"],
                                             iter_num="1", target_metric="", agg_type="avg", y_scale="linear"):
    if target_metric == "":
        target_metric = target_filename
    path = data_dir + "/intermediate_data_" + intermediate_input_id  # use your path
    all_files = glob.glob(os.path.join(path, target_filename + "*.csv"))
    df_list = []
    for filename in all_files:
        metric_df_per_run = pd.read_csv(filename)
        df_list.append(metric_df_per_run)
    metric_df = pd.concat(df_list, ignore_index=True)

    thread_ids = metric_df['thread_id'].unique()
    print(thread_ids)
    sys_ids = metric_df['sys_id'].unique()
    benchmarks = []
    is_mixed_workload = config.get("mixed_workload", False)
    generate_per_num_queries = True
    target_num_queries = 0
    if is_mixed_workload:
        generate_per_num_queries = config["mixed_workload_params"].get("generate_per_num_queries", True)
        target_num_queries = config['mixed_workload_params']['target_num_queries']

    for sys_id in sys_ids:
        benchmark_name = sys_id.split("_")[1]
        if benchmark_name == "NYT":
            benchmark_name = "NYTODQ"
        if benchmark_name not in benchmarks:
            benchmarks.append(benchmark_name)

    for benchmark_name in benchmarks:
        agg_metrics = {}
        x_var_keys = {}
        non_group_text = ""
        for thread_id in thread_ids:
            filtered_metric_df = metric_df[metric_df['thread_id'] == thread_id].copy()
            if filtered_metric_df.empty:
                continue

            assert (len(filtered_metric_df['sys_id'].unique()) == 1)
            sys_id_str = filtered_metric_df['sys_id'].unique()[0]
            sys_id_parts = sys_id_str.split("_")
            agg_metric_types = None
            if is_mixed_workload:
                if "Flink" in sys_id_str:
                    continue
                agg_metric_types = filtered_metric_df['agg_metric_type'].unique()

            scheduling_mode = sys_id_parts[config['part_indices']["scheduling_mode"]]
            if non_group_text == "" and scheduling_mode != "TB":
                non_group_text = get_non_group_text(config, labels, non_group_types, sys_id_parts)
            group_id = get_group_id(config, group_by_types, sys_id_parts)
            if independent_var == "scheduling_policy" or "," in independent_var:
                if "," in independent_var:
                    ind_var_components = independent_var.split(",")
                    x_var_key = ""
                    for ind_var in ind_var_components:
                        x_var_key += sys_id_parts[config['part_indices'][ind_var]] + " , "
                        x_var_key = x_var_key[:-3]
                else:
                    x_var_key = sys_id_parts[config['part_indices'][independent_var]]
            else:
                x_var_key = int(sys_id_parts[config['part_indices'][independent_var]])

            if is_mixed_workload:
                num_queries = int(sys_id_parts[config['part_indices']["num_queries"]])
                if num_queries != target_num_queries:
                    continue

            if is_mixed_workload and not generate_per_num_queries:
                for agg_metric_type in agg_metric_types:
                    agg_metric_name = agg_metric_type.split("S")[0].upper()
                    x_var_key = agg_metric_name
                    if x_var_key not in x_var_keys:
                        x_var_keys[x_var_key] = []
            else:
                if x_var_key not in x_var_keys:
                    x_var_keys[x_var_key] = []

            if is_mixed_workload:
                for agg_metric_type in agg_metric_types:
                    agg_metric_name = agg_metric_type.split("S")[0].upper()
                    derived_group_id = group_id + "_" + agg_metric_name
                    if derived_group_id not in agg_metrics:
                        agg_metrics[derived_group_id] = []
                        if generate_per_num_queries:
                            x_var_keys[x_var_key].append(derived_group_id)
                        else:
                            x_var_keys[agg_metric_name].append(derived_group_id)
            elif group_id not in agg_metrics:
                agg_metrics[group_id] = []
                x_var_keys[x_var_key].append(group_id)

            old_target_metric = target_metric
            old_unit_scale = unit_scale
            if target_metric.endswith('schedTime_counter') or target_metric.endswith('schedCount_counter'):
                target_metric = re.sub(r"(schedCount|schedTime)", r"\1Diff", target_metric)
                target_var = config['target_vars'][target_metric]
                filtered_metric_df[target_var] = (filtered_metric_df[target_var] -
                                                  filtered_metric_df[target_var].shift(1, fill_value=0))
            else:
                target_var = config['target_vars'][target_metric]

            # filtered_metric_df[target_var] = filtered_metric_df[target_var].astype("float64")
            group_by_columns = ["thread_id", "timestamp", "rel_time"]
            if is_mixed_workload:
                group_by_columns = ["thread_id", "agg_metric_type", "timestamp", "rel_time"]

            if agg_type == "sum":
                agg_metric_df = filtered_metric_df.groupby(group_by_columns).sum(numeric_only=True)
            else:
                agg_metric_df = filtered_metric_df.groupby(group_by_columns).mean(numeric_only=True)
            if scheduling_mode in ["Flink", "Saber"] and target_metric == "latency":
                unit_scale = 1.0
            elif scheduling_mode in ["StreamBox"] and target_metric == "latency":
                unit_scale = 1000.0

            if is_mixed_workload:
                indexed_agg_metric_df = agg_metric_df.reset_index()
                for agg_metric_type in agg_metric_types:
                    filtered_agg_metric_df = indexed_agg_metric_df[
                        indexed_agg_metric_df["agg_metric_type"] == agg_metric_type]
                    agg_metric = filtered_agg_metric_df.loc[:, target_var].mean() / unit_scale
                    agg_metric_name = agg_metric_type.split("S")[0].upper()
                    derived_group_id = group_id + "_" + agg_metric_name
                    agg_metrics[derived_group_id].append(agg_metric)
            else:
                timespan = filtered_metric_df['rel_time'].max() - filtered_metric_df['rel_time'].min()
                if config.get('reject_when_insufficient_data_points', True) and (timespan < 155):
                    print("Rejecting " + group_id + " due to not having enough data")
                    agg_metrics[group_id].append(0)
                else:
                    agg_metric = agg_metric_df.loc[:, target_var].mean() / unit_scale
                    agg_metrics[group_id].append(agg_metric)

            target_metric = old_target_metric
            unit_scale = old_unit_scale

        if target_metric.endswith('schedTime_counter') or target_metric.endswith('schedCount_counter'):
            target_metric = re.sub(r"(schedCount|schedTime)", r"\1Diff", target_metric)
        x_var_keys = dict(sorted(x_var_keys.items()))
        sorted_agg_metrics = {}
        sorted_unfiltered_agg_metrics = {}
        outlier_threshold = config.get('outlier_threshold', {target_metric: 200.0}).get(target_metric, 200.0)
        for x_var_key, group_ids in x_var_keys.items():
            for group_id in group_ids:
                if config.get('reject_outliers', True):
                    filtered_data = reject_outliers(agg_metrics[group_id], outlier_threshold)
                    reqd_data_points = config.get('reqd_data_points', 5)
                    if len(filtered_data) < reqd_data_points:
                        if config.get('raise_exception_on_too_many_outliers', True):
                            raise Exception("Cannot find " + str(reqd_data_points) + " data points within IQR")
                        else:
                            print("Too many outliers - " + group_id + ":" + str(len(filtered_data)))
                            print(filtered_data)
                            print(agg_metrics[group_id])
                            sorted_agg_metrics[group_id] = agg_metrics[group_id]
                    else:
                        sorted_agg_metrics[group_id] = filtered_data[:reqd_data_points]
                sorted_unfiltered_agg_metrics[group_id] = agg_metrics[group_id]
        if config.get('reject_outliers', True):
            agg_metrics = sorted_agg_metrics
        else:
            agg_metrics = sorted_unfiltered_agg_metrics

        x_var_idx = 1
        if is_mixed_workload and not generate_per_num_queries:
            x_var_idx = config['mixed_workload_params']['x_var_idx']
        # agg_metrics = dict(sorted(agg_metrics.items()))
        for graph_type in graph_type_list:
            if graph_type == "line":
                plot_line_graph_for_metric(config, labels, target_metric, results_dir, agg_metrics, independent_var,
                                           target_metric, non_group_text,
                                           derive_plot_title(config, labels, benchmark_name, target_metric),
                                           agg_type + "_" + target_metric + "_" + benchmark_name,
                                           intermediate_input_id, iter_num,
                                           sci_labels=sci_labels, show_plot=True, y_scale=y_scale)
            else:
                if config.get("use_special_fn", False):
                    plot_bar_graph_for_metric_special(config, labels, results_dir, agg_metrics, independent_var,
                                                      target_metric,
                                                      non_group_text,
                                                      derive_plot_title(config, labels, benchmark_name, target_metric),
                                                      agg_type + "_" + target_metric + "_" + benchmark_name,
                                                      intermediate_input_id, iter_num,
                                                      sci_labels=sci_labels, show_plot=True, y_scale=y_scale)
                else:
                    plot_bar_graph_for_metric(config, labels, results_dir, agg_metrics, independent_var, target_metric,
                                              non_group_text,
                                              derive_plot_title(config, labels, benchmark_name, target_metric),
                                              agg_type + "_" + target_metric + "_" + benchmark_name,
                                              intermediate_input_id, iter_num,
                                              sci_labels=sci_labels, show_plot=True, y_scale=y_scale,
                                              idx_for_xvar_in_sysid=x_var_idx)


def process_and_flush_target_flink_metrics(config, labels, data_dir, intermediate_data_dir, target_filename,
                                           independent_var, target_var_filter, experiment_ids, group_by_types,
                                           non_group_types, target_metric="", agg_type="avg",
                                           op_name_dict=None):
    if target_metric == "":
        target_metric = target_filename

    experiment_id_li = experiment_ids.split("|")
    pseudo_thread_id = 1
    for experiment_id in experiment_id_li:
        path = data_dir + "/" + experiment_id
        all_files = glob.glob(os.path.join(path, target_filename + "*.csv"))
        df_list = []
        for filename in all_files:
            if os.path.getsize(filename) == 0:
                print("Skipping empty file: " + filename)
                continue
            metric_df_per_run = pd.read_csv(filename)
            filename_parts = os.path.basename(filename).split("_")
            metric_df_per_run["thread_id"] = pseudo_thread_id
            pseudo_thread_id += 1
            # Enjima_YSB_30000000_BlockBasedBatch_SBPriority_5_NonPreemptive_LatencyOptimized_2_12288_50_Micros_1_1_PreAllocate_384_32
            derived_sys_id = derive_sys_id(config, filename_parts, target_metric)
            metric_df_per_run[
                "sys_id"] = derived_sys_id
            num_queries = derived_sys_id.split("_")[config["part_indices"]["num_queries"]]
            metric_df_per_run["num_queries"] = int(num_queries)
            df_list.append(metric_df_per_run)

        if not df_list:
            continue
        all_metric_df = pd.concat(df_list, ignore_index=True)
        all_metric_df = all_metric_df.sort_values(by=["num_queries", "time"])
        op_id_name_mapping_dict = None
        if target_metric == "latency":
            all_metric_df['operator_name'] = all_metric_df['operator_id'].map(op_name_dict)
            slice_columns = ['name', 'thread_id', 'sys_id', 'time', 'operator_name', 'mean']
            rename_columns = {'time': 'timestamp', 'operator_name': 'metric_type', 'mean': 'avg'}
        else:
            op_id_name_mapping_df = all_metric_df.loc[:, ['operator_name', 'operator_id']]
            op_id_name_mapping_dict = pd.Series(op_id_name_mapping_df.operator_name.values,
                                                index=op_id_name_mapping_df.operator_id).to_dict()
            slice_columns = ['name', 'thread_id', 'sys_id', 'time', 'count', 'operator_name', 'rate']
            rename_columns = {'time': 'timestamp', 'operator_name': 'metric_type', 'rate': 'throughput'}
        metric_df = all_metric_df.loc[:, slice_columns]
        metric_df.rename(columns=rename_columns, inplace=True)
        thread_ids = metric_df['thread_id'].unique()
        print(thread_ids)
        sys_ids = metric_df['sys_id'].unique()
        benchmarks = []
        for sys_id in sys_ids:
            benchmark_name = sys_id.split("_")[1]
            if benchmark_name not in benchmarks:
                benchmarks.append(benchmark_name)

        df_to_flush = None
        for _ in benchmarks:
            agg_metrics = {}
            non_group_text = ""
            for thread_id in thread_ids:
                filtered_metric_df = metric_df[metric_df['thread_id'] == thread_id].copy()
                filtered_metric_df = filtered_metric_df[
                    filtered_metric_df['metric_type'].str.contains(target_var_filter, regex=True)]
                if filtered_metric_df.empty:
                    continue

                assert (len(filtered_metric_df['sys_id'].unique()) == 1)
                sys_id_parts = filtered_metric_df['sys_id'].unique()[0].split("_")

                if non_group_text == "" and sys_id_parts[config['part_indices']["scheduling_mode"]] != "TB":
                    non_group_text = get_non_group_text(config, labels, non_group_types, sys_id_parts)
                group_id = get_group_id(config, group_by_types, sys_id_parts)

                if group_id not in agg_metrics:
                    agg_metrics[group_id] = []

                old_target_metric = target_metric
                if target_metric.endswith('schedTime_counter') or target_metric.endswith('schedCount_counter'):
                    target_metric = re.sub(r"(schedCount|schedTime)", r"\1Diff", target_metric)
                    target_var = config['target_vars'][target_metric]
                    filtered_metric_df[target_var] = (filtered_metric_df[target_var] -
                                                      filtered_metric_df[target_var].shift(1, fill_value=0))

                filtered_metric_df['rel_time'] = filtered_metric_df['timestamp'].subtract(
                    filtered_metric_df['timestamp'].min()).div(1_000_000_000)
                flink_time_lower = config['flink']['time_lower']
                flink_time_upper = config['flink']['time_upper']
                filtered_metric_df = filtered_metric_df[
                    (filtered_metric_df['rel_time'] > flink_time_lower) & (
                            filtered_metric_df['rel_time'] <= flink_time_upper)]

                if independent_var == "num_queries":
                    if agg_type == "sum":
                        agg_metric_df = filtered_metric_df.groupby(
                            ["thread_id", "sys_id", "timestamp", "rel_time"]).sum(numeric_only=True)
                    else:
                        agg_metric_df = filtered_metric_df.groupby(
                            ["thread_id", "sys_id", "timestamp", "rel_time"]).mean(numeric_only=True)
                    agg_metric_df = agg_metric_df.reset_index()
                    if df_to_flush is None:
                        df_to_flush = agg_metric_df
                    else:
                        df_to_flush = pd.concat([df_to_flush, agg_metric_df], ignore_index=True)
                else:
                    if df_to_flush is None:
                        df_to_flush = filtered_metric_df
                    else:
                        df_to_flush = pd.concat([df_to_flush, filtered_metric_df], ignore_index=True)
                target_metric = old_target_metric

            print(df_to_flush)
            df_to_flush.to_csv(
                intermediate_data_dir + "/" + target_metric + "_" + experiment_id.replace("/", "_") + ".csv",
                index=False)
        return op_id_name_mapping_dict


def derive_sys_id(config, filename_parts, target_metric):
    if target_metric == "latency":
        bench_name = filename_parts[12].upper()
        sched_period = filename_parts[14].rstrip("ms")
        iter_num = filename_parts[17].lstrip("iter")
        num_queries = filename_parts[19].lstrip("q")
        input_rate = config['flink']['input_rate']
        if not bench_name.startswith("MIXED") and filename_parts[20].startswith("i"):
            input_rate = str(int(filename_parts[20].lstrip("i")) // 10)
        num_workers = config['flink']['num_workers']
        max_memory = config['flink']['max_memory']
        bp_threshold = config['flink']['bp_threshold']
        derived_sys_id = "Flink_" + bench_name + "_" + input_rate + "_Default_Flink_" + iter_num + "_Default_Default_" + num_workers + "_" + max_memory + "_" + bp_threshold + "_Millis_" + sched_period + "_" + num_queries + "_OnDemand_0_0"
    else:
        bench_name = filename_parts[6].upper()
        sched_period = filename_parts[8].rstrip("ms")
        iter_num = filename_parts[11].lstrip("iter")
        num_queries = filename_parts[13].lstrip("q")
        input_rate = config['flink']['input_rate']
        if not bench_name.startswith("MIXED") and filename_parts[14].startswith("i"):
            input_rate = str(int(filename_parts[14].lstrip("i")) // 10)
        num_workers = config['flink']['num_workers']
        max_memory = config['flink']['max_memory']
        bp_threshold = config['flink']['bp_threshold']
        derived_sys_id = "Flink_" + bench_name + "_" + input_rate + "_Default_Flink_" + iter_num + "_Default_Default_" + num_workers + "_" + max_memory + "_" + bp_threshold + "_Millis_" + sched_period + "_" + num_queries + "_OnDemand_0_0"
    return derived_sys_id


def process_target_sum_metrics(config, labels, data_dir, results_dir, csv_columns, target_filename, independent_var,
                               target_var_filter, exp_id,
                               column_dtypes,
                               group_by_types, non_group_types, sci_labels=False, unit_scale=1.0, iter_num="1",
                               target_metric="", agg_type="avg"):
    time_lower = config['time_lower']
    time_upper = config['time_upper']

    if target_metric == "":
        target_metric = target_filename
    metric_df = pd.read_csv(data_dir + "/" + exp_id + "/" + target_filename + ".csv", names=csv_columns,
                            dtype=column_dtypes)
    metric_df = metric_df[metric_df['metric_type'].str.contains(target_var_filter, regex=True)]
    metric_df['raw_op_name'] = \
        metric_df['metric_type'].str.split("_" + target_metric, expand=True)[0].str.rsplit("_", n=1, expand=True)[0]
    metric_df['op_name'] = metric_df['raw_op_name'].apply(get_derived_op_name)

    thread_ids = metric_df['thread_id'].unique()
    print(thread_ids)
    op_names = metric_df['op_name'].unique()
    op_names.sort()
    print(op_names)

    sys_ids = metric_df['sys_id'].unique()
    benchmarks = []
    for sys_id in sys_ids:
        benchmark_name = sys_id.split("_")[1]
        if benchmark_name not in benchmarks:
            benchmarks.append(benchmark_name)

    for benchmark_name in benchmarks:
        agg_metrics = {}
        non_group_text = ""
        for thread_id in thread_ids:
            for op_name in op_names:
                filtered_metric_df = metric_df[
                    (metric_df['thread_id'] == thread_id) & (metric_df['op_name'] == op_name)].copy()
                if filtered_metric_df.empty:
                    continue

                assert (len(filtered_metric_df['sys_id'].unique()) == 1)
                sys_id_parts = filtered_metric_df['sys_id'].unique()[0].split("_")
                scheduling_mode = sys_id_parts[config['part_indices']["scheduling_mode"]]
                if scheduling_mode == "TB" and target_metric.endswith('scheduledCount_gauge'):
                    continue

                if non_group_text == "" and sys_id_parts[config['part_indices']["scheduling_mode"]] != "TB":
                    non_group_text = get_non_group_text(config, labels, non_group_types, sys_id_parts)
                group_id = get_group_id(config, group_by_types, sys_id_parts) + "_" + op_name

                if group_id not in agg_metrics:
                    agg_metrics[group_id] = []

                old_target_metric = target_metric
                target_var = config['target_vars'][target_metric]

                filtered_metric_df['rel_time'] = filtered_metric_df['timestamp'].subtract(
                    filtered_metric_df['timestamp'].min()).div(1_000)
                filtered_metric_df = filtered_metric_df[
                    (filtered_metric_df['rel_time'] > time_lower) & (filtered_metric_df['rel_time'] <= time_upper)]

                if re.search("num_queries", independent_var):
                    if agg_type == "sum_diff":
                        agg_metric_df = filtered_metric_df.groupby(["thread_id", "timestamp", "rel_time"]).sum()
                        agg_metric = (agg_metric_df.tail(1).loc[:, target_var].values[0] -
                                      agg_metric_df.head(1).loc[:, target_var].values[0]) / unit_scale
                    elif agg_type == "avg_diff":
                        agg_metric_df = filtered_metric_df.groupby(["thread_id", "timestamp", "rel_time"]).mean()
                        agg_metric = (agg_metric_df.tail(1).loc[:, target_var].values[0] -
                                      agg_metric_df.head(1).loc[:, target_var].values[0]) / unit_scale
                    elif agg_type == "sum_mean":
                        agg_metric_df = filtered_metric_df.groupby(["thread_id", "timestamp", "rel_time"]).sum()
                        agg_metric = agg_metric_df.loc[:, target_var].mean() / unit_scale
                    else:
                        agg_metric_df = filtered_metric_df.groupby(["thread_id", "timestamp", "rel_time"]).mean()
                        agg_metric = agg_metric_df.loc[:, target_var].mean() / unit_scale
                else:
                    agg_metric = filtered_metric_df.loc[:, target_var].mean() / unit_scale
                agg_metrics[group_id].append(agg_metric)
                target_metric = old_target_metric

        # agg_metrics = dict(sorted(agg_metrics.items()))
        if agg_type == "sum_diff":
            target_metric = target_metric + "_sum"
        plot_bar_graph_for_metric(config, labels, results_dir, agg_metrics, independent_var, target_metric,
                                  non_group_text,
                                  derive_plot_title(config, labels, benchmark_name, target_metric),
                                  agg_type + "_" + target_metric + "_" + benchmark_name,
                                  exp_id, iter_num,
                                  sci_labels=sci_labels, show_plot=True)


def targeted_timeseries_metrics(config, labels, data_dir, results_dir, metrics_group, csv_columns, target_var, exp_id,
                                column_dtypes, target_metric,
                                target_iter_num='1'):
    time_lower = config['time_lower']
    time_upper = config['time_upper']

    metric_df = pd.read_csv(data_dir + "/" + exp_id + "/" + metrics_group + ".csv", names=csv_columns,
                            dtype=column_dtypes)
    metric_df = metric_df[metric_df['metric_type'].str.contains(target_metric, regex=True)]
    thread_ids = metric_df['thread_id'].unique()
    metric_types = metric_df['metric_type'].unique()
    print(thread_ids)
    print(metric_types)
    sys_ids = metric_df['sys_id'].unique()
    benchmarks = []
    for sys_id in sys_ids:
        benchmark_name = sys_id.split("_")[1]
        if benchmark_name not in benchmarks:
            benchmarks.append(benchmark_name)

    for benchmark_name in benchmarks:
        combined_df = pd.DataFrame()
        for thread_id in thread_ids:
            filtered_metric_df = metric_df[
                (metric_df['thread_id'] == thread_id) & (
                    metric_df['metric_type'].str.contains(target_metric, regex=True))].copy()
            if target_metric.endswith('_time_ms') or target_metric.endswith('_flt'):
                target_metric = target_metric + "_diff"
                filtered_metric_df['value'] = (filtered_metric_df['value'] -
                                               filtered_metric_df['value'].shift(1, fill_value=0))
            # assert (len(filtered_metric_df['sys_id'].unique()) == 1)
            sys_id_parts = filtered_metric_df['sys_id'].unique()[0].split("_")
            exp_iter = sys_id_parts[config['part_indices']['iter']]
            if exp_iter != target_iter_num:
                continue

            filtered_metric_df['rel_time'] = filtered_metric_df['timestamp'].subtract(
                filtered_metric_df['timestamp'].min()).div(1_000)
            filtered_metric_df = filtered_metric_df[
                (filtered_metric_df['rel_time'] > time_lower) & (filtered_metric_df['rel_time'] <= time_upper)]
            if filtered_metric_df.empty:
                continue
            group_by_types = config['group_by_types'].split(",")
            for group_by_type in group_by_types:
                filtered_metric_df[group_by_type] = sys_id_parts[config['part_indices'][group_by_type]]
            filter_types = config['targeted_metrics']['filter_types'].split(",")
            if filter_types:
                for filter_type in filter_types:
                    filtered_vals = config['targeted_metrics'][filter_type].split(",")
                    filtered_metric_df = filtered_metric_df[filtered_metric_df[filter_type].isin(filtered_vals)]
            combined_df = pd.concat([combined_df, filtered_metric_df], ignore_index=True)

        if not combined_df.empty:
            print(benchmark_name)
            target_metric_type = metrics_group
            unit_scale = config['unit_scale'].get(target_metric_type, 1.0)
            combined_df[target_var] = combined_df[target_var] / unit_scale
            plot_combined_metric(config, labels, target_metric_type, results_dir, combined_df, "rel_time", target_var,
                                 target_metric,
                                 get_title(labels, target_metric_type + '_timeseries') + " (" + get_benchmark_label(
                                     labels,
                                     benchmark_name) + ")",
                                 "combined_" + target_metric_type,
                                 exp_id, target_iter_num)


def process_experiment_results(exp_info, exp_to_run, grouped_experiment=False, group_id=None):
    exp_config_file = exp_info['exp_config_file'][exp_to_run]
    config = load_config(exp_config_file)
    labels = load_labels()
    latency_columns, latency_column_dtypes = get_columns_with_dtypes(config, 'latency')
    target_columns, target_column_dtypes = get_columns_with_dtypes(config, 'tp')
    count_columns, count_column_dtypes = get_columns_with_dtypes(config, 'count')
    sys_metrics_columns, sys_metrics_column_dtypes = get_columns_with_dtypes(config, 'sys_metrics')
    sys_metrics_to_skip = config['sys_metrics_to_skip'].split(",")
    op_metrics_columns, op_metrics_column_dtypes = get_columns_with_dtypes(config, 'op_metrics')
    op_metrics_to_skip = config['op_metrics_to_skip'].split(",")
    perf_stat_columns, perf_stat_column_dtypes = get_columns_with_dtypes(config, 'perf_stat')
    data_dir = config['data_dir']
    exp_date_id = config['exp_date_id']
    intermediate_id = config['intermediate_id']
    processing_stage = config['processing_stage']
    multistage_processing = config['multistage_processing']
    data_wrangling_only = config.get('data_wrangling_only', False)
    if processing_stage not in [0, 3] and (not multistage_processing or exp_date_id != "0"):
        print(
            "processing_stage > 0, but multistage_processing or exp_date_id not set correctly. Script is not configured correctly!. Exiting.")
        exit(1)
    if exp_date_id != "0" and intermediate_id != "0":
        print("Both exp_dat_id and intermediate_id are non-zero. Script is not configured correctly!. Exiting.")
        exit(1)
    if exp_date_id == "0" and intermediate_id == "0":
        print("Both exp_dat_id and intermediate_id are zero. Script is not configured correctly!. Exiting.")
        exit(1)
    if multistage_processing:
        exp_date_id = config['combined_data_ids']

    intermediate_data_dir = data_dir + "/intermediate_data_" + intermediate_id
    results_base_dir = config['results_dir']
    results_dir = None
    if grouped_experiment:
        results_dir = results_base_dir + "/" + exp_info["grouped_results_dir"][group_id]
    else:
        results_dir = results_base_dir + "/" + exp_date_id
        if processing_stage == 2 or processing_stage == 4:
            results_dir = results_base_dir + "/from_intermediate_" + intermediate_id

    skip_event_count_graphs = exp_info["skip_event_count_graphs"]

    op_metrics_subdir = "operator_metrics"
    sys_metrics_subdir = "system_metrics"
    targeted_metrics_subdir = "targeted_metrics"
    if processing_stage != 1:
        os.makedirs(results_dir, exist_ok=True)
        os.makedirs(results_dir + "/" + op_metrics_subdir, exist_ok=True)
        os.makedirs(results_dir + "/" + sys_metrics_subdir, exist_ok=True)
        os.makedirs(results_dir + "/" + targeted_metrics_subdir, exist_ok=True)
    time_lower = config['time_lower']
    time_upper = config['time_upper']
    analyze_batch_stats = config['analyze_batch_stats']
    analyze_sched_stats = config['analyze_sched_stats']
    tp_latency_only = config['tp_latency_only']
    do_normal_run = config['do_normal_run']
    analyze_individual_op_metrics = config['analyze_individual_op_metrics']
    analyze_agg_op_metrics = config['analyze_agg_op_metrics']
    analyze_grouped_op_metrics = config['analyze_grouped_op_metrics']
    analyze_sys_stats = config['analyze_sys_stats']
    run_targeted_metrics = config['run_targeted_metrics']
    bench_name = config['current_bench_name']
    independent_var = config['independent_var']
    preferred_graph_type = config['preferred_graph_type'].split(",")
    latency_unit_scale = 1.0 if (
            bench_name == "lrb" or bench_name == "mixed_lrb_ysb") else config['unit_scale'].get('latency', 1.0)
    exp_id_to_log = exp_date_id
    if processing_stage == 2:
        exp_id_to_log = intermediate_id
    print("Reading from {} with experiment ID(s) {} - Time bounds=[{},{}]".format(config['data_dir'], exp_id_to_log,
                                                                                  time_lower, time_upper))
    all_possible_groups = config['all_possible_groups'].split(",")
    configured_group_by_types = config['group_by_types'].split(",")
    configured_non_group_types = [param_type for param_type in all_possible_groups if
                                  param_type not in configured_group_by_types]
    if data_wrangling_only:
        if config.get('data_wrangling_params', {"gen_overhead": True}).get("gen_overhead", True):
            process_sched_overhead_no_graphs(config, data_dir, results_dir, exp_date_id)
            process_prior_overhead_no_graphs(config, data_dir, results_dir, exp_date_id)
            exit(0)
        target_filter_str = ".*_\\d+_scheduledTime_gauge"
        target_filter_str2 = ".*_\\d+_scheduledCount_gauge"
        process_agg_metrics_no_graphs_specialized(config, data_dir, results_dir, op_metrics_columns,
                                                  "operator_metrics",
                                                  target_filter_str, target_filter_str2, exp_date_id,
                                                  op_metrics_column_dtypes)
    if tp_latency_only:
        if processing_stage == 1:
            os.makedirs(intermediate_data_dir, exist_ok=True)
            if exp_date_id.startswith("flink"):
                flink_host = config['flink_host']
                target_filename = "taskmanager_job_task_operator_numRecordsOutPerSecond_" + flink_host + "_" + bench_name[
                                                                                                               :3] + "_default_50ms_1_1parts_"
                if exp_date_id.endswith("mixed"):
                    target_filename = "taskmanager_job_task_operator_numRecordsOutPerSecond_*_default_50ms_1_1parts_"

                derived_op_name_dict = process_and_flush_target_flink_metrics(config, labels, data_dir,
                                                                              intermediate_data_dir, target_filename,
                                                                              independent_var,
                                                                              "Source:",
                                                                              exp_date_id,
                                                                              configured_group_by_types,
                                                                              configured_non_group_types,
                                                                              target_metric="event_count",
                                                                              agg_type="sum")
                process_and_flush_target_flink_metrics(config, labels, data_dir, intermediate_data_dir, target_filename,
                                                       independent_var,
                                                       "Source:",
                                                       exp_date_id,
                                                       configured_group_by_types,
                                                       configured_non_group_types, target_metric="throughput",
                                                       agg_type="sum")
                target_filename = "taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency_" + flink_host + "_" + bench_name[
                                                                                                                                       :3] + "_default_50ms_1_1parts_"
                if exp_date_id.endswith("mixed"):
                    target_filename = "taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency_*_default_50ms_1_1parts_"

                process_and_flush_target_flink_metrics(config, labels, data_dir, intermediate_data_dir, target_filename,
                                                       independent_var,
                                                       "Sink:",
                                                       exp_date_id,
                                                       configured_group_by_types, configured_non_group_types,
                                                       target_metric="latency", op_name_dict=derived_op_name_dict)
            else:
                target_filter_event_count = bench_name + "_src_\\d+_out_counter"
                target_filter_throughput = bench_name + "_src_\\d+_outThroughput_gauge"
                target_filter_latency = "latency_histogram"
                if config.get("use_custom_target_filter", False):
                    target_filter_event_count = config["target_filter"]["event_count"]
                    target_filter_throughput = config["target_filter"]["throughput"]
                    target_filter_latency = config["target_filter"]["latency"]
                process_and_flush_target_metrics(config, labels, data_dir, intermediate_data_dir, count_columns,
                                                 "event_count", independent_var,
                                                 target_filter_event_count,
                                                 exp_date_id,
                                                 count_column_dtypes, configured_group_by_types,
                                                 configured_non_group_types,
                                                 agg_type="sum")
                process_and_flush_target_metrics(config, labels, data_dir, intermediate_data_dir, target_columns,
                                                 "throughput", independent_var,
                                                 target_filter_throughput,
                                                 exp_date_id, target_column_dtypes, configured_group_by_types,
                                                 configured_non_group_types,
                                                 agg_type="sum")
                process_and_flush_target_metrics(config, labels, data_dir, intermediate_data_dir, latency_columns,
                                                 "latency", independent_var, target_filter_latency,
                                                 exp_date_id, latency_column_dtypes,
                                                 configured_group_by_types, configured_non_group_types)
        elif processing_stage == 2:
            if not skip_event_count_graphs:
                process_target_metrics_from_intermediate(config, labels, data_dir, results_dir, "event_count",
                                                         independent_var, intermediate_id,
                                                         configured_group_by_types, configured_non_group_types,
                                                         graph_type_list=preferred_graph_type,
                                                         agg_type="sum", unit_scale=1000000000.0)
            process_target_metrics_from_intermediate(config, labels, data_dir, results_dir, "throughput",
                                                     independent_var,
                                                     intermediate_id, configured_group_by_types,
                                                     configured_non_group_types, graph_type_list=preferred_graph_type,
                                                     agg_type="sum", unit_scale=1000000.0)
            plot_y_scale_latency = config.get("y_scale", {"latency": "log"})["latency"]
            process_target_metrics_from_intermediate(config, labels, data_dir, results_dir, "latency", independent_var,
                                                     intermediate_id,
                                                     configured_group_by_types, configured_non_group_types,
                                                     graph_type_list=preferred_graph_type,
                                                     unit_scale=latency_unit_scale, y_scale=plot_y_scale_latency)
        elif processing_stage == 3:
            process_latency_vs_tput_graph(config, labels, results_dir, target_columns, latency_columns, "throughput",
                                          "latency", "throughput",
                                          bench_name + "_src_\\d+_outThroughput_gauge", "latency_histogram",
                                          exp_date_id, target_column_dtypes, latency_column_dtypes,
                                          configured_group_by_types,
                                          configured_non_group_types, graph_type=preferred_graph_type, y_scale="linear")
        else:
            if not skip_event_count_graphs:
                process_target_metrics(config, labels, data_dir, results_dir, count_columns, "event_count",
                                       independent_var,
                                       bench_name + "_src_\\d+_out_counter",
                                       exp_date_id, count_column_dtypes, configured_group_by_types,
                                       configured_non_group_types, sci_labels=True, agg_type="sum",
                                       graph_type=preferred_graph_type, unit_scale=1000000.0)
            process_target_metrics(config, labels, data_dir, results_dir, target_columns, "throughput", independent_var,
                                   bench_name + "_src_\\d+_outThroughput_gauge",
                                   exp_date_id, target_column_dtypes, configured_group_by_types,
                                   configured_non_group_types,
                                   agg_type="sum", graph_type=preferred_graph_type, unit_scale=1000000.0)
            latency_y_scale = config.get("y_scale", {"latency": "log"})["latency"]
            process_target_metrics(config, labels, data_dir, results_dir, latency_columns, "latency", independent_var,
                                   "latency_histogram", exp_date_id,
                                   latency_column_dtypes, configured_group_by_types, configured_non_group_types,
                                   unit_scale=latency_unit_scale, graph_type=preferred_graph_type,
                                   y_scale=latency_y_scale)
        return
    if exp_date_id.startswith("flink"):
        print("Cannot do experiments other than tp_latency_only with Flink data! Exiting...")
        exit(1)
    if do_normal_run:
        if analyze_individual_op_metrics:
            process_target_sum_metrics(config, labels, data_dir, results_dir, op_metrics_columns, "operator_metrics",
                                       independent_var,
                                       ".*_\\d+_scheduledTime_gauge", exp_date_id,
                                       op_metrics_column_dtypes, configured_group_by_types, configured_non_group_types,
                                       target_metric="scheduledTime_gauge", agg_type="sum_diff", unit_scale=1000.0)
            process_target_sum_metrics(config, labels, data_dir, results_dir, op_metrics_columns, "operator_metrics",
                                       independent_var,
                                       ".*_\\d+_scheduledTime_gauge", exp_date_id,
                                       op_metrics_column_dtypes, configured_group_by_types, configured_non_group_types,
                                       sci_labels=True,
                                       target_metric="scheduledTime_gauge", agg_type="avg_diff", unit_scale=1000.0)

            process_target_sum_metrics(config, labels, data_dir, results_dir, op_metrics_columns, "operator_metrics",
                                       independent_var,
                                       ".*_\\d+_scheduledCount_gauge", exp_date_id,
                                       op_metrics_column_dtypes, configured_group_by_types, configured_non_group_types,
                                       sci_labels=True,
                                       target_metric="scheduledCount_gauge", agg_type="sum_diff")
        if analyze_batch_stats:
            process_target_sum_metrics(config, labels, data_dir, results_dir, op_metrics_columns, "operator_metrics",
                                       independent_var,
                                       ".*_\\d+_batchSizeAvg_gauge", exp_date_id,
                                       op_metrics_column_dtypes, configured_group_by_types, configured_non_group_types,
                                       target_metric="batchSizeAvg_gauge")

        if analyze_agg_op_metrics:
            process_target_sum_metrics(config, labels, data_dir, results_dir, op_metrics_columns, "operator_metrics",
                                       independent_var,
                                       ".*_\\d+_pendingEvents_gauge", exp_date_id,
                                       op_metrics_column_dtypes, configured_group_by_types, configured_non_group_types,
                                       sci_labels=False,
                                       target_metric="pendingEvents_gauge")
            process_target_sum_metrics(config, labels, data_dir, results_dir, op_metrics_columns, "operator_metrics",
                                       independent_var,
                                       ".*_\\d+_cost_gauge", exp_date_id,
                                       op_metrics_column_dtypes, configured_group_by_types, configured_non_group_types,
                                       target_metric="cost_gauge")

            process_target_metrics(config, labels, data_dir, results_dir, op_metrics_columns, "operator_metrics",
                                   independent_var,
                                   ".*_\\d+_scheduledTime_gauge", exp_date_id,
                                   op_metrics_column_dtypes, configured_group_by_types, configured_non_group_types,
                                   sci_labels=True,
                                   target_metric="scheduledTime_gauge", agg_type="sum", unit_scale=1000.0)

        if analyze_sched_stats:
            process_target_metrics(config, labels, data_dir, results_dir, count_columns, "event_count", independent_var,
                                   "schedTime_counter", exp_date_id,
                                   target_column_dtypes, configured_group_by_types, configured_non_group_types,
                                   sci_labels=True,
                                   target_metric="schedTime_counter", agg_type="sum")
            process_target_metrics(config, labels, data_dir, results_dir, count_columns, "event_count", independent_var,
                                   "schedCount_counter", exp_date_id,
                                   target_column_dtypes, configured_group_by_types, configured_non_group_types,
                                   sci_labels=True,
                                   target_metric="schedCount_counter")

        tp_agg_metrics_dict = process_target_metrics(config, labels, data_dir, results_dir, target_columns,
                                                     "throughput",
                                                     independent_var,
                                                     bench_name + "_src_\\d+_outThroughput_gauge",
                                                     exp_date_id, target_column_dtypes, configured_group_by_types,
                                                     configured_non_group_types,
                                                     sci_labels=True, agg_type="sum")
        process_target_metrics(config, labels, data_dir, results_dir, latency_columns, "latency", independent_var,
                               "latency_histogram", exp_date_id,
                               latency_column_dtypes, configured_group_by_types, configured_non_group_types,
                               unit_scale=1000.0)
        if analyze_grouped_op_metrics:
            process_grouped_operator_metrics(config, labels, results_dir, op_metrics_columns, "value", independent_var,
                                             exp_date_id, op_metrics_column_dtypes, op_metrics_to_skip,
                                             configured_group_by_types, configured_non_group_types,
                                             sci_labels=False)
        if analyze_sys_stats:
            if config['sys_stats_to_analyze']['process_grouped_sys_metrics']:
                process_grouped_system_metrics(config, labels, results_dir, sys_metrics_subdir, sys_metrics_columns,
                                               "value",
                                               independent_var, exp_date_id, sys_metrics_column_dtypes,
                                               sys_metrics_to_skip,
                                               configured_group_by_types, configured_non_group_types,
                                               sci_labels=False)
            if config['sys_stats_to_analyze']['cpu']:
                process_agg_grouped_system_metrics(config, labels, results_dir, sys_metrics_subdir, sys_metrics_columns,
                                                   tp_agg_metrics_dict,
                                                   "value",
                                                   independent_var, exp_date_id, sys_metrics_column_dtypes,
                                                   sys_metrics_to_skip,
                                                   configured_group_by_types, configured_non_group_types,
                                                   "u_time_ms,s_time_ms", "cpu",
                                                   sci_labels=False)
            if config['sys_stats_to_analyze']['mem']:
                process_agg_grouped_system_metrics(config, labels, results_dir, sys_metrics_subdir, sys_metrics_columns,
                                                   tp_agg_metrics_dict,
                                                   "value",
                                                   independent_var, exp_date_id, sys_metrics_column_dtypes,
                                                   sys_metrics_to_skip,
                                                   configured_group_by_types, configured_non_group_types,
                                                   "rss_kb", "mem",
                                                   sci_labels=False)
    if run_targeted_metrics:
        run_aggregate_metrics = config['targeted_metrics']['run_aggregate_metrics']
        run_timeseries_metrics = config['targeted_metrics']['run_timeseries_metrics']
        target_iter = config['targeted_metrics']['target_iter']
        if run_aggregate_metrics:
            target_metrics_columns = op_metrics_columns
            target_metrics_column_dtypes = op_metrics_column_dtypes
            targeted_aggregate_metrics(config, labels, results_dir, targeted_metrics_subdir, "operator_metrics",
                                       target_metrics_columns, "value", "num_queries", exp_date_id,
                                       target_metrics_column_dtypes, bench_name + "_src_\\d+_scheduledTime_gauge",
                                       configured_group_by_types, configured_non_group_types, use_regex_match=True,
                                       target_iter=target_iter)

            target_metrics_columns = target_columns
            target_metrics_column_dtypes = target_column_dtypes
            targeted_aggregate_metrics(config, labels, results_dir, targeted_metrics_subdir, "throughput",
                                       target_metrics_columns, "throughput", "num_queries", exp_date_id,
                                       target_metrics_column_dtypes, bench_name + "_src_\\d+_outThroughput_gauge",
                                       configured_group_by_types, configured_non_group_types, use_regex_match=True,
                                       target_iter=target_iter)

            target_metrics_columns = latency_columns
            target_metrics_column_dtypes = latency_column_dtypes
            targeted_aggregate_metrics(config, labels, results_dir, targeted_metrics_subdir, "latency",
                                       target_metrics_columns,
                                       "avg", "num_queries", exp_date_id,
                                       target_metrics_column_dtypes, bench_name + "_sink_\\d+_latency_histogram",
                                       configured_group_by_types, configured_non_group_types, use_regex_match=True,
                                       target_iter=target_iter)
        if run_timeseries_metrics:
            # targeted_timeseries_metrics("system_metrics", sys_metrics_columns, "value", exp_date_id,
            #                             sys_metrics_column_dtypes,
            #                             "cpu_percent", str(target_iter))
            # targeted_timeseries_metrics("system_metrics", sys_metrics_columns, "value", exp_date_id,
            #                             sys_metrics_column_dtypes,
            #                             "vm_usage_kb", str(target_iter))
            # targeted_timeseries_metrics("system_metrics", sys_metrics_columns, "value", exp_date_id,
            #                             sys_metrics_column_dtypes,
            #                             "rss_kb", str(target_iter))
            # latency_grad_columns, latency_grad_column_dtypes = get_columns_with_dtypes(config, 'latency_gradient')
            # target_metric = config['timeseries_metrics']['target_metric']
            # targeted_timeseries_metrics("latency_gradient", latency_grad_columns, "avg_grad", exp_date_id,
            #                             latency_grad_column_dtypes,
            #                             target_metric, str(target_iter))
            if processing_stage == 0:
                target_metric_types = config['timeseries_metrics']['target_metric_types'].split(',')
                for target_metric_type in target_metric_types:
                    target_metric_name = config['timeseries_metrics'][target_metric_type]['metric_name']
                    target_columns, target_column_dtypes = get_columns_with_dtypes(config, target_metric_name)
                    target_metric = config['timeseries_metrics'][target_metric_type]['target_metric']
                    target_var = config['timeseries_metrics'][target_metric_type]['target_var']
                    targeted_timeseries_metrics(config, labels, data_dir, results_dir, target_metric_type,
                                                target_columns, target_var, exp_date_id,
                                                target_column_dtypes,
                                                target_metric, str(target_iter))
            elif processing_stage == 1:
                target_metric_types = config['timeseries_metrics']['target_metric_types'].split(',')
                for target_metric_type in target_metric_types:
                    target_metric_name = config['timeseries_metrics'][target_metric_type]['metric_name']
                    target_columns, target_column_dtypes = get_columns_with_dtypes(config, target_metric_name)
                    target_metric = config['timeseries_metrics'][target_metric_type]['target_metric']
                    target_var = config['timeseries_metrics'][target_metric_type]['target_var']
                    flush_timeseries_metrics(config, target_metric_type, target_columns, target_var, exp_date_id,
                                             target_column_dtypes,
                                             target_metric, str(target_iter))
            elif processing_stage == 2:
                target_metric_types = config['timeseries_metrics']['target_metric_types'].split(',')
                for target_metric_type in target_metric_types:
                    target_metric_name = config['timeseries_metrics'][target_metric_type]['metric_name']
                    target_metric = config['timeseries_metrics'][target_metric_type]['target_metric']
                    target_var = config['timeseries_metrics'][target_metric_type]['target_var']
                    plot_timeseries_metrics_from_intermediate(config, labels, results_dir, target_metric_type,
                                                              target_var, exp_date_id, target_metric,
                                                              str(target_iter))


if __name__ == '__main__':
    known_types = {
        'int': int,
        'float': float,
        'str': str
    }

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--parallelism", default="1")
    parser.add_argument("-sp", "--src_parallelism", default="1")
    parser.add_argument("-i", "--numiters", default=5, type=int)
    parser.add_argument("--host", default="tem120")
    args = parser.parse_args()

    exp_info = load_exp_sel()
    print_colors = exp_info['print_colors']
    if print_colors:
        tab20b_colors = plt.colormaps['tab20b'].colors
        i = 0
        for color in tab20b_colors:
            i = i + 1
            print(str(i) + ":" + mcolors.to_hex(color))
        exit(0)
    experiments_to_run = exp_info['exp_to_run'].split(",")
    grouped_experiment = False
    group_id = None
    if len(experiments_to_run) == 1 and experiments_to_run[0].startswith("all"):
        group_id = experiments_to_run[0]
        if group_id == "all_experiments":
            experiments_to_run = []
            for group in exp_info["grouped_experiments"]:
                experiments_to_run.extend(exp_info["grouped_experiments"][group].split(","))
        else:
            experiments_to_run = exp_info['grouped_experiments'][group_id].split(",")
        grouped_experiment = True

    for exp_to_run in experiments_to_run:
        process_experiment_results(exp_info, exp_to_run, grouped_experiment, group_id)
