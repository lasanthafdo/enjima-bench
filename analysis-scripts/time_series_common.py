import glob
import os

from analytics_common import *


def flush_timeseries_metrics(config, target_filename, csv_columns, target_var, exp_id, column_dtypes, target_metric,
                             target_iter_num='1'):
    data_dir = config['data_dir']
    intermediate_id = config['intermediate_id']
    intermediate_data_dir = data_dir + "/intermediate_data_" + intermediate_id
    os.makedirs(intermediate_data_dir, exist_ok=True)
    time_lower = config['time_lower']
    time_upper = config['time_upper']

    experiment_ids = config['combined_data_ids']
    experiment_id_li = experiment_ids.split("|")
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

        sys_ids_to_skip = []
        if experiment_id in experiment_ids_with_sid_skips:
            sys_ids_to_skip = experiment_ids_with_sid_skips[experiment_id].split(",")

        if sys_ids_to_skip:
            pattern_str = ""
            for sys_id in sys_ids_to_skip:
                pattern_str += sys_id + "|"
            pattern_str = pattern_str[:-1]
            pattern = re.compile(pattern_str)
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

                assert (len(filtered_metric_df['sys_id'].unique()) == 1)
                curr_sys_id = filtered_metric_df['sys_id'].unique()[0]
                sys_id_parts = curr_sys_id.split("_")
                if sys_ids_to_skip and pattern.match(curr_sys_id):
                    print("Skipping sys ID: " + curr_sys_id)
                    continue

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
                print(combined_df)
                combined_df.to_csv(intermediate_data_dir + "/" + target_filename + "_" + experiment_id + ".csv",
                                   index=False)

            # target_metric_type = metrics_group
            # unit_scale = config['unit_scale'].get(target_metric_type, 1.0)
            # combined_df[target_var] = combined_df[target_var] / unit_scale
            # plot_combined_metric(config, labels, target_metric_type, results_dir, combined_df, "rel_time", target_var,
            #                      target_metric,
            #                      get_title(labels, target_metric_type + '_timeseries') + " (" + get_benchmark_label(
            #                          labels,
            #                          benchmark_name) + ")",
            #                      "combined_" + target_metric_type,
            #                      exp_id, target_iter_num)


def plot_timeseries_metrics_from_intermediate(config, labels, results_dir, metrics_group, target_var,
                                              exp_id, target_metric, target_iter_num='1'):
    data_dir = config['data_dir']
    intermediate_id = config['intermediate_id']
    intermediate_data_dir = data_dir + "/intermediate_data_" + intermediate_id

    if target_metric == "":
        target_metric = metrics_group
    all_files = glob.glob(os.path.join(intermediate_data_dir, metrics_group + "*.csv"))
    df_list = []
    for filename in all_files:
        metric_df_per_run = pd.read_csv(filename)
        df_list.append(metric_df_per_run)
    combined_df = pd.concat(df_list, ignore_index=True)

    thread_ids = combined_df['thread_id'].unique()
    print(thread_ids)
    sys_ids = combined_df['sys_id'].unique()
    benchmarks = []
    for sys_id in sys_ids:
        benchmark_name = sys_id.split("_")[1]
        if benchmark_name == "NYT":
            benchmark_name = "NYTODQ"
        if benchmark_name not in benchmarks:
            benchmarks.append(benchmark_name)

    if not combined_df.empty:
        for benchmark_name in benchmarks:
            target_metric_type = metrics_group
            unit_scale = config['unit_scale'].get(target_metric_type, 1.0)
            combined_df[target_var] = combined_df[target_var] / unit_scale
            basic_plot_title = derive_plot_title(config, labels, target_metric_type + '_timeseries',
                                                 target_var)
            plot_title = basic_plot_title if not basic_plot_title else basic_plot_title + " (" + get_benchmark_label(
                labels, benchmark_name) + ")"
            plot_combined_metric(config, labels, target_metric_type, results_dir, combined_df, "rel_time",
                                 target_var, target_metric, plot_title, "combined_" + target_metric_type, exp_id,
                                 target_iter_num)
