import argparse
import glob
import json
import os

import pandas as pd

from analytics_common import *

pd.set_option('display.max_columns', 10)


def analyze_perf_stats(csv_columns, column_dtypes):
    path = data_dir  # use your path
    all_files = glob.glob(os.path.join(path, "perf_stat_*.txt"))

    li = []

    for filename in all_files:
        file_parts = os.path.basename(filename).split("_")
        sched_mode = file_parts[2]
        process_mode = file_parts[3]
        sched_policy = file_parts[4]
        num_queries = file_parts[5]
        df = pd.read_csv(filename, header=None, skiprows=2, skipfooter=3, sep=';', names=csv_columns)
        df = df.loc[(df['counter_value'] != "<not supported>") & (df['unit_label'] != "msec")]
        for column in column_dtypes:
            df[column].fillna(0.0, inplace=True)
            df[column] = df[column].astype(column_dtypes[column])
        df['sched_mode'] = sched_mode
        df['process_mode'] = process_mode
        df['sched_policy'] = sched_policy
        df['num_queries'] = int(num_queries)
        li.append(df)

    perf_stat_df = pd.concat(li, axis=0, ignore_index=True)
    perf_stat_df = perf_stat_df[
        (perf_stat_df['timestamp'] > config['time_lower']) & (perf_stat_df['timestamp'] < config['time_upper'])]
    agg_perf_df = perf_stat_df.groupby(
        ['process_mode', 'sched_mode', 'sched_policy', 'num_queries', 'event_name',
         'optional_metric_unit_label']).mean().reset_index()
    pd.options.display.float_format = '{:.2f}'.format
    print(agg_perf_df)
    filtered_ipc_df = perf_stat_df[perf_stat_df['optional_metric_unit_label'] == 'insn per cycle']
    agg_ipc_df = filtered_ipc_df.groupby(
        ['process_mode', 'sched_mode', 'sched_policy', 'num_queries', 'event_name'])[
        'optional_metric_val'].mean().reset_index()
    print(agg_ipc_df)


def analyze_toplev_stats(exp_date_id):
    ifixed_tplat_df = pd.read_csv(data_dir + "/" + exp_date_id + "/ifixed_out.csv")
    ifixed_tplat_df['exp_mode'] = 'ifixed'
    qfixed_tplat_df = pd.read_csv(data_dir + "/" + exp_date_id + "/qfixed_out.csv")
    qfixed_tplat_df['exp_mode'] = 'qfixed'
    tplat_df = pd.concat([ifixed_tplat_df, qfixed_tplat_df], axis=0, ignore_index=True)
    sys_id_series = tplat_df['system_id'].str.split('_', expand=True)
    tplat_df['num_queries'] = sys_id_series[13].astype(int)
    tplat_df['input_rate'] = sys_id_series[2].astype(int)

    path = data_dir + "/" + exp_date_id  # use your path
    all_files = glob.glob(os.path.join(path, "toplev_*.csv"))

    li = []

    for filename in all_files:
        file_parts = os.path.basename(filename).split("_")
        exp_mode = file_parts[1]
        microarch_level = file_parts[2]
        input_rate = int(file_parts[3][1:]) * 1000000
        num_queries = int(file_parts[4].split(".csv")[0][1:])
        df = pd.read_csv(filename, sep=';')[:-1]
        df['exp_mode'] = exp_mode
        df['microarch_level'] = microarch_level
        df['input_rate'] = input_rate
        df['num_queries'] = num_queries
        extracted_tplat = tplat_df[(tplat_df['exp_mode'] == exp_mode) & (tplat_df['input_rate'] == input_rate) & (
                tplat_df['num_queries'] == num_queries)]
        assert (len(extracted_tplat) == 1)
        df['overall_tp'] = extracted_tplat.iloc[0]['overall_tp']
        df['last_avg_latency'] = extracted_tplat.iloc[0]['last_avg_latency']
        li.append(df)

    toplev_stat_df = pd.concat(li, axis=0, ignore_index=True)
    print(toplev_stat_df)

    exp_modes = toplev_stat_df['exp_mode'].unique()
    microarch_levels = toplev_stat_df['microarch_level'].unique()
    print(exp_modes, microarch_levels)

    for exp_mode in exp_modes:
        filtered_df = toplev_stat_df[
            (toplev_stat_df['exp_mode'] == exp_mode) & (toplev_stat_df['microarch_level'] == 'l3')]
        bottleneck_df = filtered_df[filtered_df['Bottleneck'] == '<=='].copy()
        level_splits = bottleneck_df['Area'].str.split(".", n=3, expand=True)
        bottleneck_df['L1_bottleneck'] = level_splits[0]
        bottleneck_df['L2_bottleneck'] = level_splits[1]
        bottleneck_df['L3_bottleneck'] = level_splits[2]
        bottleneck_df['L1_overhead_pct'] = 0.0
        bottleneck_df['L1_overhead_unit'] = 'N/A'
        bottleneck_df['L2_overhead_pct'] = 0.0
        bottleneck_df['L2_overhead_unit'] = 'N/A'
        for index, row in bottleneck_df.iterrows():
            target_val_l1 = row['L1_bottleneck']
            extracted_l1 = filtered_df[
                (filtered_df['Area'] == target_val_l1) & (filtered_df['num_queries'] == row['num_queries']) & (
                        filtered_df['input_rate'] == row['input_rate'])].iloc[0]
            bottleneck_df.at[index, 'L1_overhead_pct'] = extracted_l1['Value']
            bottleneck_df.at[index, 'L1_overhead_unit'] = extracted_l1['Unit']

            if row['L2_bottleneck'] is not None:
                target_val_l2 = target_val_l1 + "." + row['L2_bottleneck']
                extracted_l2 = filtered_df[
                    (filtered_df['Area'] == target_val_l2) & (filtered_df['num_queries'] == row['num_queries']) & (
                            filtered_df['input_rate'] == row['input_rate'])].iloc[0]
                bottleneck_df.at[index, 'L2_overhead_pct'] = extracted_l2['Value']
                bottleneck_df.at[index, 'L2_overhead_unit'] = extracted_l2['Unit']
            else:
                bottleneck_df.at[index, 'Value'] = 0.0
                bottleneck_df.at[index, 'Unit'] = 'N/A'

        summary_df = bottleneck_df.loc[:, [
                                              'input_rate', 'num_queries', 'L1_bottleneck', 'L1_overhead_pct',
                                              'L1_overhead_unit', 'L2_bottleneck', 'L2_overhead_pct',
                                              'L2_overhead_unit', 'L3_bottleneck', 'Value', 'Unit', 'overall_tp',
                                              'last_avg_latency']].copy()
        summary_df.rename(columns={'Value': 'L3_overhead_pct', 'Unit': 'L3_overhead_unit'}, inplace=True)
        if exp_mode == 'ifixed':
            summary_df.sort_values(by='num_queries', inplace=True)
        else:
            summary_df.sort_values(by='input_rate', inplace=True)
        print(summary_df)
        summary_df.to_csv(data_dir + "/" + exp_date_id + "/" + exp_mode + "_summary.csv", index=False)


def analyze_schedule_gap_stats():
    counters_df = pd.read_csv(data_dir + "/event_count.csv", names=count_columns, dtype=count_column_dtypes)
    thread_ids = counters_df['thread_id'].unique()
    for thread_id in thread_ids:
        sched_gap_df = counters_df[
            (counters_df['metric_type'].str.contains("scheduleGap")) & (counters_df['thread_id'] == thread_id)].copy()
        if sched_gap_df.empty:
            continue
        sched_gap_df['rel_time'] = sched_gap_df['timestamp'].subtract(
            sched_gap_df['timestamp'].min()).div(1_000)
        sched_gap_df['operator_name'] = sched_gap_df['metric_type'].str.rsplit(pat="_", n=2, expand=True)[0]
        sched_gap_time_df = sched_gap_df[sched_gap_df['metric_type'].str.contains('scheduleGapTime')].copy()
        sched_gap_time_df.rename(columns={'count': 'gap_time'}, inplace=True)
        sched_gap_time_df.sort_values(by=['operator_name', 'timestamp'], inplace=True)
        sched_gap_time_df['gap_time_diff'] = sched_gap_time_df.groupby('operator_name')['gap_time'].diff().fillna(
            sched_gap_time_df['gap_time'])
        sched_gap_count_df = sched_gap_df[sched_gap_df['metric_type'].str.contains('scheduleGap_counter')].loc[:,
                             ['thread_id', 'timestamp', 'operator_name', 'count']].copy()
        sched_gap_count_df.rename(columns={'count': 'gap_count'}, inplace=True)
        sched_gap_count_df.sort_values(by=['operator_name', 'timestamp'], inplace=True)
        sched_gap_count_df['gap_count_diff'] = sched_gap_count_df.groupby('operator_name')['gap_count'].diff().fillna(
            sched_gap_count_df['gap_count'])
        joined_sched_gap_df = pd.merge(sched_gap_time_df, sched_gap_count_df,
                                       on=['thread_id', 'timestamp', 'operator_name'])
        joined_sched_gap_df['gap_time_avg'] = joined_sched_gap_df['gap_time_diff'] / joined_sched_gap_df[
            'gap_count_diff']
        summary_df = joined_sched_gap_df[
                         (joined_sched_gap_df['rel_time'] > 1)].loc[:,
                     ['thread_id', 'timestamp', 'rel_time', 'operator_name', 'gap_time_diff', 'gap_count_diff',
                      'gap_time_avg']].copy()
        exp_thread_ids = summary_df['thread_id'].unique()
        assert len(exp_thread_ids) == 1 and exp_thread_ids[0] == thread_id
        exp_thread_id = exp_thread_ids[0]
        print(summary_df)
        summary_df.to_csv(
            results_dir + "/" + targeted_metrics_subdir + "/schedule_gap_summary_" + str(exp_thread_id) + ".csv",
            index=False)


def analyze_latency_stats():
    latency_df = pd.read_csv(data_dir + "/latency.csv", names=count_columns, dtype=count_column_dtypes)
    sched_gap_df = latency_df[latency_df['metric_type'].str.contains("scheduleGap")].copy()
    sched_gap_df['rel_time'] = sched_gap_df['timestamp'].subtract(
        sched_gap_df['timestamp'].min()).div(1_000)
    sched_gap_df['operator_name'] = sched_gap_df['metric_type'].str.rsplit(pat="_", n=2, expand=True)[0]
    sched_gap_time_df = sched_gap_df[sched_gap_df['metric_type'].str.contains('scheduleGapTime')].copy()
    sched_gap_time_df.rename(columns={'count': 'gap_time'}, inplace=True)
    sched_gap_time_df.sort_values(by=['operator_name', 'timestamp'], inplace=True)
    sched_gap_time_df['gap_time_diff'] = sched_gap_time_df.groupby('operator_name')['gap_time'].diff().fillna(
        sched_gap_time_df['gap_time'])
    sched_gap_count_df = sched_gap_df[sched_gap_df['metric_type'].str.contains('scheduleGap_counter')].loc[:,
                         ['thread_id', 'timestamp', 'operator_name', 'count']].copy()
    sched_gap_count_df.rename(columns={'count': 'gap_count'}, inplace=True)
    sched_gap_count_df.sort_values(by=['operator_name', 'timestamp'], inplace=True)
    sched_gap_count_df['gap_count_diff'] = sched_gap_count_df.groupby('operator_name')['gap_count'].diff().fillna(
        sched_gap_count_df['gap_count'])
    joined_sched_gap_df = pd.merge(sched_gap_time_df, sched_gap_count_df,
                                   on=['thread_id', 'timestamp', 'operator_name'])
    joined_sched_gap_df['gap_time_avg'] = joined_sched_gap_df['gap_time_diff'] / joined_sched_gap_df['gap_count_diff']
    summary_df = joined_sched_gap_df[
                     (joined_sched_gap_df['rel_time'] > 1)].loc[:,
                 ['thread_id', 'timestamp', 'rel_time', 'operator_name', 'gap_time_diff', 'gap_count_diff',
                  'gap_time_avg']].copy()
    exp_thread_ids = summary_df['thread_id'].unique()
    assert len(exp_thread_ids) == 1
    exp_thread_id = exp_thread_ids[0]
    print(summary_df)
    summary_df.to_csv(
        results_dir + "/" + targeted_metrics_subdir + "/schedule_gap_summary_" + str(exp_thread_id) + ".csv",
        index=False)


def load_config():
    try:
        with open("config.json", "r") as file:
            return json.load(file)
    except FileNotFoundError:
        raise Exception("Configuration file 'config.json' not found. Please create it based on 'config.example.json'.")
    except json.JSONDecodeError:
        raise Exception("Error parsing 'config.json'. Please ensure it's correctly formatted.")


def get_filename(metric_type):
    type_parts = metric_type.split("_")
    size = len(type_parts)
    return type_parts[size - 2] + "_" + type_parts[size - 1] + "_" + type_parts[size - 4] + "_" + type_parts[size - 3]


def get_benchmark_label(benchmark_name):
    return config['benchmark_labels'][benchmark_name]


if __name__ == '__main__':
    known_types = {
        'int': int,
        'float': float,
        'str': str
    }

    var_type = known_types['int']
    parser = argparse.ArgumentParser()
    parser.add_argument("expdate_id")
    parser.add_argument("-p", "--parallelism", default="1")
    parser.add_argument("-sp", "--src_parallelism", default="1")
    parser.add_argument("-i", "--numiters", default=5, type=int)
    parser.add_argument("--host", default="tem120")
    args = parser.parse_args()

    config = load_config()
    count_columns, count_column_dtypes = get_columns_with_dtypes('count')
    latency_columns, latency_column_dtypes = get_columns_with_dtypes('latency')
    tp_columns, tp_column_dtypes = get_columns_with_dtypes('tp')

    sys_metrics_columns, sys_metrics_column_dtypes = get_columns_with_dtypes('sys_metrics')
    sys_metrics_to_skip = config['sys_metrics_to_skip'].split(",")

    op_metrics_columns, op_metrics_column_dtypes = get_columns_with_dtypes('op_metrics')
    op_metrics_to_skip = config['op_metrics_to_skip'].split(",")

    perf_stat_columns, perf_stat_column_dtypes = get_columns_with_dtypes('perf_stat')

    exp_date_id = args.expdate_id
    data_dir = config['additional_metrics_dir'] + "/" + exp_date_id
    if exp_date_id == 'google_tests_rel':
        data_dir = config['source_dir'] + "/cmake-build-relwithdebinfo/google-tests/metrics"
        results_dir = data_dir
    elif exp_date_id == 'google_tests_debug':
        data_dir = config['source_dir'] + "/cmake-build-debug/google-tests/metrics"
        results_dir = data_dir
    else:
        results_dir = config['results_dir'] + "/" + exp_date_id

    os.makedirs(results_dir, exist_ok=True)
    op_metrics_subdir = "operator_metrics"
    os.makedirs(results_dir + "/" + op_metrics_subdir, exist_ok=True)
    sys_metrics_subdir = "system_metrics"
    os.makedirs(results_dir + "/" + sys_metrics_subdir, exist_ok=True)
    targeted_metrics_subdir = "targeted_metrics"
    os.makedirs(results_dir + "/" + targeted_metrics_subdir, exist_ok=True)

    time_lower = config['time_lower']
    time_upper = config['time_upper']

    print("Reading from {} with experiment ID {} with time bounds of [{},{}]".format(config['data_dir'], exp_date_id,
                                                                                     time_lower, time_upper))

    if exp_date_id == 'google_tests_debug' or exp_date_id == 'google_tests_rel' or exp_date_id.startswith('2024'):
        analyze_schedule_gap_stats()
        # analyze_latency_stats()
    else:
        analyze_toplev_stats(exp_date_id)
    analyze_perf_stats(perf_stat_columns, perf_stat_column_dtypes)
