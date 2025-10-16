import os

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib as mpl
import numpy as np
import pandas as pd
import regex as re

PENDING_EVENTS_GAUGE = "pendingEvents_gauge"
COST_GAUGE = "cost_gauge"
SELECTIVITY_GAUGE = "selectivity_gauge"
SCHEDULED_TIME_GAUGE = "scheduledTime_gauge"
SCHEDULED_COUNT_GAUGE = "scheduledCount_gauge"
SCHEDULED_TIME_DIFF_GAUGE = "scheduledTimeDiff_gauge"
SCHEDULED_COUNT_DIFF_GAUGE = "scheduledCountDiff_gauge"
BATCH_SIZE_AVG_GAUGE = "batchSizeAvg_gauge"
LATENCY_GRAD_GAUGE = "latencyGrad_gauge"

known_types = {
    'int': int,
    'float': float,
    'str': str
}


def reject_outliers(in_data, m=2.):
    data = pd.Series(in_data)
    d = np.abs(data - np.median(data))
    mdev = np.median(d)
    s = d / mdev if mdev else np.zeros(len(d))
    out_data = data[s < m]
    return out_data.tolist()


def get_label(config, labels, metric_type):
    if metric_type.endswith(PENDING_EVENTS_GAUGE):
        return labels['graph_labels'][PENDING_EVENTS_GAUGE].split(",")[1]
    elif metric_type.endswith(COST_GAUGE):
        return labels['graph_labels'][COST_GAUGE].split(",")[1]
    elif metric_type.endswith(SELECTIVITY_GAUGE):
        return labels['graph_labels'][SELECTIVITY_GAUGE].split(",")[1]
    elif metric_type.endswith(SCHEDULED_TIME_GAUGE):
        return labels['graph_labels'][SCHEDULED_TIME_GAUGE].split(",")[1]
    elif metric_type.endswith(SCHEDULED_COUNT_GAUGE):
        return labels['graph_labels'][SCHEDULED_COUNT_GAUGE].split(",")[1]
    elif metric_type.endswith(SCHEDULED_TIME_DIFF_GAUGE):
        return labels['graph_labels'][SCHEDULED_TIME_DIFF_GAUGE].split(",")[1]
    elif metric_type.endswith(SCHEDULED_COUNT_DIFF_GAUGE):
        return labels['graph_labels'][SCHEDULED_COUNT_DIFF_GAUGE].split(",")[1]
    elif metric_type.endswith(BATCH_SIZE_AVG_GAUGE):
        return labels['graph_labels'][BATCH_SIZE_AVG_GAUGE].split(",")[1]
    elif metric_type.endswith(LATENCY_GRAD_GAUGE):
        return labels['graph_labels'][LATENCY_GRAD_GAUGE].split(",")[1]

    if metric_type == "latency":
        metric_type = metric_type + "_" + config.get("y_scale", {metric_type: "log"})[metric_type]
    return labels['graph_labels'].get(metric_type, "Unknown,Unknown").split(",")[1]


def get_plot_label(config, labels, plot_type, metric_type):
    if plot_type == "generic":
        return get_label(config, labels, metric_type)

    if metric_type.endswith(PENDING_EVENTS_GAUGE):
        return labels['plot_labels'][plot_type][PENDING_EVENTS_GAUGE].split(",")[1]
    elif metric_type.endswith(COST_GAUGE):
        return labels['plot_labels'][plot_type][COST_GAUGE].split(",")[1]
    elif metric_type.endswith(SELECTIVITY_GAUGE):
        return labels['plot_labels'][plot_type][SELECTIVITY_GAUGE].split(",")[1]
    elif metric_type.endswith(SCHEDULED_TIME_GAUGE):
        return labels['plot_labels'][plot_type][SCHEDULED_TIME_GAUGE].split(",")[1]
    elif metric_type.endswith(SCHEDULED_COUNT_GAUGE):
        return labels['plot_labels'][plot_type][SCHEDULED_COUNT_GAUGE].split(",")[1]
    elif metric_type.endswith(SCHEDULED_TIME_DIFF_GAUGE):
        return labels['plot_labels'][plot_type][SCHEDULED_TIME_DIFF_GAUGE].split(",")[1]
    elif metric_type.endswith(SCHEDULED_COUNT_DIFF_GAUGE):
        return labels['plot_labels'][plot_type][SCHEDULED_COUNT_DIFF_GAUGE].split(",")[1]
    elif metric_type.endswith(BATCH_SIZE_AVG_GAUGE):
        return labels['plot_labels'][plot_type][BATCH_SIZE_AVG_GAUGE].split(",")[1]
    elif metric_type.endswith(LATENCY_GRAD_GAUGE):
        return labels['graph_labels'][LATENCY_GRAD_GAUGE].split(",")[1]

    return labels['plot_labels'][plot_type][metric_type].split(",")[1]


def get_title(labels, metric_type):
    if metric_type.endswith(PENDING_EVENTS_GAUGE):
        op_name = metric_type.split("_{}".format(PENDING_EVENTS_GAUGE))[0]
        return labels['graph_labels'][PENDING_EVENTS_GAUGE].split(",")[0] + " (" + op_name + ")"
    elif metric_type.endswith(COST_GAUGE):
        op_name = metric_type.split("_{}".format(COST_GAUGE))[0]
        return labels['graph_labels'][COST_GAUGE].split(",")[0] + " (" + op_name + ")"
    elif metric_type.endswith(SELECTIVITY_GAUGE):
        op_name = metric_type.split("_{}".format(SELECTIVITY_GAUGE))[0]
        return labels['graph_labels'][SELECTIVITY_GAUGE].split(",")[0] + " (" + op_name + ")"
    elif metric_type.endswith(SCHEDULED_TIME_GAUGE):
        op_name = metric_type.split("_{}".format(SCHEDULED_TIME_GAUGE))[0]
        return labels['graph_labels'][SCHEDULED_TIME_GAUGE].split(",")[0] + " (" + op_name + ")"
    elif metric_type.endswith(SCHEDULED_COUNT_GAUGE):
        op_name = metric_type.split("_{}".format(SCHEDULED_COUNT_GAUGE))[0]
        return labels['graph_labels'][SCHEDULED_COUNT_GAUGE].split(",")[0] + " (" + op_name + ")"
    elif metric_type.endswith(SCHEDULED_TIME_DIFF_GAUGE):
        op_name = metric_type.split("_{}".format(SCHEDULED_TIME_DIFF_GAUGE))[0]
        return labels['graph_labels'][SCHEDULED_TIME_DIFF_GAUGE].split(",")[0] + " (" + op_name + ")"
    elif metric_type.endswith(SCHEDULED_COUNT_DIFF_GAUGE):
        op_name = metric_type.split("_{}".format(SCHEDULED_COUNT_DIFF_GAUGE))[0]
        return labels['graph_labels'][SCHEDULED_COUNT_DIFF_GAUGE].split(",")[0] + " (" + op_name + ")"
    elif metric_type.endswith(BATCH_SIZE_AVG_GAUGE):
        op_name = metric_type.split("_{}".format(BATCH_SIZE_AVG_GAUGE))[0]
        return labels['graph_labels'][BATCH_SIZE_AVG_GAUGE].split(",")[0] + " (" + op_name + ")"
    elif metric_type.endswith(LATENCY_GRAD_GAUGE):
        op_name = metric_type.split("_{}".format(LATENCY_GRAD_GAUGE))[0]
        return labels['graph_labels'][LATENCY_GRAD_GAUGE].split(",")[0] + " (" + op_name + ")"

    return labels['graph_labels'].get(metric_type, "Unknown,Unknown").split(",")[0]


def get_filename(metric_type):
    type_parts = metric_type.split("_")
    size = len(type_parts)
    return type_parts[size - 2] + "_" + type_parts[size - 1] + "_" + type_parts[size - 4] + "_" + type_parts[size - 3]


def get_filename_for_sysmetrics(metric_type):
    return metric_type


def get_benchmark_label(config, benchmark_name):
    return config['benchmark_labels'][benchmark_name]


def get_columns_with_dtypes(config, metric_name):
    columns = config[metric_name + '_columns'].split(",")
    tp_col_dtypes = config[metric_name + '_col_dtypes'].split(",")
    column_dtypes = dict.fromkeys(columns)
    for key, value in zip(column_dtypes.keys(), tp_col_dtypes):
        column_dtypes[key] = known_types[value]
    return columns, column_dtypes


def derive_config_label(config, labels, input_text):
    input_parts = input_text.split("_")
    if input_parts[1] == "Flink":
        return labels['config_labels']['Flink']
    if config['label_by_parts']:
        label_str = ""
        is_thread_based = False
        for input_part in input_parts:
            if input_part == "TB":
                is_thread_based = True
                continue
            if input_part.isnumeric():
                input_part_num = int(input_part)
                if input_part_num > 1000000:
                    label_str += str(input_part_num / 1000000) + "M "
                else:
                    label_str += input_part + " "
            else:
                if is_thread_based and input_part == "InputQueueSize":
                    input_part = "TB_InputQueueSize"
                label_str += labels['config_labels'][input_part] + " "

        label_str = label_str[:-1]
        return label_str
    else:
        if config.get('label_short_circuit', False):
            rsplit_max = config['label_short_circuit_params']['rsplit_max']
            input_parts = input_text.rsplit("_", rsplit_max)
            label_id = input_parts[0]
            label_str = labels['full_labels'].get(label_id, "Unknown")
            label_str += "[" + input_parts[1] + "]"
            return label_str

        return labels['full_labels'].get(input_text, "Unknown")


def derive_bar_color(config, labels, input_text):
    first_part, sys_id = input_text.split("_", maxsplit=1)
    if not first_part.isnumeric():
        sys_id = input_text
    # TODO Following logic to be used only for variance of input rate with same num. queries
    if first_part.isnumeric():
        sys_id = str(int(first_part) // 1000000) + "_" + sys_id

    if config.get('label_short_circuit', False):
        rsplit_max = config['label_short_circuit_params']['rsplit_max']
        input_parts = input_text.rsplit("_", rsplit_max)
        label_id = input_parts[1][:3]
        bar_color = labels['bar_colors'].get(label_id, "tab:blue")
        return bar_color

    bar_color = labels['bar_colors'].get(sys_id, "tab:blue")
    return bar_color


def derive_hatch_style(labels, input_text):
    first_part, sys_id = input_text.split("_", maxsplit=1)
    if not first_part.isnumeric():
        sys_id = input_text
    # TODO Following logic to be used only for variance of input rate with same num. queries
    # if first_part.isnumeric():
    #     sys_id = str(int(first_part) // 4000000) + "_" + sys_id
    hatch_styles = labels['hatch_styles'].get(sys_id, "")
    return hatch_styles


def derive_line_marker(labels, input_text):
    first_part, sys_id = input_text.split("_", maxsplit=1)
    if not first_part.isnumeric():
        sys_id = input_text
    # TODO Following logic to be used only for variance of input rate with same num. queries
    if first_part.isnumeric():
        sys_id = str(int(first_part) // 1000000) + "_" + sys_id
    line_marker = labels['line_markers'].get(sys_id, '.')
    return line_marker


def derive_marker_size(labels, input_text):
    first_part, sys_id = input_text.split("_", maxsplit=1)
    if not first_part.isnumeric():
        sys_id = input_text
    # TODO Following logic to be used only for variance of input rate with same num. queries
    if first_part.isnumeric():
        sys_id = str(int(first_part) // 1000000) + "_" + sys_id
    line_marker = labels['marker_size'].get(sys_id, '5')
    return line_marker


def derive_line_style(config, labels, input_text):
    if config.get('label_short_circuit', False):
        style_map = {"LRB": "-", "YSB": "-", "NYT": "-"}
        rsplit_max = config['label_short_circuit_params']['rsplit_max']
        input_parts = input_text.rsplit("_", rsplit_max)
        line_style = style_map[input_parts[1][:3]]

        return line_style

    line_style = labels['line_styles'].get(input_text, '-')
    return line_style


def get_derived_op_name(raw_op_name):
    if raw_op_name == "ysb_src" or raw_op_name == "lrb_src":
        return "1-Source"
    elif raw_op_name == "eventFilter" or raw_op_name == "typeFilter":
        return "2-Filter"
    elif raw_op_name == "project":
        return "3-Project"
    elif raw_op_name == "travelFilter":
        return "4-Filter"
    elif raw_op_name == "staticEqJoin":
        return "4-StaticJoin"
    elif raw_op_name == "accidentWindow":
        return "5-AccidentWindow"
    elif raw_op_name == "timeWindow":
        return "5-TimeWindow"
    elif raw_op_name == "speedWindow":
        return "6-SpeedWindow"
    elif raw_op_name == "ysb_sink":
        return "6-Sink"
    elif raw_op_name == "countWindow":
        return "7-CountWindow"
    elif raw_op_name == "countAndSpeedJoin":
        return "8-CountAndSpeedJoin"
    elif raw_op_name == "tollAndAccidentCoGroup":
        return "9-TollAndAccidentCoGroup"
    elif raw_op_name == "lrb_sink":
        return "10-Sink"
    else:
        return raw_op_name


def get_group_id(config, group_by_types, sys_id_parts):
    group_id = sys_id_parts[config['part_indices']["benchmark_name"]]
    for param_type in group_by_types:
        param_index = config['part_indices'][param_type]
        group_id += "_" + sys_id_parts[param_index]
    return group_id


def get_non_group_text(config, labels, non_group_types, sys_id_parts):
    non_group_text = ""
    for non_group_type in non_group_types:
        # if non_group_type == "num_workers":
        #     non_group_text += labels['graph_labels'][non_group_type].split(",")[1] + ": " + sys_id_parts[
        #         config['part_indices'][non_group_type]] + " or 8, "
        # else:
        non_group_text += labels['graph_labels'][non_group_type].split(",")[1] + ": " + sys_id_parts[
            config['part_indices'][non_group_type]] + ", "
    non_group_text = non_group_text[:-2]
    return non_group_text


def derive_plot_title(config, labels, benchmark_name, target_var):
    if config['set_title']:
        return ("p95 " if target_var == "p95" else "") + get_title(labels, target_var) + " (" + get_benchmark_label(
            labels,
            benchmark_name) + ")"
    else:
        return None


def get_exec_mode(preempt_mode, priority_type, schedule_mode):
    if schedule_mode == "TB":
        return schedule_mode
    elif schedule_mode == "SBPriority":
        schedule_mode = "SB"
    exec_mode = schedule_mode + " (" + preempt_mode + " - " + priority_type + ")"
    return exec_mode


# autolabel function copied directly from https://decomposition.al/blog/2015/11/29/a-better-way-to-add-labels-to-bar-charts-with-matplotlib/
def autolabel(rects, ax, y_err, max_measurement, y_scale):
    # Get y-axis height to calculate label position from.
    y_bottom, _ = ax.get_ylim()
    y_height = max_measurement - y_bottom

    for i, rect in enumerate(rects):
        height = rect.get_height() + y_err[i]

        # Fraction of axis height taken up by this rectangle
        p_height = (height / y_height)

        # If we can fit the label above the column, do that;
        # otherwise, put it inside the column.
        threshold = 0.8 if y_scale == "log" else 0.9
        if p_height > threshold:  # arbitrary; 95% looked good to me.
            if y_scale == "log":
                label_position = height ** 0.25
            else:
                label_position = height - (y_height * 0.7)
        else:
            if y_scale == "log":
                label_position = (1 + height) ** 1.05
            else:
                label_position = height + (y_height * 0.02)

        label_str = "{:.2f}".format(height) if height < 100 else "{:.0f}".format(height)
        ax.text(rect.get_x() + rect.get_width() / 1.8, label_position, label_str,
                ha='center', va='bottom', rotation=90)


def label_plot(label_points, ax, y_val, label_rotation=90, halign="center", valign="bottom", txtfontsize=16):
    for x_val, label_str in label_points.items():
        ax.text(x_val, y_val, label_str, ha=halign, va=valign, rotation=label_rotation, fontsize=txtfontsize)


def autolabel_latency_vs_tput(rects, ax, max_measurement, y_scale, unit_scale=1.0):
    # Get y-axis height to calculate label position from.
    y_bottom, _ = ax.get_ylim()
    y_height = max_measurement - y_bottom

    for i, rect in enumerate(rects):
        height = rect.get_height()

        # Fraction of axis height taken up by this rectangle
        p_height = (height / y_height)

        # If we can fit the label above the column, do that;
        # otherwise, put it inside the column.
        threshold = 0.8 if y_scale == "log" else 1.2
        if p_height > threshold:  # arbitrary; 95% looked good to me.
            if y_scale == "log":
                label_position = height ** 0.25
            else:
                label_position = height - (y_height * 0.7)
        else:
            if y_scale == "log":
                label_position = (1 + height) ** 1.05
            else:
                label_position = height + (y_height * 0.02)

        label_val = height / unit_scale
        label_str = "{:.2f}".format(label_val) if label_val < 100 else "{:.0f}".format(label_val)
        ax.text(rect.get_x() + rect.get_width() / 1.8, label_position, label_str,
                ha='center', va='bottom', rotation=90)


def plot_bar_graph_for_metric(config, labels, results_dir, agg_data_series, x_var, y_var, non_group_text, plot_title,
                              plot_filename, exp_id, iter_num, sci_labels=False, show_plot=False, y_scale="linear",
                              idx_for_xvar_in_sysid=1):
    handle_flink_for_oom = config.get("handle_flink_for_oom", False)
    handle_sbox_for_oom = config.get("handle_flink_for_oom", False)
    is_mixed_workload = config.get("mixed_workload", False)
    generate_per_num_queries = True
    if is_mixed_workload:
        generate_per_num_queries = config["mixed_workload_params"].get("generate_per_num_queries", True)
    x_var_components = []
    if "," in x_var:
        x_label = ""
        x_var_components = x_var.split(",")
        for x_var in x_var_components:
            x_label += get_label(config, labels, x_var) + " , "
        x_label = x_label[:-3]
        print(x_label)
    else:
        x_label = get_label(config, labels, x_var)
        if is_mixed_workload and not generate_per_num_queries:
            x_label = get_label(config, labels, x_var + "_mixed")
    y_label = get_label(config, labels, y_var)
    agg_avg_data = {}
    x_ci = {}
    x_var_modes = []
    for xy_var_mode, y_var_vals in agg_data_series.items():
        if not x_var_components:
            x_var_mode = xy_var_mode.split("_")[idx_for_xvar_in_sysid]
        else:
            x_var_mode = ""
            xy_var_mode_parts = xy_var_mode.split("_")
            for i, x_var in enumerate(x_var_components):
                x_var_mode += xy_var_mode_parts[i + 1] + "_"
            x_var_mode = x_var_mode[:-1]
            print(x_var_mode)

        # Queries to skip
        x_var_skip_list = config['query_skip_list'].split(",")
        if x_var_mode in x_var_skip_list:
            continue

        if is_mixed_workload and not generate_per_num_queries:
            y_var_idx = config["mixed_workload_params"]["y_var_idx"]
            front_partition_pattern = config["mixed_workload_params"]["front_partition_pattern"]
            if y_var_idx < 0:
                candidate_str = xy_var_mode
            else:
                candidate_str = xy_var_mode.partition(x_var_mode)[y_var_idx]
            candidate_str = candidate_str.partition(front_partition_pattern)[2]
            y_var_mode = candidate_str.strip("_")
        else:
            y_var_mode = xy_var_mode.partition(x_var_mode + "_")[2]
        if x_var_mode not in x_var_modes:
            x_var_modes.append(x_var_mode)
        if y_var_mode not in agg_avg_data:
            agg_avg_data[y_var_mode] = []
            x_ci[y_var_mode] = []
        agg_avg_data[y_var_mode].append(np.mean(y_var_vals))
        vals_series = pd.Series(y_var_vals)
        x_ci[y_var_mode].append(vals_series.agg(
            lambda x_vals: np.sqrt(x_vals.pow(2).mean() - pow(x_vals.mean(), 2)) * 1.96 / np.sqrt(x_vals.size)))

    print("agg_avg_data: " + str(agg_avg_data))
    print("x_ci: " + str(x_ci))
    print("x_var_modes: " + str(x_var_modes))

    label_fmt_style = "{:,.2f}"
    if y_label == "Latency (ms)" or y_label == "Cost (ms/event)":
        label_fmt_style = "{:,.2f}"
    elif y_label == "Throughput (events/sec)":
        label_fmt_style = "{:,.0f}"

    x = np.arange(len(x_var_modes))  # the label locations
    bar_width_scale_factor = config['bar_width_scale_factor']
    label_padding = config['label_padding']

    set_measurement_label = config['set_measurement_label'].get(y_var, False)
    mark_oom_data_points = config.get('mark_oom', {y_var: False})[y_var]
    has_oom_data_points = False
    plot_line_width = config['plot_line_width']
    plot_font_size = config['plot_font_size']
    plt.rc('font', size=plot_font_size)
    mpl.rcParams['hatch.linewidth'] = plot_line_width  # Set desired linewidth (e.g., 0.5 points)

    appearance_order_tags = config.get('appearance_order_tags', "appearance_order").split(",")
    for appearance_order_tag in appearance_order_tags:
        appearance_order = config[appearance_order_tag].split(',')

        fig, ax = plt.subplots(figsize=(16, 10))
        multiplier = 0
        num_types = len(appearance_order)
        width = 0.84 / num_types  # the width of the bars
        max_measurement = 0
        for y_var_mode in appearance_order:
            measurement = agg_avg_data[y_var_mode]
            bar_heights = [i + j for i, j in zip(measurement, x_ci[y_var_mode])]
            if max_measurement < max(bar_heights):
                max_measurement = max(bar_heights)
        for y_var_mode in appearance_order:
            measurement = agg_avg_data[y_var_mode]
            manual_plot_labels = {}
            # Special handling of missing values for Flink with mixed workloads
            if handle_flink_for_oom and y_var_mode == "Default_Flink_Default_OnDemand" and len(x) > len(measurement):
                print("Detected missing values for Flink in mixed workloads. Appending zeroes")
                num_missing_vals = len(x) - len(measurement)
                next_x_val = len(measurement)
                for i in range(num_missing_vals):
                    measurement.append(0)
                    x_ci[y_var_mode].append(np.nan)
                    manual_plot_labels[next_x_val * (multiplier + 1)] = "Flink - OOM"
                    next_x_val += 1
                has_oom_data_points = True

            if handle_sbox_for_oom and y_var_mode == "Default_StreamBox_Default_OnDemand" and len(x) > len(measurement):
                print("Detected missing values for StreamBox. Appending zeroes")
                num_missing_vals = len(x) - len(measurement)
                next_x_val = len(measurement)
                for i in range(num_missing_vals):
                    measurement.append(0)
                    x_ci[y_var_mode].append(np.nan)
                    manual_plot_labels[next_x_val * (multiplier + 1)] = "StreamBox - OOM"
                    next_x_val += 1
                has_oom_data_points = True

            offset = width * multiplier
            y_var_label = derive_config_label(config, labels, y_var_mode)
            bar_color = derive_bar_color(config, labels, y_var_mode)
            hatch_style = derive_hatch_style(labels, y_var_mode)
            rects = ax.bar(x + offset, measurement, width * bar_width_scale_factor,
                           label=y_var_label, hatch=hatch_style, edgecolor=bar_color,
                           color='w', linewidth=plot_line_width)
            err_label = "" if multiplier < num_types - 1 else "95% Confidence"
            curr_yerr = x_ci[y_var_mode]
            ax.errorbar(x + offset, measurement, yerr=curr_yerr, capsize=5, capthick=2, color='k',
                        label=err_label,
                        linestyle='None')
            if set_measurement_label:
                autolabel(rects, ax, curr_yerr, max_measurement, y_scale)
            if mark_oom_data_points and has_oom_data_points:
                data_point_height = config['oom_data_point_height'].get(y_var, 1)
                label_plot(manual_plot_labels, ax, data_point_height)
                has_oom_data_points = False
            print(y_var_label.ljust(15) + ": [" + " ".join(str(round(x, 2)).ljust(8) for x in measurement) + "]")
            multiplier += 1

        ax.set_xlabel(x_label, labelpad=label_padding)
        ax.set_ylabel(y_label, labelpad=label_padding)
        ax.set_yscale(y_scale)
        if plot_title:
            ax.set_title(plot_title)

        ax.tick_params(width=plot_line_width)
        x_tick_scale = config['x_tick_scale'].get(x_var, 1)
        if x_tick_scale > 1:
            x_tick_labels = [str(int(x) / x_tick_scale) for x in x_var_modes]
        else:
            x_tick_labels = x_var_modes
        ax.set_xticks(x + (0.5 * num_types) * width, x_tick_labels, rotation=0)

        plot_file_type = config['plot_file_type']
        show_legend = config['show_legend']
        if show_legend and not config.get("skip_legend", True):
            ax.legend(loc='upper left', bbox_to_anchor=(1.05, 1.0))

        if sci_labels:
            ax.ticklabel_format(axis='y', style='sci', scilimits=(6, 6))
        if y_scale == "linear" and config.get('set_y_min_zero', {y_var: True})[y_var]:
            ax.set_ylim(bottom=0)
        y_max = config['y_max'].get(y_var, 0)
        if y_max > 0:
            ax.set_ylim(top=y_max)
        ax.margins(y=0.2)

        text_y_pos = ax.get_ylim()[1] * 0.1
        text_x_pos = len(x_var_modes) * 1.02
        set_non_group_text = config['set_non_group_text']
        if set_non_group_text:
            plt.text(text_x_pos, text_y_pos, non_group_text, size=12,
                     ha="left", wrap=True,
                     bbox=dict(boxstyle="square",
                               ec=(1., 0.5, 0.5),
                               fc=(1., 0.8, 0.8),
                               ))
        fig.tight_layout()

        plot_type_suffix = appearance_order_tag.split("appearance_order")[1]
        if config['exp_id_based_names']:
            plot_filepath = results_dir + "/" + plot_filename + "_bar_" + exp_id + "_iter" + iter_num
        else:
            plot_filename_suffix = config['plot_filename_suffix']
            plot_filepath = results_dir + "/" + plot_filename + "_bar_" + plot_filename_suffix + plot_type_suffix
        plt.savefig(plot_filepath + "." + plot_file_type, bbox_inches='tight')
        if show_plot:
            plt.show()
        plt.close()
        if not show_legend and not config.get("skip_legend", True):
            label_params = ax.get_legend_handles_labels()
            ncols_legend = config['ncols_legend']
            figl, axl = plt.subplots(figsize=(2 * ncols_legend, 1))
            axl.axis(False)
            legend_prop_size = config['legend_prop_size']
            axl.legend(*label_params, loc='center', bbox_to_anchor=(0.5, 0.5), prop={"size": legend_prop_size},
                       ncol=ncols_legend, frameon=False)
            figl.savefig(plot_filepath + "_legend_only." + plot_file_type, bbox_inches='tight')
            if show_plot:
                figl.show()
            plt.close(figl)


def plot_bar_graph_for_metric_special(config, labels, results_dir, agg_data_series, x_var, y_var, non_group_text,
                                      plot_title,
                                      plot_filename, exp_id, iter_num, sci_labels=False, show_plot=False,
                                      y_scale="linear"):
    x_var_components = []
    if "," in x_var:
        x_var_components = x_var.split(",")
        if x_var == "processing_mode,scheduling_mode,scheduling_policy,allocation_mode":
            x_label = "System under test"
        else:
            x_label = ""
            for x_var in x_var_components:
                x_label += get_label(config, labels, x_var) + " , "
            x_label = x_label[:-3]
            print(x_label)
    else:
        x_label = get_label(config, labels, x_var)
    y_label = get_label(config, labels, y_var)
    agg_avg_data = {}
    x_ci = {}
    x_var_modes = []
    for xy_var_mode, y_var_vals in agg_data_series.items():
        if not x_var_components:
            x_var_mode = xy_var_mode.split("_")[1]
        else:
            x_var_mode = ""
            xy_var_mode_parts = xy_var_mode.split("_")
            x_var_offset = config.get('x_var_offset', 0)
            for i, x_var in enumerate(x_var_components):
                x_var_mode += xy_var_mode_parts[x_var_offset + 1] + "_"
                x_var_offset += 1
            x_var_mode = x_var_mode[:-1]
            print(x_var_mode)

        # Queries to skip
        x_var_skip_list = config['x_var_skip_list'].split(",")
        if x_var_mode in x_var_skip_list:
            continue

        y_var_offset = config.get('y_var_offset', 2)
        y_var_mode = xy_var_mode.partition(x_var_mode + "_")[y_var_offset]
        if not y_var_mode:
            y_var_mode = x_var_mode
        if x_var_mode not in x_var_modes:
            x_var_modes.append(x_var_mode)
        if y_var_mode not in agg_avg_data:
            agg_avg_data[y_var_mode] = []
            x_ci[y_var_mode] = []
        agg_avg_data[y_var_mode].append(np.mean(y_var_vals))
        vals_series = pd.Series(y_var_vals)
        x_ci[y_var_mode].append(vals_series.agg(
            lambda x_vals: np.sqrt(x_vals.pow(2).mean() - pow(x_vals.mean(), 2)) * 1.96 / np.sqrt(x_vals.size)))

    print("agg_avg_data: " + str(agg_avg_data))
    print("x_ci: " + str(x_ci))
    print("x_var_modes: " + str(x_var_modes))

    label_fmt_style = "{:,.2f}"
    if y_label == "Latency (ms)" or y_label == "Cost (ms/event)":
        label_fmt_style = "{:,.2f}"
    elif y_label == "Throughput (events/sec)":
        label_fmt_style = "{:,.0f}"

    x = np.arange(len(x_var_modes))  # the label locations
    multiplier = 0
    bar_width_scale_factor = config['bar_width_scale_factor']
    label_padding = config['label_padding']

    plot_font_size = config['plot_font_size']
    plt.rc('font', size=plot_font_size)
    fig, ax = plt.subplots(figsize=(16, 10))
    x_tick_labels = []
    plot_line_width = config['plot_line_width']
    mpl.rcParams['hatch.linewidth'] = plot_line_width  # Set desired linewidth (e.g., 0.5 points)
    line_alpha = config.get('line_alpha', 0.8)
    if config['use_appearance_order']:
        appearance_order = config['appearance_order'].split(',')
        num_types = len(appearance_order)
        max_measurement = 0
        for y_var_mode in appearance_order:
            measurement = agg_avg_data[y_var_mode]
            bar_heights = [i + j for i, j in zip(measurement, x_ci[y_var_mode])]
            if max_measurement < max(bar_heights):
                max_measurement = max(bar_heights)
        for y_var_mode in appearance_order:
            measurement = agg_avg_data[y_var_mode]
            y_var_label = derive_config_label(config, labels, y_var_mode)
            bar_color = derive_bar_color(config, labels, y_var_mode)
            hatch_style = derive_hatch_style(labels, y_var_mode)
            x_tick_labels.append(y_var_label)
            # ax.bar(x[multiplier], measurement, label=y_var_label, hatch=hatch_style, edgecolor='w',
            #        color=bar_color)
            ax.bar(x[multiplier], measurement, label=y_var_label, edgecolor=bar_color,
                   color='w', hatch=hatch_style, linewidth=plot_line_width, alpha=line_alpha)
            multiplier += 1
    else:
        num_types = len(agg_avg_data.keys())
        width = 0.84 / num_types  # the width of the bars
        for y_var_mode, measurement in agg_avg_data.items():
            offset = width * multiplier
            y_var_label = derive_config_label(config, labels, y_var_mode)
            rects = ax.bar(x + offset, measurement, width * bar_width_scale_factor, label=y_var_label)
            # ax.errorbar(x + offset, measurement, yerr=x_ci[y_var_mode], capsize=3, color='k',
            #             label=y_var_label + " - Error",
            #             linestyle='None')
            # ax.bar_label(rects, padding=label_padding, fmt=label_fmt_style, rotation=90, label_type='edge')
            multiplier += 1

    ax.set_xlabel(x_label, labelpad=label_padding)
    ax.set_ylabel(y_label, labelpad=label_padding)
    ax.set_yscale(y_scale)
    if plot_title:
        ax.set_title(plot_title)

    ax.tick_params(width=plot_line_width, length=20)
    ax.set_xticks(x, x_tick_labels, rotation=0)

    plot_file_type = config['plot_file_type']
    show_legend = config['show_legend']
    if show_legend and not config.get("skip_legend", True):
        ax.legend(loc='upper left', bbox_to_anchor=(1.05, 1.0))

    if sci_labels:
        ax.ticklabel_format(axis='y', style='sci', scilimits=(6, 6))
    if y_scale == "linear":
        ax.set_ylim(bottom=0)
    y_max = config['y_max'].get(y_var, 0)
    if y_max > 0:
        ax.set_ylim(top=y_max)
    ax.margins(y=0.2)

    text_y_pos = ax.get_ylim()[1] * 0.1
    text_x_pos = len(x_var_modes) * 1.02
    set_non_group_text = config['set_non_group_text']
    if set_non_group_text:
        plt.text(text_x_pos, text_y_pos, non_group_text, size=12,
                 ha="left", wrap=True,
                 bbox=dict(boxstyle="square",
                           ec=(1., 0.5, 0.5),
                           fc=(1., 0.8, 0.8),
                           ))
    fig.tight_layout()
    if config['exp_id_based_names']:
        plot_filepath = results_dir + "/" + plot_filename + "_bar_" + exp_id + "_iter" + iter_num
    else:
        plot_filename_suffix = config['plot_filename_suffix']
        plot_filepath = results_dir + "/" + plot_filename + "_bar_" + plot_filename_suffix
    plt.savefig(plot_filepath + "." + plot_file_type, bbox_inches='tight')
    if show_plot:
        plt.show()
    plt.close()
    if not show_legend and not config.get("skip_legend", True):
        label_params = ax.get_legend_handles_labels()
        ncols_legend = config['ncols_legend']
        figl, axl = plt.subplots(figsize=(2 * ncols_legend, 1))
        axl.axis(False)
        legend_prop_size = config['legend_prop_size']
        axl.legend(*label_params, loc='center', bbox_to_anchor=(0.5, 0.5), prop={"size": legend_prop_size},
                   ncol=ncols_legend, frameon=False)
        figl.savefig(plot_filepath + "_legend_only." + plot_file_type, bbox_inches='tight')
        if show_plot:
            figl.show()
        plt.close(figl)


def plot_line_graph_for_metric(config, labels, target_metric_type, results_dir, agg_data_series, x_var, y_var,
                               non_group_text, plot_title, plot_filename, exp_id,
                               iter_num, sci_labels=False, show_plot=False, y_scale="linear"):
    handle_flink_for_oom = config.get("handle_flink_for_oom", False)
    handle_sbox_for_oom = config.get("handle_sbox_for_oom", False)
    x_var_components = []
    if "," in x_var:
        x_label = ""
        x_var_components = x_var.split(",")
        for x_var in x_var_components:
            x_label += get_label(config, labels, x_var) + " , "
        x_label = x_label[:-3]
        print(x_label)
    elif x_var == "opt_val":
        x_label = config.get('custom_x_label', "Optional Value")
    else:
        x_label = get_label(config, labels, x_var)
    y_label = get_label(config, labels, y_var)
    agg_avg_data = {}
    x_ci = {}
    x_var_modes = []
    for xy_var_mode, y_var_vals in agg_data_series.items():
        if not x_var_components:
            x_var_mode = xy_var_mode.split("_")[1]
        else:
            x_var_mode = ""
            xy_var_mode_parts = xy_var_mode.split("_")
            for i, x_var in enumerate(x_var_components):
                x_var_mode += xy_var_mode_parts[i + 1] + "_"
            x_var_mode = x_var_mode[:-1]
            print(x_var_mode)

        # Elements to skip
        query_skip_list = config['query_skip_list'].split(",")
        if x_var_mode in query_skip_list:
            continue

        y_var_mode = xy_var_mode.partition(x_var_mode + "_")[2]
        # Y var modes to skip
        # if y_var_mode in ["24000000_BlockBasedBatch_SBPriority_LatencyOptimized_PreAllocate"]:
        #     continue
        if x_var_mode not in x_var_modes:
            x_var_modes.append(x_var_mode)
        if y_var_mode not in agg_avg_data:
            agg_avg_data[y_var_mode] = []
            x_ci[y_var_mode] = []
        agg_avg_data[y_var_mode].append(np.mean(y_var_vals))
        vals_series = pd.Series(y_var_vals)
        x_ci[y_var_mode].append(vals_series.agg(
            lambda x_vals: np.sqrt(x_vals.pow(2).mean() - pow(x_vals.mean(), 2)) * 1.96 / np.sqrt(x_vals.size)))

    print("agg_avg_data: " + str(agg_avg_data))
    print("x_ci: " + str(x_ci))
    print("x_var_modes: " + str(x_var_modes))

    mark_oom_data_points = config.get('mark_oom', {y_var: False})[y_var]
    has_oom_data_points = False

    x = np.arange(len(x_var_modes))  # the label locations
    plot_font_size = config['plot_font_size']
    plt.rc('font', size=plot_font_size)
    plot_line_width = config['plot_line_width']
    line_alpha = config.get('line_alpha', 0.8)

    appearance_order_tags = config.get('appearance_order_tags', "appearance_order").split(",")
    for appearance_order_tag in appearance_order_tags:
        appearance_order = config[appearance_order_tag].split(',')

        multiplier = 0
        fig_width = config.get('fig_width', {target_metric_type: 12})[target_metric_type]
        fig_height = config.get('fig_height', {target_metric_type: 8})[target_metric_type]
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))
        num_y_vars = len(appearance_order)
        # width = 0.4 / num_y_vars  # the width of interleaving
        # multiplier = -1 * (num_y_vars // 2)
        max_measurement = 0
        for y_var_mode in appearance_order:
            measurement = agg_avg_data[y_var_mode]
            bar_heights = [i + j for i, j in zip(measurement, x_ci[y_var_mode])]
            if max_measurement < max(bar_heights):
                max_measurement = max(bar_heights)

        for y_var_mode in appearance_order:
            measurement = agg_avg_data[y_var_mode]
            # Special handling of missing values for Flink with mixed workloads
            manual_plot_labels = {}
            if handle_flink_for_oom and y_var_mode == "Default_Flink_Default_OnDemand" and len(x) > len(measurement):
                print("Detected missing values for Flink in mixed workloads. Appending zeroes")
                num_missing_vals = len(x) - len(measurement)
                x_val = len(measurement) - 1
                for i in range(num_missing_vals):
                    measurement.append(0)
                    x_ci[y_var_mode].append(np.nan)
                    if not has_oom_data_points:
                        manual_plot_labels[x_val] = "    OOM (Flink)"
                        has_oom_data_points = True
            if handle_sbox_for_oom and y_var_mode == "Default_StreamBox_Default_OnDemand" and any(
                    val == 0 for val in measurement):
                print("Detected missing values for StreamBox. Appending zeroes")
                num_missing_vals = measurement.count(0)
                x_val = measurement.index(0) - 1
                for i in range(num_missing_vals):
                    if not has_oom_data_points:
                        manual_plot_labels[x_val] = "  OOM (StreamBox)"
                        has_oom_data_points = True

            # Replace 0 with NaN
            measurement_arr = np.array(measurement, dtype=float)
            measurement_arr = np.where(measurement_arr == 0, np.nan, measurement_arr)
            measurement = measurement_arr.tolist()

            y_var_label = derive_config_label(config, labels, y_var_mode)
            bar_color = derive_bar_color(config, labels, y_var_mode)
            line_marker = derive_line_marker(labels, y_var_mode)
            line_style = derive_line_style(config, labels, y_var_mode)
            marker_size = derive_marker_size(labels, y_var_mode)
            # offset = width * multiplier
            ax.plot(x, measurement, label=y_var_label, color=bar_color, linewidth=plot_line_width, linestyle=line_style,
                    marker=line_marker, markersize=marker_size, alpha=line_alpha)
            if config['show_err_bars']:
                err_label = "" if multiplier < num_y_vars - 1 else "95% Confidence"
                curr_yerr = x_ci[y_var_mode]
                ax.errorbar(x, measurement, yerr=curr_yerr, capsize=plot_line_width, capthick=plot_line_width / 2,
                            color='k',
                            label=err_label, linestyle='None')
            if mark_oom_data_points and has_oom_data_points:
                data_point_height = measurement[list(manual_plot_labels.keys())[0]] + config['oom_data_point_height'].get(y_var, 1)
                label_plot(manual_plot_labels, ax, data_point_height, 0, "left", "center",
                           round(0.8 * plot_font_size, 0))
                has_oom_data_points = False
            # auto_label_line(ax, measurement, x, max_measurement, plot_font_size, y_scale)
            print(y_var_label.ljust(15) + ": [" + " ".join(str(round(x, 2)).ljust(8) for x in measurement) + "]")
            multiplier += 1

        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)
        ax.set_yscale(y_scale)
        ax.set_title(plot_title)
        ax.tick_params(width=plot_line_width, size=plot_line_width * 3)
        x_tick_scale = config['x_tick_scale'].get(x_var, 1)
        if x_tick_scale > 1:
            x_tick_labels = [str(int(x) / x_tick_scale) for x in x_var_modes]
        else:
            x_tick_labels = x_var_modes
        x_tick_rotation = config.get('x_tick_rotation', 0)
        ax.set_xticks(x, x_tick_labels, rotation=x_tick_rotation)

        plot_file_type = config['plot_file_type']
        show_legend = config['show_legend']
        if show_legend and not config.get("skip_legend", True):
            legend_coords_x_var = config['legend_coords'][x_var]
            bbox_anchor_x = legend_coords_x_var['x']
            bbox_anchor_y = legend_coords_x_var['y']
            legend_prop_size = config['legend_prop_size']
            ax.legend(loc='upper left', bbox_to_anchor=(bbox_anchor_x, bbox_anchor_y), prop={"size": legend_prop_size})

        if sci_labels:
            ax.ticklabel_format(axis='y', style='sci', scilimits=(6, 6))
        if y_scale == "linear":
            if config.get('set_y_min_zero', {y_var: True})[y_var]:
                ax.set_ylim(bottom=0)
            elif config.get('y_min', {y_var: 0})[y_var] != 0:
                y_min = config['y_min'][y_var]
                ax.set_ylim(bottom=y_min)
                
        y_max = config['y_max'].get(y_var, 0)
        if y_max > 0:
            ax.set_ylim(top=y_max)
        # ax.margins(y=0.2)

        set_non_group_text = config['set_non_group_text']
        if set_non_group_text:
            text_y_pos = ax.get_ylim()[1] * 0.1
            text_x_pos = len(x_var_modes) * 1.02
            plt.text(text_x_pos, text_y_pos, non_group_text, size=12,
                     ha="left", wrap=True,
                     bbox=dict(boxstyle="square",
                               ec=(1., 0.5, 0.5),
                               fc=(1., 0.8, 0.8),
                               ))

        fig.tight_layout()
        plot_type_suffix = appearance_order_tag.split("appearance_order")[1]
        if config['exp_id_based_names']:
            plot_filepath = results_dir + "/" + plot_filename + "_line_" + exp_id + "_iter" + iter_num
        else:
            plot_filename_suffix = config['plot_filename_suffix']
            plot_filepath = results_dir + "/" + plot_filename + "_line_" + plot_filename_suffix + plot_type_suffix
        plt.savefig(plot_filepath + "." + plot_file_type, bbox_inches='tight')
        if show_plot:
            plt.show()
        plt.close()
        # if appearance_order_tag == "appearance_order_external":
        #    exit(0)
        if not show_legend and not config.get("skip_legend", True):
            label_params = ax.get_legend_handles_labels()
            if config.get('add_oom_to_legend', False):
                custom_patch = mpatches.Patch(color='w', label='Custom Item')
                label_params[0].append(custom_patch)
                label_params[1].append('OOM = Out Of Memory')

            ncols_legend = config['ncols_legend']
            figl, axl = plt.subplots(figsize=(2 * ncols_legend, 1))
            axl.axis(False)
            legend_prop_size = config['legend_prop_size']
            axl.legend(*label_params, loc='center', bbox_to_anchor=(0.5, 0.5), prop={"size": legend_prop_size},
                       ncol=ncols_legend, frameon=False, columnspacing=4)
            figl.savefig(plot_filepath + "_legend_only." + plot_file_type, bbox_inches='tight')
            if show_plot:
                figl.show()
            plt.close(figl)


def plot_bar_graph_for_latency_vs_tput(config, labels, results_dir, agg_data_series, x_var, y_var, non_group_text,
                                       plot_title,
                                       plot_filename, exp_id, iter_num, sci_labels=False, show_plot=False,
                                       y_scale="linear"):
    x_var_for_label = 'input_rate'
    x_var_components = []
    if "," in x_var:
        x_label = ""
        x_var_components = x_var.split(",")
        for x_var in x_var_components:
            x_label += get_label(config, labels, x_var) + " , "
        x_label = x_label[:-3]
        print(x_label)
    else:
        x_label = get_label(config, labels, x_var_for_label)
    y_label = get_label(config, labels, y_var)
    tput_label = get_label(config, labels, x_var)
    agg_avg_data = {}
    x_ci = {}
    x_var_vals = {}
    x_var_modes = []
    # x_var_mode to skip
    x_var_skip_list = config['x_var_skip_list'].split(",")
    for xy_var_mode, xy_var_vals in agg_data_series.items():
        if not x_var_components:
            x_var_mode = xy_var_mode.split("_")[1]
        else:
            x_var_mode = ""
            xy_var_mode_parts = xy_var_mode.split("_")
            for i, x_var in enumerate(x_var_components):
                x_var_mode += xy_var_mode_parts[i + 1] + "_"
            x_var_mode = x_var_mode[:-1]
            print(x_var_mode)

        if x_var_mode in x_var_skip_list:
            continue
        if x_var_mode not in x_var_modes:
            x_var_modes.append(x_var_mode)

        xy_vals_lists = list(map(list, zip(*xy_var_vals)))
        y_var_mode = xy_var_mode.partition(x_var_mode + "_")[2]
        if y_var_mode not in agg_avg_data:
            agg_avg_data[y_var_mode] = []
            x_var_vals[y_var_mode] = []
            x_ci[y_var_mode] = []
        agg_avg_data[y_var_mode].append(np.mean(xy_vals_lists[1]))
        x_var_val = round(np.mean(xy_vals_lists[0]), 2)
        x_var_vals[y_var_mode].append(x_var_val)
        vals_series = pd.Series(xy_vals_lists[1])
        x_ci[y_var_mode].append(vals_series.agg(
            lambda x_vals: np.sqrt(x_vals.pow(2).mean() - pow(x_vals.mean(), 2)) * 1.96 / np.sqrt(x_vals.size)))

    print("agg_avg_data: " + str(agg_avg_data))
    print("x_ci: " + str(x_ci))
    print("x_var_modes: " + str(x_var_modes))
    print("x_var_vals: " + str(x_var_vals))

    label_fmt_style = "{:,.2f}"
    if y_label == "Latency (ms)" or y_label == "Cost (ms/event)":
        label_fmt_style = "{:,.2f}"
    elif y_label == "Throughput (events/sec)":
        label_fmt_style = "{:,.0f}"

    x = np.arange(len(x_var_modes))  # the label locations
    multiplier = 0
    bar_width_scale_factor = config['bar_width_scale_factor']
    label_padding = config['label_padding']

    fig, ax = plt.subplots(figsize=(16, 10))
    ax2 = ax.twinx()
    plot_font_size = config['plot_font_size']
    plt.rc('font', size=plot_font_size)
    plot_line_width = config['plot_line_width']
    set_measurement_label = config['set_measurement_label'].get(y_var, False)
    if config['use_appearance_order']:
        appearance_order = config['appearance_order'].split(',')
        num_types = len(appearance_order)
        width = 0.2 / num_types  # the width of the bars
        max_measurement_latency = 0
        max_measurement_tput = 0
        for y_var_mode in appearance_order:
            latency_vals = agg_avg_data[y_var_mode]
            tput_vals = x_var_vals[y_var_mode]
            if max_measurement_latency < max(latency_vals):
                max_measurement_latency = max(latency_vals)
            if max_measurement_tput < max(tput_vals):
                max_measurement_tput = max(tput_vals)
        for y_var_mode in appearance_order:
            latency_vals = agg_avg_data[y_var_mode]
            offset = width * multiplier
            multiplier += 1
            latency_offset = width * multiplier
            y_var_label = derive_config_label(config, labels, y_var_mode)
            bar_color = derive_bar_color(config, labels, y_var_mode)
            tput_vals = x_var_vals[y_var_mode]
            latency_rects = ax.bar(x + latency_offset, latency_vals, width * bar_width_scale_factor,
                                   label=y_var_label + " Latency", hatch='//', color=bar_color, edgecolor='k',
                                   alpha=0.8)
            tput_rects = ax2.bar(x + offset, tput_vals, width * bar_width_scale_factor,
                                 label=y_var_label + " Throughput", hatch='..', edgecolor=bar_color,
                                 color='w')
            if set_measurement_label:
                autolabel_latency_vs_tput(latency_rects, ax, max_measurement_latency, y_scale)
                autolabel_latency_vs_tput(tput_rects, ax2, max_measurement_tput, y_scale)
            multiplier += 2
    else:
        num_types = len(agg_avg_data.keys())
        width = 0.84 / num_types  # the width of the bars
        for y_var_mode, latency_vals in agg_avg_data.items():
            offset = width * multiplier
            y_var_label = derive_config_label(config, labels, y_var_mode)
            rects = ax.bar(x + offset, latency_vals, width * bar_width_scale_factor, label=y_var_label)
            ax.errorbar(x + offset, latency_vals, yerr=x_ci[y_var_mode], capsize=3, color='k',
                        label=y_var_label + " - Error",
                        linestyle='None')
            ax.bar_label(rects, padding=label_padding, fmt=label_fmt_style, rotation=90, label_type='edge')
            multiplier += 1

    ax.set_xlabel(x_label, labelpad=label_padding)
    ax.set_ylabel(y_label, labelpad=label_padding)
    ax2.set_ylabel(tput_label, labelpad=label_padding)
    ax.set_yscale(y_scale)
    if plot_title:
        ax.set_title(plot_title)

    ax.tick_params(width=3)
    x_tick_scale = config['x_tick_scale'].get(x_var_for_label, 1)
    if x_tick_scale > 1:
        x_tick_labels = [str(int(x) / x_tick_scale) for x in x_var_modes]
    else:
        x_tick_labels = x_var_modes
    ax.set_xticks(x + (1.5 * num_types) * width, x_tick_labels, rotation=0)

    plot_file_type = config['plot_file_type']
    show_legend = config['show_legend']
    if show_legend and not config.get("skip_legend", True):
        ax.legend(loc='upper left', bbox_to_anchor=(0.01, .99))
        ax2.legend(loc='upper left', bbox_to_anchor=(0.01, .85))

    if sci_labels:
        ax.ticklabel_format(axis='y', style='sci', scilimits=(6, 6))
    # if y_scale == "linear":
    #     ax.set_ylim(bottom=0)
    y_max = config['y_max'].get(y_var, 0)
    if y_max > 0:
        ax.set_ylim(top=y_max)
    ax.margins(y=0.2)

    text_y_pos = ax.get_ylim()[1] * 0.1
    text_x_pos = len(x_var_modes) * 1.02
    set_non_group_text = config['set_non_group_text']
    if set_non_group_text:
        plt.text(text_x_pos, text_y_pos, non_group_text, size=12,
                 ha="left", wrap=True,
                 bbox=dict(boxstyle="square",
                           ec=(1., 0.5, 0.5),
                           fc=(1., 0.8, 0.8),
                           ))
    plot_font_size = config['plot_font_size']
    plt.rc('font', size=plot_font_size)
    fig.tight_layout()
    if config['exp_id_based_names']:
        plot_filepath = results_dir + "/" + plot_filename + "_bar_" + exp_id + "_iter" + iter_num
    else:
        plot_filename_suffix = config['plot_filename_suffix']
        plot_filepath = results_dir + "/" + plot_filename + "_bar_" + plot_filename_suffix
    plt.savefig(plot_filepath + "." + plot_file_type, bbox_inches='tight')
    if show_plot:
        plt.show()
    plt.close()
    if not show_legend and not config.get("skip_legend", True):
        label_params = ax.get_legend_handles_labels()
        ncols_legend = config['ncols_legend']
        figl, axl = plt.subplots(figsize=(2 * ncols_legend, 1))
        axl.axis(False)
        legend_prop_size = config['legend_prop_size']
        axl.legend(*label_params, loc='center', bbox_to_anchor=(0.5, 0.5), prop={"size": legend_prop_size},
                   ncol=ncols_legend, frameon=False)
        figl.savefig(plot_filepath + "_legend_only." + plot_file_type, bbox_inches='tight')
        if show_plot:
            figl.show()
        plt.close(figl)


def plot_line_graph_for_latency_vs_tput(config, labels, results_dir, agg_data_series, x_var, y_var, non_group_text,
                                        plot_title,
                                        plot_filename, exp_id,
                                        iter_num, sci_labels=False, show_plot=False, y_scale="linear"):
    x_var_components = []
    if "," in x_var:
        x_label = ""
        x_var_components = x_var.split(",")
        for x_var in x_var_components:
            x_label += get_label(config, labels, x_var) + " , "
        x_label = x_label[:-3]
        print(x_label)
    else:
        x_label = get_label(config, labels, x_var)
    y_label = get_label(config, labels, y_var)
    agg_avg_data = {}
    x_ci = {}
    x_var_vals = {}
    # x_var_mode to skip
    x_var_skip_list = config['x_var_skip_list'].split(",")
    for xy_var_mode, xy_var_vals in agg_data_series.items():
        if not x_var_components:
            x_var_mode = xy_var_mode.split("_")[1]
        else:
            x_var_mode = ""
            xy_var_mode_parts = xy_var_mode.split("_")
            for i, x_var in enumerate(x_var_components):
                x_var_mode += xy_var_mode_parts[i + 1] + "_"
            x_var_mode = x_var_mode[:-1]
            print(x_var_mode)

        if x_var_mode in x_var_skip_list:
            continue

        xy_vals_lists = list(map(list, zip(*xy_var_vals)))
        y_var_mode = xy_var_mode.partition(x_var_mode + "_")[2]
        if y_var_mode not in agg_avg_data:
            agg_avg_data[y_var_mode] = []
            x_var_vals[y_var_mode] = []
            x_ci[y_var_mode] = []
        agg_avg_data[y_var_mode].append(np.mean(xy_vals_lists[1]))
        x_var_val = round(np.mean(xy_vals_lists[0]), 2)
        x_var_vals[y_var_mode].append(x_var_val)
        vals_series = pd.Series(xy_vals_lists[1])
        x_ci[y_var_mode].append(vals_series.agg(
            lambda x_vals: np.sqrt(x_vals.pow(2).mean() - pow(x_vals.mean(), 2)) * 1.96 / np.sqrt(x_vals.size)))

    print("agg_avg_data: " + str(agg_avg_data))
    print("x_ci: " + str(x_ci))
    print("x_var_vals: " + str(x_var_vals))

    multiplier = 0

    fig, ax = plt.subplots(figsize=(12, 8))
    plot_font_size = config['plot_font_size']
    plt.rc('font', size=plot_font_size)
    plot_line_width = config['plot_line_width']
    if config['use_appearance_order']:
        appearance_order = config['appearance_order'].split(',')
        num_y_vars = len(appearance_order)
        max_measurement = 0
        for y_var_mode in appearance_order:
            measurement = agg_avg_data[y_var_mode]
            bar_heights = [i + j for i, j in zip(measurement, x_ci[y_var_mode])]
            if max_measurement < max(bar_heights):
                max_measurement = max(bar_heights)
        for y_var_mode in appearance_order:
            measurement = agg_avg_data[y_var_mode]
            y_var_label = derive_config_label(config, labels, y_var_mode)
            bar_color = derive_bar_color(config, labels, y_var_mode)
            line_marker = derive_line_marker(labels, y_var_mode)
            line_style = derive_line_style(config, labels, y_var_mode)
            x = x_var_vals[y_var_mode]
            ax.plot(x, measurement, label=y_var_label, color=bar_color, linewidth=plot_line_width, linestyle=line_style,
                    marker=line_marker, markersize=plot_line_width * 4)
            if config['show_err_bars']:
                err_label = "" if multiplier < num_y_vars - 1 else "95% Confidence"
                curr_yerr = x_ci[y_var_mode]
                ax.errorbar(x, measurement, yerr=curr_yerr, capsize=5, capthick=plot_line_width, color='k',
                            label=err_label, linestyle='None')
            # auto_label_line(ax, measurement, x, max_measurement)
            multiplier += 1
    else:
        for y_var_mode, measurement in agg_avg_data.items():
            y_var_label = derive_config_label(config, labels, y_var_mode)
            x = x_var_vals[y_var_mode]
            ax.plot(x, measurement, label=y_var_label)
            ax.errorbar(x, measurement, yerr=x_ci[y_var_mode], capsize=3, color='k',
                        label=y_var_label + " - Error",
                        linestyle='None')
            max_measurement = max(measurement)
            # auto_label_line(ax, measurement, x, max_measurement, plot_font_size)
            multiplier += 1

    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_yscale(y_scale)
    ax.set_title(plot_title)
    ax.tick_params(width=3)

    plot_file_type = config['plot_file_type']
    bbox_anchor_x = config['legend_coords'][x_var]['x']
    bbox_anchor_y = config['legend_coords'][x_var]['y']
    ax.legend(loc='upper left', bbox_to_anchor=(bbox_anchor_x, bbox_anchor_y))
    if sci_labels:
        ax.ticklabel_format(axis='y', style='sci', scilimits=(6, 6))
    if y_scale == "linear":
        ax.set_ylim(bottom=0)
    y_max = config['y_max'].get(y_var, 0)
    if y_max > 0:
        ax.set_ylim(top=y_max)
    # ax.margins(y=0.2)

    set_non_group_text = config['set_non_group_text']
    if set_non_group_text:
        text_y_pos = ax.get_ylim()[1] * 0.1
        text_x_pos = len(x_var_vals) * 1.02
        plt.text(text_x_pos, text_y_pos, non_group_text, size=12,
                 ha="left", wrap=True,
                 bbox=dict(boxstyle="square",
                           ec=(1., 0.5, 0.5),
                           fc=(1., 0.8, 0.8),
                           ))

    fig.tight_layout()
    if config['exp_id_based_names']:
        plot_filepath = results_dir + "/" + plot_filename + "_line_" + exp_id + "_iter" + iter_num
    else:
        plot_filename_suffix = config['plot_filename_suffix']
        plot_filepath = results_dir + "/" + plot_filename + "_line_" + plot_filename_suffix
    plt.savefig(plot_filepath + "." + plot_file_type, bbox_inches='tight')
    if show_plot:
        plt.show()
    plt.close()


def auto_label_line(ax, measurement, x, max_measurement, font_size, y_scale="linear"):
    max_x = max(x)
    label_padding_x = 0.005 * max_x
    def_label_padding_y = 0.01 * max_measurement
    if y_scale == "linear":
        for x_coord, y_coord in zip(x, measurement):
            label_padding_y = def_label_padding_y
            p_height = y_coord / max_measurement
            if p_height > 0.95:
                label_padding_y = -4 * label_padding_y
            ax.text(x_coord + label_padding_x, y_coord + label_padding_y, f"{y_coord:.2f}", fontsize=font_size)
    else:
        for x_coord, y_coord in zip(x, measurement):
            label_padding_y = 0
            p_height = y_coord / max_measurement
            if p_height > 0.95:
                label_padding_y = -4 * label_padding_y
            pos = (1 + y_coord) ** 1.05
            ax.text(x_coord + label_padding_x, pos + label_padding_y, f"{y_coord:.2f}", fontsize=font_size, rotation=45)


def plot_stacked_bar_graph_for_metric(config, labels, results_dir, agg_data_series, x_var, y_var, non_group_text,
                                      plot_title, plot_filename, exp_id, iter_num, metric_types, sci_labels=False,
                                      show_plot=False, plot_type="generic",
                                      y_scale="linear"):
    x_var_components = []
    if "," in x_var:
        x_label = ""
        x_var_components = x_var.split(",")
        for x_var in x_var_components:
            x_label += get_plot_label(config, labels, plot_type, x_var) + " , "
        x_label = x_label[:-3]
        print(x_label)
    else:
        x_label = get_plot_label(config, labels, plot_type, x_var)
    y_label = get_plot_label(config, labels, plot_type, y_var)
    agg_avg_data = {}
    x_ci = {}
    x_var_modes = []
    for xy_var_mode, y_var_vals in agg_data_series.items():
        if not x_var_components:
            x_var_mode = xy_var_mode.split("_")[1]
        else:
            x_var_mode = ""
            xy_var_mode_parts = xy_var_mode.split("_")
            for i, x_var in enumerate(x_var_components):
                x_var_mode += xy_var_mode_parts[i + 1] + "_"
            x_var_mode = x_var_mode[:-1]
            print(x_var_mode)

        # Queries to skip
        x_var_skip_list = config['x_var_skip_list'].split(",")
        if x_var_mode in x_var_skip_list:
            continue

        y_var_mode = xy_var_mode.partition(x_var_mode + "_")[2]
        if x_var_mode not in x_var_modes:
            x_var_modes.append(x_var_mode)
        if y_var_mode not in agg_avg_data:
            agg_avg_data[y_var_mode] = []
            x_ci[y_var_mode] = []
        agg_avg_data[y_var_mode].append(np.mean(y_var_vals))
        vals_series = pd.Series(y_var_vals)
        x_ci[y_var_mode].append(vals_series.agg(
            lambda x_vals: np.sqrt(x_vals.pow(2).mean() - pow(x_vals.mean(), 2)) * 1.96 / np.sqrt(x_vals.size)))

    print("agg_avg_data: " + str(agg_avg_data))
    print("x_ci: " + str(x_ci))
    print("x_var_modes: " + str(x_var_modes))

    label_fmt_style = "{:,.2f}"
    if y_label == "Latency (ms)" or y_label == "Cost (ms/event)":
        label_fmt_style = "{:,.2f}"
    elif y_label == "Throughput (events/sec)":
        label_fmt_style = "{:,.0f}"

    x = np.arange(len(x_var_modes))  # the label locations
    multiplier = 0
    bar_width_scale_factor = 0.8
    label_padding = 10

    fig, ax = plt.subplots(figsize=(16, 8))
    if config['use_appearance_order']:
        appearance_order = config['stacked_appearance_order'][y_var].split(',')
        num_metric_types = len(metric_types)
        tot_types = len(appearance_order)
        num_types = tot_types // num_metric_types
        width = 0.84 / num_types  # the width of the bars
        bottom = {}
        iter_count = 0
        err_bar = {}
        for y_var_mode in appearance_order:
            measurement = agg_avg_data[y_var_mode]
            iter_count += 1
            if multiplier not in bottom:
                bottom[multiplier] = np.zeros(len(measurement))
            offset = width * multiplier
            if iter_count <= num_types:
                bar_color = derive_bar_color(config, labels, y_var_mode)
                edge_color = 'k'
            else:
                edge_color = derive_bar_color(config, labels, y_var_mode)
                bar_color = 'w'
            y_var_label = derive_config_label(config, labels, y_var_mode)
            hatch_style = derive_hatch_style(labels, y_var_mode)
            ax.bar(x + offset, measurement, width * bar_width_scale_factor, label=y_var_label, color=bar_color,
                   bottom=bottom[multiplier], hatch=hatch_style, edgecolor=edge_color)
            if iter_count <= (tot_types - num_types):
                err_bar[multiplier] = x_ci[y_var_mode]
            else:
                if multiplier not in err_bar:
                    err_bar[multiplier] = x_ci[y_var_mode]
                else:
                    err_bar[multiplier] = [sum(err) for err in zip(err_bar[multiplier], x_ci[y_var_mode])]
                err_label = "" if iter_count < tot_types else "95% Confidence"
                ax.errorbar(x + offset, bottom[multiplier] + measurement, yerr=err_bar[multiplier], capsize=5,
                            capthick=2,
                            color='k',
                            label=err_label,
                            linestyle='None')

            print(y_var_label.ljust(15) + ":\t[" + "\t".join(str(round(x, 2)).ljust(8) for x in measurement) + "\t]")
            bottom[multiplier] += measurement
            multiplier += 1
            multiplier = multiplier % num_types
    else:
        num_metric_types = len(metric_types)
        num_types = len(agg_avg_data.keys()) // num_metric_types
        width = 0.84 / num_types  # the width of the bars
        bottom = {}
        iter_count = 0
        err_bar = {}
        for y_var_mode, measurement in agg_avg_data.items():
            iter_count += 1
            if multiplier not in bottom:
                bottom[multiplier] = np.zeros(len(measurement))
            offset = width * multiplier
            y_var_label = derive_config_label(config, labels, y_var_mode)
            rects = ax.bar(x + offset, measurement, width * bar_width_scale_factor, label=y_var_label,
                           bottom=bottom[multiplier])
            if iter_count <= num_types:
                err_bar[multiplier] = x_ci[y_var_mode]
            else:
                err_bar[multiplier] = [sum(err) for err in zip(err_bar[multiplier], x_ci[y_var_mode])]
                ax.errorbar(x + offset, bottom[multiplier] + measurement, yerr=err_bar[multiplier], capsize=3,
                            color='k',
                            label=y_var_label + " - Error",
                            linestyle='None')
            # if y_var == "throughput" and multiplier == 1:
            #     label_padding = 3 + multiplier * 12
            # ax.bar_label(rects, padding=label_padding, fmt=label_fmt_style, rotation=90, label_type='edge')
            bottom[multiplier] += measurement
            multiplier += 1
            multiplier = multiplier % num_types

    ax.set_xlabel(x_label, labelpad=label_padding)
    ax.set_ylabel(y_label, labelpad=label_padding)
    ax.set_yscale(y_scale)
    if plot_title:
        ax.set_title(plot_title)

    ax.tick_params(width=3)
    x_tick_scale = config['x_tick_scale'][x_var]
    if x_tick_scale > 1:
        x_tick_labels = [str(int(x) / x_tick_scale) for x in x_var_modes]
    else:
        x_tick_labels = x_var_modes
    ax.set_xticks(x + (0.25 * num_types) * width, x_tick_labels, rotation=0)

    plot_file_type = config['plot_file_type']
    show_legend = config['show_legend']
    if show_legend and not config.get("skip_legend", True):
        ax.legend(loc='upper left', bbox_to_anchor=(1.05, 1.0))

    if sci_labels:
        ax.ticklabel_format(axis='y', style='sci', scilimits=(6, 6))
    if y_scale == "linear":
        ax.set_ylim(bottom=0)
    y_max = config['y_max'].get(y_var, 0)
    if y_max > 0:
        ax.set_ylim(top=y_max)
    ax.margins(y=0.2)

    text_y_pos = ax.get_ylim()[1] * 0.1
    text_x_pos = len(x_var_modes) * 1.02
    set_non_group_text = config['set_non_group_text']
    if set_non_group_text:
        plt.text(text_x_pos, text_y_pos, non_group_text, size=12,
                 ha="left", wrap=True,
                 bbox=dict(boxstyle="square",
                           ec=(1., 0.5, 0.5),
                           fc=(1., 0.8, 0.8),
                           ))
    plot_font_size = config['plot_font_size']
    plt.rc('font', size=plot_font_size)
    fig.tight_layout()

    if config['exp_id_based_names']:
        plot_filepath = results_dir + "/" + plot_filename + "_bar_" + exp_id + "_iter" + iter_num
    else:
        plot_filename_suffix = config['plot_filename_suffix']
        plot_filepath = results_dir + "/" + plot_filename + "_bar_" + plot_filename_suffix
    plt.savefig(plot_filepath + "." + plot_file_type, bbox_inches='tight')
    if show_plot:
        plt.show()
    plt.close()
    if not show_legend and not config.get("skip_legend", True):
        label_params = ax.get_legend_handles_labels()
        ncols_legend = config['ncols_legend']
        figl, axl = plt.subplots(figsize=(2.8 * ncols_legend, 2))
        axl.axis(False)
        legend_prop_size = config['legend_prop_size']
        axl.legend(*label_params, loc='center', bbox_to_anchor=(0.5, 0.5), prop={"size": legend_prop_size},
                   ncol=ncols_legend, frameon=False)
        figl.savefig(plot_filepath + "_legend_only." + plot_file_type, bbox_inches='tight')
        if show_plot:
            figl.show()
        plt.close(figl)


def plot_bar_graph_for_target_metric(config, labels, results_dir, agg_data_series, x_var, y_var, non_group_text,
                                     plot_title,
                                     plot_filename,
                                     exp_date_id,
                                     target_iter, sci_labels=False, show_plot=False):
    x_label = get_label(config, labels, x_var)
    y_label = get_label(config, labels, y_var)
    agg_avg_data = {}
    x_ci = {}
    x_var_modes = []
    for xy_var_mode, y_var_vals in agg_data_series.items():
        x_var_mode = xy_var_mode.split("_")[1]
        y_var_mode = xy_var_mode.partition("_")[2].partition("_")[2]
        if x_var_mode not in x_var_modes:
            x_var_modes.append(x_var_mode)
        if y_var_mode not in agg_avg_data:
            agg_avg_data[y_var_mode] = []
            x_ci[y_var_mode] = []
        if target_iter > 0:
            agg_avg_data[y_var_mode].append(y_var_vals[target_iter - 1])
        else:
            agg_avg_data[y_var_mode].append(np.mean(y_var_vals))

        vals_series = pd.Series(y_var_vals)
        x_ci[y_var_mode].append(vals_series.agg(
            lambda x: np.sqrt(x.pow(2).mean() - pow(x.mean(), 2)) * 1.96 / np.sqrt(x.size)))

    print("agg_avg_data: " + str(agg_avg_data))
    print("x_ci: " + str(x_ci))
    print("x_var_modes: " + str(x_var_modes))

    if target_iter > 0:
        print("target_iter: " + str(target_iter))

    x = np.arange(len(x_var_modes))  # the label locations
    num_types = len((agg_avg_data).keys())
    width = 0.8 / num_types  # the width of the bars
    multiplier = 0

    fig, ax = plt.subplots(figsize=(36, 12))

    ax.set_prop_cycle(color=[
        '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5', '#c49c94', '#f7b6d2', '#c7c7c7', '#1f77b4', '#ff7f0e',
        '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f'])

    for y_var_mode, measurement in agg_avg_data.items():
        offset = width * multiplier - 0.25
        ax.bar(x + offset, measurement, width, label=y_var_mode)
        if target_iter <= 0:
            ax.errorbar(x + offset, measurement, yerr=x_ci[y_var_mode], capsize=3, color='k',
                        label=y_var_mode + " - Error",
                        linestyle='None')
        multiplier += 1

    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(plot_title)
    ax.set_xticks(x + width / 2, x_var_modes)
    ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.75))
    if sci_labels:
        ax.ticklabel_format(axis='y', style='sci', scilimits=(6, 6))
    ax.set_ylim(bottom=0)
    ax.margins(y=0.2)

    text_y_pos = ax.get_ylim()[1] * 0.01
    plt.text(5.1, text_y_pos, non_group_text, size=12,
             ha="left", wrap=True,
             bbox=dict(boxstyle="square",
                       ec=(1., 0.5, 0.5),
                       fc=(1., 0.8, 0.8),
                       ))

    fig.tight_layout()
    plot_file_type = config['plot_file_type']
    plt.savefig(
        results_dir + "/" + plot_filename + "_targeted_bar_" + exp_date_id + "_iter" + str(
            target_iter) + "." + plot_file_type)
    if show_plot:
        plt.show()
    plt.close()


def process_grouped_operator_metrics(config, labels, op_metrics_subdir, csv_columns, target_var, independent_var,
                                     exp_date_id,
                                     column_dtypes,
                                     op_metrics_skip, group_by_types, non_group_types, iter="1",
                                     sci_labels=False):
    metric_df = pd.read_csv(config['data_dir'] + "/" + exp_date_id + "/operator_metrics.csv", names=csv_columns,
                            dtype=column_dtypes)
    metric_df['derived_metric_type'] = metric_df['metric_type'].str.replace(r'\d+_', '', regex=True)
    thread_ids = metric_df['thread_id'].unique()
    metric_types = metric_df['derived_metric_type'].unique()
    print(thread_ids)
    print(metric_types)
    sys_ids = metric_df['sys_id'].unique()
    benchmarks = []
    for sys_id in sys_ids:
        benchmark_name = sys_id.split("_")[1]
        if benchmark_name not in benchmarks:
            benchmarks.append(benchmark_name)

    agg_metrics = {}
    non_group_text = ""
    for thread_id in thread_ids:
        for metric_type in metric_types:
            if metric_type in op_metrics_skip:
                continue
            filtered_metric_df = metric_df[
                (metric_df['thread_id'] == thread_id) & (metric_df['derived_metric_type'] == metric_type)].copy()
            if filtered_metric_df.empty:
                continue
            if metric_type.endswith('scheduledTime_gauge') or metric_type.endswith('scheduledCount_gauge'):
                metric_type = re.sub(r"(scheduledCount|scheduledTime)", r"\1Diff", metric_type)
                filtered_metric_df['value'] = (filtered_metric_df['value'] -
                                               filtered_metric_df['value'].shift(1, fill_value=0))
            if metric_type not in agg_metrics:
                agg_metrics[metric_type] = {}
            assert (len(filtered_metric_df['sys_id'].unique()) == 1)
            sys_id_parts = filtered_metric_df['sys_id'].unique()[0].split("_")

            scheduling_mode = sys_id_parts[config['part_indices']["scheduling_mode"]]
            if scheduling_mode == "TB" and metric_type.endswith('scheduledCountDiff_gauge'):
                continue

            if non_group_text == "" and scheduling_mode != "TB":
                non_group_text = get_non_group_text(config, labels, non_group_types, sys_id_parts)
            group_id = get_group_id(config, group_by_types, sys_id_parts)

            if group_id not in agg_metrics[metric_type]:
                agg_metrics[metric_type][group_id] = []

            filtered_metric_df['rel_time'] = filtered_metric_df['timestamp'].subtract(
                filtered_metric_df['timestamp'].min()).div(1_000)
            filtered_metric_df = filtered_metric_df[
                (filtered_metric_df['rel_time'] > config['time_lower']) & (
                        filtered_metric_df['rel_time'] <= config['time_upper'])]
            if independent_var == "num_queries":
                avg_metric_df = filtered_metric_df.groupby("timestamp").mean()
                avg_metric = avg_metric_df.loc[:, target_var].mean()
            else:
                avg_metric = filtered_metric_df.loc[:, target_var].mean()
            agg_metrics[metric_type][group_id].append(avg_metric)

    print(agg_metrics)
    for benchmark_name in benchmarks:
        for metric_type in metric_types:
            if metric_type in op_metrics_skip:
                continue
            if metric_type.endswith('scheduledTime_gauge') or metric_type.endswith('scheduledCount_gauge'):
                metric_type = re.sub(r"(scheduledCount|scheduledTime)", r"\1Diff", metric_type)

            filtered_agg_metrics = agg_metrics[metric_type]
            for derived_group_id in filtered_agg_metrics:
                print(
                    "Avg. " + metric_type + "(" + derived_group_id + "): " + '{0:.5f}'.format(
                        np.mean(filtered_agg_metrics[derived_group_id])))
            plot_bar_graph_for_metric(config, labels, op_metrics_subdir, filtered_agg_metrics, independent_var,
                                      metric_type, non_group_text,
                                      derive_plot_title(config, labels, benchmark_name, metric_type),
                                      "avg_" + get_filename(
                                          metric_type) + "_" + benchmark_name, exp_date_id, iter,
                                      sci_labels=sci_labels, show_plot=True)


def process_grouped_system_metrics(config, labels, results_dir, sys_metrics_subdir, csv_columns, target_var,
                                   independent_var, exp_date_id, column_dtypes,
                                   sys_metrics_skip, group_by_types, non_group_types, iter="1",
                                   sci_labels=False):
    if config['multistage_processing'] and config['processing_stage'] == 4:
        combined_data_ids = config['combined_data_ids'].split("|")
        all_files = []
        for combined_data_id in combined_data_ids:
            all_files.append(config['data_dir'] + "/" + combined_data_id + "/system_metrics.csv")
        df_list = []
        for filename in all_files:
            metric_df_per_run = pd.read_csv(filename, names=csv_columns, dtype=column_dtypes)
            df_list.append(metric_df_per_run)
        metric_df = pd.concat(df_list, ignore_index=True)
    else:
        metric_df = pd.read_csv(config['data_dir'] + "/" + exp_date_id + "/system_metrics.csv", names=csv_columns,
                                dtype=column_dtypes)
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

    agg_metrics = {}
    non_group_text = ""
    for thread_id in thread_ids:
        for metric_type in metric_types:
            if metric_type in sys_metrics_skip:
                continue
            filtered_metric_df = metric_df[
                (metric_df['thread_id'] == thread_id) & (metric_df['metric_type'] == metric_type)].copy()
            if metric_type.endswith('_time_ms') or metric_type.endswith('_flt'):
                metric_type = metric_type + "_diff"
                filtered_metric_df['value'] = (filtered_metric_df['value'] -
                                               filtered_metric_df['value'].shift(1, fill_value=0))
            if metric_type not in agg_metrics:
                agg_metrics[metric_type] = {}
            assert (len(filtered_metric_df['sys_id'].unique()) == 1)
            sys_id_parts = filtered_metric_df['sys_id'].unique()[0].split("_")
            if non_group_text == "" and sys_id_parts[config['part_indices']["scheduling_mode"]] != "TB":
                non_group_text = get_non_group_text(config, labels, non_group_types, sys_id_parts)
            group_id = get_group_id(config, group_by_types, sys_id_parts)

            if group_id not in agg_metrics[metric_type]:
                agg_metrics[metric_type][group_id] = []

            filtered_metric_df['rel_time'] = filtered_metric_df['timestamp'].subtract(
                filtered_metric_df['timestamp'].min()).div(1_000)
            filtered_metric_df = filtered_metric_df[
                (filtered_metric_df['rel_time'] > config['time_lower']) & (
                        filtered_metric_df['rel_time'] <= config['time_upper'])]
            unit_scale = config['unit_scale'].get(metric_type, 1.0)
            avg_metric = filtered_metric_df.loc[:, target_var].mean() / unit_scale
            agg_metrics[metric_type][group_id].append(avg_metric)

    for benchmark_name in benchmarks:
        for metric_type in metric_types:
            if metric_type in sys_metrics_skip:
                continue
            if metric_type.endswith('_time_ms') or metric_type.endswith('_flt'):
                metric_type = metric_type + "_diff"

            filtered_agg_metrics = agg_metrics[metric_type]
            for derived_group_id in filtered_agg_metrics:
                print(
                    "Avg. " + metric_type + "(" + derived_group_id + "): " + '{0:.5f}'.format(
                        np.mean(filtered_agg_metrics[derived_group_id])))

            plot_bar_graph_for_metric(config, labels, results_dir, filtered_agg_metrics, independent_var, metric_type,
                                      non_group_text,
                                      derive_plot_title(config, labels, benchmark_name, metric_type),
                                      sys_metrics_subdir + "/avg_" + get_filename_for_sysmetrics(
                                          metric_type) + "_" + benchmark_name, exp_date_id, iter,
                                      sci_labels=sci_labels)


def process_agg_grouped_system_metrics(config, labels, results_dir, sys_metrics_subdir, csv_columns,
                                       tp_agg_metrics_dict, target_var,
                                       independent_var, exp_date_id, column_dtypes,
                                       sys_metrics_skip, group_by_types, non_group_types, target_metrics, type,
                                       iter="1",
                                       sci_labels=False):
    experiment_ids_with_skips = config.get("thread_ids_to_skip", [])
    thread_ids_to_skip = []
    if config['multistage_processing'] and config['processing_stage'] == 4:
        combined_data_ids = config['combined_data_ids'].split("|")
        all_files = []
        for combined_data_id in combined_data_ids:
            if combined_data_id in experiment_ids_with_skips:
                thread_ids_to_skip.extend(experiment_ids_with_skips[combined_data_id].split(","))
            all_files.append(config['data_dir'] + "/" + combined_data_id + "/system_metrics.csv")
        df_list = []
        for filename in all_files:
            metric_df_per_run = pd.read_csv(filename, names=csv_columns, dtype=column_dtypes)
            df_list.append(metric_df_per_run)
        metric_df = pd.concat(df_list, ignore_index=True)
    else:
        metric_df = pd.read_csv(config['data_dir'] + "/" + exp_date_id + "/system_metrics.csv", names=csv_columns,
                                dtype=column_dtypes)
    thread_ids = metric_df['thread_id'].unique()
    metric_types = target_metrics.split(",")
    metric_type_agg = target_metrics.replace(",", "_")
    print(thread_ids)
    print(metric_types)
    sys_ids = metric_df['sys_id'].unique()
    benchmarks = []
    for sys_id in sys_ids:
        benchmark_name = sys_id.split("_")[1]
        if benchmark_name not in benchmarks:
            benchmarks.append(benchmark_name)

    agg_metrics = {}
    x_var_keys = {}
    non_group_text = ""
    for thread_id in thread_ids:
        if str(thread_id) in thread_ids_to_skip:
            print("Skipping thread id : " + str(thread_id))
            continue

        for metric_type in metric_types:
            if metric_type in sys_metrics_skip:
                continue
            filtered_metric_df = metric_df[
                (metric_df['thread_id'] == thread_id) & (metric_df['metric_type'] == metric_type)].copy()
            if metric_type.endswith('_time_ms') or metric_type.endswith('_flt'):
                metric_type = metric_type + "_diff"
                filtered_metric_df['value'] = (filtered_metric_df['value'] -
                                               filtered_metric_df['value'].shift(1, fill_value=0))

            assert (len(filtered_metric_df['sys_id'].unique()) == 1)
            sys_id_parts = filtered_metric_df['sys_id'].unique()[0].split("_")
            if non_group_text == "" and sys_id_parts[config['part_indices']["scheduling_mode"]] != "TB":
                non_group_text = get_non_group_text(config, labels, non_group_types, sys_id_parts)
            original_group_id = get_group_id(config, group_by_types, sys_id_parts)
            if type == "cpu":
                group_id_suffix = get_group_id_suffix(metric_type)
                group_id = original_group_id + "_" + group_id_suffix
            else:
                group_id = original_group_id

            x_var_key = int(sys_id_parts[config['part_indices'][independent_var]])
            if x_var_key not in x_var_keys:
                x_var_keys[x_var_key] = []
            if group_id not in agg_metrics:
                agg_metrics[group_id] = []
                x_var_keys[x_var_key].append(group_id)

            if group_id not in agg_metrics:
                agg_metrics[group_id] = []

            filtered_metric_df['rel_time'] = filtered_metric_df['timestamp'].subtract(
                filtered_metric_df['timestamp'].min()).div(1_000)
            filtered_metric_df = filtered_metric_df[
                (filtered_metric_df['rel_time'] > config['time_lower']) & (
                        filtered_metric_df['rel_time'] <= config['time_upper'])]
            unit_scale = config['unit_scale'].get(metric_type, 1.0)
            avg_metric = filtered_metric_df.loc[:, target_var].mean() / unit_scale
            if config['normalize_res_usage'][metric_type_agg]:
                tp_val_idx = len(agg_metrics[group_id])
                # We divide by 10 to get per second and divide by 1_000_000 to get per million
                agg_tp_val_in_millions = tp_agg_metrics_dict[original_group_id][tp_val_idx] / 1_000_000
                normalized_metric = avg_metric / agg_tp_val_in_millions
                normalized_metric = normalized_metric / 10
                agg_metrics[group_id].append(normalized_metric)
            else:
                agg_metrics[group_id].append(avg_metric)

    x_var_keys = dict(sorted(x_var_keys.items()))
    sorted_agg_metrics = {}
    for x_var_key, group_ids in x_var_keys.items():
        for group_id in group_ids:
            filtered_data = reject_outliers(agg_metrics[group_id], 200)
            if len(filtered_data) < 5:
                raise Exception("Cannot find 5 data points within IQR")
            # print(group_id + ":" + str(len(filtered_data)))
            # print(filtered_data)
            sorted_agg_metrics[group_id] = filtered_data[:5]
    agg_metrics = sorted_agg_metrics

    for benchmark_name in benchmarks:
        plot_stacked_bar_graph_for_metric(config, labels, results_dir, agg_metrics, independent_var, metric_type_agg,
                                          non_group_text,
                                          derive_plot_title(config, labels, benchmark_name, metric_type_agg),
                                          sys_metrics_subdir + "/avg_" + get_filename_for_sysmetrics(
                                              metric_type_agg) + "_" + benchmark_name, exp_date_id, iter, metric_types,
                                          sci_labels=sci_labels, show_plot=True)


def get_group_id_suffix(metric_type):
    group_id_suffix = ""
    if metric_type == "u_time_ms_diff":
        group_id_suffix = "UserTime"
    elif metric_type == "s_time_ms_diff":
        group_id_suffix = "SystemTime"
    return group_id_suffix


def targeted_aggregate_metrics(config, labels, results_dir, targeted_metrics_subdir, metrics_group, csv_columns,
                               target_var, independent_var, exp_date_id,
                               column_dtypes, target_metric, group_by_types, non_group_types, use_regex_match=False,
                               target_iter=0, sci_labels=False):
    metric_df = pd.read_csv(config['data_dir'] + "/" + exp_date_id + "/" + metrics_group + ".csv", names=csv_columns,
                            dtype=column_dtypes)
    metric_df = metric_df[metric_df['metric_type'].str.contains(target_metric, regex=use_regex_match)]
    metric_df['query_num'] = metric_df['metric_type'].str.rsplit("_", n=3).str[1]
    metric_df['iter_idx'] = metric_df['sys_id'].str.split("_", n=6).str[5]
    thread_ids = metric_df['thread_id'].unique()
    metric_types = metric_df['metric_type'].unique()
    query_nums = metric_df['query_num'].unique()
    print(thread_ids)
    print(metric_types)
    sys_ids = metric_df['sys_id'].unique()
    benchmarks = []
    for sys_id in sys_ids:
        benchmark_name = sys_id.split("_")[1]
        if benchmark_name not in benchmarks:
            benchmarks.append(benchmark_name)

    agg_metrics = {}
    non_group_text = ""

    metric_type = target_metric.split("_")[-2] + "_" + target_metric.split("_")[-1]
    for benchmark_name in benchmarks:
        for thread_id in thread_ids:
            for query_num in query_nums:
                filtered_metric_df = metric_df[
                    (metric_df['thread_id'] == thread_id)].copy()
                sys_id_parts = filtered_metric_df['sys_id'].unique()[0].split("_")
                if metric_type.endswith('scheduledTime_gauge') or metric_type.endswith('scheduledCount_gauge'):
                    metric_type = re.sub(r"(scheduledCount|scheduledTime)", r"\1Diff", metric_type)
                    filtered_metric_df['value'] = (filtered_metric_df['value'] -
                                                   filtered_metric_df['value'].shift(1, fill_value=0))
                if metric_type not in agg_metrics:
                    agg_metrics[metric_type] = {}

                if independent_var == "num_queries":
                    filtered_metric_df = filtered_metric_df[(filtered_metric_df['query_num'] == query_num)]
                if target_iter > 0:
                    filtered_metric_df = filtered_metric_df[(filtered_metric_df['iter_idx'] == str(target_iter))]

                if not filtered_metric_df.empty:
                    assert (len(filtered_metric_df['sys_id'].unique()) == 1)

                if non_group_text == "" and sys_id_parts[config['part_indices']["scheduling_mode"]] != "TB":
                    non_group_text = get_non_group_text(config, labels, non_group_types, sys_id_parts)
                group_id = get_group_id(config, group_by_types, sys_id_parts) + "_" + query_num

                if group_id not in agg_metrics[metric_type]:
                    agg_metrics[metric_type][group_id] = []

                filtered_metric_df['rel_time'] = filtered_metric_df['timestamp'].subtract(
                    filtered_metric_df['timestamp'].min()).div(1_000)
                filtered_metric_df = filtered_metric_df[
                    (filtered_metric_df['rel_time'] > config['time_lower']) & (
                            filtered_metric_df['rel_time'] <= config['time_upper'])]
                avg_metric = filtered_metric_df.loc[:, target_var].mean()
                agg_metrics[metric_type][group_id].append(avg_metric)

        print(agg_metrics)
        for benchmark_name in benchmarks:
            if metric_type.endswith('scheduledTime_gauge') or metric_type.endswith('scheduledCount_gauge'):
                metric_type = re.sub(r"(scheduledCount|scheduledTime)", r"\1Diff", metric_type)

            filtered_agg_metrics = agg_metrics[metric_type]
            for derived_group_id in filtered_agg_metrics:
                print(
                    "Avg. " + metric_type + "(" + derived_group_id + "): " + '{0:.5f}'.format(
                        np.mean(filtered_agg_metrics[derived_group_id])))
            filtered_agg_metrics = dict(sorted(filtered_agg_metrics.items()))
            plot_bar_graph_for_target_metric(config, labels, results_dir, filtered_agg_metrics, independent_var,
                                             metric_type, non_group_text,
                                             derive_plot_title(config, labels, benchmark_name, metric_type),
                                             targeted_metrics_subdir + "/avg_" + get_filename(
                                                 metric_type) + "_" + benchmark_name, exp_date_id, target_iter,
                                             sci_labels=sci_labels, show_plot=True)


def plot_combined_metric(config, labels, target_metric_type, results_dir, data_df, x_var, y_var, target_metric,
                         plot_title, plot_filename,
                         exp_id, iter_num, sci_labels=False, show_plot=True):
    data_df["rel_time"] = data_df["rel_time"].round(0)
    data_df.set_index("rel_time", inplace=True)
    ts_group_by_fields = config['targeted_metrics']['group_by_types'].split(",")
    avg_data_point_gby_fields = ts_group_by_fields.copy()
    avg_data_point_gby_fields.append('rel_time')
    grouped_data = data_df.groupby(avg_data_point_gby_fields)
    agg_function = config['agg_function'][target_metric_type]
    if agg_function == "sum":
        intermediate_df = grouped_data[[y_var]].sum()
    elif agg_function == "mean":
        intermediate_df = grouped_data[[y_var]].mean()
    else:
        raise Exception("Unsupported agg function!")

    intermediate_df.reset_index(inplace=True)
    avg_over_time = config.get('avg_over_time', {target_metric_type: False})[target_metric_type]
    if avg_over_time:
        avg_time_interval = config['avg_over_time_config'].get('time_interval', 10)
        intermediate_df['rel_time_10'] = (intermediate_df['rel_time'] - 1) // avg_time_interval
        over_time_gby_fields = ts_group_by_fields.copy()
        over_time_gby_fields.append('rel_time_10')
        intermediate_df = intermediate_df.groupby(over_time_gby_fields)[[y_var]].mean()
        intermediate_df.reset_index(inplace=True)
        intermediate_df['rel_time'] = intermediate_df['rel_time_10'] * avg_time_interval

    intermediate_df.set_index("rel_time", inplace=True)
    plot_data = intermediate_df.groupby(ts_group_by_fields)[y_var]
    # plot_data = data_df.groupby(ts_group_by_fields)[y_var]
    agg_avg_data = {}
    x_ci = {}
    x = []
    for name, group_series in plot_data:
        x_data_zip, y_data_zip = zip(*group_series.items())
        if len(x) < len(list(x_data_zip)):
            x = list(x_data_zip)

        y_data = list(y_data_zip)
        sched_mode = name[0]
        priority_type = name[1]
        proc_mode = "QueueBasedSingle" if sched_mode == "TB" else "BlockBasedBatch"
        alloc_mode = "PreAllocate" if "Adaptive" in priority_type else "OnDemand"

        if len(name) > 2:
            # Temp. Hack. Figure out a more elegant way later
            metric_type_info = name[2]
            op_id = [s for s in metric_type_info.split("_") if s.isdigit()]
            metric_type = metric_type_info[:3].upper() + op_id[0]
            y_var_mode = proc_mode + "_" + sched_mode + "_" + priority_type + "_" + alloc_mode + "_" + metric_type
            # End. of temp. hack
        else:
            y_var_mode = proc_mode + "_" + sched_mode + "_" + priority_type + "_" + alloc_mode

        agg_avg_data[y_var_mode] = y_data
        x_ci[y_var_mode] = [0] * len(y_data)

    multiplier = 0
    fig_width = config.get('fig_width', {target_metric_type: 12})[target_metric_type]
    fig_height = config.get('fig_height', {target_metric_type: 8})[target_metric_type]
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    plot_font_size = config['plot_font_size']
    plt.rc('font', size=plot_font_size)
    plot_line_width = config['plot_line_width']
    line_alpha = config.get('line_alpha', 0.8)
    if config['use_appearance_order']:
        appearance_order = config['appearance_order'].split(',')
        num_y_vars = len(appearance_order)
        # width = 0.4 / num_y_vars  # the width of interleaving
        # multiplier = -1 * (num_y_vars // 2)
        max_measurement = 0
        for y_var_mode in appearance_order:
            measurement = agg_avg_data[y_var_mode]
            bar_heights = [i + j for i, j in zip(measurement, x_ci[y_var_mode])]
            if max_measurement < max(bar_heights):
                max_measurement = max(bar_heights)
        for y_var_mode in appearance_order:
            measurement = agg_avg_data[y_var_mode]
            y_var_label = derive_config_label(config, labels, y_var_mode)
            bar_color = derive_bar_color(config, labels, y_var_mode)
            line_marker = "None"
            if config['use_line_markers']:
                line_marker = derive_line_marker(labels, y_var_mode)
            line_style = derive_line_style(config, labels, y_var_mode)
            # offset = width * multiplier
            ax.plot(x, measurement, label=y_var_label, color=bar_color, linewidth=plot_line_width, linestyle=line_style,
                    marker=line_marker, markersize=plot_line_width * 3, alpha=line_alpha)
            if config['show_err_bars']:
                err_label = "" if multiplier < num_y_vars - 1 else "95% Confidence"
                curr_yerr = x_ci[y_var_mode]
                ax.errorbar(x, measurement, yerr=curr_yerr, capsize=plot_line_width, capthick=plot_line_width / 2,
                            color='k',
                            label=err_label, linestyle='None')
            # auto_label_line(ax, measurement, x, max_measurement, plot_font_size, y_scale)
            print(y_var_label + ": [" + " ".join(str(round(x, 2)) for x in measurement) + "]")
            multiplier += 1
    else:
        plot_data.plot(marker=".", ax=ax)

    y_scale = config['y_scale'].get(target_metric_type, "linear")
    ax.set_yscale(y_scale)
    x_label = get_label(config, labels, x_var)
    y_label = get_label(config, labels, target_metric_type + '_timeseries')

    if sci_labels:
        plt.ticklabel_format(axis='y', style='sci', scilimits=(6, 6))
    # if (y_scale == "linear"):
    #     plt.ylim(bottom=0)
    y_max = config['y_max'].get(target_metric_type, 0)
    if y_max > 0:
        plt.ylim(top=y_max)
    y_min = config['y_min'].get(target_metric_type, -1)
    if y_min >= 0:
        plt.ylim(bottom=y_min)

    ax.set_xlabel(x_label, fontsize=plot_font_size)
    ax.set_ylabel(y_label, fontsize=plot_font_size)
    ax.set_yscale(y_scale)
    if plot_title:
        ax.set_title(plot_title, fontsize=plot_font_size)
    ax.tick_params(width=plot_line_width, size=plot_line_width * 3, labelsize=plot_font_size)

    plot_file_type = config['plot_file_type']
    show_legend = config['show_legend']
    if show_legend and not config.get("skip_legend", True):
        legend_coords_x_var = config['legend_coords'][x_var]
        bbox_anchor_x = legend_coords_x_var['x']
        bbox_anchor_y = legend_coords_x_var['y']
        legend_prop_size = config['legend_prop_size']
        ax.legend(loc='upper left', bbox_to_anchor=(bbox_anchor_x, bbox_anchor_y), prop={"size": legend_prop_size})

    if sci_labels:
        ax.ticklabel_format(axis='y', style='sci', scilimits=(6, 6))

    fig.tight_layout()
    if config['exp_id_based_names']:
        plot_filepath = results_dir + "/" + plot_filename + "_line_" + exp_id + "_iter" + iter_num
    else:
        filter_types = config['targeted_metrics']['filter_types'].split(",")
        assert (len(filter_types) == 1)
        filtered_vals = None
        if filter_types:
            for filter_type in filter_types:
                filtered_vals = config['targeted_metrics'][filter_type].split(",")
        assert (filtered_vals is not None and len(filtered_vals) == 1)
        benchname = config['current_bench_name']
        plot_type_id = filter_types[0] + "_" + filtered_vals[0] + "_" + benchname
        plot_filename_suffix = config['plot_filename_suffix']
        plot_filepath = results_dir + "/" + plot_filename + "_line_" + plot_type_id + "_" + plot_filename_suffix
    plt.savefig(plot_filepath + "." + plot_file_type)
    if show_plot:
        plt.show()
    plt.close()
    if not show_legend and not config.get("skip_legend", True):
        label_params = ax.get_legend_handles_labels()
        ncols_legend = config['ncols_legend']
        figl, axl = plt.subplots(figsize=(2 * ncols_legend, 1))
        axl.axis(False)
        legend_prop_size = config['legend_prop_size']
        axl.legend(*label_params, loc='center', bbox_to_anchor=(0.5, 0.5), prop={"size": legend_prop_size},
                   ncol=ncols_legend, frameon=False, columnspacing=4)
        figl.savefig(plot_filepath + "_legend_only." + plot_file_type, bbox_inches='tight')
        if show_plot:
            figl.show()
        plt.close(figl)


def process_latency_vs_tput_graph(config, labels, results_dir, csv_columns_tput, csv_columns_latency,
                                  target_filename_tput, target_filename_latency, independent_var,
                                  target_var_filter_tput, target_var_filter_latency, exp_id, column_dtypes_tput,
                                  column_dtypes_latency,
                                  group_by_types, non_group_types, sci_labels=False, iter_num="1",
                                  target_metric="", graph_type="bar", y_scale="linear"):
    if target_metric == "":
        target_metric = target_filename_latency
    tput_metric_df = pd.read_csv(config['data_dir'] + "/" + exp_id + "/" + target_filename_tput + ".csv",
                                 names=csv_columns_tput,
                                 dtype=column_dtypes_tput)
    tput_thread_ids = tput_metric_df['thread_id'].unique()
    print(tput_thread_ids)
    sys_ids = tput_metric_df['sys_id'].unique()
    benchmarks = []
    for sys_id in sys_ids:
        benchmark_name = sys_id.split("_")[1]
        if benchmark_name not in benchmarks:
            benchmarks.append(benchmark_name)

    latency_metric_df = pd.read_csv(config['data_dir'] + "/" + exp_id + "/" + target_filename_latency + ".csv",
                                    names=csv_columns_latency,
                                    dtype=column_dtypes_latency)
    latency_thread_ids = latency_metric_df['thread_id'].unique()
    assert (latency_thread_ids.all() == tput_thread_ids.all())
    assert len(benchmarks) == 1
    for benchmark_name in benchmarks:
        agg_metrics_tput = {}
        non_group_text = ""
        for thread_id in tput_thread_ids:
            filtered_metric_df = tput_metric_df[tput_metric_df['thread_id'] == thread_id].copy()

            filtered_metric_df = filtered_metric_df[
                filtered_metric_df['metric_type'].str.contains(target_var_filter_tput, regex=True)]
            if filtered_metric_df.empty:
                continue

            assert (len(filtered_metric_df['sys_id'].unique()) == 1)
            sys_id_parts = filtered_metric_df['sys_id'].unique()[0].split("_")

            if non_group_text == "" and sys_id_parts[config['part_indices']["scheduling_mode"]] != "TB":
                non_group_text = get_non_group_text(config, labels, non_group_types, sys_id_parts)
            group_id = get_group_id(config, group_by_types, sys_id_parts)

            if group_id not in agg_metrics_tput:
                agg_metrics_tput[group_id] = []

            target_var = config['target_vars'][target_filename_tput]
            filtered_metric_df['rel_time'] = filtered_metric_df['timestamp'].subtract(
                filtered_metric_df['timestamp'].min()).div(1_000)
            filtered_metric_df = filtered_metric_df[
                (filtered_metric_df['rel_time'] > config['time_lower']) & (
                        filtered_metric_df['rel_time'] <= config['time_upper'])]

            agg_metric_df = filtered_metric_df.groupby(["thread_id", "timestamp", "rel_time"]).sum()
            agg_metric = agg_metric_df.loc[:, target_var].mean() / 1_000_000.0
            agg_metrics_tput[group_id].append(agg_metric)

    for benchmark_name in benchmarks:
        agg_metrics_latency = {}
        non_group_text = ""
        for thread_id in latency_thread_ids:
            filtered_metric_df = latency_metric_df[latency_metric_df['thread_id'] == thread_id].copy()

            filtered_metric_df = filtered_metric_df[
                filtered_metric_df['metric_type'].str.contains(target_var_filter_latency, regex=True)]
            if filtered_metric_df.empty:
                continue

            assert (len(filtered_metric_df['sys_id'].unique()) == 1)
            sys_id_parts = filtered_metric_df['sys_id'].unique()[0].split("_")

            if non_group_text == "" and sys_id_parts[config['part_indices']["scheduling_mode"]] != "TB":
                non_group_text = get_non_group_text(config, labels, non_group_types, sys_id_parts)
            group_id = get_group_id(config, group_by_types, sys_id_parts)

            if group_id not in agg_metrics_latency:
                agg_metrics_latency[group_id] = []

            target_var = config['target_vars'][target_filename_latency]

            filtered_metric_df['rel_time'] = filtered_metric_df['timestamp'].subtract(
                filtered_metric_df['timestamp'].min()).div(1_000)
            filtered_metric_df = filtered_metric_df[
                (filtered_metric_df['rel_time'] > config['time_lower']) & (
                        filtered_metric_df['rel_time'] <= config['time_upper'])]

            agg_metric_df = filtered_metric_df.groupby(["thread_id", "timestamp", "rel_time"]).mean()
            agg_metric = agg_metric_df.loc[:, target_var].mean() / 1.0
            agg_metrics_latency[group_id].append(agg_metric)

    agg_metrics_merged = {}
    for sys_id, tput_values in agg_metrics_tput.items():
        agg_points = [(a, b) for a, b in zip(tput_values, agg_metrics_latency[sys_id])]
        agg_metrics_merged[sys_id] = agg_points

    if graph_type == "bar":
        plot_bar_graph_for_latency_vs_tput(config, labels, results_dir, agg_metrics_merged, independent_var,
                                           target_metric,
                                           non_group_text,
                                           derive_plot_title(config, labels, benchmark_name, target_metric),
                                           "latency_vs_tput_" + benchmark_name,
                                           exp_id, iter_num,
                                           sci_labels=sci_labels, show_plot=True, y_scale=y_scale)
    else:
        plot_line_graph_for_latency_vs_tput(config, labels, results_dir, agg_metrics_merged, independent_var,
                                            target_metric,
                                            non_group_text,
                                            derive_plot_title(config, labels, benchmark_name, target_metric),
                                            "latency_vs_tput_" + benchmark_name,
                                            exp_id, iter_num,
                                            sci_labels=sci_labels, show_plot=True, y_scale=y_scale)
    return agg_metrics_tput


def get_op_id(op_name):
    match op_name:
        case "lrb_src":
            return "01"
        case "typeFilter":
            return "02"
        case "project":
            return "03"
        case "travelFilter":
            return "04"
        case "accidentWindow":
            return "05"
        case "countWindow":
            return "06"
        case "speedWindow":
            return "07"
        case "countAndSpeedJoin":
            # Toll calculation
            return "08"
        case "tollAndAccidentCoGroup":
            return "09"
        case "lrb_sink":
            return "10"
        case _:
            raise KeyError(op_name)


def process_agg_metrics_no_graphs_specialized(config, data_dir, results_dir, csv_columns, target_filename,
                                              target_var_filter, target_var_filter2,
                                              experiment_ids,
                                              column_dtypes):
    experiment_id_li = experiment_ids.split("|")
    for experiment_id in experiment_id_li:
        metric_df = pd.read_csv(data_dir + "/" + experiment_id + "/" + target_filename + ".csv", names=csv_columns,
                                dtype=column_dtypes)
        thread_ids = metric_df['thread_id'].unique()
        print(thread_ids)

        sched_time_metric_df = metric_df[
            metric_df['metric_type'].str.contains(target_var_filter, regex=True)].copy()

        sched_time_metric_df['input_rate'] = sched_time_metric_df['sys_id'].str.split("_", expand=True)[
            config["part_indices"]["input_rate"]]
        sched_time_metric_df['num_queries'] = sched_time_metric_df['sys_id'].str.split("_", expand=True)[
            config["part_indices"]["num_queries"]]
        sched_time_metric_df['sched_mode'] = sched_time_metric_df['sys_id'].str.split("_", expand=True)[
            config["part_indices"]["scheduling_mode"]]
        sched_time_metric_df['sched_policy'] = sched_time_metric_df['sys_id'].str.split("_", expand=True)[
            config["part_indices"]["scheduling_policy"]]
        sched_time_metric_df['query_id'] = \
            sched_time_metric_df['metric_type'].str.partition("_scheduledTime_gauge", expand=True)[0].str[-1]
        sched_time_metric_df['op_name'] = \
            sched_time_metric_df['metric_type'].str.partition("_scheduledTime_gauge", expand=True)[0].str.rpartition(
                "_")[0]
        sched_time_metric_df['op_order'] = \
            sched_time_metric_df['op_name'].apply(lambda op_name: get_op_id(op_name))

        sched_time_metric_df['shifted_val'] = sched_time_metric_df.groupby(['thread_id', 'metric_type'])[
            'value'].shift(1, fill_value=0)
        sched_time_metric_df['sched_time_during_interval'] = sched_time_metric_df['value'] - sched_time_metric_df[
            'shifted_val']

        # Get sched count stats as well. Derive rel_time. Join by timestamp and query id and op name. Divide to get execution time per scheduled run.
        sched_count_metric_df = metric_df[
            metric_df['metric_type'].str.contains(target_var_filter2, regex=True)].copy()
        sched_count_metric_df['query_id'] = \
            sched_count_metric_df['metric_type'].str.partition("_scheduledCount_gauge", expand=True)[0].str[-1]
        sched_count_metric_df['op_name'] = \
            sched_count_metric_df['metric_type'].str.partition("_scheduledCount_gauge", expand=True)[0].str.rpartition(
                "_")[0]

        sched_count_metric_df['shifted_val'] = sched_count_metric_df.groupby(['thread_id', 'metric_type'])[
            'value'].shift(1, fill_value=0)
        sched_count_metric_df['sched_count_during_interval'] = sched_count_metric_df['value'] - sched_count_metric_df[
            'shifted_val']

        merged_df = pd.merge(sched_time_metric_df[
                                 ['thread_id', 'timestamp', 'num_queries', 'sched_mode', 'sched_policy', 'query_id',
                                  'op_name', 'op_order', 'sched_time_during_interval']],
                             sched_count_metric_df[
                                 ['thread_id', 'timestamp', 'query_id', 'op_name', 'sched_count_during_interval']],
                             on=['thread_id', 'timestamp', 'query_id', 'op_name'])
        merged_df['avg_sched_time'] = merged_df['sched_time_during_interval'] / merged_df['sched_count_during_interval']
        merged_df['rel_time'] = merged_df['timestamp'].subtract(sched_time_metric_df['timestamp'].min())
        merged_df['sched_time_per_sec'] = merged_df['sched_time_during_interval'] / 10.0
        merged_df['sched_count_per_sec'] = merged_df['sched_count_during_interval'] / 10.0
        per_query_agg_df = \
            merged_df.groupby(['num_queries', 'sched_mode', 'sched_policy', 'query_id', 'op_order', 'op_name'])[
                ['avg_sched_time', 'sched_time_per_sec', 'sched_count_during_interval']].mean().round(2)
        per_query_agg_df.reset_index(inplace=True)
        agg_df = merged_df.groupby(['sched_mode', 'sched_policy', 'num_queries', 'op_order', 'op_name'])[
            ['avg_sched_time', 'sched_time_per_sec', 'sched_count_per_sec']].mean().round(
            {'avg_sched_time': 2, 'sched_time_per_sec': 2, 'sched_count_per_sec': 0})
        agg_df.reset_index(inplace=True)

        os.makedirs(results_dir + "/tabular", exist_ok=True)
        per_query_agg_df.to_csv(results_dir + "/tabular/per_query_op_exec_time_" + experiment_id + ".csv", index=False)
        agg_df.to_csv(results_dir + "/tabular/op_exec_time_" + experiment_id + ".csv", index=False)
        print("Wrote agg scheduling stats to file: " + results_dir + "/tabular/op_exec_time_" + experiment_id + ".csv")


def process_sched_overhead_no_graphs(config, data_dir, results_dir, exp_date_id):
    sched_stat_df = pd.read_csv(data_dir + "/logs/sched_overhead_stats.csv",
                                names=['sys_id', 'match_cnt', 'type', 'total_ms', 'total_cnt', 'avg_us'])
    sched_stat_df['bench'] = sched_stat_df['sys_id'].str.split("_", expand=True)[1]
    sched_stat_df['algorithm'] = sched_stat_df['sys_id'].str.split("_", expand=True)[7]
    agg_sched_stat_df = sched_stat_df.groupby(['bench', 'algorithm'])[['total_ms', 'total_cnt', 'avg_us']].mean().round(
        2)
    agg_sched_stat_df.reset_index(inplace=True)
    print("\nScheduling Stats\n---------")
    print(agg_sched_stat_df)

    tot_agg_df = sched_stat_df.groupby('bench')[['total_ms', 'total_cnt', 'avg_us']].mean().round(2)
    tot_agg_df.reset_index(inplace=True)
    print("\nAgg. Scheduling Stats\n---------")
    print(tot_agg_df)

    os.makedirs(results_dir + "/tabular", exist_ok=True)
    agg_sched_stat_df.to_csv(results_dir + "/tabular/sched_overhead_stats_" + exp_date_id + ".csv", index=False)
    tot_agg_df.to_csv(results_dir + "/tabular/agg_sched_overhead_stats_" + exp_date_id + ".csv", index=False)


def process_prior_overhead_no_graphs(config, data_dir, results_dir, exp_date_id):
    priority_stat_df = pd.read_csv(data_dir + "/logs/prior_overhead_stats.csv",
                                   names=['sys_id', 'match_cnt', 'type', 'total_ms', 'total_cnt', 'avg_us'])
    priority_stat_df['bench'] = priority_stat_df['sys_id'].str.split("_", expand=True)[1]
    priority_stat_df['algorithm'] = priority_stat_df['sys_id'].str.split("_", expand=True)[7]
    agg_priority_stat_df = priority_stat_df.groupby(['bench', 'algorithm'])[
        ['total_ms', 'total_cnt', 'avg_us']].mean().round(
        2)
    agg_priority_stat_df.reset_index(inplace=True)
    agg_priority_stat_df.to_csv(results_dir + "/tabular/prior_overhead_stats_" + exp_date_id + ".csv")
    print("\nPriority stats\n---------")
    print(agg_priority_stat_df)

    tot_agg_df = priority_stat_df.groupby('bench')[['total_ms', 'total_cnt', 'avg_us']].mean().round(2)
    tot_agg_df.reset_index(inplace=True)
    print("\nAgg. Priority Stats\n---------")
    print(tot_agg_df)

    os.makedirs(results_dir + "/tabular", exist_ok=True)
    agg_priority_stat_df.to_csv(results_dir + "/tabular/priority_overhead_stats_" + exp_date_id + ".csv", index=False)
    tot_agg_df.to_csv(results_dir + "/tabular/agg_priority_overhead_stats_" + exp_date_id + ".csv", index=False)
