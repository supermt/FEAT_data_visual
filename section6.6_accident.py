import gzip
import os.path

from utils.traversal import get_log_dirs, get_log_and_std_files
import pandas as pd

from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_dirs, get_log_and_std_files

STALL_REASON = ["memtable", "level0", "pending_compaction_bytes"]

color_map = {"PM": "rgb(68,114,196)", "NVMeSSD": "rgb(237,125,49)", "SATASSD": "rgb(165,165,165)",
             "SATAHDD": "rgb(255,192,0)"}


def extract_change_frequency_and_resource_utilization(input_dir):
    input_dirs = get_log_dirs(input_dir)

    rows = []
    for log_dir in input_dirs:
        stdout_file, LOG_file, report_csv, pid_stat_file, iostat_text = get_log_and_std_files(log_dir,
                                                                                              with_stat_csv=True,
                                                                                              splitted_iostat=True,
                                                                                              multi_tasks=True)
        report_src = report_csv[0]

        report_df = pd.read_csv(report_src)

        shifted_report_df = report_df.shift(1)
        changes = shifted_report_df - report_df
        change_count = 0
        for row in changes.iloc:
            if row["batch_size"] != 0 or row["thread_num"] != 0:
                change_count += 1

        pid_stat_df = pd.read_csv(pid_stat_file)

        join_df = pid_stat_df.set_index('secs').join(report_df.set_index("secs_elapsed")).dropna()
        cpu_utils = join_df["cpu_percent"] / join_df["thread_num"]
        cpu_utils = round(cpu_utils.mean(), 2)

        if ".gz" in iostat_text:
            iostat_lines = gzip.open(iostat_text, "r").readlines()
            iostat_lines = [x.decode('utf-8') for x in iostat_lines]
        else:
            iostat_lines = open(iostat_text, "r", encoding="utf-8").readlines()
        IOSTAT = []
        IOSTAT_COLUMN_NAMES = "Device             tps    MB_read/s    MB_wrtn/s    MB_read    MB_wrtn"
        IOSTAT_COLUMN = IOSTAT_COLUMN_NAMES.split()

        for line in iostat_lines:
            if "nvme" in line:
                IOSTAT.append(line.split())
        IOSTAT = pd.DataFrame(IOSTAT, columns=IOSTAT_COLUMN)
        MBPS = IOSTAT["MB_wrtn/s"].astype(float)
        average_mbps = MBPS.mean()
        hyper_para_value = log_dir.split(os.path.sep)[-4]

        duration = StdoutReader(stdout_file).stall_duration.split()[0].split(":")
        duration = float(duration[2]) + int(duration[1]) * 60 + int(duration[0]) * 3600

        row = [hyper_para_value, cpu_utils, change_count, round(average_mbps, 2), duration]
        rows.append(row)

    return rows


def extract_tuned_resource_utilization(tuned_group_log):
    input_dirs = get_log_dirs(tuned_group_log)

    rows = []
    for log_dir in input_dirs:
        stdout_file, LOG_file, report_csv, pid_stat_file, iostat_text = get_log_and_std_files(log_dir,
                                                                                              with_stat_csv=True,
                                                                                              splitted_iostat=True,
                                                                                              multi_tasks=True)
        report_src = report_csv[0]

        report_df = pd.read_csv(report_src)

        pid_stat_df = pd.read_csv(pid_stat_file)

        cpu_utils = pid_stat_df["cpu_percent"] / int(StdoutReader(stdout_file).cpu_count.replace("CPU", ""))
        cpu_utils = round(cpu_utils.mean(), 2)

        if ".gz" in iostat_text:
            iostat_lines = gzip.open(iostat_text, "r").readlines()
            iostat_lines = [x.decode('utf-8') for x in iostat_lines]
        else:
            iostat_lines = open(iostat_text, "r", encoding="utf-8").readlines()
        IOSTAT = []
        IOSTAT_COLUMN_NAMES = "Device             tps    MB_read/s    MB_wrtn/s    MB_read    MB_wrtn"
        IOSTAT_COLUMN = IOSTAT_COLUMN_NAMES.split()

        for line in iostat_lines:
            if "nvme" in line:
                IOSTAT.append(line.split())
        IOSTAT = pd.DataFrame(IOSTAT, columns=IOSTAT_COLUMN)
        MBPS = IOSTAT["MB_wrtn/s"].astype(float)
        average_mbps = MBPS.mean()
        hyper_para_value = log_dir.split(os.path.sep)[-4]

        duration = StdoutReader(stdout_file).stall_duration.split()[0].split(":")
        duration = float(duration[2]) + int(duration[1]) * 60 + int(duration[0]) * 3600

        row = [hyper_para_value, cpu_utils, round(average_mbps, 2), duration]

    return row


def get_plot_dict(log_dir):
    start_time = 500
    end_time = 1500
    report_dict = {}
    dirs = get_log_dirs(log_dir)

    for log_dir in dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)

        basic_info = StdoutReader(stdout_file)
        # devices.append(basic_info.device)
        qps_file = ""
        for csv_name in report_csv:
            if "_0_" in csv_name:
                qps_file = csv_name
                break
        qps_df = pd.read_csv(report_csv[0])
        qps_df = qps_df[qps_df["secs_elapsed"] < end_time]
        time_gap = qps_df["secs_elapsed"] - qps_df["secs_elapsed"].shift(1).fillna(0)
        qps_df["interval_qps"] /= (time_gap * 1000)
        avg_speed = basic_info.benchmark_results["mixgraph"][1].replace(" ops/sec", "")
        # print(avg_speed)
        qps_df["avg_qps"] = int(avg_speed) / 1000
        report_dict[basic_info.device] = qps_df

    return report_dict


if __name__ == '__main__':
    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    import matplotlib as mpl
    import matplotlib.pyplot as plt

    mpl.rcParams['figure.figsize'] = (16, 7)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16.5
    mpl.rcParams['font.family'] = "Arial"
    mpl.rcParams['lines.markersize'] = 10
    mpl.rcParams["legend.loc"] = "lower center"

    # group[device]["secs_elapsed"], group[device]["interval_qps"], "r"

    # default_changes["NVMeSSD"]["interval_qps"]
    import numpy as np

    devices = ["NVMeSSD"]

    fig, axes = plt.subplots(len(devices) + 1, 2, sharey='all', sharex='all')
    mix_graph_dir = "Eurosys/section6_accidents/mix_graph/"

    groups = {"FEAT": None, "tuned": None}

    end_time = 1500

    for group in groups:
        groups[group] = get_plot_dict(mix_graph_dir + group)

    sinex = list(np.arange(0, end_time, 0.5))
    a = 1000
    b = -7.3e-08 * 1000 * 1000  # count by us
    d = 45000
    put_ratio = 0.14
    average_entry_size = 512

    throughput = [put_ratio * (a * np.sin(b * x) + d) for x in sinex]
    print(throughput)
    column = 0
    for group in groups:
        row = 0
        for device in devices:
            request_speed = axes[row, column].twinx()
            request_speed.plot(sinex, throughput, "r--", alpha=0.5)
            # request_speed.set_yticks([])
            qps_df = groups[group][device]
            axes[row, column].plot(qps_df["secs_elapsed"], qps_df["interval_qps"])
            row += 1
        column += 1
    fig.show()
