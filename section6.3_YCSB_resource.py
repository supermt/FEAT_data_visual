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

        tuner = log_dir.split(os.path.sep)[-5]
        workload = log_dir.split(os.path.sep)[-4]
        device = StdoutReader(stdout_file).device
        disk_bytes = pid_stat_df["disk_write_bytes"]
        disk_bytes -= disk_bytes.shift(1)
        disk_bytes /= 1000000
        average_mbps = disk_bytes.mean()

        row = [tuner, device, workload, cpu_utils, round(average_mbps, 2)]
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

        disk_bytes = pid_stat_df["disk_write_bytes"]
        disk_bytes -= disk_bytes.shift(1)
        disk_bytes /= 1000000
        average_mbps = disk_bytes.mean()

        tuner = log_dir.split(os.path.sep)[-5]
        workload = log_dir.split(os.path.sep)[-4]
        device = StdoutReader(stdout_file).device

        row = [tuner, device, workload, cpu_utils, round(average_mbps, 2)]
        rows.append(row)

    return rows


if __name__ == '__main__':

    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    base_log_prefix = "Eurosys/pm_server_ycsb_60+60/"

    tuners = ["SILK-SILK-D", "SILK", "tuned"]

    result_list = []

    for hyper_para in tuners:
        log_prefix = base_log_prefix + hyper_para
        rows = extract_tuned_resource_utilization(log_prefix)

        result_list.extend(rows)

    FEAT_log_prefix = base_log_prefix + "FEAT"
    rows = extract_change_frequency_and_resource_utilization(FEAT_log_prefix)
    result_list.extend(rows)

    print(result_list)

    result_df = pd.DataFrame(result_list,
                             columns=["Tuner", "Device", "workload", "cpu utils", "avg bandwidth"])

    result_df.to_csv("csv_results/ycsb_resource_utilization.csv", index=False, sep=" ")
