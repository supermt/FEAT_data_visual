# here is an example from online-ml/river
import os

import utils.stdoutreader
from utils import stdoutreader
from utils.stdoutreader import *
from utils.traversal import get_log_and_std_files, get_log_dirs
import gzip

import pandas as pd

IOSTAT_COLUMN_NAMES = "Device             tps    MB_read/s    MB_wrtn/s    MB_read    MB_wrtn"
IOSTAT_COLUMN = IOSTAT_COLUMN_NAMES.split()

PM_device_mapper = {
    "SATA HDD": "sdc",
    "SATA SSD": "sda",
    "NVMe SSD": "nvme0n1",
    "PM": "pmem1"
}


def normalize_cpu_utils(pid_stat_file):
    pid_stat_csv = pd.read_csv(pid_stat_file)
    # cpu_num = int(pid_stat_file.split(os.sep)[-3].replace("CPU", ""))
    cpu_utils = pid_stat_csv["cpu_percent"].astype(float)
    normalized_cpu = cpu_utils.mean()
    return normalized_cpu


def normalized_disk_utils(iostat_text, device):
    if ".gz" in iostat_text:
        iostat_lines = gzip.open(iostat_text, "r").readlines()
        iostat_lines = [x.decode('utf-8') for x in iostat_lines]
    else:
        iostat_lines = open(iostat_text, "r", encoding="utf-8").readlines()
    IOSTAT = []
    for line in iostat_lines:
        if PM_device_mapper[device] in line:
            IOSTAT.append(line.split())
    IOSTAT = pd.DataFrame(IOSTAT, columns=IOSTAT_COLUMN)
    MBPS = IOSTAT["MB_wrtn/s"].astype(float)
    avg_disk_utils = MBPS.mean()
    return avg_disk_utils


if __name__ == '__main__':

    groups = ['auto-tuned', 'FEAT', 'SILK-P', 'SILK-O']
    base_dir_prefix = "../FAST/section6.3_fillrandom/RocksDB7.56/"
    suffixs = [""]
    devices = ["PM", "NVMe SSD", "SATA SSD", "SATA HDD"]

    rows = []
    for group in groups:
        for suffix in suffixs:
            log_dir_prefix = base_dir_prefix + suffix + "/" + group
            print(log_dir_prefix)
            dirs = get_log_dirs(log_dir_prefix)
            for log_dir in dirs:
                print(log_dir)
                stdout_file, LOG_file, report_csv, stat_csv, io_stat = get_log_and_std_files(log_dir,
                                                                                             with_stat_csv=True,
                                                                                             splitted_iostat=True)
                std_info = stdoutreader.StdoutReader(stdout_file)
                device = utils.stdoutreader.format_device(std_info.device)
                cpu_utils = normalize_cpu_utils(stat_csv)
                disk_utils = normalized_disk_utils(io_stat, device)
                row = [group, device, cpu_utils, disk_utils]
                rows.append(row)
    columns = ["group", "device", "cpu %", "disk %"]
    result_pd = pd.DataFrame(rows, columns=columns)
    result_pd.to_csv("../csv_results/fillrandom/756/disk_utils.csv", sep="\t", index=False)

    group_list = list(result_pd.groupby("group", as_index=False))

    average_and_std_rows = []
    metrics = ["qps", "stall secs", "p99", "p99.99"]
    metrics_avg = {x: x + " avg" for x in metrics}
    metrics_std = {x: x + " std" for x in metrics}

    for group in group_list:
        avg_df = group[1].groupby("device", as_index=False).mean().round(2)
        std_df = group[1].groupby("device", as_index=False).std().round(2)
        avg_df.rename(columns=metrics_avg, inplace=True)
        std_df.rename(columns=metrics_std, inplace=True)
        merged_df = avg_df.merge(std_df, on="device")
        merged_df["group"] = group[0]
        average_and_std_rows.append(merged_df)

    average_and_std_df = pd.concat(average_and_std_rows, axis=0, ignore_index=True)
    print(average_and_std_df)

    # average_and_std_df.to_csv("../csv_results/fillrandom/756/tail_latency.csv", sep="\t", index=False)
