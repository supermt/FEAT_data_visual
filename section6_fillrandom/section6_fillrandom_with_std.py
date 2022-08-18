# here is an example from online-ml/river
import utils.stdoutreader
from utils import stdoutreader
from utils.stdoutreader import *
from utils.traversal import get_log_and_std_files, get_log_dirs

import pandas as pd

if __name__ == '__main__':

    groups = ['default', 'SILK', 'tuned', "FEAT", 'FEA', 'TEA']
    base_dir_prefix = "../FAST/section6.3_fillrandom/temp/"
    suffixs = ["1", "2", "3"]
    devices = ["PM", "NVMe SSD", "SATA SSD", "SATA HDD"]

    rows = []
    for group in groups:
        for suffix in suffixs:
            log_dir_prefix = base_dir_prefix + "PM_fillrandom" + suffix + "/" + group
            print(log_dir_prefix)
            dirs = get_log_dirs(log_dir_prefix)
            for log_dir in dirs:
                stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)
                std_info = stdoutreader.StdoutReader(stdout_file)
                cpu = std_info.cpu_count
                device = utils.stdoutreader.format_device(std_info.device)
                fillrandom_speed = int(std_info.get_benchmark_ops("fillrandom"))
                stall_duration = float(std_info.stall_duration_sec)
                # p99 = float(std_info.fillrandom_hist["P99"])
                # p9999 = float(std_info.fillrandom_hist["P99.99"])
                row = [group, device, fillrandom_speed, stall_duration]
                # row = [group, device, fillrandom_speed, stall_duration, p99, p9999]
                rows.append(row)
    columns = ["group", "device", "qps", "stall secs"]
    # columns = ["group", "device", "qps", "stall secs", "p99", "p99.99"]
    result_pd = pd.DataFrame(rows, columns=columns)

    group_list = list(result_pd.groupby("group", as_index=False))

    average_and_std_rows = []
    metrics = ["qps", "stall secs"]
    # metrics = ["qps", "stall secs", "p99", "p99.99"]
    # metrics_avg = {x: x + " avg" for x in metrics}
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

    average_and_std_df.to_csv("../csv_results/fillrandom/all_with_std.csv", sep="\t", index=False)
