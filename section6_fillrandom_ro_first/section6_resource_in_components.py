# here is an example from online-ml/river
import gzip
import os

import utils.stdoutreader
from utils import stdoutreader
from utils.stdoutreader import *
from utils.traversal import get_log_and_std_files, get_log_dirs

import pandas as pd

PM_device_mapper = {
    "SATAHDD": "sdc",
    "SATASSD": "sda",
    "NVMeSSD": "nvme0n1",
    "PM": "pmem1"
}
IOSTAT_COLUMN_NAMES = "Device             tps    MB_read/s    MB_wrtn/s    MB_read    MB_wrtn"
IOSTAT_COLUMN = IOSTAT_COLUMN_NAMES.split()


def get_tail_latency(std_info, workload):
    op_map = {
        "a": ["update", "read"],
        "b": ["update", "read"],
        "c": ["read"],
        "d": ["insert", "read"],
        "e": ["insert", "seek"],
        "f": ["read", "update"],
    }
    hist_map = {
        "update": std_info.updaterandom_hist,
        "insert": std_info.fillrandom_hist,
        "read": std_info.readrandom_hist,
        "seek": std_info.seek_hist,
        # "a": [std_info.updaterandom_hist, std_info.readrandom_hist],
        # "b": [std_info.updaterandom_hist, std_info.readrandom_hist],
        # "c": [std_info.readrandom_hist],
        # "d": [std_info.fillrandom_hist, std_info.readrandom_hist],
        # "e": [std_info.fillrandom_hist, std_info.seek_hist],
        # "f": [std_info.fillrandom_hist, std_info.updaterandom_hist],
    }

    result_rows = []
    for op in op_map[workload]:
        result_row = [op, float(hist_map[op]["P99"]), float(hist_map[op]["P99.99"])]
        result_rows.append(result_row)
        # result_row = [workload_request_type[workload].replace("_hist", "").replace("random")]
    return result_rows


if __name__ == '__main__':

    groups = ['FEAT', 'TEA', "FEA"]
    base_dir_prefix = "../FAST/section6.1_FEAT_usage/"
    suffixs = [""]
    devices = ["PM", "NVMe SSD", "SATA SSD", "SATA HDD"]

    rows = []
    for group in groups:
        for suffix in suffixs:
            log_dir_prefix = base_dir_prefix + group + "/" + suffix
            print(log_dir_prefix)
            dirs = get_log_dirs(log_dir_prefix)
            for log_dir in dirs:
                workload = log_dir.split(os.sep)[-4]
                if "load" in log_dir:
                    print(workload)
                else:
                    stdout_file, LOG_file, report_csv, stat_csv, iostat_text = get_log_and_std_files(log_dir,
                                                                                                     with_stat_csv=True,
                                                                                                     splitted_iostat=True)
                    std_info = stdoutreader.StdoutReader(stdout_file)
                    cpu = std_info.cpu_count
                    device = utils.stdoutreader.format_device(std_info.device)
                    benchmark_speed = int(std_info.get_benchmark_ops("fillrandom"))
                    stat_df = pd.read_csv(stat_csv)
                    avg_rss = stat_df["rss"].mean() / (1024 * 1024 * 1024)
                    if ".gz" in iostat_text:
                        iostat_lines = gzip.open(iostat_text, "r").readlines()
                        iostat_lines = [x.decode('utf-8') for x in iostat_lines]
                    else:
                        iostat_lines = open(iostat_text, "r", encoding="utf-8").readlines()
                    IOSTAT = []
                    for line in iostat_lines:
                        if PM_device_mapper[std_info.device] in line:
                            IOSTAT.append(line.split())
                    IOSTAT = pd.DataFrame(IOSTAT, columns=IOSTAT_COLUMN)
                    MBPS = IOSTAT["MB_wrtn/s"].astype(float).mean()
                    avg_cpu = stat_df["cpu_percent"].mean()

                    rows.append(
                        [group.replace("FEAT", "ADOC").replace("FEA", "ADOC-B").replace("TEA", "ADOC-T"), device,
                         workload.capitalize(), benchmark_speed, avg_rss, MBPS, avg_cpu])

    print(rows)

    columns = ["group", "device", "workload", "qps", "rss", "disk", "avg_cpu"]
    # columns = ["group", "device", "qps", "stall secs", "p99", "p99.99"]
    result_pd = pd.DataFrame(rows, columns=columns)

    result_pd.to_csv("./csv_results/component_qps.csv", sep="\t", index=False)
    # #
    # group_list = list(result_pd.groupby("group", as_index=False))
    # # print(group_list)
    #
    # merged_df = pd.DataFrame(group_list[0][1][["device", "workload"]])
    #
    # for group in group_list:
    #     qps = list(pd.Series(group[1]["qps"]))
    #     merged_df[group[0]] = qps
    # print(merged_df)
    # merged_df.to_csv("../csv_results/ycsb/qps_grouped.csv", sep="\t", index=False)
