# here is an example from online-ml/river
import os

import utils.stdoutreader
from utils import stdoutreader
from utils.stdoutreader import *
from utils.traversal import get_log_and_std_files, get_log_dirs

import pandas as pd


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

    groups = ['default', 'SILK', 'tuned', "FEAT"]
    base_dir_prefix = "../FAST/section6.4_ycsb/ycsb"
    suffixs = ["_1"]
    devices = ["PM", "NVMe SSD", "SATA SSD", "SATA HDD"]

    rows = []
    for group in groups:
        for suffix in suffixs:
            log_dir_prefix = base_dir_prefix + suffix + "/" + group
            print(log_dir_prefix)
            dirs = get_log_dirs(log_dir_prefix)
            for log_dir in dirs:
                print(log_dir)
                stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)
                std_info = stdoutreader.StdoutReader(stdout_file)
                cpu = std_info.cpu_count
                device = utils.stdoutreader.format_device(std_info.device)
                level_size_list = std_info.level_size_results
                print(level_size_list)
                ycsb_run_speed = int(std_info.get_benchmark_ops("ycsb_run"))
                workload = log_dir.split(os.sep)[-4]
                row = [group, device, workload.capitalize(), ycsb_run_speed]
                for i in range(6):
                    row.append(level_size_list.get("L" + str(i), 0))
                rows.append(row)

    print(rows)

    columns = ["group", "device", "workload", "qps", "L0", "L1", "L2", "L3", "L4", "L5", ]
    # columns = ["group", "device", "qps", "stall secs", "p99", "p99.99"]
    result_pd = pd.DataFrame(rows, columns=columns)
    # result_pd["deep_rate"] = (result_pd["L3"]) / (
    #         result_pd["L0"] + result_pd["L1"] + result_pd["L2"] + result_pd["L3"])
    result_pd.to_csv("../csv_results/ycsb/disk_size.csv", sep="\t", index=False)
