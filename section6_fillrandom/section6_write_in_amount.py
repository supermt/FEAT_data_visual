import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_dirs, get_log_and_std_files

STALL_REASON = ["level0", "pending_compaction_bytes", "memtable"]

color_map = {"PM": "rgb(68,114,196)", "NVMeSSD": "rgb(237,125,49)", "SATASSD": "rgb(165,165,165)",
             "SATAHDD": "rgb(255,192,0)"}


def get_written_amount(log_dir, using_SILK=False):
    start_time = 0
    end_time = 3600
    report_dict = {}
    dirs = get_log_dirs(log_dir)

    for log_dir in dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
        basic_info = StdoutReader(stdout_file)
        disk_size = basic_info.tradeoff_data["Disk Size"]
        flush_size = basic_info.tradeoff_data["Flush(GB)"]
        l0_size = basic_info.tradeoff_data["L0 Size"]
        report_dict[basic_info.device] = [disk_size, flush_size, l0_size]

    return report_dict


if __name__ == '__main__':
    metrics_in_std_files = []
    temp = {}
    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    batch_size_curve = {}
    thread_number_curve = {}

    default_dir = "../FAST/section6.3_fillrandom/RocksDB7.56/default"
    auto_tuned_dir = "../FAST/section6.3_fillrandom/RocksDB7.56/auto-tuned"

    SILK_default_dir = "../FAST/section6.3_fillrandom/RocksDB7.56/SILK-D"
    SILK_paper_dir = "../FAST/section6.3_fillrandom/RocksDB7.56/SILK-P"
    SILK_Optimized = "../FAST/section6.3_fillrandom/RocksDB7.56/SILK-O"

    FEAT_dir = "../FAST/section6.3_fillrandom/RocksDB7.56/FEAT"

    dir_name = [auto_tuned_dir, SILK_paper_dir, SILK_Optimized, FEAT_dir]
    group_names = ["RocksDB-AT", "SILK-P", "SILK-O", "FEAT"]
    groups = [get_written_amount(x, True) for x in dir_name]

    result_lines = []
    group_count = 0
    for group in groups:
        for device in group:
            row = [group_names[group_count], device.replace("NVMe", "NVMe ").replace("SATA", "SATA ")]
            row.extend(group[device])
            result_lines.append(row)
        group_count += 1

    result_df = pd.DataFrame(result_lines, columns=["group", "device", "total size", "flush size", "l0 size"])
    result_df.to_csv("csv_results/disk_data_size.csv", sep="\t", index=False)
    print(result_lines)
