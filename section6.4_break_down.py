import gzip
import os.path

from utils.traversal import get_log_dirs, get_log_and_std_files
import pandas as pd

from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_dirs, get_log_and_std_files

STALL_REASON = ["memtable", "level0", "pending_compaction_bytes"]

color_map = {"PM": "rgb(68,114,196)", "NVMeSSD": "rgb(237,125,49)", "SATASSD": "rgb(165,165,165)",
             "SATAHDD": "rgb(255,192,0)"}


def extract_breakdown(input_dir):
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


if __name__ == '__main__':

    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    base_log_prefix = "Eurosys/section6_hyper_parameters/"

    hyper_parameters = ["flush_gap", "idle_rate", "slow_flush", "time_window"]
    result_list = []
    for hyper_para in hyper_parameters:
        log_prefix = base_log_prefix + hyper_para
        rows = extract_breakdown(log_prefix)
        for row in rows:
            result_line = [hyper_para]

            result_line.extend(row)

            result_list.append(result_line)

    result_df = pd.DataFrame(result_list,
                             columns=["Hyper Parameter", "value", "stall_duration", "update option", "tune", ""])

    result_df.to_csv("csv_results/hyper_parameters.csv", index=False, sep=" ")
