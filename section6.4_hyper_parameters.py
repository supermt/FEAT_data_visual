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

        # join_df = pid_stat_df.set_index('secs').join(report_df.set_index("secs_elapsed")).dropna()
        # cpu_utils = join_df["cpu_percent"] / join_df["thread_num"]
        cpu_utils = pid_stat_df["cpu_percent"]
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
            if "nvme" in line or "pm" in line or "sda" in line or "sdc" in line:
                IOSTAT.append(line.split())
        IOSTAT = pd.DataFrame(IOSTAT, columns=IOSTAT_COLUMN)
        MBPS = IOSTAT["MB_wrtn/s"].astype(float)
        average_mbps = MBPS.mean()
        hyper_para_value = log_dir.split(os.path.sep)[-4]
        std_results = StdoutReader(stdout_file)
        duration = std_results.stall_duration.split()[0].split(":")
        duration = float(duration[2]) + int(duration[1]) * 60 + int(duration[0]) * 3600

        benchmark_speed = int(std_results.benchmark_results['fillrandom'][1].split(" ")[0])

        row = [log_dir.split(os.sep)[-3].replace("StorageMaterial.", "").replace("SSD", " SSD").replace("HDD", " HDD"),
               hyper_para_value, cpu_utils, change_count,
               round(average_mbps, 2), round(duration, 2), benchmark_speed]
        rows.append(row)

    return rows


def extract_tuned_resource_utilization(log_dir):
    stdout_file, LOG_file, report_csv, pid_stat_file, iostat_text = get_log_and_std_files(log_dir,
                                                                                          with_stat_csv=True,
                                                                                          splitted_iostat=True,
                                                                                          multi_tasks=True)
    report_src = report_csv[0]

    report_df = pd.read_csv(report_src)

    pid_stat_df = pd.read_csv(pid_stat_file)

    cpu_utils = pid_stat_df["cpu_percent"]
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
        if "nvme" in line or "pm" in line or "sda" in line or "sdc" in line:
            IOSTAT.append(line.split())
    IOSTAT = pd.DataFrame(IOSTAT, columns=IOSTAT_COLUMN)
    MBPS = IOSTAT["MB_wrtn/s"].astype(float)
    average_mbps = MBPS.mean()

    std_results = StdoutReader(stdout_file)
    duration = std_results.stall_duration.split()[0].split(":")
    duration = float(duration[2]) + int(duration[1]) * 60 + int(duration[0]) * 3600

    benchmark_speed = int(std_results.benchmark_results['fillrandom'][1].split(" ")[0])

    row = [log_dir.split('/')[-3].replace("StorageMaterial.", "").replace("SSD", " SSD").replace("HDD", " HDD"),
           cpu_utils, round(average_mbps, 2), round(duration, 2), benchmark_speed]
    return row


if __name__ == '__main__':

    off_line_result_dirs = [
        "StorageMaterial.PM/20CPU/256MB",
        "StorageMaterial.NVMeSSD/12CPU/256MB",
        "StorageMaterial.SATASSD/18CPU/512MB",
        "StorageMaterial.SATAHDD/16CPU/512MB"
    ]
    tuned_dir_prefix = "Eurosys/offline_results/offline_tuning_results"

    suffix_list = ["/", "_1/", "_2/"]

    rows = []
    for suffix in suffix_list:
        for off_line_result_dir in off_line_result_dirs:
            targer_dir = tuned_dir_prefix + suffix + off_line_result_dir
            row = extract_tuned_resource_utilization(targer_dir)
            rows.append(row)

    tuned_df = pd.DataFrame(rows, columns=["device", "cpu utils", "disk utils", "stall duration", "qps"])
    tuned_df = tuned_df.groupby("device").mean().reset_index().round(2)
    tuned_df.to_csv("csv_results/hyper_parameter/tuned_average.csv", sep='\t', index=False)
    # exit(-1)
    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    base_log_prefix = "FAST/section6.2_hyper_parameters/PM_results_hyper_parameters"
    hyper_parameters = ["flush_gap", "idle_rate", "slow_flush", "time_window"]

    group_names = ["/", "_1/", "_2/"]

    result_df_list = []
    hyper_para_average_list = []
    for group_suffix in group_names:
        result_df = []
        for hyper_para in hyper_parameters:
            targer_dir = base_log_prefix + group_suffix + hyper_para
            rows = extract_change_frequency_and_resource_utilization(targer_dir)
            for row in rows:
                # row: media ,hyper_para_value, cpu_utils, change_count, average_mbps, duration
                result_line = [hyper_para]
                result_line.extend(row)
                result_df.append(result_line)
                hyper_para_average_list.append(result_line)
        result_df = pd.DataFrame(result_df,
                                 columns=["parameter", "device", "para_value", "cpu utils", "tuning actions",
                                          "disk utils", "stall duration", "qps"])
        result_df = result_df.sort_values(
            by=["parameter", "device", "para_value"],
            ignore_index=False)
        result_df_list.append(result_df)
        print(group_suffix + "finished")

    hyper_para_average_df = pd.DataFrame(hyper_para_average_list,
                                         columns=["parameter", "device", "para_value", "cpu utils", "tuning actions",
                                                  "disk utils", "stall duration", "qps"])
    hyper_para_average_df = hyper_para_average_df.groupby(
        ["parameter", "device", "para_value"]).mean().reset_index().round(2)
    hyper_para_average_df = hyper_para_average_df.sort_values(
        by=["parameter", "device", "para_value"],
        ignore_index=False)
    print(hyper_para_average_df)

    hyper_para_average_df.to_csv("csv_results/hyper_parameter/average.csv", sep="\t", index=False)

    common_columns = ["parameter", "device", "para_value"]
    data_columns = ["cpu utils", "tuning actions", "disk utils", "stall duration", "qps"]
    result_df = result_df[common_columns]
    # print(result_df)

    i = 0
    for result_df_indi in result_df_list:
        for data_column in data_columns:
            result_df[data_column] = hyper_para_average_df[data_column]
            result_df[data_column + " err " + str(i)] = result_df_indi[data_column] - hyper_para_average_df[data_column]
        i += 1
        # pass
    result_df = result_df.round(2)
    result_df = result_df.sort_values(
        by=["parameter", "device", "para_value"],
        ignore_index=False)
    print(result_df)
    result_df = result_df.reindex(sorted(result_df.columns), axis=1)

    for hyper_para in hyper_parameters:
        csv_df = result_df[result_df["parameter"] == hyper_para]
        csv_df.to_csv("csv_results/hyper_parameter/%s.csv" % hyper_para, sep='\t', index=False)
    result_df.to_csv("csv_results/hyper_parameter/hyper_result_with_error.csv", sep="\t", index=False)
