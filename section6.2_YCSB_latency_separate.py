# here is an example from online-ml/river
import os
from sys import prefix
from turtle import color

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
from plotly.graph_objs import Layout

from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_and_std_files, mkdir_p
from utils.traversal import get_log_dirs
import plotly.graph_objs as go

FONT_SIZE = 28

USEFUL_LATENCIES = ["P99", "P99.99"]


def aggreate_latency_type(keys, values):
    results = {x: 0 for x in USEFUL_LATENCIES}
    i = 0
    for key in keys:
        if key in USEFUL_LATENCIES:
            results[key] = float(values[i])
        i += 1

    return results


def append_latency_list(input_list, workload, data_row_head):
    latency_stats = []
    i = 0
    for hist_dict in input_list:
        latency_count = aggreate_latency_type(hist_dict[1], hist_dict[2])

        for key in latency_count:
            data_row = [x for x in data_row_head]
            data_row.append(workload)
            data_row.append(hist_dict[0])
            data_row.append(key)
            data_row.append(latency_count[key])
            if latency_count[key] == 0:
                pass
            else:
                latency_stats.append(data_row)
        i += 1
    return latency_stats


def extract_data_from_dir(log_dir_prefix, output_file_name, tuner="FEAT"):
    latencies = []
    throughputs = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        print(log_dir)
        if "load" in log_dir:
            continue
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, True, multi_tasks=True)
        stat_csv_data = pd.read_csv(stat_csv)
        std_result = StdoutReader(stdout_file)
        workload = log_dir.split(os.sep)[-4]
        if len(workload) > 1:
            workload = log_dir.split(os.sep)[-5]

        data_row_head = [std_result.device, ]  # int(std_result.cpu_count.replace("CPU", " ")), std_result.batch_size]

        latencies.extend(append_latency_list(std_result.hist_job_list, workload.upper(), data_row_head))

        throughput_row = []
        throughput_row.extend(data_row_head)
        throughput_row.append(workload.upper())
        throughput_row.append(float(std_result.benchmark_results["ycsb_run"][1].split(" ")[0]))
        throughputs.append(throughput_row)

    latency_pd = pd.DataFrame(latencies,
                              columns=["device", "workload", "op_type", "latency type", "latency"])
    latency_pd = latency_pd.sort_values(
        by=["op_type", "workload", "device", "latency type"],
        ignore_index=False)
    latency_pd = latency_pd.replace({
        "NVMeSSD": "NVMe SSD", "SATAHDD": "SATA HDD", "SATASSD": "SATA SSD"
    })
    #
    latency_pd["Tuner"] = tuner
    latency_pd.to_csv(output_file_name + "_latency.csv", index=False, sep=" ")

    throughput_pd = pd.DataFrame(throughputs,
                                 columns=["device", "workload", "throughput"]).sort_values(
        by=["device", "workload"],
        ignore_index=False).replace({
        "NVMeSSD": "NVMe SSD", "SATAHDD": "SATA HDD", "SATASSD": "SATA SSD"
    })
    throughput_pd["Tuner"] = tuner

    throughput_pd.to_csv(output_file_name + "running_throughput.csv", index=False, sep="\t")


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False

    # extract_data_from_dir("Eurosys/pm_server_ycsb_60+60/default",
    #                       "csv_results/ycsb_report/ycsb_default", "Default")
    # extract_data_from_dir("Eurosys/pm_server_ycsb_60+60/tuned",
    #                       "csv_results/ycsb_report/ycsb_tuned", "Tuned")

    extract_data_from_dir("Eurosys/pm_ycsb_separate/pebbles",
                          "csv_results/ycsb_report/ycsb_Pebbles", "PebblesDB")
    extract_data_from_dir("Eurosys/pm_ycsb_separate/FEAT",
                          "csv_results/ycsb_report/ycsb_FEAT", "FEAT")
    extract_data_from_dir("Eurosys/pm_ycsb_june17/ycsb_separate_running/SILK",
                          "csv_results/ycsb_report/ycsb_SILK", "SILK")
    extract_data_from_dir("Eurosys/pm_ycsb_separate/tuned",
                          "csv_results/ycsb_report/ycsb_tuned", "Tuned")
    # extract_data_from_dir("Eurosys/pm_ycsb_june17/ycsb_separate_running/FEAT",
    #                       "csv_results/ycsb_report/ycsb_FEAT", "FEAT")
    # extract_data_from_dir("Eurosys/pm_ycsb_june17/ycsb_separate_running/SILK",
    #                       "csv_results/ycsb_report/ycsb_SILK", "SILK")
    #
    # extract_data_from_dir("Eurosys/pm_hdd_only/",
    #                       "csv_results/ycsb_report/ycsb_tuned", "Tuned")
