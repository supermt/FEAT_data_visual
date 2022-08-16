# here is an example from online-ml/river
import os
from sys import prefix
from turtle import color

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
from plotly.graph_objs import Layout

from utils.feature_selection import vectorize_by_disk_op_distribution, combine_vector_with_qps
from utils.log_class import LogRecorder
from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_and_std_files, mkdir_p
from utils.traversal import get_log_dirs
import plotly.graph_objs as go


def extract_data_from_dir(log_dir_prefix, output_file_name, tuner="FEAT"):
    tune_and_flush_distribution = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, True)
        std_result = StdoutReader(stdout_file)

        batch_size = log_dir.split(os.sep)[-1]
        cpu_count = std_result.cpu_count.replace("CPU", "")

        data_row_head = [std_result.device, batch_size, cpu_count]
        throughput = float(std_result.benchmark_results["fillrandom"][2].split(" ")[0])

        report_pd = pd.read_csv(report_csv)

        log_info = LogRecorder(LOG_file)
        flush_speed = []  # MiB/s
        for flush_job in log_info.flush_df.iloc:
            flush_speed.append(flush_job["flush_size"] / (flush_job["end_time"] - flush_job["start_time"]))

        flush_speed_pd = pd.DataFrame(flush_speed, columns=["speed"])
        avg_l0_speed = log_info.l0_compaction_df["processing_speed"].mean()
        avg_compaction_speed = log_info.compaction_df["processing_speed"].mean()
        avg_flush_speed = flush_speed_pd["speed"].mean()
        speed_stdev = flush_speed_pd["speed"].std()

        avg_flush_speed = round(avg_flush_speed, 2)
        avg_l0_speed = round(avg_l0_speed, 2)

        data_row = [x for x in data_row_head]

        data_row.append(avg_flush_speed)
        data_row.append(avg_l0_speed)
        data_row.append(avg_compaction_speed)
        data_row.append(len(log_info.flush_df))
        data_row.append(len(log_info.l0_compaction_df))
        data_row.append(len(log_info.compaction_df) - len(log_info.l0_compaction_df))

        throughput = float(std_result.benchmark_results["fillrandom"][2].split(" ")[0])

        tune_and_flush_distribution.append(data_row)
        print(data_row)
    # #
    stat_df = pd.DataFrame(tune_and_flush_distribution,
                           columns=["device", "batch size", "CPU count", "flush speed", "l0 speed", "compaction speed",
                                    "flush jobs", "l0-l1 compaction", "deep compaction"]).sort_values(
        by=["device", "batch size", "CPU count"],
        ignore_index=False).replace({
        "NVMeSSD": "NVMe SSD", "SATAHDD": "SATA HDD", "SATASSD": "SATA SSD"
    })
    #
    stat_df["Tuner"] = tuner
    stat_df.to_csv(output_file_name, index=False, sep="\t")


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False

    extract_data_from_dir("Eurosys/pm_server_512MB_traverse",
                          "csv_results/512MB_flush_speed.csv", "Default")
    extract_data_from_dir("Eurosys/pm_stall_proportion_thread",
                          "csv_results/64MB_flush_speed.csv", "Default")
    extract_data_from_dir("Eurosys/pm_2to8_thread",
                          "csv_results/64MB_flush_speed_2to8.csv", "Default")
    # extract_data_from_dir("Eurosys/pm_stall_proportion_batch",
    #                       "csv_results/flush_speed_all_batches.csv", "Default")
