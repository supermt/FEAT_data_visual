# here is an example from online-ml/river
import os.path

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from utils.feature_selection import vectorize_by_compaction_output_level, generate_lsm_shape
from utils.log_class import load_log_and_qps
from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_and_std_files
from utils.traversal import get_log_dirs


def ks(x, pos):
    'The two args are the value and tick position'
    return '%1.1fk' % (x * 1e-3)


def cut_down_by_time(data_df, start_time, end_time):
    if "start_time" in data_df.keys():
        time_column_name = "start_time"
        start_time *= 1000000
        end_time *= 1000000
    if "secs_elapsed" in data_df.keys():
        time_column_name = "secs_elapsed"
    if "time sec" in data_df.keys():
        time_column_name = "time sec"

    return data_df[(data_df[time_column_name] > start_time) & (data_df[time_column_name] < end_time)]


def plot_with_thread_num(thread_num, batch_size, start_time, end_time):
    log_dir_prefix = "Eurosys/pm_server_section4_detailed_records"
    dirs = get_log_dirs(log_dir_prefix)
    key_seq = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    default_setting_qps_csv = {x: None for x in key_seq}
    default_setting_lsm_state = {x: None for x in key_seq}
    default_setting_compaction_distribution = {x: None for x in key_seq}
    stall_moments = {x: None for x in key_seq}

    log_dir = "Eurosys/pm_server_500ms_detailed_records"

    stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
    report_csv = report_csv[0]
    report_df = pd.read_csv(report_csv)
    bytes_to_GB = 1000 * 1000 * 1000
    bytes_to_MB = 1000 * 1000
    std_info = StdoutReader(stdout_file)
    report_df["interval_qps"] *= 2
    report_df["pending_bytes"] /= bytes_to_GB
    report_df["total_mem_size"] /= bytes_to_MB
    report_df["interval_qps"] /= 1000
    log_info = load_log_and_qps(LOG_file, report_csv)
    print(log_info.flush_df)
    stall_pd = log_info.phrase_warninglines(1)
    stall_pd = cut_down_by_time(stall_pd, start_time, end_time)
    mo_stall_pd = stall_pd[stall_pd["overflowing"] == "Memory Overflowing"]
    lo_stall_pd = stall_pd[stall_pd["overflowing"] == "L0 Overflowing"]
    ro_stall_pd = stall_pd[stall_pd["overflowing"] == "Redundancy Overflowing"]
    mo_stall_pd["values"] = 0
    lo_stall_pd["values"] = 1
    ro_stall_pd["values"] = 2

    report_df = cut_down_by_time(report_df, start_time, end_time)
    flush_job_df = cut_down_by_time(log_info.flush_df, start_time, end_time)

    flush_job_df["flush_speed"] = flush_job_df["flush_size"] / (flush_job_df["end_time"] - flush_job_df["start_time"])

    # slow_threshold = 100
    float(flush_job_df["flush_speed"].mean())

    fig, axes = plt.subplots(2, 1, sharex='all')
    axes[0].set_ylabel("kOps/Sec")
    axes[1].set_ylabel("MB/Sec")

    axes[0].set_title("(a) System Throughput")
    axes[1].set_title("(b) Mapping of Performance Drop and Memory Component Size")

    axes[0].plot(report_df["secs_elapsed"], report_df["interval_qps"])
    # axes[0].scatter(low_speed_flush_jobs["start_time"] / 1000000, [0] * len(low_speed_flush_jobs))
    # axes[0].scatter(mo_stall_pd["time sec"], mo_stall_pd["values"], color='red', s=4)
    # axes[0].scatter(lo_stall_pd["time sec"], lo_stall_pd["values"], color='cyan', s=4)
    # axes[0].scatter(ro_stall_pd["time sec"], ro_stall_pd["values"], color='green', s=4)

    mapping_of_thoughs_mmo = axes[1].twinx()
    mapping_of_thoughs_throughput = axes[1].twinx()
    mapping_of_thoughs_throughput.set_ylim(-300, 250)
    mapping_of_thoughs_throughput.plot(report_df["secs_elapsed"], report_df["interval_qps"])
    mapping_of_thoughs_mem_size = axes[1]
    mapping_of_thoughs_mem_size.plot(report_df["secs_elapsed"], report_df["total_mem_size"], "r--", )
    mapping_of_thoughs_mem_size.set_ylim(0, 300)
    mapping_of_thoughs_mmo.set_yticks([])
    mapping_of_thoughs_mmo.scatter(mo_stall_pd["time sec"], mo_stall_pd["values"], color='green', s=4)
    mapping_of_thoughs_mmo.set_ylim(-1, 1)
    mapping_of_thoughs_mmo.set_yticks([])

    # axes[1].scatter(low_speed_flush_jobs["start_time"] / 1000000, [0] * len(low_speed_flush_jobs))
    fig.tight_layout()
    fig.savefig('fig_results/MMO_detail_mappings.pdf', bbox_inches="tight")


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (10, 14)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16
    mpl.rcParams['lines.markersize'] = 10
    mpl.rcParams["legend.loc"] = "lower center"

    # plot_with_thread_num(2, "64MB", 0, 600)
    plot_with_thread_num(4, "64MB", 0, 600)
