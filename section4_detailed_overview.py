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


def plot_lines_on_axes(axe, x, y, marker="b", title="", unit="", ticks=False, ylim=[0, 250]):
    line, = axe.plot(x, y, marker, alpha=0.5)
    if title:
        axe.set_title(title)
    if unit:
        axe.set_ylabel(unit)
    if not ticks:
        axe.set_yticks([])
    axe.set_ylim(ylim[0], ylim[1])

    # MMO_point, = mapping_of_thoughs_mmo.plot(mo_stall_pd["time sec"], mo_stall_pd["values"], "g+")

    return line


def plot_with_thread_num(start_time, end_time):
    log_dir_prefix = "Eurosys/pm_server_section4_detailed_records"
    dirs = get_log_dirs(log_dir_prefix)
    key_seq = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    default_setting_qps_csv = {x: None for x in key_seq}
    default_setting_lsm_state = {x: None for x in key_seq}
    default_setting_compaction_distribution = {x: None for x in key_seq}
    stall_moments = {x: None for x in key_seq}

    log_dir = "Eurosys/pm_server_details_1800sec_running"

    stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
    report_csv = report_csv[0]
    report_df = pd.read_csv(report_csv)
    bytes_to_GB = 1000 * 1000 * 1000
    bytes_to_MB = 1000 * 1000
    std_info = StdoutReader(stdout_file)
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

    fig, axes = plt.subplots(2, 1, sharex="all")
    axes[0].set_ylabel("kOps/Sec")
    # axes[1].set_ylabel("MB/Sec")

    # Throughput lines
    axes[1].set_xlim(0, 600)
    axes[1].set_xticks(range(0, 700, 100))
    axes[1].set_xlabel("Elapsed Time (Sec)")
    ax_throughput = axes[0]
    ax_throughput.set_ylim(0, 250)

    #
    # throughput_line = plot_lines_on_axes(ax_throughput, report_df["secs_elapsed"],
    #                                      report_df["interval_qps"], "k", "(a) System Throughput", "kOps/Sec", True,
    #                                      [0, 350])
    # # ax_throughput.set_yticks([])
    #
    # # MMO and Memory usage
    # ax_mem_size = axes[1]
    # ax_mmo_occurrence = axes[1].twinx()
    #
    # mem_size_line = plot_lines_on_axes(ax_mem_size, report_df["secs_elapsed"],
    #                                    report_df["total_mem_size"],
    #                                    "k--",
    #                                    "(b) Memory Component Size", "MB", True, [0, 160])
    #
    # MMO_point = plot_lines_on_axes(ax_mmo_occurrence, mo_stall_pd["time sec"], mo_stall_pd["values"], "r|",
    #                                "", "", False, [-2, 0.2])

    # L0O and L0 file number
    ax_l0_files = axes[0]
    ax_l0o_occurrence = axes[0].twinx()

    l0_size_line = plot_lines_on_axes(ax_l0_files, report_df["secs_elapsed"],
                                      report_df["l0_files"],
                                      "k:",
                                      "(a) Number of L0 SSTs", "", True, [0, 26])

    l0O_point = plot_lines_on_axes(ax_l0o_occurrence, lo_stall_pd["time sec"], lo_stall_pd["values"], "r|",
                                   "", "", False, [-1, 1.2])

    # RDO and Compaction pending bytes

    ax_redundancy = axes[1]
    ax_rdo_occurrence = axes[1].twinx()

    redundant_size_line = plot_lines_on_axes(ax_redundancy, report_df["secs_elapsed"],
                                             report_df["pending_bytes"],
                                             "k-.",
                                             "(b) Redundancy Data Size", "GB", True, [0, 85])

    RDO_point = plot_lines_on_axes(ax_rdo_occurrence, ro_stall_pd["time sec"], ro_stall_pd["values"], "r|",
                                   "", "", False, [0, 2.2])

    label_list = [l0_size_line, l0O_point, redundant_size_line, ]
    plot_labels = ["Number of L0 SSTs", "Occurrence of Data Overflow", "Redundant Data Size"]
    lgd = fig.legend(label_list,
                     plot_labels,
                     ncol=2,
                     frameon=False,
                     shadow=False)
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.35)
    # axes[1].scatter(low_speed_flush_jobs["start_time"] / 1000000, [0] * len(low_speed_flush_jobs))

    fig.show()
    fig.savefig('fig_results/mapping_of_overflows.pdf')


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 4)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16
    mpl.rcParams['font.family'] = "Arial"
    mpl.rcParams['lines.markersize'] = 10
    mpl.rcParams["legend.loc"] = "lower center"

    # plot_with_thread_num(2, "64MB", 0, 600)
    plot_with_thread_num(0, 600)
