# here is an example from online-ml/river
import gzip
import os.path

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from utils.feature_selection import vectorize_by_compaction_output_level, generate_lsm_shape
from utils.log_class import load_log_and_qps
from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_and_std_files
from utils.traversal import get_log_dirs

IOSTAT_COLUMN_NAMES = "Device             tps    MB_read/s    MB_wrtn/s    MB_read    MB_wrtn"
IOSTAT_COLUMN = IOSTAT_COLUMN_NAMES.split()

PM_device_mapper = {
    "SATAHDD": "sdc",
    "SATASSD": "sda",
    "NVMeSSD": "nvme0n1",
    "PM": "pmem0"
}


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


def get_MBPS(iostat_text):
    if ".gz" in iostat_text:
        iostat_lines = gzip.open(iostat_text, "r").readlines()
        iostat_lines = [x.decode('utf-8') for x in iostat_lines]
    else:
        iostat_lines = open(iostat_text, "r", encoding="utf-8").readlines()
    IOSTAT = []
    for line in iostat_lines:
        if PM_device_mapper["NVMeSSD"] in line:
            IOSTAT.append(line.split())
    IOSTAT = pd.DataFrame(IOSTAT, columns=IOSTAT_COLUMN)
    MBPS = IOSTAT["MB_wrtn/s"].astype(float)
    return MBPS / 2400 * 100


def plot_lines_on_axes(axe, x, y, marker="b", title="", unit="", ticks=False, ylim=[0, 250], legend_name="", alpha=1):
    line, = axe.plot(x, y, marker, alpha=alpha, label=legend_name)
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

    # log_dir = "Eurosys/pm_server_details_1800sec_running"
    stdout_file, LOG_file, report_csv, stat_csv, io_stat = get_log_and_std_files(log_dir_prefix, multi_tasks=True,
                                                                                 with_stat_csv=True,
                                                                                 splitted_iostat=True)
    report_csv = report_csv[0]
    report_df = pd.read_csv(report_csv)
    bytes_to_GB = 1024 * 1024 * 1024
    bytes_to_MB = 1024 * 1024
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

    fig, axes = plt.subplots(3, 1, sharex="all")
    axes[0].set_ylabel("kOps/Sec")
    # axes[1].set_ylabel("MB/Sec")

    # Throughput lines
    axes[-1].set_xlim(start_time, end_time)
    axes[-1].set_xticks(range(start_time, end_time, 100))
    axes[-1].set_xlabel("Elapsed Time (Sec)")
    ax_throughput = axes[0]
    ax_throughput.set_ylim(0, 250)

    # L0O and L0 file number
    ax_l0_files = axes[0]
    ax_l0o_occurrence = axes[0].twinx()
    l0_size_line = plot_lines_on_axes(ax_l0_files, report_df["secs_elapsed"],
                                      report_df["l0_files"],
                                      "k:",
                                      "(a) Number of L0 SSTs", "", True, [0, 26], legend_name="L0 Number")

    l0O_point = plot_lines_on_axes(ax_l0o_occurrence, lo_stall_pd["time sec"], lo_stall_pd["values"], "b|",
                                   "", "", False, [-1, 1.2], legend_name="L0 Stalls", alpha=0.5)

    label_list = [l0_size_line, l0O_point]
    plot_labels = ["Number of L0 SSTs", "L0 Stalls"]
    lgd = axes[0].legend(label_list,
                         plot_labels,
                         ncol=2,
                         frameon=False,
                         shadow=False, loc=(0.15, 0.01))

    # RDO and Compaction pending bytes

    ax_redundancy = axes[1]
    ax_rdo_occurrence = axes[1].twinx()

    redundant_size_line = plot_lines_on_axes(ax_redundancy, report_df["secs_elapsed"],
                                             report_df["pending_bytes"],
                                             "k-.",
                                             "(b) Redundant Data Size", "GB", True, [0, 85], legend_name="Redundancy")

    RDO_point = plot_lines_on_axes(ax_rdo_occurrence, ro_stall_pd["time sec"], ro_stall_pd["values"], "r|",
                                   "", "", False, [0, 2.2], legend_name="PS stall")

    label_list = [redundant_size_line, RDO_point]
    plot_labels = ["Redundant Data Size", "PS Stall"]
    lgd = axes[1].legend(label_list,
                         plot_labels,
                         ncol=2,
                         frameon=False,
                         shadow=False)
    # Resource Utilization
    ax_cpu = axes[2]
    ax_disk = axes[2]
    ax_cpu.set_ylabel("Utilization (%)")
    ax_cpu.set_ylim(0, 90)
    ax_disk.set_ylim(0, 90)
    disk_utilization = get_MBPS(io_stat)
    cpu_utils = pd.read_csv(stat_csv)["cpu_percent"] / 4

    disk_util_line = plot_lines_on_axes(ax_disk, range(start_time, end_time), disk_utilization[start_time:end_time],
                                        "k--",
                                        "(c) Resource Utilization", "", True, [0, 150], legend_name="Bandwidth")

    cpu_util_line = plot_lines_on_axes(ax_cpu, range(start_time, end_time), cpu_utils[start_time:end_time],
                                       "r-.",
                                       "", "", True, [0, 150], legend_name="Bandwidth", alpha=0.5)

    label_list = [disk_util_line, cpu_util_line]
    plot_labels = ["Disk Utilization", "CPU Utilization"]
    lgd = axes[2].legend(label_list,
                         plot_labels,
                         ncol=2,
                         frameon=False,
                         shadow=False, loc="upper center")

    fig.tight_layout()
    # fig.subplots_adjust(bottom=0.35)
    fig.align_ylabels(axes)
    fig.show()
    fig.savefig('fig_results/mapping_of_overflows.pdf')


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16
    mpl.rcParams['font.family'] = "Arial"
    mpl.rcParams['lines.markersize'] = 10
    mpl.rcParams["legend.loc"] = "lower center"

    # plot_with_thread_num(2, "64MB", 0, 600)
    plot_with_thread_num(0, 600)
