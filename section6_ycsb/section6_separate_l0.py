# here is an example from online-ml/river
import os

from matplotlib import pyplot as plt

import utils.stdoutreader
from utils import stdoutreader
from utils.stdoutreader import *
from utils.traversal import get_log_and_std_files, get_log_dirs

import pandas as pd
from utils.log_class import load_log_and_qps


def get_rss_array(stat_csv):
    stat_df = pd.read_csv(stat_csv)
    memory_cost = stat_df["rss"] / (1024 * 1024)
    # print(memory_cost)
    return memory_cost



# def LSM_state_array(LOG_file, report_csv):
#     log_recorder = load_log_and_qps(LOG_file, report_csv)
#     time_line = []
#     for lsm_state in log_recorder.lsm_state_df.iloc:
#         time_line.append([lsm_state["time_micro"] / 1000000, lsm_state["lsm_state"][0]])
#     l0_file_line = pd.DataFrame(time_line, columns=["time", "l0_num"]).sort_values(by=["time"])
#     report_df = pd.read_csv(report_csv)
#     return l0_file_line, report_df

def LSM_state_array(LOG_file, report_csv):
    log_recorder = load_log_and_qps(LOG_file, report_csv)
    time_line = []
    flush_df = log_recorder.flush_df
    for flush_job in flush_df.iloc:
        time_line.append([flush_job["start_time"] / 1000000, flush_job["lsm_state"][0]])
    compaction_df = log_recorder.compaction_df
    for compaction_job in compaction_df.iloc:
        time_line.append([compaction_job["start_time"] / 1000000, compaction_job["lsm_state"][0]])

    l0_file_line = pd.DataFrame(time_line, columns=["time", "l0_num"]).sort_values(by=["time"])
    report_df = pd.read_csv(report_csv)
    return l0_file_line, report_df


if __name__ == '__main__':

    groups = ['auto-tuned', 'SILK-P', "SILK-O", "FEAT"]
    base_dir_prefix = "../FAST/section6_ycsb_default_running_pm_server/"
    suffixs = ["1"]
    devices = ["PM", "NVMe SSD", "SATA SSD", "SATA HDD"]

    rows = []

    group_results = {x: {} for x in groups}

    for group in groups:
        for suffix in suffixs:
            group_results[group] = {x: [] for x in devices}
            log_dir_prefix = base_dir_prefix + group + "/" + suffix + "/a/run/"
            dirs = get_log_dirs(log_dir_prefix)
            for log_dir in dirs:
                print(log_dir)
                stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)
                std_info = stdoutreader.StdoutReader(stdout_file)
                device = utils.stdoutreader.format_device(std_info.device)
                rss_time_map = get_rss_array(stat_csv)
                l0_df, qps_df = LSM_state_array(LOG_file, report_csv)
                print(l0_df)
                group_results[group][device].extend([l0_df, qps_df, rss_time_map])

    for group in group_results:
        for device in devices:
            print(group_results[group][device][0])

    num_devices = len(devices)
    num_groups = len(groups)

    import matplotlib as mpl

    mpl.rcParams['figure.figsize'] = (8, 4.5)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16.5
    mpl.rcParams['font.family'] = "Arial"
    mpl.rcParams['lines.markersize'] = 10
    mpl.rcParams["legend.loc"] = "lower center"
    col_count = 0

    fig, axes = plt.subplots(num_devices, num_groups, sharey='all', sharex='all')

    for i in range(num_groups):
        # axes[num_devices - 1, i].set_xlabel("Elapsed Time (Sec)")
        axes[0, i].set_title(groups[i])

    for group in group_results:
        row_count = 0
        for device in devices:
            axes[row_count, 0].set_ylabel(
                device.replace("NVMe SSD", "NS").replace("SATA SSD", "SD").replace("SATA HDD", "HD"))
            axes[row_count, col_count].plot(group_results[group][device][0]["time"],
                                            group_results[group][device][0]["l0_num"], "r--")
            row_count += 1
        col_count += 1

    fig.tight_layout()
    fig.subplots_adjust(bottom=0.125, left=0.12)
    fig.text(0.025, 0.3, "Number of L0 Files", ha='center', rotation="vertical", font={"size": 16.5})
    fig.text(0.5, 0.005, "Elapsed Time (Sec)", ha='center', font={"size": 16.5})

    fig.savefig('fig_results/l0_comparison.png')
    fig.savefig('fig_results/l0_comparison.pdf')
