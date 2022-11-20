import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_dirs, get_log_and_std_files

STALL_REASON = ["level0", "pending_compaction_bytes", "memtable"]

color_map = {"PM": "rgb(68,114,196)", "NVMeSSD": "rgb(237,125,49)", "SATASSD": "rgb(165,165,165)",
             "SATAHDD": "rgb(255,192,0)"}


def aggreate_stall_type(stall_dict):
    results = {x: 0 for x in STALL_REASON}
    for key in stall_dict:
        for stall_reason in STALL_REASON:
            if stall_reason in key:
                results[stall_reason] += int(stall_dict[key])
    return results


def stdout_to_dict(stdout_recorder):
    temp_dict = {}
    temp_dict["throughput"] = stdout_recorder.benchmark_results["fillrandom"][1].split(" ")[0]
    temp_dict["threads"] = int(stdout_recorder.cpu_count.replace("CPU", ""))
    temp_dict["batch_size"] = stdout_recorder.batch_size.replace("MB", "")
    temp_dict["device"] = stdout_recorder.device

    temp_dict.update(aggreate_stall_type(stdout_recorder.stall_reasons))

    return temp_dict


def pick_the_most_frequent_set(tuning_steps):
    df_mode = tuning_steps.mode()
    most_frequent_thread = int(df_mode["thread_num"][0])
    most_frequent_batch = int(df_mode["batch_size"][0])
    print(most_frequent_batch, most_frequent_thread)

    candidate_thread_no = [1, 4, 12]
    most_frequent_thread = min(candidate_thread_no, key=lambda x: abs(x - most_frequent_thread))

    candidate_batch_size = [64, 128, 256]
    most_frequent_batch = min(candidate_batch_size, key=lambda x: abs(x - most_frequent_batch))
    return most_frequent_batch, most_frequent_thread


def get_plot_dict(log_dir, using_SILK=False):
    start_time = 0
    end_time = 3600
    report_dict = {}
    dirs = get_log_dirs(log_dir)

    for log_dir in dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)

        basic_info = StdoutReader(stdout_file)
        # devices.append(basic_info.device)
        qps_file = ""
        for csv_name in report_csv:
            print(report_csv)
            if using_SILK:
                if ("_0_" not in csv_name) and ("_1_" not in csv_name):
                    qps_file = csv_name
                    break
            elif "_0_" in csv_name:
                qps_file = csv_name
                break
        qps_df = pd.read_csv(qps_file)
        qps_df = qps_df[qps_df["secs_elapsed"] < end_time]
        time_gap = qps_df["secs_elapsed"] - qps_df["secs_elapsed"].shift(1).fillna(0)
        qps_df["interval_qps"] /= (time_gap * 1000)
        avg_speed = basic_info.benchmark_results["fillrandom"][1].replace(" ops/sec", "")

        qps_df["avg_qps"] = round(int(avg_speed) / 1000, 1)
        report_dict[basic_info.device] = qps_df
        print(qps_df)

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

    dir_name = [default_dir, auto_tuned_dir, SILK_default_dir, SILK_paper_dir, SILK_Optimized, FEAT_dir]
    groups = [get_plot_dict(x, True) for x in dir_name]
    # FEAT_warm_changes = get_plot_dict(FEAT_warm_dir)

    group_names = ["RocksDB-DF", "RocksDB-AT", "SILK-D", "SILK-P", "SILK-O", "ADOC"]

    std_df = pd.read_csv("../csv_results/fillrandom/all_with_std.csv", sep="\t")
    print(std_df)

    mpl.rcParams['figure.figsize'] = (16, 5)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16.5
    mpl.rcParams['font.family'] = "Arial"
    mpl.rcParams['lines.markersize'] = 10
    mpl.rcParams["legend.loc"] = "lower center"

    num_devices = len(devices)
    num_groups = len(groups)
    fig, axes = plt.subplots(num_devices, num_groups, sharey='all', sharex='all')

    for i in range(num_groups):
        # axes[num_devices - 1, i].set_xlabel("Elapsed Time (Sec)")
        axes[0, i].set_title(group_names[i])

        # plot the changes of tuning knobs

    col_count = 0
    for group in groups:
        row_count = 0
        for device in devices:
            axes[row_count, 0].set_ylabel(
                device.replace("NVMe", "NVMe\n").replace("SATA", "SATA\n"))
            axes[row_count, col_count].plot(group[device]["secs_elapsed"], group[device]["interval_qps"], "r")
            axes[row_count, col_count].plot(group[device]["secs_elapsed"], group[device]["avg_qps"], "k--",
                                            linewidth=2.9)
            annot_size = 15
            target_row = std_df[(std_df["group"] == group_names[col_count]) & (
                    std_df["device"] == devices[row_count].replace("NVMe", "NVMe ").replace("SATA", "SATA "))]
            std_qps = str(round(target_row["qps std"].values[0] / 1000, 1))
            avg_qps = str(round(target_row["qps avg"].values[0] / 1000, 1))

            axes[row_count, col_count].annotate(
                "avg:" + avg_qps, xy=(0, 320),
                fontsize=annot_size,
                # ,int(group[device]["avg_qps"].mean())+100)
            )
            axes[row_count, col_count].annotate(
                "std_v:" + std_qps,
                xy=(1800, 320),
                fontsize=annot_size,
                color="#0000A0"
                # ,int(group[device]["avg_qps"].mean())+100)
            )
            axes[row_count, col_count].set_ylim(0, 450)
            axes[row_count, col_count].set_yticks([0, 300])
            row_count += 1
        col_count += 1

    fig.tight_layout()
    fig.subplots_adjust(bottom=0.1, left=0.08)
    fig.text(0.01, 0.20, "System Throughput (kOps/Sec)", ha='center', rotation="vertical")
    fig.text(0.5, 0.01, "Elapsed Time (Sec)", ha='center')
    fig.savefig('fig_results/speed_comparison-with-std.png')
    fig.savefig('fig_results/speed_comparison-with-std.pdf')
