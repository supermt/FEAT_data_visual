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
                if "_0_" not in csv_name:
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
    default_dir = "../FAST/section6.3_fillrandom/temp/PM_fillrandom1/default"
    SILK_dir = "../FAST/section6.3_fillrandom/temp/PM_fillrandom1/SILK"
    Tuned_dir = "../FAST/section6.3_fillrandom/temp/PM_fillrandom2/tuned"

    TEA_only_dir = "../FAST/section6.3_fillrandom/temp/PM_fillrandom3/TEA"
    FEA_only_dir = "../FAST/section6.3_fillrandom/temp/PM_fillrandom2/FEA"
    FEAT_dir = "../FAST/section6.3_fillrandom/temp/PM_fillrandom3/FEAT"
    # FEAT_warm_dir = "Eurosys/FEAT_usage_version3/FEAT"

    default_changes = get_plot_dict(default_dir)
    SILK_changes = get_plot_dict(SILK_dir, True)
    Tuned_changes = get_plot_dict(Tuned_dir)
    TEA_changes = get_plot_dict(TEA_only_dir)
    FEA_changes = get_plot_dict(FEA_only_dir)
    FEAT_changes = get_plot_dict(FEAT_dir)
    # FEAT_warm_changes = get_plot_dict(FEAT_warm_dir)

    groups = [default_changes, SILK_changes, Tuned_changes, FEAT_changes, FEA_changes, TEA_changes]
    # groups = [default_changes, SILK_changes, Tuned_changes, FEAT_changes, FEA_changes, TEA_changes, FEAT_warm_changes]
    group_names = ["Default", "SILK", "Tuned", "FEAT", "FEA", "TEA", ]
    # group_names = ["Default", "SILK", "Tuned", "FEAT", "FEA", "TEA", "FEAT_warm"]

    mpl.rcParams['figure.figsize'] = (16, 7)
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
            axes[row_count, 0].set_ylabel(device.replace("NVMe", "NVMe ").replace("SATA", "SATA "))
            axes[row_count, col_count].plot(group[device]["secs_elapsed"], group[device]["interval_qps"], "r")
            axes[row_count, col_count].plot(group[device]["secs_elapsed"], group[device]["avg_qps"], "k--",
                                            linewidth=2.9)
            axes[row_count, col_count].annotate(
                round(group[device]["avg_qps"].mean(), 1), xy=(250, 350)
                # ,int(group[device]["avg_qps"].mean())+100)
            )
            axes[row_count, col_count].annotate(
                round(group[device]["interval_qps"].std(), 1), xy=(2500, 350), color="#0000A0"
                # ,int(group[device]["avg_qps"].mean())+100)
            )
            axes[row_count, col_count].set_ylim(0, 450)
            axes[row_count, col_count].set_yticks([0, 300])
            row_count += 1
        col_count += 1

    fig.tight_layout()
    fig.text(0.5, 0.01, "Elapsed Time (Sec)", ha='center')
    fig.show()
    fig.savefig('../fig_results/speed_comparison.pdf')
