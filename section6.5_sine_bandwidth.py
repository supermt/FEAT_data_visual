import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objs as go

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


def prettify_the_fig(fig, font_size=20):
    fig.update_layout(showlegend=False, font={"size": font_size}, paper_bgcolor='rgba(0,0,0,0)',
                      plot_bgcolor='rgba(0,0,0,0)')
    fig.update_layout(
        margin=go.layout.Margin(
            t=30,
            b=0,  # bottom margin
        )
    )


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


def get_plot_dict(log_dir):
    start_time = 500
    end_time = 3600
    report_dict = {}
    dirs = get_log_dirs(log_dir)

    for log_dir in dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)

        basic_info = StdoutReader(stdout_file)
        # devices.append(basic_info.device)
        qps_file = ""
        for csv_name in report_csv:
            if "_0_" in csv_name:
                qps_file = csv_name
                break
        qps_df = pd.read_csv(report_csv[0])
        qps_df = qps_df[qps_df["secs_elapsed"] < end_time]
        time_gap = qps_df["secs_elapsed"] - qps_df["secs_elapsed"].shift(1).fillna(0)
        qps_df["interval_qps"] /= (time_gap * 1000)
        avg_speed = basic_info.benchmark_results["fillrandom"][1].replace(" ops/sec", "")
        # print(avg_speed)
        qps_df["avg_qps"] = int(avg_speed) / 1000
        report_dict[basic_info.device] = qps_df
        print(qps_df)

    return report_dict


if __name__ == '__main__':
    metrics_in_std_files = []
    temp = {}
    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    Tuned_dir = "Eurosys/pm_fillrandom_dynamic_bandwidth/tuned"
    FEAT_dir = "Eurosys/pm_fillrandom_dynamic_bandwidth/FEAT"

    Tuned_changes = get_plot_dict(Tuned_dir)
    FEAT_changes = get_plot_dict(FEAT_dir)

    groups = [Tuned_changes, FEAT_changes]
    group_names = ["Tuned", "FEAT"]

    mpl.rcParams['figure.figsize'] = (8, 4)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16.5
    mpl.rcParams['font.family'] = "Arial"
    mpl.rcParams['lines.markersize'] = 10
    mpl.rcParams["legend.loc"] = "lower center"

    num_devices = len(devices)
    num_groups = len(groups)
    fig, axes = plt.subplots(num_devices, num_groups, sharey='all', sharex='all')

    for i in range(num_groups):
        axes[num_devices - 1, i].set_xlabel("Elapsed Time (Sec)")
        axes[0, i].set_title(group_names[i])

        # plot the changes of tuning knobs

    col_count = 0
    for group in groups:
        row_count = 0
        for device in devices:
            axes[row_count, 0].set_ylabel(
                device.replace("NVMeSSD", "NS").replace("SATASSD", "SD").replace("SATAHDD", "HD"))
            axes[row_count, col_count].plot(group[device]["secs_elapsed"], group[device]["interval_qps"], "r")
            axes[row_count, col_count].plot(group[device]["secs_elapsed"], group[device]["avg_qps"], "k--",
                                            linewidth=2.9)
            axes[row_count, col_count].annotate(
                round(group[device]["avg_qps"].mean(), 2), xy=(250, 300)
                # ,int(group[device]["avg_qps"].mean())+100)
            )
            axes[row_count, col_count].annotate(
                round(group[device]["interval_qps"].std(), 2), xy=(2500, 300), color="#0000A0"
                # ,int(group[device]["avg_qps"].mean())+100)
            )

            axes[row_count, col_count].set_ylim(0, 450)
            axes[row_count, col_count].set_yticks([0, 300])
            row_count += 1
        col_count += 1

    fig.tight_layout()
    plt.subplots_adjust(wspace=0.1, hspace=0)
    fig.show()
    fig.savefig('fig_results/bandwidth_limit.pdf')
