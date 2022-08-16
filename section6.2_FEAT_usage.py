import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objs as go
from cycler import cycler

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
    end_time = 600
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
        qps_df["interval_qps"] /= time_gap
        report_dict[basic_info.device] = qps_df

    return report_dict


if __name__ == '__main__':
    metrics_in_std_files = []
    temp = {}
    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    batch_size_curve = {}
    thread_number_curve = {}
    TEA_only_dir = "Eurosys/FEAT_v9/TEA"
    FEA_only_dir = "Eurosys/FEAT_v9/FEA"
    FEAT_dir = "Eurosys/FEAT_usage_version2/FEAT"

    TEA_changes = get_plot_dict(TEA_only_dir)
    FEA_changes = get_plot_dict(FEA_only_dir)
    FEAT_changes = get_plot_dict(FEAT_dir)

    mpl.rcParams['figure.figsize'] = (8, 7)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16.5
    mpl.rcParams['font.family'] = "Arial"
    mpl.rcParams['lines.markersize'] = 10
    mpl.rcParams["legend.loc"] = "lower center"
    mpl.rcParams["axes.prop_cycle"] = cycler('color', ['#00009e', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b',
                                                       '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'])

    num_devices = len(devices)
    fig, axes = plt.subplots(num_devices, 2, sharey='col', sharex='all')

    axes[0, 0].set_title("# of Threads")
    axes[0, 1].set_title("Batch Size")

    axes[num_devices - 1, 0].set_xlabel("Elapsed Time (Sec)")
    axes[num_devices - 1, 1].set_xlabel("Elapsed Time (Sec)")
    # plot the changes of tuning knobs

    row_count = 0
    for device in devices:
        TEA_line, = axes[row_count, 0].plot(TEA_changes[device]["secs_elapsed"]
                                            , TEA_changes[device]["thread_num"], "k--", alpha=0.5)
        # FEA_line, = axes[row_count, 0].plot(FEA_changes[device]["secs_elapsed"]
        #                                     , FEA_changes[device]["thread_num"], "r--")
        FEAT_line, = axes[row_count, 0].plot(FEAT_changes[device]["secs_elapsed"]
                                             , FEAT_changes[device]["thread_num"], "r")

        # axes[row_count, 1].plot(TEA_changes[device]["secs_elapsed"]
        #                         , TEA_changes[device]["batch_size"], alpha=0.6)
        FEA_line, = axes[row_count, 1].plot(FEA_changes[device]["secs_elapsed"]
                                            , FEA_changes[device]["batch_size"], "k:", alpha=0.5)
        axes[row_count, 1].plot(FEAT_changes[device]["secs_elapsed"]
                                , FEAT_changes[device]["batch_size"], "r")

        axes[row_count, 0].set_ylabel(device.replace("NVMe", "NVMe ").replace("SATA", "SATA "))
        row_count += 1
    label_list = [TEA_line, FEA_line, FEAT_line]

    plot_labels = ["TEA", "FEA", "FEAT"]
    lgd = fig.legend(label_list,
                     plot_labels,
                     ncol=3,
                     frameon=False,
                     shadow=False)
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.15)
    fig.show()
    fig.savefig('fig_results/tuning_knob_changing.pdf')
