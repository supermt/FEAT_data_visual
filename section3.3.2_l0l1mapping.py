# here is an example from online-ml/river
import os.path

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from utils.feature_selection import vectorize_by_compaction_output_level, generate_lsm_shape
from utils.log_class import load_log_and_qps
from utils.traversal import get_log_and_std_files
from utils.traversal import get_log_dirs


def ks(x, pos):
    'The two args are the value and tick position'
    return '%1.1fk' % (x * 1e-3)


def plot_with_thread_num(thread_num, batch_size, start_time, end_time):
    log_dir_prefix = "Eurosys/pm_server_increasing_threads"
    dirs = get_log_dirs(log_dir_prefix)
    key_seq = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    default_setting_qps_csv = {x: None for x in key_seq}
    default_setting_lsm_state = {x: None for x in key_seq}
    default_setting_compaction_distribution = {x: None for x in key_seq}
    stall_moments = {x: None for x in key_seq}

    for log_dir in dirs:
        if str(thread_num) + "CPU" == log_dir.split(os.sep)[-2] and batch_size in log_dir:
            stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)

            media = log_dir.split(os.sep)[-3].replace("StorageMaterial.", "")
            data_set = load_log_and_qps(LOG_file, report_csv)
            stall_pd = data_set.phrase_warninglines(1)
            file_states = generate_lsm_shape(data_set)
            bucket_df = vectorize_by_compaction_output_level(data_set, 7)
            default_setting_qps_csv[media] = report_csv
            default_setting_lsm_state[media] = file_states
            # l0_stall_moment = file_states[file_states["level0"] >= 20]
            # l0_stall_moment = pd.DataFrame(l0_stall_moment, columns=["time_micro", "level0"]).reindex()
            default_setting_compaction_distribution[media] = bucket_df["l0compactions"]

            stall_moments[media] = stall_pd

    material_count = len(default_setting_qps_csv)

    fig, axes = plt.subplots(material_count, 1)
    lines_for_label = []

    marks = {"L0 Overflowing": "r.", "Redundancy Overflowing": "r.", "Memory Overflowing": "r."}
    labels = {"L0 Overflowing": None, "Redundancy Overflowing": None, "Memory Overflowing": None}

    for i in range(material_count):
        media = key_seq[i]
        qps_df = pd.read_csv(default_setting_qps_csv[media])
        throughput_line, = axes[i].plot(range(start_time, end_time),
                                        qps_df["interval_qps"][start_time:end_time] / 1000)
        # horizone, = axes[i].plot(range(0, end_time), [0] * end_time, "m--")
        axes[i].set_ylim(-100, 450)
        axes[i].set_xlim([start_time, end_time])
        axes[i].set_yticks([])
        l0compactions = default_setting_compaction_distribution[media][start_time:end_time]
        event_ax = axes[i].twinx()
        l0_l1_scatter, = event_ax.plot(range(start_time, end_time), (l0compactions >= 1) * 3, "r.")
        stalls = stall_moments[media]

        # stall_value = l0_stalls["level0"] >= 20
        # mark_height = 3
        # for mark in marks:
        #     line_label, = event_ax.plot(stalls["time sec"], (stalls["overflowing"] == mark) * mark_height, marks[mark])
        #     labels[mark] = line_label
        #     mark_height += 1

        event_ax.set_ylim(2, 10)
        axes[i].set_yticks([0, 300])  # ["L0 Comp", "LO", "RO", "MO"])
        # axes[i].set_ylabel("kOps/sec")
        event_ax.set_yticks([])
        event_ax.set_ylabel(media.replace("SATA", "SATA ").replace("NVMe", "NVMe "))
        event_ax.set_xlim(start_time, end_time)
        # temp_ax.set_ylabel("l0 com")
        # axes[i].set_title(key_seq[i].replace("SATA", "SATA ").replace("NVMe", "NVMe "))
    axes[i].set_xlabel("elapsed time (sec)", fontsize=20)

    fig.text(0, 0.5, 'System Throughput (kOps/Sec)', va='center', rotation='vertical')
    plt.tight_layout()

    # lgd = ax.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, -0.1))
    label_list = [throughput_line, l0_l1_scatter]
    label_list.extend(list(labels.values()))

    print(label_list)
    # plt.xlim([start_time, end_time])
    lgd = fig.legend(label_list,
                     ["Throughput", "L0-L1 compaction", ],
                     #  "Data overflowing", "Redundancy overflowing",                      "Memory overflowing"],
                     ncol=3, frameon=False,
                     shadow=False)
    fig.subplots_adjust(bottom=0.2)
    fig.show()
    plt.savefig("fig_results/%d_threads_mapping_%d_to_%d.pdf" % (thread_num, start_time, end_time))
    plt.savefig("fig_results/%d_threads_mapping_%d_to_%d.png" % (thread_num, start_time, end_time))


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (10, 6)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16
    mpl.rcParams['lines.markersize'] = 10
    mpl.rcParams["legend.loc"] = "lower center"

    # plot_with_thread_num(2, "64MB", 0, 600)
    plot_with_thread_num(20, "64MB", 0, 600)
