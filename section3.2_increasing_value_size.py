# here is an example from online-ml/river
import os

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from utils.log_class import LogRecorder
from utils.traversal import get_log_and_std_files
from utils.traversal import get_log_dirs


def load_log_and_qps(log_file, ground_truth_csv):
    # load the data
    return LogRecorder(log_file, ground_truth_csv)


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (12, 10)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16

    log_dir_prefix = "Eurosys/pm_server_default_increasing_value_size/"
    dirs = get_log_dirs(log_dir_prefix)
    key_seq = ["PM", "NVMe SSD", "SATA SSD", "SATA HDD"]
    media_list = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    media_dict = dict(zip(media_list, key_seq))
    value_list = [100, 500, 1000, 4000]
    col_num = len(value_list)

    default_setting_qps_csv = {x: None for x in key_seq}
    material_count = len(default_setting_qps_csv)

    fig, axes = plt.subplots(material_count, col_num, sharey=True, sharex=True)

    for col in range(col_num):
        default_setting_qps_csv = {x: None for x in key_seq}
        for log_dir in dirs:
            if ("1CPU" in log_dir or "2CPU" in log_dir) and "64MB" in log_dir and value_list[col] == \
                    int(log_dir.split(os.sep)[-4]):
                stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
                media = log_dir.split(os.sep)[-3].replace("StorageMaterial.", "")
                media = media_dict[media]
                default_setting_qps_csv[media] = report_csv
                print(report_csv)

        for i in range(material_count):
            media = key_seq[i]
            qps_df = pd.read_csv(default_setting_qps_csv[media])
            qps_df = qps_df[qps_df["secs_elapsed"] < 600]
            axes[i, col].plot(qps_df["secs_elapsed"], qps_df["interval_qps"] / 1000)
            axes[i, col].set_ylim(0, 450)

            value_size_string = int(value_list[col])
            fig_title = key_seq[i] + "\n" + str(value_size_string) + " bytes"
            axes[i, col].set_title(fig_title)

            if col == 0:
                axes[i, col].set_ylabel("kOps/Sec", fontsize=20)
            if i == 3:
                axes[i, col].set_xlabel("Elapsed time (Sec)", fontsize=20)
            plt.tight_layout()

    plt.savefig("fig_results/section3.2/motivation-rate-limiting-qps-600.pdf")
    plt.savefig("fig_results/section3.2/motivation-rate-limiting-qps-600.png")
