# here is an example from online-ml/river
import os

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from utils.traversal import get_log_and_std_files
from utils.traversal import get_log_dirs

if __name__ == '__main__':

    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16
    mpl.rcParams['font.family'] = "Arial"
    mpl.rcParams['lines.markersize'] = 10
    mpl.rcParams["legend.loc"] = "lower center"

    log_dir_prefix = "Eurosys/pm_results_rate_limiting_600_1000_bytes/"
    dirs = get_log_dirs(log_dir_prefix)
    key_seq = ["PM", "NVMe SSD", "SATA SSD", "SATA HDD"]
    media_list = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    media_dict = dict(zip(media_list, key_seq))
    rate_list = ['15000000', '30000000', "NoLimit"]
    row_count = len(rate_list)

    default_setting_qps_csv = {x: None for x in key_seq}
    material_count = len(default_setting_qps_csv)

    fig, axes = plt.subplots(row_count, material_count, sharey='row', sharex='all')

    for row in range(row_count):
        default_setting_qps_csv = {x: None for x in key_seq}
        for log_dir in dirs:
            if ("1CPU" in log_dir or "2CPU" in log_dir) and "64MB" in log_dir and rate_list[row] == \
                    log_dir.split(os.sep)[-4]:
                stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
                media = log_dir.split(os.sep)[-3].replace("StorageMaterial.", "")
                media = media_dict[media]
                default_setting_qps_csv[media] = report_csv
                print(report_csv)

        for i in range(material_count):
            print(i, row)
            media = key_seq[i]
            qps_df = pd.read_csv(default_setting_qps_csv[media])
            qps_df = qps_df[qps_df["secs_elapsed"] < 600]
            axes[row, i].plot(qps_df["interval_qps"] / 1000)
            # axes[row, i].set_ylim(0, 450)
            if rate_list[row] != "NoLimit":
                throughput_mbps = int(rate_list[row]) / 1000000
                fig_title = key_seq[i] + "\n" + str(throughput_mbps) + " MB/s"
            else:
                throughput_mbps = "No Limit"
                fig_title = key_seq[i] + "\n" + str(throughput_mbps)
            axes[row, i].set_title(fig_title)
            if i == 0:
                axes[row, i].set_ylabel("kOps/sec", fontsize=20)
            if row == 3:
                axes[row, i].set_xlabel("elapsed time (sec)", fontsize=20)
        plt.tight_layout()

        plt.savefig("fig_results/motivation-rate-limiting-qps-600.pdf")
        plt.savefig("fig_results/motivation-rate-limiting-qps-600.png")
