# here is an example from online-ml/river

import matplotlib as mpl
import matplotlib.pyplot as plt

import utils.stdoutreader
from utils.feature_selection import *
from utils.log_class import LogRecorder
from utils.traversal import get_log_and_std_files, mkdir_p, get_log_dirs


def plot_cpu_util(dirs, log_prefix, output_prefix, fig_name, condition=""):
    for log_dir in dirs:
        if condition in log_dir:
            stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)

            report_df = read_report_csv_with_change_points(report_csv)
            stat_df = read_stat_csv_new(stat_csv)
            fig, (ax1, ax2) = plt.subplots(2, 1)
            ax1.plot(report_df["secs_elapsed"], report_df["interval_qps"], color="r")
            ax1.set_ylabel("qps")
            ax1.set_ylim(0, 600000)

            ax2.plot(stat_df["secs_elapsed"], stat_df["cpu_utils"], color="b")
            ax2.set_ylabel("cpu_utils")
            ax2.set_ylim(0, 1200)

            plt.tight_layout()
            # report_df[plot_features].plot(subplots=True)
            output_path = output_prefix + "/%s/" % log_dir.replace(log_prefix, "").replace("/", "_")
            mkdir_p(output_path)
            plt.savefig("{}/{}.pdf".format(output_path, fig_name), bbox_inches="tight")
            plt.savefig("{}/{}.png".format(output_path, fig_name), bbox_inches="tight")
            plt.clf()


def plot_stat(dirs, log_prefix, output_prefix, fig_name, condition=""):
    for log_dir in dirs:
        if condition in log_dir:
            stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)

            report_df = read_report_csv_with_change_points(report_csv)
            stat_df = read_stat_csv(stat_csv)
            plt.subplot(311)
            plt.plot(report_df["secs_elapsed"], report_df["interval_qps"], color="r")
            plt.ylabel("qps")
            plt.ylim(0, 600000)

            plt.subplot(312)
            plt.plot(stat_df["secs_elapsed"], stat_df["cpu_utils"], color="b")
            plt.ylabel("cpu_utils")
            plt.plot()
            plt.ylim(0, 1200)

            plt.subplot(313)
            plt.plot(stat_df["secs_elapsed"], stat_df["disk_usage"], color="c")
            # plt.plot(stat_df["secs_elapsed"], [2e7 for x in stat_df["secs_elapsed"]], color="r")
            plt.ylabel("disk_utils")
            # plt.hlines(1e7, 0, stat_df["secs_elapsed"].tolist()[-1], colors="r", linestyles="dashed")
            # plt.hlines(2e7, 0, stat_df["secs_elapsed"].tolist()[-1], colors="g", linestyles="dashed")
            # plt.hlines(3e7, 0, stat_df["secs_elapsed"].tolist()[-1], colors="b", linestyles="dashed")

            # plt.plot()
            #
            # plt.subplot(414)
            # plt.plot(report_df["secs_elapsed"], report_df["change_points"], color="g")
            # plt.ylabel(r"SST Size")
            # plt.ylim(0, 16)

            plt.tight_layout()
            # report_df[plot_features].plot(subplots=True)
            output_path = output_prefix + "/%s/" % log_dir.replace(log_prefix, "").replace("/", "_")
            mkdir_p(output_path)
            plt.savefig("{}/{}.pdf".format(output_path, fig_name), bbox_inches="tight")
            plt.savefig("{}/{}.png".format(output_path, fig_name), bbox_inches="tight")
            plt.clf()


if __name__ == '__main__':
    log_dir_prefix = "Eurosys/pm_server_increasing_threads"

    dirs = get_log_dirs(log_dir_prefix)
    rows = []
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)
        std_info = utils.stdoutreader.StdoutReader(stdout_file)
        stat_df = pd.read_csv(stat_csv)
        avg_normalized_util = stat_df["cpu_percent"].mean()
        cpu_count = std_info.cpu_count.replace("CPU", "")
        avg_normalized_util /= int(cpu_count)
        avg_normalized_util = round(avg_normalized_util, 2)
        row = [std_info.device, cpu_count, avg_normalized_util]
        rows.append(row)

    result_pd = pd.DataFrame(rows, columns=["device", "# of Threads", "Normalized CPU Utilization"])
    result_pd.to_csv("csv_results/section3_idle_resource/cpu_util.csv", sep=" ", index=False)
