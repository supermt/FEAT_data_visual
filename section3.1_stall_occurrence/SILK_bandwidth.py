import matplotlib.pyplot as plt

from utils.traversal import get_log_dirs, get_log_and_std_files
from utils.stdoutreader import StdoutReader
from utils.log_class import LogRecorder
import pandas as pd

STALL_REASON = ["memtable", "level0", "pending_compaction_bytes"]

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


def extract_stall_and_duration(input_dir, target_cpu='8CPU', target_batch="64MB"):
    input_dirs = get_log_dirs(input_dir)

    stall_dict = {}

    stall_duration = {}

    throughput = {}

    for log_dir in input_dirs:
        if target_cpu in log_dir and target_batch in log_dir:
            stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
            basic_info = StdoutReader(stdout_file)
            stall_dict[basic_info.device] = aggreate_stall_type(basic_info.stall_reasons)
            stall_duration[basic_info.device] = basic_info.stall_duration

            throughput[basic_info.device] = (basic_info.benchmark_results["fillrandom"][1].split(" ")[0])

    return [stall_dict, stall_duration, throughput]


if __name__ == '__main__':

    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    LOG_DIR = "../FAST/PM_SILK_bandwidth/"
    log_dirs = get_log_dirs(LOG_DIR)

    fig, axes = plt.subplots(4, 2)
    device_map = {"PM": 0, "NVMe SSD": 1, "SATA SSD": 2, "SATA HDD": 3}

    for log_dir in log_dirs:
        print(log_dir)
        stdout_file, LOG_file, report_csv, pid_stat_file, iostat_text = get_log_and_std_files(log_dir,
                                                                                              with_stat_csv=True,
                                                                                              splitted_iostat=True,
                                                                                              multi_tasks=False)

        stat_df = pd.read_csv(pid_stat_file)
        std_info = StdoutReader(stdout_file)
        print(std_info.device.replace("SSD",))
        MBPS = (stat_df['disk_write_bytes'].shift(-1) - stat_df['disk_write_bytes']) / 1000000
        LOG_decoded = LogRecorder(LOG_file)

        # print(LOG_decoded.phrase_warninglines()["overflowing"][0])

    plt.show()
    # result_df = pd.DataFrame(result_list,
    #                          columns=["Device", "batch size", "Tuner", "Stall Duration", "Throughput", "MO", "LO",
    #                                   "RO"])
    # result_df.to_csv("csv_results/section3_SILK_compare_stall_duration.csv", index=False, sep="\t")
