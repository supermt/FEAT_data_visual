from utils.traversal import get_log_dirs, get_log_and_std_files
from utils.stdoutreader import StdoutReader
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


def stall_string_to_duration_secs(duration):
    duration = duration.split(" ")[0].split(":")
    duration = float(duration[2]) + int(duration[1]) * 60 + int(duration[0]) * 3600
    duration = round(duration, 2)
    return duration


if __name__ == '__main__':

    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    LOG_DIR = "Eurosys/pm_server_increasing_batch/"
    input_dirs = get_log_dirs(LOG_DIR)

    result_list = []

    for log_dir in input_dirs:
        print(log_dir)
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
        basic_info = StdoutReader(stdout_file)
        stall_list = aggreate_stall_type(basic_info.stall_reasons)
        stall_duration = basic_info.stall_duration
        stall_duration = stall_string_to_duration_secs(stall_duration)
        result_row = [basic_info.device.replace("SSD", " SSD").replace("HDD", " HDD"),
                      basic_info.cpu_count.replace("CPU", ""),
                      basic_info.batch_size.replace("MB", ""),
                      stall_duration,
                      sum(stall_list.values())
                      ]
        result_row.extend(stall_list.values())
        result_list.append(result_row)
        # print(stall_duration)
    result_df = pd.DataFrame(result_list,
                             columns=["device", "# of thread", "batch size", 'duration', "occurrence", "mo", 'lo',
                                      'ro'])
    result_df.to_csv("csv_results/section5_3d_plotting.csv", index=False, sep="\t")
    # result_list = []
    # for i in range(len(devices)):
    #     for set_name in target_map:
    #         device = devices[i]
    #         tuner = set_name.split("_")[0]
    #         target_batch_size = set_name.split("_")[1]
    #
    #         # transform into secs
    #         duration = target_map[set_name][1][device].split(" ")[0].split(":")
    #         duration = float(duration[2]) + int(duration[1]) * 60 + int(duration[0]) * 3600
    #         duration = round(duration, 2)
    #
    #         row = [device.replace("SSD", " SSD").replace("HDD", " HDD"), target_batch_size, tuner, duration]
    #         throughput = target_map[set_name][2][device]
    #         row.append(throughput)
    #         row.extend(target_map[set_name][0][device].values())
    #         result_list.append(row)
    # result_df = pd.DataFrame(result_list,
    #                          columns=["Device", "batch size", "Tuner", "Stall Duration", "Throughput", "MO", "LO",
    #                                   "RO"])
    # result_df.to_csv("csv_results/section3_SILK_compare_stall_duration.csv", index=False, sep="\t")
