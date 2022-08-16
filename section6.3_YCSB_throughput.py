import pandas as pd

from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_dirs, get_log_and_std_files

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


def extract_stall_and_duration(input_dir):
    input_dirs = get_log_dirs(input_dir)

    stall_dict = {}

    stall_duration = {}
    load_throughput = {}
    run_throughput = {}

    for log_dir in input_dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
        basic_info = StdoutReader(stdout_file)
        stall_dict[basic_info.device] = aggreate_stall_type(basic_info.stall_reasons)
        stall_duration[basic_info.device] = basic_info.stall_duration
        load_throughput[basic_info.device] = basic_info.benchmark_results["ycsb_load"]
        run_throughput[basic_info.device] = basic_info.benchmark_results["ycsb_run"]
    return [stall_dict, stall_duration, load_throughput, run_throughput]


if __name__ == '__main__':

    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    base_log_prefix = "Eurosys/pm_server_ycsb_60+60/"

    workloads = ["a", 'b', 'c', 'd', 'e', 'f']
    result_list = []
    for workload in workloads:
        YCSB_log_prefix = base_log_prefix
        target_map = {
            "YCSB_Default_1000": extract_stall_and_duration(
                YCSB_log_prefix + "default/" + workload),
            "YCSB_Tuned_1000": extract_stall_and_duration(
                YCSB_log_prefix + "tuned/" + workload),
            "YCSB_FEAT_1000": extract_stall_and_duration(YCSB_log_prefix + "FEAT/" + workload),
            "YCSB_SILK_1000": extract_stall_and_duration(YCSB_log_prefix + "SILK/" + workload)
        }

        for i in range(len(devices)):

            device = devices[i]
            row_head = [device, workload]

            for set_name in target_map:
                row = []
                tuner = set_name.split("_")[1]

                result_pack = target_map[set_name]

                duration = result_pack[1][device].split(" ")[0].split(":")
                duration = float(duration[2]) + int(duration[1]) * 60 + int(duration[0]) * 3600
                duration = round(duration, 2)

                row = [tuner, duration]

                load_performance = result_pack[2][device]
                run_performance = result_pack[3][device]

                row.append(load_performance[1].replace(" ops/sec", ""))
                row.append(run_performance[1].replace(" ops/sec", ""))
                row_head.extend(row)

            result_list.append(row_head)

    columns = ["Device", "workload"]
    for entry in target_map:
        tuner_name = entry.split("_")[1]
        columns.extend([tuner_name, tuner_name + " Stall Duration", tuner_name + " Load", tuner_name + " Run"])

    result_df = pd.DataFrame(result_list, columns=columns
                             )
    result_df.to_csv("csv_results/ycsb_report/system_throughput.csv", index=False, sep=" ")
