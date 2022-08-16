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
    p99 = {}
    p999 = {}
    p9999 = {}

    for log_dir in input_dirs:
        print(log_dir)
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
        basic_info = StdoutReader(stdout_file)
        stall_dict[basic_info.device] = aggreate_stall_type(basic_info.stall_reasons)
        stall_duration[basic_info.device] = basic_info.stall_duration
        load_throughput[basic_info.device] = basic_info.benchmark_results["fillrandom"]
        p99[basic_info.device] = basic_info.fillrandom_hist["P99"]
        p999[basic_info.device] = basic_info.fillrandom_hist["P99.9"]
        p9999[basic_info.device] = basic_info.fillrandom_hist["P99.99"]

    return [stall_dict, stall_duration, load_throughput, p99, p999, p9999]


if __name__ == '__main__':

    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    base_log_prefix = "Eurosys/FEAT_usage_version2/"

    tuners = ["baseline", "SILK", "tuned", "FEAT", "FEA", "TEA"]
    result_list_plain = []
    result_list = []
    target_map = {}
    for tuner in tuners:
        log_prefix = base_log_prefix
        target_map.update({"stress_" + tuner + "_1000": extract_stall_and_duration(
            log_prefix + tuner)})

    for i in range(len(devices)):
        device = devices[i]
        row_head = [device.replace("SSD", " SSD").replace("HDD", " HDD")]

        for set_name in target_map:
            row = [device.replace("SSD", " SSD").replace("HDD", " HDD")]
            result_pack = target_map[set_name]
            duration = result_pack[1][device].split(" ")[0].split(":")
            duration = float(duration[2]) + int(duration[1]) * 60 + int(duration[0]) * 3600
            duration = round(duration, 2)
            row_plain = [duration]

            throughput = result_pack[2][device]
            ops = throughput[1].replace(" ops/sec", "")
            row_plain.append(ops)
            p99 = result_pack[3][device]
            p999 = result_pack[4][device]
            p9999 = result_pack[5][device]

            row_plain.extend([float(p99), float(p999), float(p9999)])

            row.extend([duration, ops, float(p99), float(p999), float(p9999)])
            row.append(set_name.split("_")[1].replace("baseline", "Default").replace("tuned", "Tuned"))
            result_list.append(row)
            row_head.extend(row_plain)
        result_list_plain.append(row_head)

    columns = ["Device"]
    for entry in target_map:
        tuner_name = entry.split("_")[1]
        columns.extend([tuner_name + " Stall Duration",
                        tuner_name + " Throughput", tuner_name + "p99",
                        tuner_name + "p999",
                        tuner_name + "p99.99"])

    result_df = pd.DataFrame(result_list_plain, columns=columns)
    result_df.to_csv("csv_results/FEAT_p99_latency_and_stall_plain.csv", index=False, sep="\t")

    result_df = pd.DataFrame(result_list,
                             columns=["Device", "Stall Duration", "Throughput", "P99", "P99.9", "P99.99", "Tuner"])
    result_df.to_csv("csv_results/FEAT_p99_latency_and_stall.csv", index=False, sep="\t")
