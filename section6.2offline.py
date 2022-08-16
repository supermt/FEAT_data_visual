import pandas as pd

from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_dirs, get_log_and_std_files

STALL_REASON = ["memtable", "level0", "pending_compaction_bytes"]

color_map = {"PM": "rgb(68,114,196)", "NVMeSSD": "rgb(237,125,49)", "SATASSD": "rgb(165,165,165)",
             "SATAHDD": "rgb(255,192,0)"}

if __name__ == '__main__':

    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    base_log_prefix = "Eurosys/offline_results/offline_tuning_results"
    suffixs = ['', '_1', '_2']
    result_dfs = []
    for suffix in suffixs:
        input_log_dirs = base_log_prefix + suffix
        dirs = get_log_dirs(input_log_dirs)
        result = []
        for dir in dirs:
            stdout_file, LOG_file, report_csv = get_log_and_std_files(dir)
            std_result = StdoutReader(stdout_file)
            result.append([std_result.device.replace("SSD", " SSD").replace("HDD", " HDD"),
                           std_result.cpu_count.replace("CPU", ""),
                           std_result.batch_size.replace("MB", ""),
                           std_result.benchmark_results["fillrandom"][1].replace(" ops/sec", "")
                           ])
        result_df = pd.DataFrame(result, columns=["Device", "Number of Threads", "Batch Size", "System Throughput"])
        result_dfs.append(result_df)
        # result_df.to_csv("csv_results/offline_throughput.csv", index=False, sep="\t")

    df = result_df[["Device", "Number of Threads", "Batch Size"]]
    df["throughput mean"] = 0
    i = 0
    throughput_column_list = []
    column_list = []

    for result_df in result_dfs:
        column_prefix = "System Throughput"
        column = result_df[column_prefix]
        # throughput_column_list.append(column.values.tolist())
        df[column_prefix + str(i)] = column
        column_list.append(column_prefix + str(i))
        df["throughput mean"] += column.astype(int)
        i += 1
    # print(throughput_column_list)
    df["throughput mean"] /= i
    df["throughput mean"] = df["throughput mean"].round(2)

    df.to_csv("csv_results/offline_throughput.csv", index=False, sep="\t")
