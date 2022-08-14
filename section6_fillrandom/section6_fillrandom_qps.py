# here is an example from online-ml/river
import utils.stdoutreader
from utils import stdoutreader
from utils.stdoutreader import *
from utils.traversal import get_log_and_std_files, get_log_dirs

import pandas as pd

if __name__ == '__main__':

    groups = ["tuned", 'default']
    base_dir_prefix = "../FAST/section6.3_fillrandom/"
    suffixs = ["1", "2", "3"]
    devices = ["PM", "NVMe SSD", "SATA SSD", "SATA HDD"]

    rows = []
    for group in groups:
        yerror_dict = {x: [] for x in devices}
        for suffix in suffixs:
            log_dir_prefix = base_dir_prefix + group + "/" + group + suffix
            print(log_dir_prefix)
            dirs = get_log_dirs(log_dir_prefix)
            for log_dir in dirs:
                stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)
                std_info = stdoutreader.StdoutReader(stdout_file)
                cpu = std_info.cpu_count
                device = utils.stdoutreader.format_device(std_info.device)
                fillrandom_speed = std_info.get_benchmark_ops("fillrandom")
                yerror_dict[device].append(fillrandom_speed)

        for device in yerror_dict:
            mean = round(sum(yerror_dict[device]) / len(yerror_dict[device]), 2)
            std = sum([round(x - mean, 2) for x in yerror_dict[device]]) / len(yerror_dict)
            row = [group, device, mean, round(std, 2)]
            rows.append(row)

    columns = ["group", "device", "average", "standard err"]

    result_pd = pd.DataFrame(rows, columns=columns)
    result_pd.to_csv("../csv_results/fillrandom/qps.csv", sep="\t", index=False)
