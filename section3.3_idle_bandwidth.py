# here is an example from online-ml/river
import gzip

import pandas as pd

from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_and_std_files, mkdir_p
from utils.traversal import get_log_dirs

PM_device_mapper = {
    "SATAHDD": "sdc",
    "SATASSD": "sda",
    "NVMeSSD": "nvme0n1",
    "PM": "pmem1"
}

IOSTAT_COLUMN_NAMES = "Device             tps    MB_read/s    MB_wrtn/s    MB_read    MB_wrtn"
IOSTAT_COLUMN = IOSTAT_COLUMN_NAMES.split()


def count_for_one_case(target_file_dir="", case_name=""):
    log_dir_prefix = target_file_dir
    average_MPBS_pd = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        print(log_dir)
        stdout_file, LOG_file, report_csv, stat_csv, iostat_text = get_log_and_std_files(log_dir, True, True)

        std_result = StdoutReader(stdout_file)
        stat_df = pd.read_csv(stat_csv)
        #
        # if ".gz" in iostat_text:
        #     iostat_lines = gzip.open(iostat_text, "r").readlines()
        #     iostat_lines = [x.decode('utf-8') for x in iostat_lines]
        # else:
        #     iostat_lines = open(iostat_text, "r", encoding="utf-8").readlines()
        # IOSTAT = []
        # for line in iostat_lines:
        #     if PM_device_mapper[std_result.device] in line:
        #         IOSTAT.append(line.split())
        # IOSTAT = pd.DataFrame(IOSTAT, columns=IOSTAT_COLUMN)
        # MBPS = IOSTAT["MB_wrtn/s"].astype(float)
        MBPS = (stat_df['disk_write_bytes'].shift(-1) - stat_df['disk_write_bytes']) / 1000000

        data_row = [std_result.device, int(std_result.cpu_count.replace("CPU", "")), std_result.batch_size,
                    round(MBPS.mean(), 2), round(MBPS.max(), 2)]
        average_MPBS_pd.append(data_row)

    average_MPBS_pd = pd.DataFrame(average_MPBS_pd, columns=["device", "cpu", "batch size", "avg", "max"])
    average_MPBS_pd.sort_values(by=["cpu", "device"])
    average_MPBS_pd.to_csv("csv_results/section3_idle_resource/bandwidth/" + case_name + ".csv", index=False, sep=" ")
    pass


if __name__ == '__main__':
    count_for_one_case("Eurosys/pm_server_increasing_threads", "3600s_64MB_traverse")
    # count_for_one_case("Eurosys/pm_server_SILK_dead_lock_test", "SILK_thread_traverse")
    count_for_one_case("Eurosys/pm_server_512MB_traverse", "3600s_512MB_traverse")
