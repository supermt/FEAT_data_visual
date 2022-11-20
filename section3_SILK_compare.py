from utils.traversal import get_log_dirs, get_log_and_std_files
from utils.stdoutreader import StdoutReader
import pandas as pd
import gzip

IOSTAT_COLUMN_NAMES = "Device             tps    MB_read/s    MB_wrtn/s    MB_read    MB_wrtn"
IOSTAT_COLUMN = IOSTAT_COLUMN_NAMES.split()

PM_device_mapper = {
    "SATAHDD": "sdc",
    "SATASSD": "sda",
    "NVMeSSD": "nvme0n1",
    "PM": "pmem0"
}
if __name__ == '__main__':

    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    LOG_DIR = "FAST/PM_SILK_bandwidth/"
    log_dirs = get_log_dirs(LOG_DIR)

    rows = []
    for log_dir in log_dirs:
        stdout_file, LOG_file, report_csv, stat_csv, iostat_text = get_log_and_std_files(log_dir, True, True)
        std_result = StdoutReader(stdout_file)
        stat_df = pd.read_csv(stat_csv)

        if ".gz" in iostat_text:
            iostat_lines = gzip.open(iostat_text, "r").readlines()
            iostat_lines = [x.decode('utf-8') for x in iostat_lines]
        else:
            iostat_lines = open(iostat_text, "r", encoding="utf-8").readlines()
        IOSTAT = []
        for line in iostat_lines:
            if PM_device_mapper[std_result.device] in line:
                IOSTAT.append(line.split())
        IOSTAT = pd.DataFrame(IOSTAT, columns=IOSTAT_COLUMN)
        MBPS = IOSTAT["MB_wrtn/s"].astype(float)
        stall_duration = std_result.stall_duration_sec
        data_row = ["SILK", std_result.device.replace("SSD", " SSD").replace("HDD", " HDD"),
                    int(std_result.cpu_count.replace("CPU", "")), std_result.batch_size,
                    round(MBPS.mean(), 2), round(MBPS.max(), 2), stall_duration]
        print(data_row)
        rows.append(data_row)

    LOG_DIR = "Eurosys/pm_server_increasing_batch/1"
    log_dirs = get_log_dirs(LOG_DIR)

    for log_dir in log_dirs:
        if ("8CPU" in log_dir and "18CPU" not in log_dir) and ("64MB" in log_dir or "512MB" in log_dir):
            stdout_file, LOG_file, report_csv, stat_csv, iostat_text = get_log_and_std_files(log_dir, True, True)
            std_result = StdoutReader(stdout_file)
            stat_df = pd.read_csv(stat_csv)

            if ".gz" in iostat_text:
                iostat_lines = gzip.open(iostat_text, "r").readlines()
                iostat_lines = [x.decode('utf-8') for x in iostat_lines]
            else:
                iostat_lines = open(iostat_text, "r", encoding="utf-8").readlines()
            IOSTAT = []
            for line in iostat_lines:
                if PM_device_mapper[std_result.device] in line:
                    IOSTAT.append(line.split())
            IOSTAT = pd.DataFrame(IOSTAT, columns=IOSTAT_COLUMN)
            MBPS = IOSTAT["MB_wrtn/s"].astype(float)
            stall_duration = std_result.stall_duration_sec
            data_row = ["RocksDB", std_result.device.replace("SSD", " SSD").replace("HDD", " HDD"),
                        int(std_result.cpu_count.replace("CPU", "")),
                        std_result.batch_size,
                        round(MBPS.mean(), 2), round(MBPS.max(), 2), stall_duration]
            rows.append(data_row)

    result_df = pd.DataFrame(rows,
                             columns=["group", "device", "threads", "batch size", "avg bandwidth", "peak bandwidth",
                                      "stall duration"])

    result_df.to_csv("csv_results/section3_idle_resource/SILK_resource.csv", index=False, sep="\t")
