import os
import re
import pathlib

BENCHMARK_RESULT_REGEX = r"[a-zA-z]+\s*:\s*[0-9]+\.[0-9]+\smicros\/op\s[0-9]+\sops\/sec;.*"
LEVEL_SIZE_RESULT_REGEX = r".*L[0-9]\s+[0-9]+\/[0-9]+\s+[0-9]+\.[0-9]+ (GB|MB).*"

COMPACTION_STAT_METRICS = ["Flush(GB)", "Cumulative compaction", "Interval compaction", "Stalls(count)"]

DB_STAT_METRICS = ["Cumulative writes"]

COMPACTION_COLUMN_NAME_MAP = {"GB write": "write size", "MB/s write": "write throughput",
                              "GB read": "read size", "MB/s read": "read throughput",
                              "seconds": "time"}

READING_LINE = "** Level"

STATISTICS_MEASURES = ["rocksdb.block.cache.miss", "rocksdb.block.cache.hit", "rocksdb.block.cache.add",
                       "rocksdb.db.mutex.wait.micros"]

WRITE_LATENCY = "Microseconds per write:"
READ_LATENCY = "Microseconds per read:"
UPDATE_LATENCY = "Microseconds per update:"
SEEK_LATENCY = "Microseconds per seek:"

STALL_INFLUENCE_HEADERS = ["max pending bytes", "stops with pending bytes", "stops with Total SST file size"]


def get_level_size_entry(line):
    line_splitter = line.split()
    data = line_splitter[2]
    unit = line_splitter[3]
    data = float(data)
    if unit == "MB":
        data /= 1000

    data = round(data, 2)
    return {line_splitter[0]: data}


def get_benchmark_entry(line):
    benchmark = line.split(":")[0].replace(" ", "")
    metrics_reg = r"[0-9]+(\.)*[0-9]+\s[a-zA-Z]+\/[a-zA-Z]+"
    matches = re.finditer(metrics_reg, line, re.MULTILINE)
    metrics = []
    end_num = 0
    for matchNum, match in enumerate(matches, start=1):
        metrics.append(match.group())
        end_num = match.end()

    if end_num == len(line):
        notes = ""
    else:
        notes = line[end_num:]
    metrics.append(notes)
    return {benchmark: metrics}


def get_db_stat_entry(line):
    regex = r"([0-9]+\.)*[0-9]+[a-zA-Z]*\swrites"
    matches = re.search(regex, line, re.M | re.I)
    total_write_op = matches.group()[:-7]
    ingest_metrics = line.split("ingest:")[1].split(",")
    temp_list = ingest_metrics[0].split(" ")
    total_wirte_size = float(temp_list[1])
    total_wirte_unit = temp_list[2]
    if total_wirte_unit == "GB":
        total_wirte_size *= 1024
    elif total_wirte_unit == "MB":
        total_wirte_size *= 1
    return {"total_write_op": total_write_op, "total_write_size(MB)": total_wirte_size}


def get_compaction_stat_entry(line, entry_name):
    line = line.replace(entry_name + ":", "")
    if entry_name == "Flush(GB)":
        return {entry_name: float(line.split(" ")[2][:-1])}
    elif entry_name == "Stalls(count)":
        result_map = {}
        different_stalls = line.split(",")[:-1]
        for stall in different_stalls:
            stall_num_matches = re.search(r"[0-9]+", stall)
            stall_num = int(stall_num_matches.group(0))
            column_name = stall[stall_num_matches.end(0) + 1:]
            result_map[column_name] = stall_num
        return result_map
    else:
        # Cumulative compaction and Interval compaction
        result_map = {}
        line = line.replace(entry_name + ":", "")
        compaction_metrics = line.split(",")
        for metrics in compaction_metrics:
            match = re.search(r"([0-9]+\.)*[0-9]+", metrics)
            numeric_data = float(match.group(0))
            column_name = metrics[match.end(0) + 1:]
            column_name = entry_name + " " + COMPACTION_COLUMN_NAME_MAP[column_name]
            result_map[column_name] = numeric_data
        return result_map


def process_stat_line(line):
    result_map = {}

    regex = r"[a-zA-Z]+[0-9]*(\.[0-9]+)*:\s([0-9]+\.)*[0-9]+"
    matches = re.finditer(regex, line, re.MULTILINE)

    for matchNum, match in enumerate(matches, start=1):
        result_entry = match.group().split(":")
        result_map[result_entry[0]] = result_entry[1]
    return result_map


class StdoutReader:

    def split_the_file(self):
        line_counter = 0

        for line in self.filelines:
            matchObj_benchmark = re.match(BENCHMARK_RESULT_REGEX, line)
            matchObj_level_size = re.match(LEVEL_SIZE_RESULT_REGEX, line)
            if matchObj_benchmark:
                self.line_map["benchmarks"].append(line_counter)
                self.benchmark_results.update(get_benchmark_entry(line))

            if matchObj_level_size:
                self.line_map["level_size"].append(line_counter)
                self.level_size_results.update(get_level_size_entry(line))

            for compaction_metrics in COMPACTION_STAT_METRICS:
                if compaction_metrics in line:
                    self.line_map["compaction_stat"].append(line_counter)
                    temp_map = get_compaction_stat_entry(line, compaction_metrics)
                    if compaction_metrics == "Stalls(count)":
                        self.stall_reasons = temp_map
                    self.tradeoff_data.update(temp_map)

            for db_metrics in DB_STAT_METRICS:
                if db_metrics in line:
                    self.line_map["db_stats"].append(line_counter)
                    self.tradeoff_data.update(get_db_stat_entry(line))

            if WRITE_LATENCY in line:
                self.fillrandom_hist.update(process_stat_line(self.filelines[line_counter + 1]))
                self.fillrandom_hist.update(process_stat_line(self.filelines[line_counter + 2]))
                self.fillrandom_hist.update(
                    process_stat_line(
                        self.filelines[line_counter + 3].replace("Percentiles: ", "")))  # remove useless header part
                temp_keys = self.fillrandom_hist.keys()
                temp_values = self.fillrandom_hist.values()
                self.hist_job_list.append(["write", list(temp_keys), list(temp_values)])

            if READ_LATENCY in line:
                self.readrandom_hist.update(process_stat_line(self.filelines[line_counter + 1]))
                self.readrandom_hist.update(process_stat_line(self.filelines[line_counter + 2]))
                self.readrandom_hist.update(
                    process_stat_line(
                        self.filelines[line_counter + 3].replace("Percentiles: ", "")))  # remove useless header part
                temp_keys = self.readrandom_hist.keys()
                temp_values = self.readrandom_hist.values()
                self.hist_job_list.append(["read", list(temp_keys), list(temp_values)])

            if SEEK_LATENCY in line:
                self.seek_hist.update(process_stat_line(self.filelines[line_counter + 1]))
                self.seek_hist.update(process_stat_line(self.filelines[line_counter + 2]))
                self.seek_hist.update(
                    process_stat_line(
                        self.filelines[line_counter + 3].replace("Percentiles: ", "")))  # remove useless header part
                temp_keys = self.seek_hist.keys()
                temp_values = self.seek_hist.values()
                self.hist_job_list.append(["seek", list(temp_keys), list(temp_values)])

            if UPDATE_LATENCY in line:
                self.updaterandom_hist.update(process_stat_line(self.filelines[line_counter + 1]))
                self.updaterandom_hist.update(process_stat_line(self.filelines[line_counter + 2]))
                self.updaterandom_hist.update(
                    process_stat_line(
                        self.filelines[line_counter + 3].replace("Percentiles: ", "")))  # remove useless header part
                temp_keys = self.updaterandom_hist.keys()
                temp_values = self.updaterandom_hist.values()
                self.hist_job_list.append(["update", list(temp_keys), list(temp_values)])

            if "Cumulative stall:" in line:
                self.stall_duration = line.split(",")[0].replace("Cumulative stall: ", "")
            if READING_LINE in line:
                level_key = "level" + line[9]
                self.read_latency_map[level_key].update(process_stat_line(self.filelines[line_counter + 1]))
            for measure in STATISTICS_MEASURES:
                if measure + " COUNT" in line:
                    self.tradeoff_data[measure] = int(line.split(":")[1][1:])

            for stall_impact in STALL_INFLUENCE_HEADERS:
                if stall_impact in line:
                    self.stall_influence_data[stall_impact] = int(line.split(":")[1])
            line_counter += 1
        pass

    def read_the_compaction_waiting_time(self):
        thread_pri_list_range = {}

        i = 0
        while i < len(self.filelines):
            if "Thread States for Pool Priority:" in self.filelines[i]:
                key = self.filelines[i].split(":")[-1].replace(" ", "")
                thread_pri_list_range[key] = 0
            if "micro seconds waiting for next mission" in self.filelines[i]:
                thread_pri_list_range[key] = i + 1
            i += 1
        pass

        plot_threads = ["Low", "High"]
        end_plots = ["L0", "User"]
        start_line = [thread_pri_list_range[x] for x in plot_threads]
        print(start_line)
        results = {}
        for temp_line in self.filelines[start_line[0]:]:
            split_list = temp_line.replace(" ", "").split(":")
            if len(split_list) == 1:
                break
            else:
                key = split_list[0]
                value = int(split_list[1])
                if "compaction" + key not in results:
                    results["compaction" + key] = []

            results["compaction" + key].append(value)

        for temp_line in self.filelines[start_line[1]:]:
            split_list = temp_line.replace(" ", "").split(":")
            if len(split_list) == 1:
                break
            else:
                key = split_list[0]
                value = int(split_list[1])
                if "flush" + key not in results:
                    results["flush" + key] = []

            results["flush" + key].append(value)

        return results

    def __init__(self, input_file, with_hist=False, multi_tasks=False):
        input_file = pathlib.Path(input_file).absolute()
        input_file = str(input_file)
        self.stdout_file = input_file
        self.filelines = [x.replace("\n", "") for x in open(input_file, "r").readlines()]
        self.benchmark_results = {}
        self.tradeoff_data = {}
        self.compaction_task_waiting_time = {}
        self.flush_task_waiting_time = {}
        # add basic information
        base_info = self.stdout_file.split(os.sep)
        self.cpu_count = base_info[-3]
        self.batch_size = base_info[-2]
        self.device = base_info[-4].replace("StorageMaterial.", "")
        self.stall_reasons = {}

        marked_lines = ["benchmarks", "compaction_stat", "file_reading", "db_stats",
                        "statistics", "level_size"]
        self.line_map = {x: [] for x in marked_lines}

        # extracting read latency
        level_num = 7
        self.read_latency_map = {"level" + str(x): {"Count": 0, "Average": 0, "StdDev": 0} for x in range(level_num)}

        self.fillrandom_hist = {}
        self.readrandom_hist = {}
        self.updaterandom_hist = {}
        self.seek_hist = {}

        self.level_size_results = {}

        self.hist_job_list = []

        self.stall_influence_data = {}
        self.stall_duration = []

        # handle the file
        self.split_the_file()
        return
