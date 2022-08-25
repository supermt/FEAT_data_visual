import datetime
import json
import re
import time
import gzip

import pandas as pd

from utils.feature_selection import read_report_csv_with_change_points

MS_TO_SEC = 1000000

target_workloads = [""]


def load_log_and_qps(log_file, ground_truth_csv):
    # load the data
    return LogRecorder(log_file, ground_truth_csv)


class LogRecorder:

    @staticmethod
    def get_start_time(log_line, jet_lag):
        regex = r"\d+\/\d+\/\d+-\d+:\d+:\d+\.\d+"
        matches = re.search(regex, log_line, re.MULTILINE)
        machine_start_time = matches.group(0)
        start_time = datetime.datetime.strptime(
            machine_start_time, "%Y/%m/%d-%H:%M:%S.%f")
        start_time_micros = int(time.mktime(
            start_time.timetuple())) * MS_TO_SEC + start_time.microsecond
        start_time_micros -= jet_lag * 3600 * MS_TO_SEC
        return start_time_micros

    def pair_the_flush_jobs(self):
        flush_features = ["job", "start_time", "end_time", "flush_size", "lsm_state"]
        flush_io_stats = ["file_write_nanos", "file_range_sync_nanos", "file_fsync_nanos", "file_prepare_write_nanos",
                          "file_cpu_write_nanos", "file_cpu_read_nanos"]
        if self.iostat:
            flush_features.extend(flush_io_stats)
        self.flush_df = pd.DataFrame(
            columns=flush_features)
        flush_start_array = [
            x for x in self.log_lines if x["event"] == "flush_started"]
        # job_id = [x["job"] for x in flush_start_array]
        # print(job_id)
        flush_end_array = [
            x for x in self.log_lines if x["event"] == "flush_finished"]
        # flush_df = pd.DataFrame(["job","start_time","end_time","flush_size"])
        for start_event, index in zip(flush_start_array, range(len(flush_start_array))):
            if "total_data_size" in start_event:
                flush_event_row = [start_event["job"],
                                   start_event['time_micros'] -
                                   self.start_time_micros,
                                   flush_end_array[index]["time_micros"] - self.start_time_micros,
                                   start_event["total_data_size"], flush_end_array[index]["lsm_state"]]
            else:
                flush_event_row = [start_event["job"],
                                   start_event['time_micros'] -
                                   self.start_time_micros,
                                   flush_end_array[index]["time_micros"] - self.start_time_micros,
                                   start_event["memory_usage"], flush_end_array[index]["lsm_state"]]
            if self.iostat:
                flush_io_stats_value = [flush_end_array[index][x] for x in flush_io_stats]
                flush_event_row.extend(flush_io_stats_value)
            self.flush_df.loc[index] = flush_event_row
        # print(self.flush_df)

    def get_the_compaction_jobs(self):
        # unlike flush, the compaction processes can be run in parallel,
        # which means one compaction that starts later can be finished eariler
        # so we need to sort it by the time_micros first
        compaction_start_df = pd.DataFrame(
            [x for x in self.log_lines if x["event"] == "compaction_started"]).sort_values("job")
        compaction_end_df = pd.DataFrame(
            [x for x in self.log_lines if x["event"] == "compaction_finished"]).sort_values("job")
        # choose the useful columns only
        compaction_start_df = compaction_start_df[[
            "time_micros", "input_data_size", "job", "compaction_reason"]]
        compaction_end_feature = [
            "time_micros", "compaction_time_micros", "compaction_time_cpu_micros", "total_output_size", "lsm_state",
            "output_level", "num_input_records", "num_output_records"]
        io_stat_feature = ["file_write_nanos", "file_range_sync_nanos", "file_fsync_nanos", "file_prepare_write_nanos"]
        if self.iostat:
            compaction_end_feature.extend(io_stat_feature)

        compaction_end_df = compaction_end_df[compaction_end_feature]
        compaction_start_df["time_micros"] -= self.start_time_micros
        compaction_end_df["time_micros"] -= self.start_time_micros
        # let the time_micros minus the start time,

        compaction_start_df = compaction_start_df.rename(
            columns={"time_micros": "start_time"})

        compaction_end_df = compaction_end_df.rename(
            columns={"time_micros": "end_time"})

        # concat the data frames
        self.compaction_df = pd.concat(
            [compaction_start_df, compaction_end_df], axis=1)
        self.compaction_df["processing_speed"] = list(self.compaction_df["input_data_size"] / self.compaction_df[
            "compaction_time_micros"])
        self.l0_compaction_df = self.compaction_df[self.compaction_df["compaction_reason"] == "LevelL0FilesNum"]
        self.l0_compaction_df["processing_speed"] = list(
            self.l0_compaction_df["input_data_size"] / self.l0_compaction_df[
                "compaction_time_micros"])

        pass

    def phrase_warninglines(self, jet_lag=0):
        stall_points = []
        for line in self.warning_lines:
            stall_point = []
            line_time_sec = (LogRecorder.get_start_time(line, jet_lag) - self.start_time_micros) / 1000000
            if "level-0" in line:
                stall_point = [line_time_sec, "L0 Overflowing"]
            if "pending compaction" in line:
                stall_point = [line_time_sec, "Redundancy Overflowing"]
            if "memtables" in line:
                stall_point = [line_time_sec, "Memory Overflowing"]
            # else:
            #     stall_point = [line_time_sec, line]
            stall_points.append(stall_point)
        # print(stall_points)
        return pd.DataFrame(stall_points, columns=["time sec", "overflowing"])

    def record_real_time_qps(self, record_file, with_DOTA=False):
        self.qps_df = read_report_csv_with_change_points(record_file)
        pass

    def __init__(self, log_file, record_file="", with_DOTA=False, iostat=False):

        self.start_time_micros = 0
        self.log_lines = []
        self.iostat = iostat
        self.compaction_df = pd.DataFrame()
        self.l0_compaction_df = pd.DataFrame()
        self.qps_df = pd.DataFrame()

        if ".gz" in log_file:
            file_lines = gzip.open(log_file, "r").readlines()
            file_lines = [x.decode('utf-8') for x in file_lines]
        else:
            file_lines = open(log_file, "r").readlines()

        UNIST_or_not = "pm" in log_file or "PM" in log_file
        is_SILK = "SILK" in log_file
        jet_lag = 0
        if UNIST_or_not:
            jet_lag = 1

        self.start_time_micros = self.get_start_time(file_lines[0], jet_lag)
        self.log_lines = []
        self.warning_lines = []
        for line in file_lines:
            line_string = re.search('(\{.+\})', line)
            if line_string:
                log_row = json.loads(line_string[0])
                self.log_lines.append(log_row)
            if "[WARN]" in line:
                self.warning_lines.append(line)
        if not is_SILK:
            self.pair_the_flush_jobs()
            self.get_the_compaction_jobs()
        if record_file != "":
            self.record_real_time_qps(record_file, with_DOTA)
