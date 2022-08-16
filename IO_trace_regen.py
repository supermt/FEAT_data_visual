import pandas as pd

WRITE_INTENSIVE_DEVICE = 38
io_trace = pd.read_csv("IO_trace/io_trace_head.csv", header=None,
                       names=["device", "OP", "offset", "length", "timestamp"])

target_trace = io_trace[io_trace["OP"] == "W"]
print("search complete")
target_trace.to_csv("write_only_trace.csv")
print("write complete")
