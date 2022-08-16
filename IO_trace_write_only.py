import pandas as pd

WRITE_INTENSIVE_DEVICE = 38
io_trace = pd.read_csv("IO_trace/write_only_trace.csv", header=None,
                       names=["device", "OP", "offset", "length", "timestamp"])
# io_trace = pd.DataFrame(io_trace, columns=["device", "OP", "offset", "length", "timestamp"])

device_list = pd.unique(io_trace["device"])

target_trace = io_trace[io_trace["device"] == WRITE_INTENSIVE_DEVICE]

target_trace.to_csv("IO_trace/38_write_only.csv")
#
# time_stamp = target_trace["timestamp"]
# time_range = (time_stamp.max() - time_stamp.min()) / 1000000
# print(time_range)
#
# MBPS = []
# start_time = time_stamp.min()
# elapsed_time = 1
# for row in target_trace.iloc:
#     total_write = 0
#     total_write += row["length"]
#     if row["timestamp"] - start_time > elapsed_time * 1000000:
#         elapsed_time += 1
#         MBPS.append(total_write / 1000000)
#         total_write = 0
#
# import matplotlib.pyplot as plt
#
# plt.plot(MBPS)
# plt.show()
# # print(MBPS)
