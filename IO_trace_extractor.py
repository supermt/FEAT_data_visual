import pandas as pd

WRITE_INTENSIVE_DEVICE = 38
io_trace = pd.read_csv("IO_trace/38_write_only.csv")

time_stamp = io_trace["timestamp"]
print(time_stamp)
time_range = (time_stamp.max() - time_stamp.min()) / 1000000
print(time_range)

MBPS = []
start_time = time_stamp.min()
elapsed_time = 0

for elapsed_time in range(3600):
    time_range = [elapsed_time * 1000000 + start_time, (1 + elapsed_time) * 1000000 + start_time]
    # print(time_range)
    print(elapsed_time)
    input = sum(io_trace[(io_trace["timestamp"] > time_range[0]) &
                         (io_trace["timestamp"] < time_range[1])]["length"])
    MBPS.append(input)

pd.DataFrame(MBPS).to_csv("IO_trace/throughput")

# for row in io_trace.iloc:
#     total_write = 0
#     total_write += row["length"]
#     if row["timestamp"] - start_time > elapsed_time * 1000000:
#         elapsed_time += 1
#         MBPS.append(total_write / 1000000)
#         total_write = 0
#     if elapsed_time > 3600:
#         break
#
# import matplotlib.pyplot as plt
#
# plt.plot(MBPS)
# plt.show()
# # print(MBPS)
