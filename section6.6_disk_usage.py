import pandas as pd
import matplotlib.pyplot as plt

total_time = []
for i in range(300, 400):
    data_period = pd.read_csv("Eurosys/bitbrains/%d.csv" % (i), sep=";\t")
    target_key = data_period.keys()[-3]
    total_time.extend(data_period[target_key].values)
    # plt.plot(disk_data["\tDisk write throughput [KB/s]"])
    # plt.show()
plt.plot(total_time)
plt.xlabel("Elapsed Time (ms)")
plt.show()
