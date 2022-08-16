import pandas as pd
import scipy.optimize as optimize
import numpy as np

pi = np.pi


def target_function(x, a, b, c, d):
    return a * np.sin(b * x + c) + d


throughput = pd.read_csv("IO_trace/throughput", header=None, names=["elapsed time", "request size"])

x = throughput["elapsed time"]
y = throughput["request size"]
Y = y / y.max()
Y *= 100

# x = np.arange(0, 10, 0.2)
# y = -0.5 * np.cos(1.1 * x) + 0.5

import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.rcParams['figure.figsize'] = (8, 2)
mpl.rcParams['axes.grid'] = False
mpl.rcParams['font.size'] = 16
mpl.rcParams['font.family'] = "Arial"
mpl.rcParams['lines.markersize'] = 10
mpl.rcParams["legend.loc"] = "lower center"
plt.plot(x, Y)
plt.xlabel("Elapsed Time (Sec)")
plt.ylabel("Bandwidth \n Usage (%)")
ax = plt.gca()
ax.spines["right"].set_color('none')
ax.spines["top"].set_color('none')
plt.tight_layout()

bandwidth_list = []
for i in range(3600):
    bandwidth_list.append([i, round(Y[i], 2)])

bandwidth_df = pd.DataFrame(bandwidth_list, columns=["elapsed time", "bandwidth"]).to_csv(
    "IO_trace/bandwidth_changes.csv", index=False)

#
# fs = np.fft.fftfreq(len(x), x[1] - x[0])
# Y = abs(np.fft.fft(y))
# freq = abs(fs[np.argmax(Y[1:]) + 1])

plt.savefig("fig_results/dynamic_bandwidth.pdf")

# a = max(y) - min(y)
# b = 2 * pi * freq
# c = 0
# d = np.mean(y)
# p0 = [a, b, c, d]
#
# para, _ = optimize.curve_fit(target_function, x, y, p0, maxfev=5000)
# print(para)
# y_fit = [target_function(a, *para) for a in x]
# plt.plot(x, y_fit, "r")
# plt.show()
