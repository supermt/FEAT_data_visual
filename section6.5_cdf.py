import pandas as pd
from matplotlib import cycler

#
std_prefix = "Eurosys/seciont6.6_GC_influence/"
tuners = ["tuned", "SILK", "FEAT"]

hist_line_dict = {x: [] for x in tuners}
#
# for tuner in tuners:
#     std_file = open(std_prefix + tuner + ".txt")
#     lines = std_file.readlines()
#     linecount = 0
#     for line in lines:
#         if "Percentiles" in line:
#             break
#         linecount += 1
#     linecount += 2
#     print(line)
#
#     for i in range(50):
#         line = lines[linecount + i]
#         hist_line_dict[tuner].append(line)
#         if lines[linecount + i + 1] == "\n":
#             break
#
# for tuner in hist_line_dict:
#     hist_lines = hist_line_dict[tuner]
#     cdf = []
#     xx = []
#     yy = []
#     for hist_line in hist_lines:
#         entries = hist_line.replace("(", "").replace(")", "").replace("[", "").replace("]", "").split()
#         x = int(entries[2])
#         xx.append(x)
#         y = int(entries[1])
#         yy.append(y)
#
#     for i in range(len(xx)):
#         cdf.append([(sum(xx[:i]) / sum(xx) * 100), yy[i]])
#
#     cdf_df = pd.DataFrame(cdf, columns=["x", "latency"])
#     cdf_df.to_csv("csv_results/cdf/" + tuner + "cdf.csv", sep=" ", index=False)

import matplotlib as mpl
import matplotlib.pyplot as plt

mpl.rcParams['figure.figsize'] = (8, 4)
mpl.rcParams['axes.grid'] = False
mpl.rcParams['font.size'] = 16.5
mpl.rcParams['font.family'] = "Arial"
mpl.rcParams['lines.markersize'] = 10
mpl.rcParams["legend.loc"] = "lower center"
mpl.rcParams["axes.prop_cycle"] = cycler('color',
                                         ['#ee0000', '#2074DE', '#38AD68', '#d62728', '#9467bd', '#8c564b', '#e377c2',
                                          '#7f7f7f', '#bcbd22', '#17becf'])

fig, axes = plt.subplots(3, 1, sharey='col', sharex='all')

colors = ['#ee0000', '#2074DE', '#38AD68']
row = 0
for tuner in tuners:
    plotdf = pd.read_csv(std_prefix + tuner + ".csv")

    plotdf = plotdf[plotdf["secs_elapsed"] < 600]

    axes[row].plot(plotdf["secs_elapsed"], plotdf["interval_qps"], color=colors[row])
    row += 1

plt.show()
# fig.show()
