import glob

import pandas as pd
import numpy as np

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

import os

from util_visualization import visualization_colors
from util_visualization import visualization_time
import ast

from collections import defaultdict

##
# Scatter plot for a single file and a single csv_label
##
# Takes all measurement points available in the file and plots it as scatter
# plot.
# This means that if you ran the benchmark with 32 threads and
# 10 repetitions, a point cloud of 320 points will be plotted for each
# cache type (cold, warm, hot)
#
# Can accept multiple csv_labels but will create one scatter plot for each
#
def plotSingleFile(filename, csv_labels, outdir):
  df=pd.read_csv(filename, index_col="labels", sep=',')

  cache_labels = ["cold_cache", "warm_cache", "hot_cache"]

  for labels in csv_labels:
    y_data = defaultdict(list)

    scale = 1.5
    fig = plt.figure(figsize=(16*scale, 9*scale))

    params = [
            {'font.size': 45},
            {'axes.labelsize': 40},
            {'legend.fontsize': 35},
            {'lines.linewidth': 6}
        ]
    for ele in params:
        plt.rcParams.update(ele)

    for label in labels.split(","):
      for cache in cache_labels:
        row = ast.literal_eval(df.loc[label][cache])
        y_data[label].append(row)

    ax1 = plt.axes()
    idx = 0

    if len(labels.split(",")) > 1:
      colors=visualization_colors.colors3_3
    else:
      colors=visualization_colors.cache_colors

    for key, val in y_data.items():
      for cacheData in val:
        ax1.scatter(
            [i for i in range(len(cacheData))],
            cacheData,
            #yerr=errorY[firstX:i],
            label=" ".join(cache_labels[idx % 3].split("_")) if len(labels.split(",")) == 1 \
                  else key + " " + " ".join(cache_labels[idx % 3].split("_")) ,
            marker="o",
            color=colors[idx % len(colors)],
            s=8**2,
            rasterized=True
            )
        idx += 1

    num_threads = filename.split("_")[-2]
    all_measurements = len(y_data[[*y_data.keys()][0]][0])
    repetitions = all_measurements / int(num_threads)
    ax1.set_xlabel("#Measurements: " + str(all_measurements) + " ( " + \
                   num_threads + " threads " + " x " + \
                   str(int(repetitions)) + " repetitions)")
    ax1.set_ylabel(visualization_time.measurement_label_dict[label])

    # needed to draw legend dynamically below x-axis label
    fig.canvas.draw()
    x_label_lowest_y_pos = ax1.xaxis.label.get_window_extent().y0 / 1000.0
    lgnd= plt.legend(framealpha=0.8,
                     fontsize=32,
                     loc='upper center',
                     bbox_to_anchor=(0.5, x_label_lowest_y_pos - 0.15),
                     ncol=3
                     )
    for i in range(len(lgnd.legend_handles)):
      lgnd.legend_handles[i].sizes = [20**2]

    if os.path.exists(outdir) == False:
        os.makedirs(outdir)

    outname = outdir + "/Scatterplot_" + \
              filename.split("/")[-1].split(".csv")[0] + "_" + \
              "_".join(labels.split(",")) + ".pdf"
    plt.savefig(outname, bbox_inches='tight', pad_inches=0.2)
    plt.close('all')

##
# Prepares data for boxplotPlotComparison()
##
# Internal function that creates one unified multi-dimensional array,
# extracting the requested data from multiple files.
#
# Clustering of data depends on "comparison_to_plot" and will be done for a
# csv_label:
# "version" = cluster by cvmfs version
# "option" = cluster by cvmfs client config
#
# "comparison_to_plot" must contain "cvmfs_internal" if requested csv_labels
# are cvmfs internal affairs counters
#
# @returns
# y_data - multi-dimensional array that can be plotted with boxplot
# x_labels - labels for the x-axis of the plot
# x_title - title for the x-axis of the plot
def _prepareData(dirname, csv_labels, version_or_option, thread, cmd,
             versions_or_options, comparison_to_plot, cache_labels):
  cvmfs_data = {}

  ## 1) get data
  if "version" in comparison_to_plot:
    option = version_or_option
    x_label_dict = visualization_time.cvmfs_version_labels_dict
    x_title = "CVMFS Version"
  elif "option" in comparison_to_plot:
    version = version_or_option
    x_label_dict = visualization_time.option_labels_dict
    x_title = "CVMFS Client Config"

  for comperator in versions_or_options:
    if "option" in comparison_to_plot:
      files = glob.glob(dirname + cmd + "_" + version + "_" + comperator +
                        "_" + thread + "_[0-9]*.csv")
    elif "version" in comparison_to_plot:
      files = glob.glob(dirname + cmd + "_" + comperator + "_" + option +
                        "_" + thread + "_[0-9]*.csv")

    cvmfs_data[comperator] = {}

    for label in csv_labels.split(","):
      cvmfs_data[comperator][label] = defaultdict(list)

    # data: combine all different runs for the same setup
    for filename in files:
      df=pd.read_csv(filename, index_col="labels", sep=',')

      for label in csv_labels.split(","):
        last_val = 0
        if not label in df.index:
          continue

        for cache in cache_labels:
          row = ast.literal_eval(df.loc[label][cache])
          for ele in row:
            # default case
            # for cvmfs_internal: cold cache == use last value as start delta
            if cache == cache_labels[0] or "cvmfs_internal" not in comparison_to_plot:
              cvmfs_data[comperator][label][cache].append(ele)
              last_val = ele
            else: # for cvmfs_internal: warm and hot cache take delta as the
                  # counters accumulate
              cvmfs_data[comperator][label][cache].append(ele - last_val)
              last_val = ele


  y_data = []
  x_labels = []

  for label in csv_labels.split(","):
    for cache in cache_labels:
      for comperator in versions_or_options:
        if len(cvmfs_data[comperator][label].keys()) < 3:
          continue
        y_data.append(cvmfs_data[comperator][label][cache])

        if len(csv_labels.split(",")) > 1:
          x_labels.append(x_label_dict[comperator] + " " + label)
        else:
          x_labels.append(x_label_dict[comperator])

  return y_data, x_labels, x_title

##
# Create boxplots that compare different benchmark configs and writes them to PDF
##
# Creates boxplots to compare multiple CVMFS benchmarks either by:
# - CVMFS client configs ("option")
# - CVMFS versions ("version")
#
# for a given cvs_label and writes them to PDF.
#
# csv_labels are defined in util_benchmark/benchmark_time.dict_time_format
# and all cvmfs internal affairs counters as shown in a .csv created by
# ../start_benchmark.py
#
# It is possible that csv_labels consists of multiple csv_labels of the "time"
# cmd (not for internal affairs). E.g. "user,system,real" will result in plotting
# each label in the same boxplot, but having its own separate "box" aka:
# user cold, user warm, user hot, system cold, ..., system hot, real cold, ...
#
# For this following might need population by the user:
# - visualization_time.cvmfs_version_labels_dict for different
#                                             CVMFS version/branches/directories
# - visualization_time.option_labels_dict for different CVMFS client configs
# - visualization_time.measurement_label_dict for combining different csv_labels
#                                             (e.g. see "user,system,real")
#
#
def boxplotPlotComparison(dirname, csv_labels, version_or_option, thread, cmd,
                          versions_or_options, comparison_to_plot, outdir):
  cache_labels = ["cold_cache", "warm_cache", "hot_cache"]

  y_data, x_labels, x_title = _prepareData(dirname, csv_labels,
                                           version_or_option, thread, cmd,
                                           versions_or_options,
                                           comparison_to_plot, cache_labels)

  if len(y_data) == 0:
    return

  scale = 1.5
  fig = plt.figure(figsize=(16*scale, 9*scale))

  params = [
          {'font.size': 45},
          {'axes.labelsize': 40},
          {'legend.fontsize': 35},
          {'lines.linewidth': 6}
      ]
  for ele in params:
      plt.rcParams.update(ele)

  ax1 = plt.axes()
                             # fill with color  # vertical box alignment
  bplot = ax1.boxplot(y_data, patch_artist=True, vert=True)

  # color the boxplot
  baseColorSchema = visualization_colors.cache_colors
  baseColorSchemaLight = visualization_colors.cache_colors_light
  threeColors = []

  counter = 0
  for cache in cache_labels:
    for i in range(len(versions_or_options)):
      threeColors.append(baseColorSchema[counter])
    counter += 1

  counter = 0
  for patch in bplot["boxes"]:
    patch.set_facecolor(threeColors[counter % len(threeColors)])
    patch.set_edgecolor(threeColors[counter % len(threeColors)])
    counter += 1

  counter = 0
  for patch in bplot["medians"]:
    patch.set_color(threeColors[counter % len(threeColors)])
    counter += 1

  doubledColors = []
  counter = 0
  for cache in cache_labels:
    for i in range(len(versions_or_options)):
      doubledColors.append(baseColorSchemaLight[counter])
      doubledColors.append(baseColorSchemaLight[counter])
    counter += 1

  counter = 0
  for patch in bplot["whiskers"]:
    patch.set_color(doubledColors[counter % len(doubledColors)])
    patch.set_linewidth(4)
    counter += 1

  counter = 0
  for patch in bplot["caps"]:
    patch.set_color(doubledColors[counter % len(doubledColors)])
    patch.set_linewidth(4)
    counter += 1
  # end color the boxplot


  ax1.set_xlabel(x_title)
  if "cvmfs_internal" in comparison_to_plot:
    # get rid of repo name in front of csv_label
    ax1.set_ylabel(visualization_time.
                   measurement_cvmfs_internal_dict[csv_labels.split("_", 1)[-1]])
  else:
    ax1.set_ylabel(visualization_time.measurement_label_dict[csv_labels])
  ax1.set_xticklabels(x_labels)

  plt.gca().set_ylim(bottom=0)
  plt.xticks(rotation=60)

  # rotate stronger if we have more labels (only happens if combining multiple
  # csv_labels)
  if (len(x_labels) > 7):
    plt.xticks(rotation=85)

  # color for the legend
  custom_lines = [Line2D([0], [0], color=baseColorSchema[0], lw=4),
                  Line2D([0], [0], color=baseColorSchema[1], lw=4),
                  Line2D([0], [0], color=baseColorSchema[2], lw=4)]

  # needed to draw legend dynamically below x-axis label
  fig.canvas.draw()
  x_label_lowest_y_pos = ax1.xaxis.label.get_window_extent().y0 / 1000.0
  ax1.legend(custom_lines,
             [ele.replace("_", " ") for ele in cache_labels],
             loc='upper center',
             bbox_to_anchor=(0.5, x_label_lowest_y_pos - 0.15),
             ncol=3)

  if os.path.exists(outdir) == False:
      os.makedirs(outdir)

  outname = outdir + "/Boxplot_" + comparison_to_plot + "-comparison_"
  outname += cmd + "_" + version_or_option + "_"
  outname += "_".join(csv_labels.split(",")) + "_" + thread + ".pdf"

  plt.savefig(outname, bbox_inches='tight', pad_inches=0.2)
  plt.close('all')
