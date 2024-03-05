#!/usr/bin/env python3

import glob

import pandas as pd
import numpy as np
import tqdm

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
             versions_or_options, comparison_to_plot, cache_labels,
             x_label_dict, x_title):
  cvmfs_data = {}

  ## 1) get data
  if "build" in comparison_to_plot:
    option = version_or_option
  elif "config" in comparison_to_plot:
    version = version_or_option

  for comperator in versions_or_options:
    if "config" in comparison_to_plot:
      files = glob.glob(dirname + cmd + "_" + version + "_" + comperator +
                        "_" + thread + "_[0-9]*.csv")
    elif "build" in comparison_to_plot:
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
                          versions_or_options, comparison_to_plot, outdir,
                          x_label_dict, x_title):
  cache_labels = ["cold_cache", "warm_cache", "hot_cache"]

  y_data, x_labels, x_title = _prepareData(dirname, csv_labels,
                                           version_or_option, thread, cmd,
                                           versions_or_options,
                                           comparison_to_plot, cache_labels,
                                           x_label_dict, x_title)

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

#cms_2.9.4.0_statfs_kernel_1_0
def _csv_header_string():
  header = "tag, command_label, build_name, client_config, threads, run_id, "
  header += "repetitions, metric, "
  
  for cache in ["cold_cache", "warm_cache", "hot_cache"]:
    for metric in ["min_val", "first_quartile", "median", "third_quartile",
                   "max_val"]:
      header += cache + "_" + metric + ", "

  # remove last comma + whitespace
  header = header[:-2]

  header += "\n"
  return header

##
# Scatter plot for a single file and a single csv_label
##
# Takes all measurement points available in the file and plots it as scatter
# plot.
# This means that if you ran the benchmark with 32 threads and
# 10 repetitions, a point cloud of 320 points will be plotted for each
# cache type (cold, warm, hot)
#
# Allow csv_label_type is either "normal" or "cvmfs_internal"
#
# Can accept multiple csv_labels but will create one scatter plot for each
#

def plotSingleFile(in_filenames, csv_labels, csv_label_type, out_dirname):
  cache_labels = ["cold_cache", "warm_cache", "hot_cache"]

  callback_extra_data = {
    "out_dirname": out_dirname,
    "cache_labels": cache_labels,
    "csv_label_type": csv_label_type
  }

  pbar = tqdm.tqdm(in_filenames)
  for in_filename in pbar:
    pbar.set_description(in_filename)
    callback_extra_data["in_filename"] = in_filename

    _prepareDataSingleFile(in_filename, csv_labels, csv_label_type,
                              cache_labels, _callback_scatter_single_file,
                              callback_extra_data)

##
# Appends or creates CSV file with data from a single file
##
# Takes all measurement points available in the file and creates the following
# quartiles [0.0, 0.25, 0.5, 0.75, 1.0] and writes them for given csv_labels 
# into the CSV file.
#
# "Tag" is needed, allows to set an identifier for all given "in_filenames"
#
# Can accept multiple csv_labels but will create a separate entry for each
# 
# Allow csv_label_type is either "normal" or "cvmfs_internal"
#
def appendToCsv(in_filenames, csv_labels, csv_label_type, out_filename, tag):
  cache_labels = ["cold_cache", "warm_cache", "hot_cache"]

  with(open(out_filename,"a")) as file:
    # file newly created
    if (file.tell() == 0):
      file.write(_csv_header_string())

    callback_extra_data = {
      "out_file": file,
      "cache_labels": cache_labels,
      "tag": tag
    }

    pbar = tqdm.tqdm(in_filenames)
    for in_filename in pbar:
      pbar.set_description(in_filename)
      callback_extra_data["in_filename"] = in_filename

      _prepareDataSingleFile(in_filename, csv_labels, csv_label_type,
                         cache_labels, callback_append_csv, callback_extra_data)

def _prepareDataSingleFile(filename, csv_labels, csv_label_type, cache_labels,
                      callback, callback_extra_data):
  df=pd.read_csv(filename, index_col="labels", sep=',')

  for labels in csv_labels:
    y_data = defaultdict(list)

    for label in labels.split(","):
      if not label in df.index:
          continue
      old_val = 0
      for cache in cache_labels:
        row = ast.literal_eval(df.loc[label][cache])

        # modify row if warm/hot metric for internal affairs
        if ("cvmfs_internal" in csv_label_type):
          if cache == cache_labels[0]:
            old_val = row[-1]
          if cache != cache_labels[0]:
            for i in range(len(row)):
              tmp_val = row[i]
              row[i] = row[i] - old_val
              old_val = tmp_val

        y_data[label].append(row)

    # quick escape in case label does not exist.
    if len(y_data) == 0:
      continue

    callback(y_data, labels, callback_extra_data)

def callback_append_csv(data, labels, extra_data):
  out_file = extra_data["out_file"]
  in_filename = extra_data["in_filename"]
  tag = extra_data["tag"]

  #remove dir
  tmp = str(in_filename.split("/")[-1])
  # split at first occurences of _
  cmd = tmp.split("_", 1)[0]
  build = tmp.split("_")[1]
  client_config = tmp.split("_", 2)[2].rsplit("_", 2)[0]
  threads = tmp.split("_")[-2]
  run = tmp.split("_")[-1].split(".")[0]
  

  out_file.write(tag + ", " + cmd + ", " + build + ", " + client_config +  ", ")
  out_file.write(threads + ", " + run)

  for key, val in data.items():
      out_file.write(", " + str(len(val[0])) +  ", " + key)
      for cacheData in val:
        quants = pd.DataFrame(cacheData).quantile([0, 0.25, 0.5, 0.75, 1])
        np_quants = np.array(quants)
        out_file.write(", " + str(np_quants[0][0]))
        out_file.write(", " + str(np_quants[1][0]))
        out_file.write(", " + str(np_quants[2][0]))
        out_file.write(", " + str(np_quants[3][0]))
        out_file.write(", " + str(np_quants[4][0]))
  
  out_file.write("\n")
      
def _callback_scatter_single_file(data, labels, extra_data):
  cache_labels = extra_data["cache_labels"]
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
  idx = 0

  if len(labels.split(",")) > 1:
    colors=visualization_colors.colors3_3
  else:
    colors=visualization_colors.cache_colors

  for key, val in data.items():
    for cacheData in val:
      ax1.scatter(
          [i for i in range(len(cacheData))],
          cacheData,
          #yerr=errorY[firstX:i],
          label=" ".join(cache_labels[idx % 3].split("_")) \
                if len(labels.split(",")) == 1 \
                else key + " " + " ".join(cache_labels[idx % 3].split("_")) ,
          marker="o",
          color=colors[idx % len(colors)],
          s=8**2,
          rasterized=True
          )
      idx += 1

  all_measurements = len(data[[*data.keys()][0]][0])

  if "cvmfs_internal" in extra_data["csv_label_type"]:
    repetitions = all_measurements
    ax1.set_xlabel("#Measurements: " + str(all_measurements) + " ( "
                    + str(int(repetitions)) + " repetitions)")
  else:
    num_threads = extra_data["in_filename"].split("_")[-2]
    repetitions = all_measurements / int(num_threads)
    ax1.set_xlabel("#Measurements: " + str(all_measurements) + " ( " +
                  num_threads + " threads " + " x " +
                  str(int(repetitions)) + " repetitions)")
  if "cvmfs_internal" in extra_data["csv_label_type"]:
    ax1.set_ylabel(visualization_time.
                  measurement_cvmfs_internal_dict[labels.split("_", 1)[-1]])
  else:
    ax1.set_ylabel(visualization_time.measurement_label_dict[labels])

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

  if os.path.exists(extra_data["out_dirname"]) == False:
      os.makedirs(extra_data["out_dirname"])

  outname = extra_data["out_dirname"] + "/Scatterplot_" + \
            extra_data["in_filename"].split("/")[-1].split(".csv")[0] + "_" + \
            "_".join(labels.split(",")) + ".pdf"
  plt.savefig(outname, bbox_inches='tight', pad_inches=0.2)
  plt.close('all')
