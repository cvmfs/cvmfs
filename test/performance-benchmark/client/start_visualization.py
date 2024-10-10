#!/usr/bin/env python3

import glob
import tqdm
import sys

from util_visualization import visualization_plotting
from util_visualization import visualization_time
from util_visualization import visualization_read_params

################################################################################
#
#                  CVMFS - PERFORMANCE BENCHMARK - VISUALIZER
#
################################################################################
#
# Visualizes the data created by
#           "CVMFS - PERFORMANCE BENCHMARK" (start_benchmark.py)
#
# 2 modes:
# 1) scatter plot of a single measurement (= file)
#     - visualization_plotting.plotSingleFile()
#     - x-axis: point cloud consisting of one point of each thread and repetition
#     - y-axis: a single label listed in the .csv created by the benchmark
#     - separate point clouds for cold, warm and hot cache
#     - can accept multiple csv_labels but will create one scatter plot for each
# 2) boxplots for comparison between 2 benchmark runs with different configs
#     - visualization_plotting.boxplotPlotComparison()
#     - x-axis: comparison metric, either "cvmfs version" or "client config"
#     - y-axis: csv_label(s) (each label gets its own box in the same plot)
#     - separate box for cold, warm and hot cache
#     - spearate plot for each command, num_threads and (option or version)
#
# csv_labels are defined in util_benchmark/benchmark_time.dict_time_format
# and all cvmfs internal affairs counters as shown in a .csv created by
# ../start_benchmark.py
#
# To use autocompletion for the command line to find the config file, see here
# https://pypi.org/project/argcomplete/#activating-global-completion
#
################################################################################

def _plot_boxplots(config, section, title, comparison_to_plot):
  if "build" in comparison_to_plot:
    comperator = "cvmfs_build_names"
    compare_label = "cvmfs_build_name_labels"
    outer_iterator = "client_configs"
    x_title = "CVMFS Version"
  elif "config" in comparison_to_plot:
    comperator = "client_configs"
    compare_label = "client_config_labels"
    outer_iterator = "cvmfs_build_names"
    x_title = "CVMFS Client Config"
  else:
    print("Wrong comparison_to_plot selected. Either 'build' or 'config'")
    exit(2)

  if (section in config.keys() and config[section]["plot"] == True):
    print("\n")
    print("============================================")
    print("Plotting:", title)
    print("============================================")

    # build full label by adding the repo in front
    csv_labels_cvmfs_internal = getCvmfsInternal(config, section)

    # compare versions
    total_itr = (len(config[section][outer_iterator])
               * len(config["_cmds"])
               * len(config["num_threads"])
               * len(config[section]["time_metrics"])
               + len(config[section][outer_iterator])
               * len(config["_cmds"])
               * len(config["num_threads"])
               * len(csv_labels_cvmfs_internal))
    pbar = tqdm.tqdm(total=total_itr)
    for outer_ele in config[section][outer_iterator]:
      for cmd in config["_cmds"]:
        for thread in config["num_threads"]:
          for label in config[section]["time_metrics"]:
            pbar.set_description(outer_ele + " " + cmd + " "+ str(thread)
                                 + " " + label)
            visualization_plotting.boxplotPlotComparison(
                        config["in_dirname"], label, outer_ele, str(thread),
                        cmd, config[section][comperator], comparison_to_plot,
                        config["out_dirname"], config[compare_label],
                        x_title)
            pbar.update(1)
          for label in csv_labels_cvmfs_internal:
            pbar.set_description(outer_ele + " " + cmd + " " + str(thread)
                                 + " " + label)
            visualization_plotting.boxplotPlotComparison(
                        config["in_dirname"], label, outer_ele, str(thread),
                        cmd, config[section][comperator],
                        comparison_to_plot + "_cvmfs_internal", config["out_dirname"],
                        config[compare_label], x_title)
            pbar.update(1)

def getCvmfsInternal(config, section):
  csv_labels_cvmfs_internal = []

  if ("internal_affairs_metrics" in config[section].keys()
      and len(config[section]["internal_affairs_metrics"]) > 0):
    for repo in config[section]["internal_affairs_repos"]:
      for label in config[section]["internal_affairs_metrics"]:
        csv_labels_cvmfs_internal.append(repo + "_" + label)

  return csv_labels_cvmfs_internal

def _createScatter(config):
  section = "scatter_plot"
  if (section in config.keys() and config[section]["plot"] == True):
    print("\n")
    print("============================================")
    print("Plotting:", "Scatter plots")
    print("============================================")
    if (    (not "cvmfs_build_names" in config[section].keys()
             or len(config[section]["cvmfs_build_names"]) == 0)
        and (not "client_configs" in config[section].keys()
             or len(config[section]["client_configs"]) == 0)):
      files = glob.glob(config["in_dirname"] + "*.csv")
    else:
      files = []
      # find the right files for given config/build
      for build in config[section]["cvmfs_build_names"]:
        for client_config in config[section]["client_configs"]:
          for thread in config["num_threads"]:
            new_files = glob.glob(config["in_dirname"] +  "*_" + build + "_" +
                                    client_config + "_" + str(thread)
                                    + "_[0-9]*.csv")
            files.extend(new_files)

    cvmfs_internal_dict = getCvmfsInternal(config, section)
    if ("time_metrics" in config[section].keys()
      and len(config[section]["time_metrics"]) > 0):
      visualization_plotting.plotSingleFile(files,
                                        config[section]["time_metrics"],
                                        "normal", config["out_dirname"])
    if len(cvmfs_internal_dict) > 0:
      visualization_plotting.plotSingleFile(files, cvmfs_internal_dict,
                                      "cvmfs_internal", config["out_dirname"])

def _appendToCsv(config):
  section = "append_to_csv"
  if (section in config.keys() and config[section]["write"] == True):
    print("\n")
    print("============================================")
    print("Plotting:", "Append to CSV")
    print("============================================")
    if (    (not "cvmfs_build_names" in config[section].keys()
             or len(config[section]["cvmfs_build_names"]) == 0)
        and (not "client_configs" in config[section].keys()
             or len(config[section]["client_configs"]) == 0)):
      files = glob.glob(config["in_dirname"] + "*.csv")
    else:
      files = []
      # find the right files for given config/build
      for build in config[section]["cvmfs_build_names"]:
        for client_config in config[section]["client_configs"]:
          for thread in config["num_threads"]:
            new_files = glob.glob(config["in_dirname"] +  "*_" + build + "_" +
                                    client_config + "_" + str(thread)
                                    + "_[0-9]*.csv")
            files.extend(new_files)

    cvmfs_internal_dict = getCvmfsInternal(config, section)

    if ("time_metrics" in config[section].keys()
      and len(config[section]["time_metrics"]) > 0):
      print("time_metrics ")
      visualization_plotting.appendToCsv(files,
                                        config[section]["time_metrics"],
                                        "normal", config[section]["full_out_name"],
                                        config[section]["tag"])
    if len(cvmfs_internal_dict) > 0:
      print("internal_affairs_metrics")
      visualization_plotting.appendToCsv(files, cvmfs_internal_dict,
                                      "cvmfs_internal",
                                      config[section]["full_out_name"],
                                      config[section]["tag"])


if __name__ == "__main__":
  config = visualization_read_params.getConfig()

  print("Extracted values from files (not YAML config):")
  print("In dirname:", config["in_dirname"])
  print("Num files found:", len(config["_files"]))
  print("Commands:", config["_cmds"])
  print("Client configs:", config["_client_configs"])
  print("Build names:", config["_cvmfs_build_names"])
  print("Num threads:", config["_num_threads"])
  print("")

  _plot_boxplots(config, "cvmfs_build_comparison",
                         "CVMFS Build Comparison", "build")
  _plot_boxplots(config, "client_config_comparison",
                         "CVMFS Client Config Comparison", "config")
  _createScatter(config)
  _appendToCsv(config)