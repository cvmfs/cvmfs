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

def _plot_boxplots(config, section_name, plot_type, title):
  subconfig = config[section_name]

  if subconfig["plot"] == False:
    print("Not plotting section", section_name, " - Skipping...")
    return

  if visualization_read_params.type_client_and_config_comparison in plot_type:
    comperator = "build_name_and_config"
    compare_label = "build_name_and_config_labels"
    outer_iterator = "no_outer_iter"  # no outer_iterator
    comparison_to_plot = "build_name_and_config"
  elif visualization_read_params.type_cvmfs_build_comparison in plot_type:
    comperator = "cvmfs_build_names"
    compare_label = "cvmfs_build_name_labels"
    outer_iterator = "client_configs"
    comparison_to_plot = "build"
  elif visualization_read_params.type_client_config_comparison in plot_type:
    comperator = "client_configs"
    compare_label = "client_config_labels"
    outer_iterator = "cvmfs_build_names"
    comparison_to_plot = "config"
  else:
    print("Wrong comparison_to_plot selected. Either 'build' or 'config'")
    exit(2)


  in_dirs = subconfig["in_dirname"]
  threads = subconfig["num_threads"]
  only_plot_cold_cache = False

  if "only_plot_cold_cache" in subconfig.keys():
    only_plot_cold_cache = subconfig["only_plot_cold_cache"]

  if "title" in subconfig.keys():
    x_title = subconfig["title"]
  else:
    x_title = title

  print("============================================")
  print("Plotting:", section_name, "-", title)
  print("============================================")

  # build full label by adding the repo in front
  csv_labels_cvmfs_internal = getCvmfsInternal(config, section_name)

  # compare versions
  total_itr = (len(subconfig[outer_iterator])
              * len(subconfig["_cmds"])
              * len(threads)
              * len(subconfig["time_metrics"])
              + len(subconfig[outer_iterator])
              * len(subconfig["_cmds"])
              * len(threads)
              * len(csv_labels_cvmfs_internal))
  pbar = tqdm.tqdm(total=total_itr)
  for outer_ele in subconfig[outer_iterator]:
    for cmd in subconfig["_cmds"]:
      for thread in threads:
        for label in subconfig["time_metrics"]:
          pbar.set_description(outer_ele + " " + cmd + " "+ str(thread)
                                + " " + label)
          visualization_plotting.boxplotPlotComparison(
                      in_dirs, label, outer_ele, str(thread),
                      cmd, subconfig[comperator], comparison_to_plot,
                      subconfig["out_dirname"], subconfig[compare_label],
                      x_title, only_plot_cold_cache)
          pbar.update(1)
        for label in csv_labels_cvmfs_internal:
          pbar.set_description(outer_ele + " " + cmd + " " + str(thread)
                                + " " + label)
          visualization_plotting.boxplotPlotComparison(
                      in_dirs, label, outer_ele, str(thread),
                      cmd, subconfig[comperator],
                      comparison_to_plot + "_cvmfs_internal", subconfig["out_dirname"],
                      subconfig[compare_label], x_title, only_plot_cold_cache)
          pbar.update(1)

def getCvmfsInternal(config, section_name):
  csv_labels_cvmfs_internal = []

  if ("internal_affairs_metrics" in config[section_name].keys()
      and len(config[section_name]["internal_affairs_metrics"]) > 0):
    for repo in config[section_name]["internal_affairs_repos"]:
      for label in config[section_name]["internal_affairs_metrics"]:
        csv_labels_cvmfs_internal.append(repo + "_" + label)

  return csv_labels_cvmfs_internal

########################################
# create Scatterplot
########################################
def _createScatter(config, section_name):
  subconfig = config[section_name]

  if subconfig["plot"] == False:
    print("Not plotting section", section_name, " - Skipping...")
    return

  print("============================================")
  print("Plotting:", section_name, "- Scatterplots")
  print("============================================")
  if (    (not "cvmfs_build_names" in subconfig.keys()
            or len(subconfig["cvmfs_build_names"]) == 0)
      and (not "client_configs" in subconfig.keys()
            or len(subconfig["client_configs"]) == 0)):
    files = []
    # local section has priority for in_dirname
    if "in_dirname" in subconfig.keys():
      for dir in subconfig["in_dirname"]:
        new_files = glob.glob(dir + "*.csv")
        files.extend(new_files)
    else:
      for dir in config["global_in_dirname"]:
        new_files = glob.glob(dir + "*.csv")
        files.extend(new_files)
  else:
    files = []
    # find the right files for given config/build
    for build in subconfig["cvmfs_build_names"]:
      for client_config in subconfig["client_configs"]:
        for thread in config["num_threads"]:
          new_files = glob.glob(config["in_dirname"] +  "*_" + build + "_" +
                                  client_config + "_" + str(thread)
                                  + "_[0-9]*.csv")
          files.extend(new_files)

  out_dirname = ""
  # local section has priority for out_dirname
  if "out_dirname" in subconfig.keys():
    out_dirname = subconfig["out_dirname"]
  if len(out_dirname) == 0:
    out_dirname = config["out_dirname"]

  cvmfs_internal_dict = getCvmfsInternal(config, section)
  if ("time_metrics" in subconfig.keys()
    and len(subconfig["time_metrics"]) > 0):
    visualization_plotting.plotSingleFile(files, subconfig["time_metrics"],
                                          "normal", out_dirname)
  if len(cvmfs_internal_dict) > 0:
    visualization_plotting.plotSingleFile(files, cvmfs_internal_dict,
                                    "cvmfs_internal", out_dirname)

def _appendToCsv(config, section_name):
  subconfig = config[section_name]

  if subconfig["write"] == False:
    print("Not writing section", section_name, " - Skipping...")
    return

  print("============================================")
  print("Plotting:", section_name, "- Append to CSV")
  print("============================================")

  #TODO TODO
  files = []
  if (    (not "cvmfs_build_names" in subconfig.keys()
            or len(subconfig["cvmfs_build_names"]) == 0)
      and (not "client_configs" in subconfig.keys()
            or len(subconfig["client_configs"]) == 0)):
     for dir in subconfig["in_dirname"]:
        new_files = glob.glob(dir + "*.csv")
        files.extend(new_files)
  else:
    # find the right files for given config/build
    for build in subconfig["cvmfs_build_names"]:
      for client_config in subconfig["client_configs"]:
        for thread in config["num_threads"]:
          for dir in subconfig["in_dirname"]:
            new_files = glob.glob(config["in_dirname"] +  "*_" + build + "_" +
                                  client_config + "_" + str(thread)
                                  + "_[0-9]*.csv")
            files.extend(new_files)

  cvmfs_internal_dict = getCvmfsInternal(config, section_name)

  if ("time_metrics" in subconfig.keys()
    and len(subconfig["time_metrics"]) > 0):
    print("time_metrics ")
    visualization_plotting.appendToCsv(files,
                                      subconfig["time_metrics"],
                                      "normal", subconfig["full_out_name"],
                                      subconfig["tag"])
  if len(cvmfs_internal_dict) > 0:
    print("internal_affairs_metrics")
    visualization_plotting.appendToCsv(files, cvmfs_internal_dict,
                                    "cvmfs_internal",
                                    subconfig["full_out_name"],
                                    subconfig["tag"])


if __name__ == "__main__":
  config = visualization_read_params.getConfig()

  print("Extracted values from files (not YAML config):")

  for section in config.keys():
    if "global_" in section:
      continue
    if section.startswith("_"):
      continue

    print(section)

    print("\n\n")
    print("Procession section", section, "of type", config[section]["type"])
    subconfig = config[section]

    if config[section]["type"] in visualization_read_params.boxplot_dict.keys():
      if config["_verbose"] == True:
        print("In dirname:", subconfig["in_dirname"])
        print("Num files found:", len(subconfig["_files"]))
        print("Commands:", subconfig["_cmds"])
        print("Client configs:", subconfig["_client_configs"])
        print("Build names:", subconfig["_cvmfs_build_names"])
        print("Build and config names:", subconfig["_build_name_and_config"])
        print("Num threads:", subconfig["_num_threads"])

      if "plot_title" in subconfig.keys():
        boxplot_title = subconfig["plot_title"]
      else:
        boxplot_title = visualization_read_params.boxplot_dict[subconfig["type"]]
      _plot_boxplots(config, section, subconfig["type"], boxplot_title)
    elif visualization_read_params.type_scatter_plot == subconfig["type"]:
      _createScatter(config, section)
    elif visualization_read_params.type_append_to_csv == subconfig["type"]:
      _appendToCsv(config, section)