#!/usr/bin/env python3

import argparse
import sys
import textwrap
import yaml
import glob
import argcomplete

from util_visualization import visualization_time

config_dict_layout = """
config = {
  "_files": <string-array>                      // internal usage: filenames found in in_dirname
  "_client_configs": <string-array>             // internal usage: client config found in _files
  "_cmds": <string-array>                       // internal usage: cmds found in _files
  "_cvmfs_build_names": <string-array>          // internal usage: _cvmfs_build_names found in _files
  "_num_threads": <int-array>                   // internal usage: _cvmfs_build_names found in _files
  "in_dirname": <string>,                       // mandatory
                                                // (the outdir used in start_benchmark.py)
  "out_dirname": <string>,                      // (default: ./results)
  "num_threads": <int-array>,                   // (default: use all found)
  "cvmfs_build_name_labels": <dict>,            // mandatory, format "cvmfs_build_name" : "(replacement) label"
  "client_config_labels" : <dict>,              // mandatory, format "client_config" : "(replacement) label"
  "cvmfs_build_comparison": {                   // optional section
    "cvmfs_build_names": <string-array>,        // mandatory if version_comparison
    "client_configs": <string-array>,           // mandatory if version_comparison
    "time_metrics": <string-array>,
    "internal_affairs_metrics": <string-array>,
    "internal_affairs_repos": <string-array>,   // mandatory if internal_affairs_metrics
    "plot": bool
  },
  "client_config_comparison": {                 // optional section
    "cvmfs_build_names": <string-array>,        // mandatory
    "client_configs": <string-array>,           // (default: use all)
    "time_metrics": <string-array>,
    "internal_affairs_metrics": <string-array>,
    "internal_affairs_repos": <string-array>,   // mandatory if internal_affairs_metrics
    "plot": bool
  },
  "scatter_plot": {                             // optional section
    "cvmfs_build_names": <string-array>,        // mandatory
    "client_configs": <string-array>,           // (default: use all)
    "time_metrics": <string-array>,
    "internal_affairs_metrics": <string-array>,
    "internal_affairs_repos": <string-array>,   // mandatory if internal_affairs_metrics
    "plot": bool
  },
  "append_to_csv": {                            // optional section
    "full_out_name": <string>,                  // mandatory
    "cvmfs_build_names": <string-array>,        // (default: use all)
    "client_configs": <string-array>,           // (default: use all)
    "time_metrics": <string-array>,
    "internal_affairs_metrics": <string-array>,
    "internal_affairs_repos": <string-array>,   // mandatory if internal_affairs_metrics
    "tag": <string>,                            // mandatory
    "write": bool
  }
}
"""
def verifyYAML(config):
  # check general options
  for label in ["in_dirname", "cvmfs_build_name_labels", "client_config_labels"]:
    if (not label in config.keys() or len(config[label]) < 1):
      print('Mandatory parameter missing: ' + label, file=sys.stderr)
      exit(22)

  if (not "out_dirname" in config.keys()):
    config["out_dirname"] = "./results"
  elif (len(config["out_dirname"]) < 3):
    print('Mandatory parameter too short: "out_dirname"', file=sys.stderr)
    exit(22)

  if (not "cvmfs_build_comparison" in config.keys()
      and not "client_config_comparison" in config.keys()
      and not "scatter_plot" in config.keys()
      and not "append_to_csv" in config.keys()):
    print('No output section defined! cvmfs_build_comparison,' +
          'client_config_comparison, scatter_plot, or append_to_csv',
          file=sys.stderr)
    exit(22)

  set_cvmfs_build_name_labels = set()
  set_client_config_labels = set()
  # check boxplot: cvmfs build comparison, client config comparison
  for section in ["cvmfs_build_comparison", "client_config_comparison"]:
    if (not section in config.keys()):
      continue


    if (not "plot" in config[section].keys()):
      config[section]["plot"] = True
    elif (type(True) != type(config[section]["plot"])):
      print('Section: "' + section + '": "plot" is not a boolean',
            file=sys.stderr)
      exit(22)

    # we skip checking this section if its not requested to be executed
    if config[section]["plot"] == False:
      continue

    for label in ["cvmfs_build_names", "client_configs"]:
      if (not label in config[section].keys()
          or len(config[section][label]) == 0):
        print('Mandatory parameter missing or empty: ' + section + ': '
              + label, file=sys.stderr)
        exit(22)
      else:
        if label == "cvmfs_build_names":
          for ele in config[section][label]:
            set_cvmfs_build_name_labels.add(ele)
        if label == "client_configs":
          for ele in config[section][label]:
            set_client_config_labels.add(ele)

    if ((not "time_metrics" in config[section].keys()
         or len(config[section]["time_metrics"]) == 0)
        and (not "internal_affairs_metrics" in config[section].keys()
             or len(config[section]["internal_affairs_metrics"]) == 0)):
      print('Section: "' + section + '" defined but neither "time_metrics" or '
            + ' "internal_affairs_metrics" defined or they are empty',
            file=sys.stderr)
      exit(22)

    if ("internal_affairs_metrics" in config[section].keys()
        and len(config[section]["internal_affairs_metrics"]) > 0):
      if (not "internal_affairs_repos" in config[section].keys()
          or len(config[section]["internal_affairs_repos"]) < 1):
        print('Section: ' + section + ' and "internal_affairs_metrics defined '
            + ' but "internal_affairs_repos" is missing or empty',
            file=sys.stderr)
        exit(22)



  # check for general labels
  for a_set, a_label in [(set_cvmfs_build_name_labels, "cvmfs_build_name_labels"),
                         (set_client_config_labels, "client_config_labels")]:
    for label in a_set:
      if not label in config[a_label].keys():
        print('Label: ' + a_label + ' does not define ' + label, file=sys.stderr)
        exit(22)

  # check there is something to compare
  if ("cvmfs_build_comparison" in config.keys()
      and len(config["cvmfs_build_comparison"]["cvmfs_build_names"]) < 2
      and config["cvmfs_build_comparison"]["plot"] != False):
    print('cvmfs_build_comparison: to few elements in "cvmfs_build_names" '
          + 'to perform comparison', file=sys.stderr)
    exit(22)

  if ("client_config_comparison" in config.keys()
      and len(config["client_config_comparison"]["client_configs"]) < 2
      and config["client_config_comparison"]["plot"] != False):
    print('client_config_comparison: to few elements in "client_configs" '
          + 'to perform comparison', file=sys.stderr)
    exit(22)

  # check scatter plot
  if "scatter_plot" in config.keys():
    if (not "plot" in config["scatter_plot"].keys()):
      config["scatter_plot"]["plot"] = True
    elif (type(True) != type(config["scatter_plot"]["plot"])):
      print('Section: "scatter_plot": "plot" is not a boolean',
            file=sys.stderr)
      exit(22)

    # we skip checking this section if its not requested to be executed
    if config["scatter_plot"]["plot"] == True:
      for label in ["cvmfs_build_names", "client_configs"]:
        if (label in config["scatter_plot"].keys()
            and len(config["scatter_plot"][label]) == 0):
          print('Section: "scatter_plot" defined but "cvmfs_build_names" or '
                + ' "client_configs" are empty. Either remove them to create the '
                + 'scatter plots for all files or restrict it', file=sys.stderr)
          exit(22)

      if ((not "time_metrics" in config["scatter_plot"].keys()
          or len(config["scatter_plot"]["time_metrics"]) == 0)
          and (not "internal_affairs_metrics" in config["scatter_plot"].keys()
              or len(config["scatter_plot"]["internal_affairs_metrics"]) == 0)):
          print('Section: "scatter_plot" defined but neither "time_metrics" or '
                + ' "internal_affairs_metrics" defined or they are empty',
                file=sys.stderr)
          exit(22)

      if ("internal_affairs_metrics" in config["scatter_plot"].keys()):
        if (not "internal_affairs_repos" in config["scatter_plot"].keys()
            or len(config["scatter_plot"]["internal_affairs_repos"]) < 1):
          print('Section: "scatter_plot" and "internal_affairs_metrics defined '
              + ' but "internal_affairs_repos" is missing or empty',
              file=sys.stderr)
          exit(22)

        if (len(config["scatter_plot"]["internal_affairs_metrics"]) == 1
            and config["scatter_plot"]["internal_affairs_metrics"] == "all"):
          config["scatter_plot"]["internal_affairs_metrics"] = ["_" + key for key in \
                            visualization_time.measurement_cvmfs_internal_dict.keys()]
          print(config["scatter_plot"]["internal_affairs_metrics"])



  # check append csv
  if "append_to_csv" in config.keys():
    if (not "write" in config["append_to_csv"].keys()):
      config["append_to_csv"]["write"] = True
    elif (type(True) != type(config["append_to_csv"]["write"])):
      print('Section: "append_to_csv": "write" is not a boolean',
            file=sys.stderr)
      exit(22)

    # we skip checking this section if its not requested to be executed
    if config["append_to_csv"]["write"] == True:
      for label in ["full_out_name", "tag"]:
        if (not label in config["append_to_csv"].keys()
            or len(config["append_to_csv"][label]) == 0):
          print('Mandatory parameter missing or too short: "append_to_csv": '
                + label, file=sys.stderr)
          exit(22)

      for label in ["cvmfs_build_names", "client_configs"]:
        if (label in config["append_to_csv"].keys()
            and len(config["append_to_csv"][label]) == 0):
          print('Section: "append_to_csv" defined but "cvmfs_build_names" or '
                + ' "client_configs" are empty. Either remove them to create the '
                + 'scatter plots for all files or restrict it', file=sys.stderr)
          exit(22)

      if ((not "time_metrics" in config["append_to_csv"].keys()
          or len(config["append_to_csv"]["time_metrics"]) == 0)
          and (not "internal_affairs_metrics" in config["append_to_csv"].keys()
              or len(config["append_to_csv"]["internal_affairs_metrics"]) == 0)):
          print('Section: "append_to_csv" defined but neither "time_metrics" or '
                + ' "internal_affairs_metrics" defined or they are empty',
                file=sys.stderr)
          exit(22)

      if ("internal_affairs_metrics" in config["append_to_csv"].keys()):
        if (not "internal_affairs_repos" in config["append_to_csv"].keys()
            or len(config["append_to_csv"]["internal_affairs_repos"]) < 1):
          print('Section: initializeConfig"append_to_csv" and "internal_affairs_metrics" defined '
              + ' but "internal_affairs_repos" is missing or empty',
              file=sys.stderr)
          exit(22)



def initConfig(config):
  config["_files"] = glob.glob(config["in_dirname"] + "*.csv")
  config["_cmds"] = list(visualization_time.getDistinctCommands(config["_files"]))
  config["_cmds"].sort(key=lambda s: s.lower())
  config["_client_configs"] = list(visualization_time.getDistinctClientConfigs(config["_files"]))
  config["_client_configs"].sort(key=lambda s: s.lower())
  config["_cvmfs_build_names"] = list(visualization_time.getDistincBuilds(config["_files"]))
  config["_cvmfs_build_names"].sort(key=lambda s: s.lower())
  config["_num_threads"] = list(visualization_time.getDistinctThreads(config["_files"]))
  config["_num_threads"].sort()

  if (not "num_threads" in config.keys()
      or len(config["num_threads"]) == 0):
    print("Automatically extracting number of threads...  ", end="")
    config["num_threads"] = list(visualization_time.getDistinctThreads(config["_files"]))
    print("done")
  config["num_threads"].sort()

  for ele in config["num_threads"]:
    if not ele in config["_num_threads"]:
      print('"num_threads" set with illegal values. Valid values are',
            config["_num_threads"], file=sys.stderr)
      exit(22)

  for section in ["cvmfs_build_comparison", "client_config_comparison",
                  "scatter_plot", "append_to_csv"]:
    if section in config.keys():
      create_output = "plot" if section != "append_to_csv" else "write"
      if config[section][create_output] == False:
        continue

      for param in ["cvmfs_build_names", "client_configs"]:
        if param in config[section].keys():
          for ele in config[section][param]:
            if not ele in config["_" + param]:
              print('"' + section + '": "' + param + '" set with illegal values. '
                    + 'Valid values are', config["_" + param], file=sys.stderr)
              exit(22)


  # if internal_affairs_metrics: ["all"]
  # use all known cvmfs internal affairs parameters
  for section in ["cvmfs_build_comparison", "client_config_comparison",
                  "scatter_plot", "append_to_csv"]:
    if section in config.keys():
        param = "internal_affairs_metrics"
        if (param in config[section].keys()
            and len(config[section][param]) == 1
            and config[section][param] == ["all"]):
          config[section][param] = [key for key in \
                      visualization_time.measurement_cvmfs_internal_dict.keys()]

def getConfig():
  parsed_args = parse_arguments()

  if (parsed_args.help_config):
    print("my special help for creating YAML configs")
    exit(0)

  if (parsed_args.help_output):
    print("my special help for different output options")
    exit(0)

  if (parsed_args.config_file):
    print("Load config", parsed_args.config_file)
    with open(parsed_args.config_file, 'r') as file:
      config = yaml.safe_load(file)
  else:
    print("No YAML config file given")
    exit(3)
    # TODO RETURN ERROR AND EXIT

  verifyYAML(config)
  initConfig(config)

  print("all good")
  return config

def parse_arguments():
  parser = argparse.ArgumentParser(
    prog='python3 start_visualization.py -c <myconfig>.yaml',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent("""
CVMFS Benchmark Visualizer
==========================

This is my
super long
    description
    """),
    epilog="end of description")
  parser.add_argument('-c', '--config-file',
                      help='YAML config file',
                      required=False).completer = getFiles
  parser.add_argument('--help-config',
                      help='More help: How to build the YAML config file',
                      required=False,
                      action="store_true")
  parser.add_argument('--help-output',
                      help='More help: Output of this program (plots, ..)',
                      required=False,
                      action="store_true")
  argcomplete.autocomplete(parser)
  return parser.parse_args()


def getFiles(prefix, parsed_args, **kwargs):
  files = glob.glob(prefix + "*")
  return files
