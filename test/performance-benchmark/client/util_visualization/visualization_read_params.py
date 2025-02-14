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
  "client_and_config_comparison": {                 // optional section
    "build_name_and_config": <string-array>,        // mandatory
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


type_cvmfs_build_comparison = "cvmfs_build_comparison"
type_client_config_comparison = "client_config_comparison"
type_client_and_config_comparison = "client_and_config_comparison"
type_scatter_plot = "scatter_plot"
type_append_to_csv = "append_to_csv"

boxplot_dict = {
  type_cvmfs_build_comparison: "CVMFS Build Comparison",
  type_client_config_comparison: "CVMFS Client Config Comparison",
  type_client_and_config_comparison: "CVMFS Client and Config Comparison"
}



all_plot_types = [type_cvmfs_build_comparison, type_client_config_comparison,
                  type_client_and_config_comparison, type_scatter_plot,
                  type_append_to_csv]

def verifyAppendCSV(subconfig, section_name):
  if (not "write" in subconfig.keys()):
    subconfig["write"] = True
  elif (type(True) != type(subconfig["write"])):
    print('In append_to_csv section:', section_name, ' - "write" is not a boolean',
          file=sys.stderr)
    exit(22)

  # we skip checking this section if its not requested to be executed
  if subconfig["write"] == False:
    print("Skipping verification of append_to_csv section:",
          section_name, "- writing is disabled")
    return

  for label in ["full_out_name", "tag"]:
    if (not label in subconfig.keys()
        or len(subconfig[label]) == 0):
      print('In append_to_csv section:', section_name,
            '- Mandatory parameter missing or too short: "append_to_csv": '
            + label, file=sys.stderr)
      exit(22)

  for label in ["cvmfs_build_names", "client_configs"]:
    if (label in subconfig.keys() and len(subconfig[label]) == 0):
      print('Append_to_csv section:', section_name, 'the defined "cvmfs_build_names" or'
            + ' "client_configs" are empty. Either remove them to create the'
            + ' scatter plots for all files or restrict it', file=sys.stderr)
      exit(22)

  if ((not "time_metrics" in subconfig.keys()
        or len(subconfig["time_metrics"]) == 0)
      and (not "internal_affairs_metrics" in subconfig.keys()
        or len(subconfig["internal_affairs_metrics"]) == 0)):
    print('Append_to_csv section:', section_name, '- neither "time_metrics" or'
          + ' "internal_affairs_metrics" are defined or they are empty',
          file=sys.stderr)
    exit(22)

  if ("internal_affairs_metrics" in subconfig.keys()):
    if (not "internal_affairs_repos" in subconfig.keys()
        or len(subconfig["internal_affairs_repos"]) < 1):
      print('In append_to_csv section:', section_name,
            '- "internal_affairs_metrics" is defined ',
            'but "internal_affairs_repos" is missing or empty', file=sys.stderr)
      exit(22)

def verifyScatterplot(subconfig, section_name):
  if (not "plot" in subconfig.keys()):
      subconfig["plot"] = True
  elif (type(True) != type(subconfig["plot"])):
    print('In Scatter plot section: ', section_name, ' "plot" is not a boolean',
          file=sys.stderr)
    exit(22)

  # we skip checking this section if its not requested to be executed
  if subconfig["plot"] == False:
    print("Skipping verification of scatter plot section:",
          section_name, "as plotting is disabled")
    return

  for label in ["cvmfs_build_names", "client_configs"]:
    if (label in subconfig.keys() and len(subconfig[label]) == 0):
      print('Scatter plot section:', section_name, 'defined but "cvmfs_build_names" or '
            + ' "client_configs" are empty. Either remove them to create the '
            + 'scatter plots for all files or restrict it', file=sys.stderr)
      exit(22)

  if ((not "time_metrics" in subconfig.keys()
        or len(subconfig["time_metrics"]) == 0)
      and (not "internal_affairs_metrics" in subconfig.keys()
        or len(subconfig["internal_affairs_metrics"]) == 0)):
    print('Scatter plot section:', section_name, ' defined but neither "time_metrics" or '
          + ' "internal_affairs_metrics" defined or they are empty',
          file=sys.stderr)
    exit(22)

  if ("internal_affairs_metrics" in subconfig.keys()):
    if (not "internal_affairs_repos" in subconfig.keys()
        or len(subconfig["internal_affairs_repos"]) < 1):
      print('Scatter plot section:', section_name, 'has "internal_affairs_metrics" defined '
          + ' but "internal_affairs_repos" is missing or empty',
          file=sys.stderr)
      exit(22)

    if (len(subconfig["internal_affairs_metrics"]) == 1
        and subconfig["internal_affairs_metrics"] == "all"):
      subconfig["internal_affairs_metrics"] = ["_" + key for key in
                      visualization_time.measurement_cvmfs_internal_dict.keys()]
      print(subconfig["internal_affairs_metrics"])

def verifyBoxplot(globalconfig, section_name, boxplot_type, selected_label_names):
  subconfig = globalconfig[section_name]
  # we skip checking this section if its not requested to be executed
  if (not "plot" in subconfig.keys()):
    subconfig["plot"] = True
  elif (type(True) != type(subconfig["plot"])):
    print('In', boxplot_type, 'section:', section_name, '"plot" is not a boolean',
          file=sys.stderr)
    exit(22)
  
  # we skip checking this section if its not requested to be executed
  if subconfig["plot"] == False:
    print("Skipping verification of", boxplot_type, 'section:',
          section_name, "as plotting is disabled")
    return
  
  for label in ["in_dirname", "num_threads"]:
    if (not label in subconfig.keys() or len(subconfig[label]) < 1):
      if ("global_" + label in globalconfig.keys() or len(globalconfig[label]) < 1):
        subconfig[label] = globalconfig["global_" + label]
      else:
        print('In', boxplot_type, 'section:', section_name,
              '- Mandatory parameter missing:', label, file=sys.stderr)
        exit(22)

  if (not "out_dirname" in subconfig.keys()):
    if ("global_out_dirname" in globalconfig.keys()):
      subconfig["out_dirname"] = globalconfig["global_out_dirname"]
    else:
      subconfig["out_dirname"] = "./results"
  if (len(subconfig["out_dirname"]) < 3):
    print('Mandatory parameter too short: "out_dirname"',
          subconfig["out_dirname"] , file=sys.stderr)
    exit(22)

  # check time_metrics
  if ((not "time_metrics" in subconfig.keys()
        or len(subconfig["time_metrics"]) == 0)
      and (not "internal_affairs_metrics" in subconfig.keys()
            or len(subconfig["internal_affairs_metrics"]) == 0)):
    print('In', boxplot_type, 'section:', section_name,
          '" defined but neither "time_metrics" or "internal_affairs_metrics"',
          'defined or they are empty', file=sys.stderr)
    exit(22)

  # check internal_affairs_metrics
  if ("internal_affairs_metrics" in subconfig.keys()
      and len(subconfig["internal_affairs_metrics"]) > 0):
    if (not "internal_affairs_repos" in subconfig.keys()
        or len(subconfig["internal_affairs_repos"]) < 1):
      print('In', boxplot_type, 'section:', section_name,
            'and "internal_affairs_metrics defined but "internal_affairs_repos"',
            'is missing or empty', file=sys.stderr)
      exit(22)

  # yaml.dump(subconfig, sys.stdout)
  for selected_label_name in selected_label_names:
    if isinstance(selected_label_name, list):
      selected_label_name = selected_label_name[0]
    if (not selected_label_name in subconfig.keys()
        or len(subconfig[label]) == 0):
      if ("global_" + selected_label_name in globalconfig.keys()):
        subconfig[selected_label_name] = globalconfig["global_" + selected_label_name]
      else:
        print('In', boxplot_type, 'section:', section_name, 
              '- Mandatory parameter missing or empty:', selected_label_name,
              file=sys.stderr)
        exit(22)

    set_labels = set()
    for ele in subconfig[selected_label_name]:
      set_labels.add(ele)

    # check if all requested labels exist
    for a_label in set_labels:
      if not a_label in subconfig[selected_label_name].keys():
        print('In', boxplot_type, 'section:', section_name, 'label:',
              selected_label_name, 'does not define', a_label, file=sys.stderr)
        exit(22)

    if (len(subconfig[selected_label_name]) < 2):
      print('In', boxplot_type, 'section:', section_name, "to few elements in",
            selected_label_name, "to perform comparison", file=sys.stderr)
      exit(22)


def verifyYAML(config):
  # check general options
  has_plots = False
  for key in config.keys():
    if not "global_" in key:
      if "type" in config[key].keys():
        if config[key]["type"] in all_plot_types:
          has_plots = True
  
  if has_plots == False:
    print('No plotting section defined! Must contain "type" of value: ',
          all_plot_types, file=sys.stderr)
    exit(22)
  
  boxplot_selection = [ ("cvmfs_build_comparison",
                            ["cvmfs_build_name_labels", "client_config_labels"],
                            ["cvmfs_build_names", "client_configs"]),
                        ("client_config_comparison",
                            ["cvmfs_build_name_labels", "client_config_labels"],
                            ["cvmfs_build_names", "client_configs"]),
                        ("client_and_config_comparison",
                            [["build_name_and_config_labels"]],
                            [["build_name_and_config"]])
                      ]
  
  for section in config.keys():
    if "global_" in section:
      continue
    subconfig = config[section]
    if "type" in subconfig.keys():
      type = subconfig["type"]
      if type in boxplot_dict.keys():
        for boxplot_type, selected_label_names, selected_names in boxplot_selection:
          if type == boxplot_type:
            verifyBoxplot(config, section, boxplot_type, selected_label_names)
            initSection(config, section, boxplot_type, selected_names)
      elif type == type_append_to_csv:
        verifyAppendCSV(subconfig, section)
        # TODO initSection?!
      elif type == type_scatter_plot:
        verifyScatterplot(subconfig, section)
        # TODO initSection?!
    else:
      print("Section", section, "has no correctly defined 'type'. Allowed values: ",
        all_plot_types, file=sys.stderr)
      exit(22)

def initSection(config, section_name, plot_type, selected_label_names):
  subconfig = config[section_name]
  create_output = "plot" if plot_type != type_append_to_csv else "write"

  if subconfig[create_output] == False:
    return

  subconfig["_files"] = list()
  for dir in subconfig["in_dirname"]:
    files = glob.glob(dir + "*.csv")
    subconfig["_files"] += files

  subconfig["_cmds"] = list(visualization_time.getDistinctCommands(subconfig["_files"]))
  subconfig["_cmds"].sort(key=lambda s: s.lower())
  subconfig["_client_configs"] = list(visualization_time.getDistinctClientConfigs(subconfig["_files"]))
  subconfig["_client_configs"].sort(key=lambda s: s.lower())
  subconfig["_cvmfs_build_names"] = list(visualization_time.getDistincBuilds(subconfig["_files"]))
  subconfig["_cvmfs_build_names"].sort(key=lambda s: s.lower())
  subconfig["_build_name_and_config"] = list(visualization_time.getDistincConfigsAndBuilds(subconfig["_files"]))
  subconfig["_build_name_and_config"].sort(key=lambda s: s.lower())
  subconfig["_num_threads"] = list(visualization_time.getDistinctThreads(subconfig["_files"]))
  subconfig["_num_threads"].sort()
  
  if (not "num_threads" in subconfig.keys()
      or len(subconfig["num_threads"]) == 0):
    print("Automatically extracting number of threads")
    subconfig["num_threads"] = subconfig["_num_threads"]
  subconfig["num_threads"].sort()

  for ele in subconfig["num_threads"]:
    if not ele in subconfig["_num_threads"]:
      print('In section:', section_name, ' - "num_threads" set with illegal values. Valid values are',
            subconfig["_num_threads"], file=sys.stderr)
      exit(22)

  for param in selected_label_names:
    if isinstance(param, list):
      param = param[0]
    if param in subconfig.keys():
      for ele in subconfig[param]:
        if not ele in subconfig["_" + param]:
          print('In section:', section_name, '"' + plot_type + '": "' + param
                + '" set with illegal values: '
                + ele  + '. Valid values are', subconfig["_" + param], file=sys.stderr)
          exit(22)

  if plot_type == type_client_and_config_comparison:
    subconfig["no_outer_iter"] = ["no_outer_iter"]

  # if internal_affairs_metrics: ["all"]
  # use all known cvmfs internal affairs parameters
  param = "internal_affairs_metrics"
  if (param in subconfig.keys() and subconfig[param] == ["all"]):
    subconfig[param] = [key for key in
                      visualization_time.measurement_cvmfs_internal_dict.keys()]

def initConfig(config):
  config["_files"] = list()
  for dir in config["global_in_dirname"]:
    files = glob.glob(dir + "*.csv")
    config["_files"] += files

  config["_cmds"] = list(visualization_time.getDistinctCommands(config["_files"]))
  config["_cmds"].sort(key=lambda s: s.lower())
  config["_client_configs"] = list(visualization_time.getDistinctClientConfigs(config["_files"]))
  config["_client_configs"].sort(key=lambda s: s.lower())
  config["_cvmfs_build_names"] = list(visualization_time.getDistincBuilds(config["_files"]))
  config["_cvmfs_build_names"].sort(key=lambda s: s.lower())
  config["_build_name_and_config"] = list(visualization_time.getDistincConfigsAndBuilds(config["_files"]))
  config["_build_name_and_config"].sort(key=lambda s: s.lower())
  config["_num_threads"] = list(visualization_time.getDistinctThreads(config["_files"]))
  config["_num_threads"].sort()

  if (not "global_num_threads" in config.keys()
      or len(config["global_num_threads"]) == 0):
    print("Automatically extracting number of threads...  ", end="")
    config["global_num_threads"] = list(visualization_time.getDistinctThreads(config["_files"]))
    print("done")
  config["global_num_threads"].sort()

  for ele in config["global_num_threads"]:
    if not ele in config["_num_threads"]:
      print('"num_threads" set with illegal values. Valid values are',
            config["_num_threads"], file=sys.stderr)
      exit(22)

  for section in config.keys():
    if not "global_" in section:
      if "type" not in config[section].keys():
        print('Section', section, " is missing the key 'type'. Allowed values: ",
              all_plot_types, file=sys.stderr)
        exit(22)



class UniqueKeyLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = set()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise ValueError("Duplicate key found in YAML: " + key)
            mapping.add(key)
        return super().construct_mapping(node, deep)

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
      try:
        config = yaml.load(file, Loader=UniqueKeyLoader)
      except ValueError as e:
        print("Error while parsing config:", parsed_args.config_file)
        print(e)
        exit(4)
  else:
    print("No YAML config file given")
    exit(3)

  verifyYAML(config)
  # initConfig(config) todo for scatterplot and appendcsv

  if (parsed_args.verbose):
    config["_verbose"] = True
  else:
    config["_verbose"] = False

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
  parser.add_argument('-v', '--verbose',
                      help='verbose logging',
                      required=False,
                      action="store_true")
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
