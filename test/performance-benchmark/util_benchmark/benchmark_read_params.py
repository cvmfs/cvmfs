import argparse
import sys
import textwrap
import yaml
import glob
import argcomplete

# pretty print config
import json

config_dict_layout = """
config = {
  "avail_client_configs": <dict>             // name and function of client config param
  "avail_cmds": <dict>                       // name, command and repo of available commands to run
  "run-cvmfs": {               // at least one run section must be available
    "commands": <string-array>                  // mandatory, names of cmds to run; must be part of avail_commands
    "cvmfs_build_dirs": <string-array>,         // mandatory, directories to build path to run different cvmfs versions
    "client_configs": <string-array>,           // mandatory, names of client_config params to enable for the runs; must be part of avail_cmds
    "num_threads": <int-array>,                 // mandatory, ist of how many threads should be run with (1 thread = 1 process)
    "out_name_replacements": <string dict>      // optional, allows to replace the <version> section of the out file name in case different setups for the same version are run
    "repetitions": int,                         // (default: 10), how many repetitions per cache pattern should be run
    "use_autofs": bool,                         // (default: off), use autofs or manually mount to /cvmfs
    "out_dirname": string                       // (default: ./data) directory name to where the results are written
    "use_cvmfs": bool                           // (default: true) if cmds do not use cvmfs, cvmfs related stuff is skipped
  },
  "run-non-cvmfs": {               // at least one run section must be available
    "commands": <string-array>                  // mandatory, names of cmds to run; must be part of avail_commands
    "num_threads": <int-array>,                 // mandatory, ist of how many threads should be run with (1 thread = 1 process)
    "repetitions": int,                         // (default: 10), how many repetitions per cache pattern should be run
    "out_dirname": string                       // (default: ./data) directory name to where the results are written
    "use_cvmfs": bool                           // (default: true) if cmds do not use cvmfs, cvmfs related stuff is skipped
    // other members listed in "run-cvmfs" will be just ignored (and if needed replaced by placeholder values)
  },
"""

avail_client_configs_layout = """
avail_client_configs = {
  "default" : ['CVMFS_HTTP_PROXY="http://ca-proxy.cern.ch:3128"',
               'CVMFS_AUTO_UPDATE=NO'],
  "symlink" : 'CVMFS_CACHE_SYMLINKS=1',
  "nocache" : 'CVMFS_KCACHE_TIMEOUT=0'
}
"""

avail_cmds_layout = """
avail_cmds = {
  "tensorflow" : { "command": "./scripts/50-tensorflow.sh",
                   "repos": [ "sft.cern.ch" ] },
  "root" :       { "command": "./scripts/51-root.sh",
                   "repos": [ "sft.cern.ch" ] },
}
"""

def verifyNoCvmfsRun(config, run_name):
  # check mandatory params
  for label in ["commands", "num_threads"]:
    if (not label in config[run_name].keys()
        or len(config[run_name][label]) == 0):
      print('In run section: "' + run_name + '": Mandatory parameter '
        + label + ' missing or empty',
        file=sys.stderr)
      exit(22)

  # check requested commands
  for cmd in config[run_name]["commands"]:
    # are they defined in avail_cmds?
    if not cmd in config["avail_cmds"].keys():
      print('In run section: "' + run_name + '": Unknown cmd '
        + cmd + ' not defined in avail_cmds',
        file=sys.stderr)
      exit(22)

    # cmds have no cvmfs repo set?
    if ("repos" in config["avail_cmds"][cmd].keys()
        and len(config["avail_cmds"][cmd]["repos"]) > 0):
      print('In non-cvmfs run section: "' + run_name + '": '
      + cmd + ' has cvmfs repos defined.\n'
      + 'Please double check!\n'
      + 'It is impossible to mix non-cvmfs and cvmfs commands in the same run section',
      file=sys.stderr)
      exit(22)

  # just to have the loops corretly running in start_benchmark.py
  config[run_name]["cvmfs_build_dirs"] = [ "dummy" ]
  config[run_name]["client_configs"] = [ ["dummy"] ]

def verifyCvmfsRun(config, run_name):
  # check mandatory params
  for label in ["commands", "cvmfs_build_dirs", "num_threads", "client_configs"]:
    if (not label in config[run_name].keys()
        or len(config[run_name][label]) == 0):
      print('In run section: "' + run_name + '": Mandatory parameter '
        + label + ' missing or empty',
        file=sys.stderr)
      exit(22)

  # check requested commands to be defined in "avail_cmds"
  for cmd in config[run_name]["commands"]:
    if not cmd in config["avail_cmds"].keys():
      print('In run section: "' + run_name + '": Unknown cmd '
        + cmd + ' not defined in avail_cmds',
        file=sys.stderr)
      exit(22)

    # cmds have cvmfs repo set?
    if "repos" in config["avail_cmds"][cmd].keys():
      if len(config["avail_cmds"][cmd]["repos"]) == 0:
        print('In cvmfs run section: "' + run_name + '": '
        + cmd + ' cvmfs repos is missing.\n'
        + 'Please double check!\n'
        + 'It is impossible to mix non-cvmfs and cvmfs commands in the same run section',
        file=sys.stderr)
      exit(22)
    else:
      print('In cvmfs run section: "' + run_name + '": '
        + cmd + ' cvmfs repos is missing.\n'
        + 'Please double check!\n'
        + 'It is impossible to mix non-cvmfs and cvmfs commands in the same run section',
        file=sys.stderr)
      exit(22)

  for client_config in config[run_name]["client_configs"]:
    for ele in client_config:
      if not ele in config["avail_client_configs"].keys():
        print('In run section: "' + run_name + '": Unknown client_config '
          + ele + ' not defined in avail_client_configs',
          file=sys.stderr)
        exit(22)


def verifyYAML(config):
  # check that commands and client configs are defined
  for label in ["avail_client_configs", "avail_cmds"]:
    if (not label in config.keys()
        or len(config[label]) == 0):
      print('Mandatory parameter missing or empty: ' + label, file=sys.stderr)
      exit(22)


  # check that at least one "run" section is contained
  has_run = False
  for key in config.keys():
    if "run" in key:
      has_run = True

  if not has_run:
    print('At least one mandatory section "run" is missing', file=sys.stderr)
    exit(22)

  # verify each run config to be correct
  for key in config.keys():
    if "run" in key:
      if (not "use_cvmfs" in config[key].keys()):
        config[key]["use_cvmfs"] = True
      elif (type(True) != type(config[key]["use_cvmfs"])):
        print('Section: "' + key + '": "use_cvmfs" is not a boolean',
              file=sys.stderr)
        exit(22)

      if config[key]["use_cvmfs"] == True:
        verifyCvmfsRun(config, key)
      else:
        verifyNoCvmfsRun(config, key)


def initConfig(config):
  for key in config.keys():
    if "run" in key:
      label = "repetitions"
      if (not label in config[key].keys()):
        config[key][label] = int(10)
      label = "out_dirname"
      if (not label in config[key].keys()
          or len(config[key][label]) == 0):
        config[key][label] = "./data/" + key + "/"
      else:
        if config[key][label][-1] != "/":
          config[key][label] += "/"


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

  print(json.dumps(config, sort_keys=True, indent=2))
  print("all good")
  return config

def parse_arguments():
  parser = argparse.ArgumentParser(
    prog='python3 start_benchmark.py -c <myconfig>.yaml',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent("""
CVMFS Benchmark
===============

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


# def setParamConfig():
  # ##############################################################################
  # ## PARAMS set by user
  # #########################

  # # selected commands to run (cmds are in util_benchmark/benchmark_cmds.py)
  # # to combine multiple cmd sets use: { **chep23_lhcb_commands, **chep23_atlas_commands}
  # commands = benchmark_cmds.chep23_selected_commands

  # # how often the command is timed in a row for each cache type
  # repetitions = 3

  # # if autofs should be used; better without
  # # if =False make sure that autofs is stopped and autofs does not continue
  # #           to mount /cvmfs (use `umount -lf /cvmfs`)
  # use_autofs = False

  # # array of build dirs of cvmfs to run the performance benchmark with
  # # see getOutname() to destinguish between same version but different branch
  # cvmfs_build_dirs = ["/home/<user>/cvmfs/build", "/home/<user>/cvmfs-other/build"]
  # thread_configs = [1] # array; with how many threads the program should be run

  # # combination of cvmfs client config that should be in addition enabled
  # # best to separate params with "_"; see setCvmfsConfig() for more
  # run_options = ["symlink_statfs_kernel_trace", "statfs_kernel"]

  # # base dir where the results should be written to
  # outdir = "./data/"

  # #########################
  # ## END PARAMS set by user
  # ##############################################################################