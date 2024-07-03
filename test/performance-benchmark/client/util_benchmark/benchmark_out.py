#!/usr/bin/env python3

import os
import glob
import pandas as pd
from collections import defaultdict
from datetime import datetime

################################################################################
# Get "base" of the output name
################################################################################
# Used as a base output name that will be extended for the different outputs:
# data, cvmfs_internals_raw, and tracing
#
# If needed to destinguish different branches/builds this is done based on
# the cvmfs_build_dir. This is just done by simple pattern matching, request
# by values listed in out_name_replacement_of_version. As such, it is important
# that the requested replacement is uniquely identifiable and has no
# "pattern-matching" overlap.
#
################################################################################
def getOutname(cvmfs_build_dir, cmd_name, client_config_list,
               cvmfs_version, out_name_replacement_of_version, str_extra):
  outname = cmd_name + "_" + cvmfs_version

  for ele in out_name_replacement_of_version.keys():
    if ele in cvmfs_build_dir:
      outname += "-" + out_name_replacement_of_version[ele]

  client_config_list.sort()

  for param in client_config_list:
    outname += "_" + param

  outname += "_" + str_extra + "_"

  return outname

def getOutnameWithNextNumber(outdir, outname, ending=".csv"):
  # find next increment number
  files = glob.glob(outdir + outname + "[0-9]*" + ending)
  new_num = len(files)

  return outname + str(new_num)


def writeStats(config, run, cvmfs_build_dir, client_config, cmd_name,
               cvmfs_version, file_extra, cvmfs_show_config,
               ulimit, uname, print_to_screen: bool):
  cmd = config["avail_cmds"][cmd_name]["command"]
  out_name_replacement_of_version = config[run]["out_name_replacement_of_version"]
  outdir = config[run]["out_dirname"]
  file_extra = "config"

  base_out = getOutname(cvmfs_build_dir, cmd_name, client_config,
               cvmfs_version, out_name_replacement_of_version, file_extra)

  out_name = getOutnameWithNextNumber(outdir, base_out, ".txt")

  stat_str = "%s" %datetime.now()
  stat_str += "\nUname: " + uname
  stat_str += "\nUlimit: " + ulimit
  stat_str += "\nCVMFS: " + cvmfs_version + " " + cvmfs_build_dir
  stat_str += "\nClient_config: " + ','.join(client_config)
  stat_str += "\nCommand name: " + cmd_name
  stat_str += "\n" + cmd

  if print_to_screen:
    print(stat_str)

  stat_str += "\n\n" + cvmfs_show_config
  stat_str += "\n"



  with open(outdir + out_name + ".txt", "w") as f:
    f.write(stat_str)

def writeAllResults(config, run, cvmfs_build_dir, client_config, cmd_name,
                    num_threads, cvmfs_version,
                    all_data, all_cvmfs_raw_dict, all_dict_tracing):
  # set output name: auto-increment so not to overwrite old results
  # outname = getOutname(cvmfs_build_dir, name, option, num_threads)
  outname = getOutname(cvmfs_build_dir,
                        cmd_name, client_config, cvmfs_version,
                        config[run]["out_name_replacement_of_version"],
                        str(num_threads))
  final_outname = getOutnameWithNextNumber(config[run]["out_dirname"], outname)

  print("Final outname:", config[run]["out_dirname"] + "/" + final_outname)

  ## write data
  writeResults(config[run]["out_dirname"], final_outname, all_data, cmd_name,
               cvmfs_version, num_threads)
  if config[run]["use_cvmfs"] == True:
    writeResultsInternalRaw(config[run]["out_dirname"], final_outname,
                            all_cvmfs_raw_dict)
    writeResultsTracing(config[run]["out_dirname"], final_outname,
                        all_dict_tracing)  


def writeResults(outdir, outname, data, cmd_label, cvmfs_version, num_threads):
  # Prepare Output
  cmd_label_dict = defaultdict()
  cvmfs_version_dict = defaultdict()
  threads_dict = defaultdict()

  for key in data["cold_cache"].keys():
    cmd_label_dict[key] = cmd_label
    cvmfs_version_dict[key] = cvmfs_version
    threads_dict[key] = num_threads

  data["command_label"] = cmd_label_dict
  data["cvmfs_version"] = cvmfs_version_dict
  data["num_threads"] = threads_dict


  df_results = pd.DataFrame.from_dict(data)
  df_results.to_csv(outdir + outname + ".csv", index_label="labels")

def writeResultsInternalRaw(outdir, outname, dict_full_cvmfs_internals):
  if len(dict_full_cvmfs_internals) < 1:
    return

  cvmfs_outdir = "cvmfs_internal_raw/"
  if os.path.isdir(outdir + cvmfs_outdir) == False:
    os.makedirs(outdir + cvmfs_outdir)

  for cache_type in dict_full_cvmfs_internals.keys():
    cache_label = cache_type.split('_')[0]

    for repo, ele in dict_full_cvmfs_internals[cache_type].items():
      idx = 0
      for single in ele:
        cvmfs_outname = outdir + cvmfs_outdir + outname + "_cvmfs_raw_" + \
                        repo + "_" + cache_label + "_" + str(idx) + ".csv"
        with open(cvmfs_outname, "w") as f:
          f.write(single)
        idx += 1

def writeResultsTracing(outdir, outname, dict_tracing):
  if len(dict_tracing[[*dict_tracing.keys()][0]]) < 1:
    return

  tracing_outdir = "cvmfs_tracing/"
  if os.path.isdir(outdir + tracing_outdir) == False:
    os.makedirs(outdir + tracing_outdir)

  for cache_type in dict_tracing.keys():
    cache_label = cache_type.split('_')[0]
    for repo, ele in dict_tracing[cache_type].items():
      idx = 0
      for single in ele:
        cvmfs_outname = outdir + tracing_outdir + outname + "_cvmfs_tracing_" + \
                        repo + "_" + cache_label + "_" + str(idx) + ".csv"
        with open(cvmfs_outname, "w") as f:
          f.write(single)
        idx += 1
