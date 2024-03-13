#!/usr/bin/env python3

import os
import glob
import pandas as pd
from collections import defaultdict

def getOutnameWithNextNumber(outdir, outname):
  # find next increment number
  files = glob.glob(outdir + outname + "[0-9]*.csv")
  new_num = len(files)

  return outname + str(new_num)

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
