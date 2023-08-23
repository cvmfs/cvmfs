#!/usr/bin/env python3
import subprocess
from functools import partial
import datetime as dt
import pandas as pd
from collections import defaultdict
import glob
import os

from util_benchmark import benchmark_cmds
from util_benchmark import benchmark_time
from util_benchmark import benchmark_cvmfs
from util_benchmark import benchmark_out

################################################################################
#
#                       CVMFS - PERFORMANCE BENCHMARK
#
################################################################################
#
# Performance benchmark to time jobs/commands executed using CVMFS.
# 
# For this the command "time" is being used to extract metrics from the system
#
# This benchmark provides automatized utilities to:
# - run on multiple  manually compiled cvmfs installations (cvmfs/build dir needed)
# - run multiple different thread configurations
# - run with multiple different CVMFS client configurations
# - auto-increment file name so that rerunning the same config does not
#   overwrite old results
#
# In the main function below, there is a section of user defined parameters
#
# Output ($outdir is defined by user):
# - csv file with all metrics extracted with "time" and cvmfs internal affairs
#   counters (dir: $outdir)
# - raw output of cvmfs internal affairs (dir: $outdir/cvmfs_internal_raw)
# - if client config uses TRACEFILE then the tracing is written to
#   (dir: $outdir/cvmfs_tracing)
#
################################################################################


################################
# Get "base" of the output name
################################
# Includes: cvmfs_version, run options and number of threads
# 
# If needed to destinguish different branches/builds this is done based on
# the build dir of cvmfs (assumption here is that different branches are
# saved/built in different folders)
#
# Used as a base to extended for the different outputs:
# data, cvmfs_internals_raw, and tracing
def getOutname(cvmfs_build_dir, name, option, num_threads):
  if "after" in cvmfs_build_dir:
    outname = name + "_" + cvmfs_version + "-after" + "_" + option + \
              "_" + str(num_threads) + "_"
  elif "before" in cvmfs_build_dir:
    outname = name + "_" + cvmfs_version + "-before" + "_" + option + \
              "_" + str(num_threads) + "_"
  elif "bisect" in cvmfs_build_dir:
    outname = name + "_" + cvmfs_version + "-bisect" + "_" + option + \
              "_" + str(num_threads) + "_"
  else:
    outname = name + "_" + cvmfs_version + "_" + option + "_" + \
              str(num_threads) + "_"

  return outname

###################
# Set CVMFS Config
###################
#
# CVMFS client config parameters chosen based on which "options" are added
# Format of options: string where each option is split by "_"
# e.g. "symlink_trace_debuglog"
# 
# When adding new options, make sure that are not partial matches of each other
# as we just check the entire option-string if the option is part of it
# (we dont split by "_" )
def setCvmfsConfig(filename, option, print_config=True):
  new_config = 'CVMFS_HTTP_PROXY="http://ca-proxy.cern.ch:3128"\n'
  new_config += "CVMFS_QUOTA_LIMIT=10000\n"
  new_config += "CVMFS_AUTO_UPDATE=NO\n"
  if "symlink" in option:
    new_config += "CVMFS_CACHE_SYMLINKS=1\n"
  if "statfs" in option:
    new_config += "CVMFS_STATFS_CACHE_TIMEOUT=600\n"
  if "debuglog" in option:
    new_config += "CVMFS_DEBUGLOG=/tmp/debuglog.cvmfs-benchmark\n"
  if "kernel" in option:
    new_config += "CVMFS_KCACHE_TIMEOUT=600\n"
  if "nocache" in option:
    new_config += "CVMFS_KCACHE_TIMEOUT=0\n"
  if "trace" in option:
    new_config += "CVMFS_TRACEFILE=/tmp/cvmfs-benchmark_cvmfs-trace-@fqrn@.log\n"

  with open(filename, "w") as f:
    f.write(new_config)

  if print_config == True:
    print("CVMFS CONFIG in /etc/cvmfs/default.local:")
    with open("/etc/cvmfs/default.local", "r") as cvmfs_config:
      for line in cvmfs_config:
        print(line, end="")

##
# Main function that times the benchmark
##
# See description at the very top of this file "CVMFS - PERFORMANCE BENCHMARK"
#
if __name__ == "__main__":
  
  ##############################################################################
  ## PARAMS set by user
  #########################

  # selected commands to run (cmds are in util_benchmark/benchmark_cmds.py)
  # to combine multiple cmd sets use: { **chep23_lhcb_commands, **chep23_atlas_commands}
  commands = benchmark_cmds.chep23_selected_commands

  # how often the command is timed in a row for each cache type
  repetitions = 3  
  
  # if autofs should be used; better without
  # if =False make sure that autofs is stopped and autofs does not continue
  #           to mount /cvmfs (use `umount -lf /cvmfs`)
  use_autofs = False

  # array of build dirs of cvmfs to run the performance benchmark with
  # see getOutname() to destinguish between same version but different branch
  cvmfs_build_dirs = ["/home/lpromber/epSft/cvmfs/build"] 
  thread_configs = [1] # array; with how many threads the program should be run

  # combination of cvmfs client config that should be in addition enabled
  # best to separate params with "_"; see setCvmfsConfig() for more
  run_options = ["symlink_statfs_kernel_trace", "statfs_kernel"]

  # base dir where the results should be written to
  outdir = "./data/"

  #########################
  ## END PARAMS set by user
  ##############################################################################

  repos = benchmark_cmds.getReposToMount(commands)
  # setup time command
  time_command = "/usr/bin/time -f " + benchmark_time.getTimeFormat()

  # if needed: make outdir
  if os.path.isdir(outdir) == False:
    os.makedirs(outdir)


  ## 1) loop over different cvmfs versions in different dirs
  for cvmfs_build_dir in cvmfs_build_dirs:
    # install cvmfs version build in directory $cvmfs_build_dir
    benchmark_cvmfs.installCVMFS(cvmfs_build_dir)

    ## 2) loop over different client configs (needs a remount of mountpoint,
    ##                                        reload is not enough!)
    for option in run_options:
      setCvmfsConfig("/etc/cvmfs/default.local", option)

      if use_autofs == True:
        print("autofs")
        benchmark_cvmfs.clear_and_remount_autofs()
      else: # version no autofs
        print("without autofs")
        benchmark_cvmfs.clear_and_mount_direct(repos)

      # get cvmfs version
      print("get cvmfs version")
      cvmfs_version = benchmark_cvmfs.getCVMFSVersion()

      ## 3) loop over commands
      for name, partial_cmd in commands.items():
        print("*** CVMFS:", cvmfs_version, cvmfs_build_dir)
        print("*** Extra client options:", option)
        print("*** command name:", name)
        print("***", partial_cmd)

        partial_cmd["time"] = time_command

        print("*** preloading proxy cache...")
        benchmark_time.preloadProxy(partial_cmd)
        print("    ...done")

        ## 4) loop over number of threads
        for num_threads in thread_configs:
          print("")
          print("*** Num threads:", num_threads)

          cache_setups = [["cold_cache", benchmark_time.wipe_cache],
                         ["warm_cache", benchmark_time.wipe_kernel_cache],
                         ["hot_cache", ""]]
          
          # dictionaries holding the results
          start_times = defaultdict()
          all_data = defaultdict()
          all_cvmfs_raw_dict = defaultdict()
          all_dict_tracing = defaultdict()

          ## 4a) time each command in each cache_setup (cold, warm, hot)
          for cache_setup in cache_setups:
            cache_label = cache_setup[0]
            cache_setup_func = cache_setup[1]

            print("    ", cache_label)

            start_times[cache_label] = dt.datetime.now()
            if callable(cache_setup_func):
              dict_cache, dict_full_cvmfs_internals, dict_tracing = \
                    benchmark_time.timeme(setup=cache_setup_func,
                                          stmt=partial(benchmark_time.do_thing,
                                                       partial_cmd, num_threads),
                                          number=1, repeat=repetitions)
            else:
              dict_cache, dict_full_cvmfs_internals, dict_tracing = \
                    benchmark_time.timeme(stmt=partial(benchmark_time.do_thing,
                                                       partial_cmd, num_threads),
                                          number=1, repeat=repetitions)
            
            print("    ...done", cache_label, "after",
                  (dt.datetime.now() - start_times[cache_label]).total_seconds(),
                  "seconds")
            all_data[cache_label] = dict_cache
            all_cvmfs_raw_dict[cache_label] = dict_full_cvmfs_internals
            all_dict_tracing[cache_label] = dict_tracing


          print("complete run time: ",
                (dt.datetime.now() - start_times[cache_setups[0][0]]).total_seconds(),
                "seconds")


          # set output name: auto-increment so not to overwrite old results
          outname = getOutname(cvmfs_build_dir, name, option, num_threads)
          final_outname = benchmark_out.getOutnameWithNextNumber(outdir, outname)

          print("final_outname", final_outname)

          ## 4b) write data
          benchmark_out.writeResults(outdir, final_outname, all_data, name,
                                     cvmfs_version, num_threads)
          benchmark_out.writeResultsInternalRaw(outdir, final_outname,
                                                all_cvmfs_raw_dict)
          benchmark_out.writeResultsTracing(outdir, final_outname,
                                            all_dict_tracing)
