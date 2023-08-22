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

def setCvmfsConfig(filename, option, print_config=True):
  new_config = 'CVMFS_HTTP_PROXY="DIRECT"\n'
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
        print(line)

if __name__ == "__main__":
  commands = benchmark_cmds.chep23_selected_commands #{ **chep23_lhcb_commands, **chep23_atlas_commands} #, **chep23_alice_commands }
  repos = benchmark_cmds.getReposToMount(commands)


  # repetitions for how often the command is timed in a row for each cache
  repetitions = 3
  use_autofs = False

  outdir = "./data/"

  # setup time command
  time_command = "/usr/bin/time -f " + benchmark_time.getTimeFormat()


  ## 1) loop over different cvmfs versions in different dirs
  for cvmfs_build_dir in ["/home/lpromber/epSft/cvmfs/build"]:
    # install cvmfs version build in directory $cvmfs_build_dir
    benchmark_cvmfs.installCVMFS(cvmfs_build_dir)

    ## 2) loop over different client configs (needs a remount of mountpoint, reload is not enough!)
    for option in ["symlink_statfs_kernel", "statfs_kernel"]:
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
        print("***", cvmfs_version, cvmfs_build_dir)
        print("***", option)
        print("***", name)
        print("***", partial_cmd)

        partial_cmd["time"] = time_command

        benchmark_time.preloadProxy(partial_cmd)

        ## 4) loop over number of threads
        for num_threads in [1]:
          print("***", num_threads)

          cache_setups = [["cold_cache", benchmark_time.wipe_cache],
                         ["warm_cache", benchmark_time.wipe_kernel_cache],
                         ["hot_cache", ""]]
          
          # dictionaries holding the results
          start_times = defaultdict()
          all_data = defaultdict()
          all_cvmfs_raw_dict = defaultdict()
          all_dict_tracing = defaultdict()

          for cache_setup in cache_setups:
            cache_label = cache_setup[0]
            cache_setup_func = cache_setup[1]

            print(cache_label)

            start_times[cache_label] = dt.datetime.now()
            if callable(cache_setup_func):
              print(cache_label, "with setup", partial_cmd)
              # dict_cache, dict_full_cvmfs_internals, dict_tracing = \
              #       benchmark_time.timeme(setup=cache_setup_func,
              #                             stmt=partial(benchmark_time.do_thing,
              #                                         partial_cmd, num_threads),
              #                             number=1, repeat=repetitions)
            else:
              print(cache_label, "without setup", partial_cmd)
              # dict_cache, dict_full_cvmfs_internals, dict_tracing = \
              #       benchmark_time.timeme(stmt=partial(benchmark_time.do_thing,
              #                                         partial_cmd, num_threads),
              #                             number=1, repeat=repetitions)
            
            print("DONE", cache_label, "after",
                  (dt.datetime.now() - start_times[cache_label]).total_seconds(),
                  "seconds")
            # all_data[cache_label] = dict_cache
            # all_cvmfs_raw_dict[cache_label] = dict_full_cvmfs_internals
            # all_dict_tracing[cache_label] = dict_tracing


          print("complete run time: ",
                (dt.datetime.now() - start_times[cache_setups[0][0]]).total_seconds(),
                "seconds")


          # set output name
          outname = getOutname(cvmfs_build_dir, name, option, num_threads)
          final_outname = benchmark_out.getOutnameWithNextNumber(outdir, outname)

          print("final_outname", final_outname)

          # print data
          # benchmark_out.writeResults(outdir, final_outname, all_data, name,
          #                            cvmfs_version, num_threads)
          # benchmark_out.writeResultsInternalRaw(outdir, final_outname,
          #                                       all_cvmfs_raw_dict)
          # benchmark_out.writeResultsTracing(outdir, final_outname,
          #                                   all_dict_tracing)

