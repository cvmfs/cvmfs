#!/usr/bin/env python3

import datetime as dt
from collections import defaultdict
import os

from util_benchmark import benchmark_cmds
from util_benchmark import benchmark_time
from util_benchmark import benchmark_cvmfs
from util_benchmark import benchmark_out
from util_benchmark import benchmark_read_params

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
# - run on multiple manually compiled cvmfs installations (build dir needed)
# - run multiple different thread configurations
# - run with multiple different CVMFS client configurations
# - auto-increment file name so that rerunning the same config does not
#   overwrite old results
# - rerun the same command with different caching levels
#   (the higher the number of repetitions, the more accurate is the average
#    performance measured by the benchmark)
#   - cold cache: no local caching, only remote on the proxy
#   - warm cache: local cvmfs cache on disk (and on remote proxy);
#                 no kernel caching between repetitions of the same command
#   - hot cache: local cvmfs cache on disk and kernel caching between
#                repetitions of the same command
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

if __name__ == "__main__":

  print("Start benchmark")

  config = benchmark_read_params.getConfig()

  print("Config read")

  for run in config.keys():
    if not "run" in run:
      continue

    if config[run]["use_cvmfs"] == True:
      repos = benchmark_cmds.getReposToMount(config[run]["commands"],
                                            config["avail_cmds"])
    # setup time command
    time_command = "/usr/bin/time -f " + benchmark_time.getTimeFormat()

    # if needed: make outdir
    if os.path.isdir(config[run]["out_dirname"]) == False:
      os.makedirs(config[run]["out_dirname"])


    ## 1) loop over different cvmfs versions in different dirs
    for cvmfs_build_dir in config[run]["cvmfs_build_dirs"]:
      # install cvmfs version build in directory $cvmfs_build_dir
      if cvmfs_build_dir.lower() != "rpm":
        print("Installing CVMFS", cvmfs_build_dir)
        benchmark_cvmfs.installCVMFS(cvmfs_build_dir)

      ## 2) loop over different client configs (needs a remount of mountpoint,
      ##                                        reload is not enough!)
      for client_config in config[run]["client_configs"]:
        # run command using cmvfs
        if config[run]["use_cvmfs"] == True:
          benchmark_cmds.setClientConfig("/etc/cvmfs/default.local", client_config,
                                        config["avail_client_configs"])
          print("\nCVMFS run -", "use autofs:",
                "true" if config[run]["use_autofs"] else "false")

          if config[run]["use_autofs"] == True:
            benchmark_cvmfs.clear_and_reload_autofs()
          else: # version no autofs
            benchmark_cvmfs.clear_and_mount_direct(repos)
          print("CVMFS mount points cleared")

          # get cvmfs version
          #print("get cvmfs version")
          cvmfs_version = benchmark_cvmfs.getCVMFSVersion()
        else: # non-cvmfs cmd
          cvmfs_version = "0.0.0"

        ## 3) loop over commands
        for cmd_name in config[run]["commands"]:
          partial_cmd = config["avail_cmds"][cmd_name]
          partial_cmd["time"] = time_command

          print("*** preloading proxy cache...")
          benchmark_time.preloadProxy(partial_cmd, max(config[run]["num_threads"]))
          print("    ...done")

          benchmark_out.writeStats(config, run, cvmfs_build_dir, client_config,
                                   cmd_name, cvmfs_version, "stats",
                                   benchmark_cvmfs.getShowConfig(partial_cmd),
                                   benchmark_cvmfs.getUlimit(),
                                   benchmark_cvmfs.getUname(), True)

          ## 4) loop over number of threads
          for num_threads in config[run]["num_threads"]:
            print("\n*** Num threads:", num_threads, run, )

            # dictionaries holding the results
            start_times = defaultdict()
            all_data = defaultdict()
            all_cvmfs_raw_dict = defaultdict()
            all_dict_tracing = defaultdict()

            cache_setups = [["cold_cache", benchmark_time.wipe_cache],
                            ["warm_cache", benchmark_time.wipe_kernel_cache],
                            ["hot_cache", ""]]
            ## 4a) time each command in each cache_setup (cold, warm, hot)
            for cache_setup in cache_setups:
              benchmark_time.runBenchmark(config, run, client_config, cmd_name,
                                 num_threads, cache_setup, start_times,
                                 all_data, all_cvmfs_raw_dict, all_dict_tracing)

            print("Complete run time (incl. loop overhead) in sec: ",
                  (dt.datetime.now() - start_times[cache_setups[0][0]])
                                                               .total_seconds())

            ## 4b) write data
            benchmark_out.writeAllResults(config, run, cvmfs_build_dir,
                            client_config, cmd_name, num_threads, cvmfs_version,
                            all_data, all_cvmfs_raw_dict, all_dict_tracing)