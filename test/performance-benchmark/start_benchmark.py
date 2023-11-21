#!/usr/bin/env python3
from functools import partial
import datetime as dt
from collections import defaultdict
import os
import numpy as np

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

  config = benchmark_read_params.getConfig()

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
        benchmark_cvmfs.installCVMFS(cvmfs_build_dir)

      ## 2) loop over different client configs (needs a remount of mountpoint,
      ##                                        reload is not enough!)
      for client_config in config[run]["client_configs"]:
        # run command using cmvfs
        if config[run]["use_cvmfs"] == True:
          benchmark_cmds.setClientConfig("/etc/cvmfs/default.local", client_config,
                                        config["avail_client_configs"])

          if config[run]["use_autofs"] == True:
            print("autofs")
            benchmark_cvmfs.clear_and_reload_autofs()
          else: # version no autofs
            print("without autofs")
            benchmark_cvmfs.clear_and_mount_direct(repos)

          # get cvmfs version
          print("get cvmfs version")
          cvmfs_version = benchmark_cvmfs.getCVMFSVersion()
        else: # non-cvmfs cmd
          cvmfs_version = "0.0.0"

        ## 3) loop over commands
        for cmd_name in config[run]["commands"]:
          partial_cmd = config["avail_cmds"][cmd_name]
          print("\n")
          print("*** CVMFS:", cvmfs_version, cvmfs_build_dir)
          print("*** Client_config:", client_config)
          print("*** Command name:", cmd_name)
          print("***", partial_cmd)

          partial_cmd["time"] = time_command

          print("*** preloading proxy cache...")
          benchmark_time.preloadProxy(partial_cmd)
          print("    ...done")

          ## 4) loop over number of threads
          for num_threads in config[run]["num_threads"]:
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
                                            arg_setup=config[run]["use_cvmfs"],
                                            stmt=partial(benchmark_time.do_thing,
                                                        partial_cmd, num_threads),
                                            number=1,
                                            repeat=config[run]["repetitions"])
              else:
                dict_cache, dict_full_cvmfs_internals, dict_tracing = \
                      benchmark_time.timeme(stmt=partial(benchmark_time.do_thing,
                                                        partial_cmd, num_threads),
                                            number=1,
                                            repeat=config[run]["repetitions"])

              print("    ...done", cache_label, "after",
                    (dt.datetime.now() - start_times[cache_label]).total_seconds(),
                    "seconds")
              all_data[cache_label] = dict_cache
              all_cvmfs_raw_dict[cache_label] = dict_full_cvmfs_internals
              all_dict_tracing[cache_label] = dict_tracing

              print("Average real time for all repetitions for", cache_label,
                    ":", np.average(all_data[cache_label]["real"]))


            print("complete run time: ",
                  (dt.datetime.now() - start_times[cache_setups[0][0]]).total_seconds(),
                  "seconds")


            # set output name: auto-increment so not to overwrite old results
            # outname = getOutname(cvmfs_build_dir, name, option, num_threads)
            outname = benchmark_cmds.getOutname(cvmfs_build_dir,
                                 cmd_name, client_config,
                                 num_threads, cvmfs_version,
                                 config[run]["out_name_replacement_of_version"])
            final_outname = benchmark_out.getOutnameWithNextNumber(
                                            config[run]["out_dirname"], outname)

            print("final_outname", final_outname)

            ## 4b) write data
            benchmark_out.writeResults(config[run]["out_dirname"], final_outname,
                                       all_data, cmd_name,
                                       cvmfs_version, num_threads)
            if config[run]["use_cvmfs"] == True:
              benchmark_out.writeResultsInternalRaw(config[run]["out_dirname"],
                                                    final_outname,
                                                    all_cvmfs_raw_dict)
              benchmark_out.writeResultsTracing(config[run]["out_dirname"],
                                                final_outname,
                                                all_dict_tracing)
