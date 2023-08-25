import glob
import tqdm

from util_visualization import visualization_plotting
from util_visualization import visualization_time

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
#
#
################################################################################

if __name__ == "__main__":
  ##############################################################################
  ## PARAMS set by user
  #########################
  # directory containing all .csv files that should be compared
  dirname = "data/final_selection/"
  # directory where to write the plots, will be created if needed
  outdir = "./results"
  # if empty will make it for all threads available
  num_threads = [32, 64]

  # csv_labels are defined in util_benchmark/benchmark_time.dict_time_format
  # and all cvmfs internal affairs counters as shown in a .csv created by
  # ../start_benchmark.py
  #
  # not yet existing labels and combinations like "user,system,real" must be
  # be added here visualization_time.measurement_label_dict
  # for readability dont combine more than 3 labels
  csv_labels = ["user,system,real", "user", "system", "real"]


  # for comparing versions
  # each version must be added to visualization_time.cvmfs_version_labels_dict
  # and define its label how it is shown in the boxplot
  versions = ["2.9.4.0", "2.11.0.0-bisect", "2.11.0.0-fix-perf"]
  versions_cvmfs_options = ['statfs_kernel', 'symlink_statfs_kernel']
  create_version_plots = True

  # for comparing cvmfs_configs
  # each option must be added to visualization_time.option_labels_dict
  # and define its label how it is shown in the boxplot
  options = ['statfs_kernel', 'symlink_statfs_kernel']
  options_cvmfs_versions = ["2.11.0.0-bisect"]
  create_option_plots = True

  create_scatter_plots = False


  # repos for which cvmfs_internal_labels should be plotted
  repos = ["sft.cern.ch", "cms-ib.cern.ch"]

  # repo name will be added as prefix
  # cvmfs_internal_labels = [
  # "_catalog_mgr.n_lookup_path", "_catalog_mgr.n_lookup_path_negative",
  # "_cvmfs.n_fs_open", "_cvmfs.n_fs_read",
  # "_cvmfs.n_fs_readlink", "_cvmfs.n_fs_stat",
  # "_download.n_requests", "_download.sz_transfer_time",
  # "_download.sz_transferred_bytes", "_fetch.n_downloads",
  # "_fetch.n_invocations", "_inode_cache.n_hit",
  # "_inode_cache.n_insert", "_inode_cache.n_miss",
  # "_inode_cache.sz_allocated",
  # "_inode_tracker.n_hit_inode", "_inode_tracker.n_hit_path",
  # "_inode_tracker.n_insert", "_linkstring.n_instances",
  # "_sqlite.n_read", "_sqlite.sz_read"
  # ]

  cvmfs_internal_labels = [
    "_catalog_mgr.n_lookup_path",
    "_cvmfs.n_fs_readlink"
  ]
  #########################
  ## END PARAMS set by user
  ##############################################################################


  files = glob.glob(dirname + "*.csv")

  cmds = list(visualization_time.getDistinctCommands(files))
  options = list(visualization_time.getDistinctOptions(files))
  if len(num_threads) == 0:
    print("Automatically extracting number of threads...  ", end="")
    num_threads = list(visualization_time.getDistinctThreads(files))
    print("done")

  cmds.sort(key=lambda s: s.lower())
  # options.sort(key=lambda s: s.lower())
  num_threads.sort()

  print("commands:", cmds)
  print("client configs:", options)
  print("num threads:", num_threads)
  print("")
  print("csv_labels", csv_labels)
  print("repos", repos)
  print("cvfms_internal_labels", cvmfs_internal_labels)


  # build full label by adding the repo in front
  csv_labels_cvmfs_internal = []
  for repo in repos:
    for label in cvmfs_internal_labels:
      csv_labels_cvmfs_internal.append(repo + label)

  if create_option_plots == True:
    print("\n\nPlotting: compare options\n")
    # compare options
    for version in options_cvmfs_versions:
      for cmd in cmds:
        for thread in num_threads:
          print(version, cmd, thread)
          for label in tqdm.tqdm(csv_labels):
            visualization_plotting.boxplotPlotComparison(dirname, label,
                                              version, str(thread), cmd, options,
                                              "option", outdir)
          for label in tqdm.tqdm(csv_labels_cvmfs_internal):
            visualization_plotting.boxplotPlotComparison(dirname, label,
                                             version, str(thread), cmd, options,
                                             "option_cvmfs_internal", outdir)

  if create_version_plots == True:
    print("\n\nPlotting: compare versions\n")
    # compare versions
    for option in versions_cvmfs_options:
      for cmd in cmds:
        for thread in num_threads:
          print(option, cmd, thread)
          for label in tqdm.tqdm(csv_labels):
            visualization_plotting.boxplotPlotComparison(dirname, label, option,
                                                     str(thread), cmd, versions,
                                                     "version", outdir)
          for label in tqdm.tqdm(csv_labels_cvmfs_internal):
            visualization_plotting.boxplotPlotComparison(dirname, label, option,
                                               str(thread), cmd, versions,
                                               "version_cvmfs_internal", outdir)


  if create_scatter_plots == True:
    print("\n\nPlotting: scatter plot\n")
    # #scatter plot single file
    for filename in tqdm.tqdm(files):
      visualization_plotting.plotSingleFile(filename, csv_labels, outdir)
