import glob
import tqdm

from util_visualization import visualization_plotting
from util_visualization import visualization_time

if __name__ == "__main__":
    dirname = "data/jakobBisectComparison/"
    csv_labels = ["user,system,real", "user", "system", "real"]
    repos = ["sft.cern.ch", "cms-ib.cern.ch"]

    files = glob.glob(dirname + "*.csv")

    cmds = list(visualization_time.getDistinctCommands(files))
    options = list(visualization_time.getDistinctOptions(files))
    num_threads = list(visualization_time.getDistinctThreads(files))

    cmds.sort(key=lambda s: s.lower())
    # options.sort(key=lambda s: s.lower())
    num_threads.sort()

    num_threads = [32, 64]
    options = ['statfs_kernel', 'symlink_statfs_kernel']

    print(cmds)
    print(options)
    print(num_threads)

    cvmfs_internal_labels = [
    "_catalog_mgr.n_lookup_path", "_catalog_mgr.n_lookup_path_negative",
    "_cvmfs.n_fs_open", "_cvmfs.n_fs_read",
    "_cvmfs.n_fs_readlink", "_cvmfs.n_fs_stat",
    "_download.n_requests", "_download.sz_transfer_time",
    "_download.sz_transferred_bytes", "_fetch.n_downloads",
    "_fetch.n_invocations", "_inode_cache.n_hit",
    "_inode_cache.n_insert", "_inode_cache.n_miss",
    "_inode_cache.sz_allocated",
    "_inode_tracker.n_hit_inode", "_inode_tracker.n_hit_path",
    "_inode_tracker.n_insert", "_linkstring.n_instances",
    "_sqlite.n_read", "_sqlite.sz_read"
    ]

    cvmfs_internal_labels = [
      "_catalog_mgr.n_lookup_path",
      "_cvmfs.n_fs_readlink"
    ]

    csv_labels_cvmfs_internal = []

    for repo in repos:
      for label in cvmfs_internal_labels:
        csv_labels_cvmfs_internal.append(repo + label)

    cvmfs_versions = ["2.9.4.0", "2.11.0.0-bisect", "2.11.0.0-fix-perf"]

    for cmd in cmds:
      for thread in num_threads:
        for label in tqdm.tqdm(csv_labels):
          visualization_plotting.boxplotPlotComparison(dirname, label, "2.11.0.0-bisect", str(thread), cmd, options, "option", "./results")
        for label in tqdm.tqdm(csv_labels_cvmfs_internal):
          visualization_plotting.boxplotPlotComparison(dirname, label, "2.11.0.0-bisect", str(thread), cmd, options, "option_cvmfs_internal", "./results")

    for option in options:
      for cmd in cmds:
        for thread in num_threads:
          print(option, cmd, thread)
          for label in tqdm.tqdm(csv_labels):
            visualization_plotting.boxplotPlotComparison(dirname, label, option, str(thread), cmd, cvmfs_versions, "version", "./results")
          for label in tqdm.tqdm(csv_labels_cvmfs_internal):
            visualization_plotting.boxplotPlotComparison(dirname, label, option, str(thread), cmd, cvmfs_versions, "version_cvmfs_internal", "./results")


    # #scatter plot single file
    # for filename in files:
    #   print("filename", filename)
    #   print("csv_labels", csv_labels)
    #   visualization_plotting.plotSingleFile(filename, csv_labels, "./results")
    #   exit(0)






