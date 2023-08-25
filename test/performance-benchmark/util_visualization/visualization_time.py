measurement_label_dict = {
"user": "User Time (s)",
"system": "System Time (s)",
"real": "Real Time (s)",
"user,system,real": "User, System and Real Time (s)",
"page_faults_major": "#Page Faults: Major",
"page_faults_minor": "#Page Faults: Minor",
"page_faults_major, page_faults_minor": "#Page Faults: Major and Minor",
"cpu": "CPU Usage (%)",
"involuntary_swap": "#Involuntary Swaps",
"voluntary_swap": "#Voluntary Swaps",
}

measurement_cvmfs_internal_dict = {
"catalog_mgr.n_lookup_path": "#Lookups of <path>",
"catalog_mgr.n_lookup_path_negative": "#Negative lookups of <path>",
"cvmfs.n_fs_open": "#Open calls (Fs)",
"cvmfs.n_fs_read": "#Read calls (Fs)",
"cvmfs.n_fs_readlink": "#Readlink calls (Fs)",
"cvmfs.n_fs_stat": "#Stat calls (Fs)",
"download.n_requests": "#Download requests",
"download.sz_transfer_time": "Total Download Transfer Time (s)",
"download.sz_transferred_bytes": "Total Downloaded Bytes",
"fetch.n_downloads": "#Fetch Requests Resulting in Downloads",
"fetch.n_invocations": "#Fetch Requests (Total)",
"inode_cache.n_hit": "#Inode Cache Hits",
"inode_cache.n_insert": "#Inode Cache Inserts",
"inode_cache.n_miss": "#Inode Cache Misses",
"inode_cache.sz_allocated": "Inode Cache: Allocated Bytes",
"inode_tracker.n_hit_inode": "#Inode Tracker Hit Inodes",
"inode_tracker.n_hit_path": "#Inode Tracker Hit <path>",
"inode_tracker.n_insert": "#Inode Tracker Inserts",
"linkstring.n_instances": "#Instances of linkstrings",
"sqlite.n_read": "#SQL reads",
"sqlite.sz_read": "SQL reads (Bytes)",
}

cvmfs_version_labels_dict = {
  "2.9.4.0": "2.9.4",
  "2.11.0.0-bisect": "2.11.0",
  "2.11.0.0": "2.11.0 (Buggy)",
  "2.11.0.0-fix-perf": "2.11.0 (J)"
}

option_labels_dict = {
  "statfs_kernel" : "Default",
  "symlink_statfs_kernel": "Symlink Caching",
  "symlink_statfs_kernel_trace": "Symlink Caching (T)"
}

def getDistinctCommands(files):
  cmds = []

  for filename in files:
    #remove dir
    tmp = str(filename.split("/")[-1])

    # split at first occurences of _
    tmp = tmp.split("_", 1)[0]

    cmds.append(tmp)

  return set(cmds)

# filename layout
# dir/my-command-option1_option2_123.csv
def getDistinctOptions(files):
  options = []

  for filename in files:
    #remove dir
    tmp = str(filename.split("/")[-1])

    # split at first occurences of _
    tmp = tmp.split("_", 2)[2]

    #split at last occurences of _
    tmp = tmp.rsplit("_", 2)[0]

    options.append(tmp)

  return set(options)

def getDistinctThreads(files):
  num_threads = []

  for filename in files:
    #remove dir
    tmp = str(filename.split("/")[-1])

    # split at first occurences of _
    tmp = tmp.split("_")[-2]

    #split at last occurences of _
    tmp = tmp.rsplit("_", 2)[0]

    num_threads.append(int(tmp))

  return set(num_threads)
