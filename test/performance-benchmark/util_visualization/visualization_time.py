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

measurement_cvmfs_internal_dict = {
  "authz.n_deny": "overall number of denied membership queries",
  "authz.n_fetch": "overall number of authz helper invocations",
  "authz.n_grant": "overall number of granted membership queries",
  "authz.no_pid": "cached pids",
  "authz.no_session": "cached sessions",
  "cache.n_certificate_hits": "Number of certificate hits",
  "cache.n_certificate_misses": "Number of certificate misses",
  "catalog_mgr.n_detach_siblings": "Number of times the CVMFS_CATALOG_WATERMARK was hit",
  "catalog_mgr.n_listing": "Number of listings",
  "catalog_mgr.n_lookup_inode": "Number of inode lookups",
  "catalog_mgr.n_lookup_path": "Number of path lookups",
  "catalog_mgr.n_lookup_path_negative": "Number of negative path lookups",
  "catalog_mgr.n_lookup_xattrs": "Number of xattrs lookups",
  "catalog_mgr.n_nested_listing": "Number of listings of nested catalogs",
  "catalog_revision": "Revision number of the root file catalog",
  "cvmfs.n_fs_dir_open": "Overall number of directory open operations",
  "cvmfs.n_fs_forget": "Number of inode forgets",
  "cvmfs.n_fs_inode_replace": "Number of stale inodes that got replaced by an up-to-date version",
  "cvmfs.n_fs_lookup": "Number of lookups",
  "cvmfs.n_fs_lookup_negative": "Number of negative lookups",
  "cvmfs.n_fs_open": "Overall number of file open operations",
  "cvmfs.n_fs_read": "Number of files read",
  "cvmfs.n_fs_readlink": "Number of links read",
  "cvmfs.n_fs_stat": "Number of stats",
  "cvmfs.n_fs_stat_stale": "Number of stats for stale (open, meanwhile changed) regular files",
  "cvmfs.n_fs_statfs": "Overall number of statsfs calls",
  "cvmfs.n_fs_statfs_cached": "Number of statsfs calls that accessed the cached statfs info",
  "cvmfs.n_io_error": "Number of I/O errors",
  "cvmfs.no_open_dirs": "Number of currently opened directories",
  "cvmfs.no_open_files": "Number of currently opened files",
  "dentry_tracker.n_insert": "overall number of added negative cache entries",
  "dentry_tracker.n_prune": "overall number of prune calls",
  "dentry_tracker.n_remove": "overall number of evicted negative cache entries",
  "download-external.n_host_failover": "Number of host failovers",
  "download-external.n_proxy_failover": "Number of proxy failovers",
  "download-external.n_requests": "Number of requests",
  "download-external.n_retries": "Number of retries",
  "download-external.sz_transfer_time": "Transfer time (milliseconds)",
  "download-external.sz_transferred_bytes": "Number of transferred bytes",
  "download.n_host_failover": "Number of host failovers",
  "download.n_proxy_failover": "Number of proxy failovers",
  "download.n_requests": "Number of requests",
  "download.n_retries": "Number of retries",
  "download.sz_transfer_time": "Transfer time (milliseconds)",
  "download.sz_transferred_bytes": "Number of transferred bytes",
  "eio.01": "EIO returned to calling process. cvmfs.cc:cvmfs_lookup()",
  "eio.02": "EIO returned to calling process. cvmfs.cc:ReplyNegative()",
  "eio.03": "EIO returned to calling process. cvmfs.cc:cvmfs_opendir()",
  "eio.04": "EIO returned to calling process. cvmfs.cc:cvmfs_open()",
  "eio.05": "EIO returned to calling process. cvmfs.cc:cvmfs_read()",
  "eio.06": "EIO returned to calling process. cvmfs.cc:cvmfs_open()",
  "eio.07": "EIO returned to calling process. cvmfs.cc:cvmfs_read()",
  "eio.08": "EIO returned to calling process. cvmfs.cc:cvmfs_read()",
  "eio.total": "EIO returned to calling process. Sum of individual eio counters",
  "fetch-external.n_downloads": "overall number of downloaded files (incl. catalogs, chunks)",
  "fetch-external.n_invocations": "overall number of object requests (incl. catalogs, chunks)",
  "fetch.n_downloads": "overall number of downloaded files (incl. catalogs, chunks)",
  "fetch.n_invocations": "overall number of object requests (incl. catalogs, chunks)",
  "inode_cache.n_drop": "Number of drops",
  "inode_cache.n_forget": "Number of forgets",
  "inode_cache.n_hit": "Number of hits",
  "inode_cache.n_insert": "Number of inserts",
  "inode_cache.n_insert_negative": "Number of negative inserts",
  "inode_cache.n_miss": "Number of misses",
  "inode_cache.n_replace": "Number of replaces",
  "inode_cache.n_update": "Number of updates",
  "inode_cache.n_update_value": "Number of value changes",
  "inode_cache.sz_allocated": "Number of allocated bytes ",
  "inode_cache.sz_size": "Total size",
  "inode_tracker.n_hit_inode": "overall number of inode lookups",
  "inode_tracker.n_hit_path": "overall number of successful path lookups",
  "inode_tracker.n_insert": "overall number of accessed inodes",
  "inode_tracker.n_miss_path": "overall number of unsuccessful path lookups",
  "inode_tracker.n_remove": "overall number of evicted inodes",
  "inode_tracker.no_reference": "currently active inodes",
  "linkstring.n_instances": "Number of instances",
  "linkstring.n_overflows": "Number of overflows",
  "md5_path_cache.n_drop": "Number of drops",
  "md5_path_cache.n_forget": "Number of forgets",
  "md5_path_cache.n_hit": "Number of hits",
  "md5_path_cache.n_insert": "Number of inserts",
  "md5_path_cache.n_insert_negative": "Number of negative inserts",
  "md5_path_cache.n_miss": "Number of misses",
  "md5_path_cache.n_replace": "Number of replaces",
  "md5_path_cache.n_update": "Number of updates",
  "md5_path_cache.n_update_value": "Number of value changes",
  "md5_path_cache.sz_allocated": "Number of allocated bytes ",
  "md5_path_cache.sz_size": "Total size",
  "namestring.n_instances": "Number of instances",
  "namestring.n_overflows": "Number of overflows",
  "page_cache_tracker.n_insert": "overall number of added page cache entries",
  "page_cache_tracker.n_open_cached": "overall number of open calls where the file's page cache is reused",
  "page_cache_tracker.n_open_direct": "overall number of direct I/O open calls",
  "page_cache_tracker.n_open_flush": "overall number of open calls where the file's page cache gets flushed",
  "page_cache_tracker.n_remove": "overall number of evicted page cache entries",
  "path_cache.n_drop": "Number of drops",
  "path_cache.n_forget": "Number of forgets",
  "path_cache.n_hit": "Number of hits",
  "path_cache.n_insert": "Number of inserts",
  "path_cache.n_insert_negative": "Number of negative inserts",
  "path_cache.n_miss": "Number of misses",
  "path_cache.n_replace": "Number of replaces",
  "path_cache.n_update": "Number of updates",
  "path_cache.n_update_value": "Number of value changes",
  "path_cache.sz_allocated": "Number of allocated bytes ",
  "path_cache.sz_size": "Total size",
  "pathstring.n_instances": "Number of instances",
  "pathstring.n_overflows": "Number of overflows",
  "sqlite.n_access": "overall number of access() calls",
  "sqlite.n_rand": "overall number of random() calls",
  "sqlite.n_read": "overall number of read() calls",
  "sqlite.n_sleep": "overall number of sleep() calls",
  "sqlite.n_time": "overall number of time() calls",
  "sqlite.no_open": "currently open sqlite files",
  "sqlite.sz_rand": "overall number of random bytes",
  "sqlite.sz_read": "overall bytes read()",
  "sqlite.sz_sleep": "overall microseconds slept  ",
}

cvmfs_version_labels_dict = {
  "2.9.4.0": "2.9.4",
  "2.11.0.0-bisect": "2.11.0",
  "2.11.0.0": "2.11.0",
  "2.11.0.0-fix-perf": "2.11.0 (J)",
  "2.11.0.0-loadctlg": "load ctlg"
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
