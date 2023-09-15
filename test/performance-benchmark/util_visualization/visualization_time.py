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
  "authz.n_deny": "Authz: #Denied membership queries",
  "authz.n_fetch": "Authz: #Authz helper invocations",
  "authz.n_grant": "Authz: #Granted membership queries",
  "authz.no_pid": "Authz: Cached pids",
  "authz.no_session": "Authz: Cached sessions",
  "cache.n_certificate_hits": "Cache: #Certificate hits",
  "cache.n_certificate_misses": "Cache: #Certificate misses",
  "catalog_mgr.n_detach_siblings": "Catalog Mgr: #Times the CVMFS_CATALOG_WATERMARK was hit",
  "catalog_mgr.n_listing": "Catalog Mgr: #Listings",
  "catalog_mgr.n_lookup_inode": "Catalog Mgr: #Inode lookups",
  "catalog_mgr.n_lookup_path": "Catalog Mgr: #Path lookups",
  "catalog_mgr.n_lookup_path_negative": "Catalog Mgr: #Negative path lookups",
  "catalog_mgr.n_lookup_xattrs": "Catalog Mgr: #Xattrs lookups",
  "catalog_mgr.n_nested_listing": "Catalog Mgr: #Listings of nested catalogs",
  "catalog_revision": "Revision number: root file catalog",
  "cvmfs.n_fs_dir_open": "FS: #Directories opened",
  "cvmfs.n_fs_forget": "FS: #Inode forgets",
  "cvmfs.n_fs_inode_replace": "FS: #Stale inodes replaced by up-to-date version",
  "cvmfs.n_fs_lookup": "FS: #Lookups",
  "cvmfs.n_fs_lookup_negative": "FS: #Negative lookups",
  "cvmfs.n_fs_open": "FS: #Files opened",
  "cvmfs.n_fs_read": "FS: #Files read",
  "cvmfs.n_fs_readlink": "FS: #Links read",
  "cvmfs.n_fs_stat": "FS: #Stats",
  "cvmfs.n_fs_stat_stale": "FS: #Stats for stale regular files",
  "cvmfs.n_fs_statfs": "FS: #Statsfs calls",
  "cvmfs.n_fs_statfs_cached": "FS: #Statsfs calls to cached statfs",
  "cvmfs.n_io_error": "#I/O errors",
  "cvmfs.no_open_dirs": "#Currently opened directories",
  "cvmfs.no_open_files": "#Currently opened files",
  "dentry_tracker.n_insert": "Dentry: #Added negative cache entries",
  "dentry_tracker.n_prune": "Dentry: #Prune calls",
  "dentry_tracker.n_remove": "Dentry: #Evicted negative cache entries",
  "download-external.n_host_failover": "External dwldmgr: #Host failovers",
  "download-external.n_proxy_failover": "External dwldmgr: #Proxy failovers",
  "download-external.n_requests": "External dwldmgr: #Requests",
  "download-external.n_retries": "External dwldmgr: #Retries",
  "download-external.sz_transfer_time": "External dwldmgr: Transfer time (msec)",
  "download-external.sz_transferred_bytes": "External dwldmgr: #Transferred bytes",
  "download.n_host_failover": "Dwldmgr: #Host failovers",
  "download.n_proxy_failover": "Dwldmgr: #Proxy failovers",
  "download.n_requests": "Dwldmgr: #Requests",
  "download.n_retries": "Dwldmgr: #Retries",
  "download.sz_transfer_time": "Dwldmgr: Transfer time (msec)",
  "download.sz_transferred_bytes": "Dwldmgr: #Transferred bytes",
  "eio.01": "#EIO in cvmfs.cc:cvmfs_lookup()",
  "eio.02": "#EIO in cvmfs.cc:ReplyNegative()",
  "eio.03": "#EIO in cvmfs.cc:cvmfs_opendir()",
  "eio.04": "#EIO in cvmfs.cc:cvmfs_open()",
  "eio.05": "#EIO in cvmfs.cc:cvmfs_read()",
  "eio.06": "#EIO in cvmfs.cc:cvmfs_open()",
  "eio.07": "#EIO in cvmfs.cc:cvmfs_read()",
  "eio.08": "#EIO in cvmfs.cc:cvmfs_read()",
  "eio.total": "EIO: Sum of individual eio counters",
  "fetch-external.n_downloads": "External fetch: #Downloaded files",
  "fetch-external.n_invocations": "External fetch: #Object requests",
  "fetch.n_downloads": "Fetch: #Downloaded files",
  "fetch.n_invocations": "Fetch: #Object requests",
  "inode_cache.n_drop": "Inode cache: #Drops",
  "inode_cache.n_forget": "Inode cache: #Forgets",
  "inode_cache.n_hit": "Inode cache: #Hits",
  "inode_cache.n_insert": "Inode cache: #Inserts",
  "inode_cache.n_insert_negative": "Inode cache: #Negative inserts",
  "inode_cache.n_miss": "Inode cache: #Misses",
  "inode_cache.n_replace": "Inode cache: #Replaces",
  "inode_cache.n_update": "Inode cache: #Updates",
  "inode_cache.n_update_value": "Inode cache: #Value changes",
  "inode_cache.sz_allocated": "Inode cache: #Allocated bytes",
  "inode_cache.sz_size": "Inode cache: Total size", # TODO which size? bytes?
  "inode_tracker.n_hit_inode": "Inode cache: #Inode lookups",
  "inode_tracker.n_hit_path": "Inode cache: #Successful path lookups",
  "inode_tracker.n_insert": "Inode cache: #Accessed inodes",
  "inode_tracker.n_miss_path": "Inode cache: #Unsuccessful path lookups",
  "inode_tracker.n_remove": "Inode cache: #Evicted inodes",
  "inode_tracker.no_reference": "Inode cache: #Currently active inodes",
  "linkstring.n_instances": "#Linkstring instances",
  "linkstring.n_overflows": "#Linkstring overflows",
  "md5_path_cache.n_drop": "MD5 Path Cache: #Drops",
  "md5_path_cache.n_forget": "MD5 Path Cache: #Forgets",
  "md5_path_cache.n_hit": "MD5 Path Cache: #Hits",
  "md5_path_cache.n_insert": "MD5 Path Cache: #Inserts",
  "md5_path_cache.n_insert_negative": "MD5 Path Cache: #Negative inserts",
  "md5_path_cache.n_miss": "MD5 Path Cache: #Misses",
  "md5_path_cache.n_replace": "MD5 Path Cache: #Replaces",
  "md5_path_cache.n_update": "MD5 Path Cache: #Updates",
  "md5_path_cache.n_update_value": "MD5 Path Cache: #Value changes",
  "md5_path_cache.sz_allocated": "MD5 Path Cache: #Allocated bytes ",
  "md5_path_cache.sz_size": "MD5 Path Cache: Total size",  # TODO which size? bytes?
  "namestring.n_instances": "#Namestring instances",
  "namestring.n_overflows": "#Namestring overflows",
  "page_cache_tracker.n_insert": "Page Cache: #Added page cache entries",
  "page_cache_tracker.n_open_cached": "Page Cache: #Open calls with cache reused",
  "page_cache_tracker.n_open_direct": "Page Cache: #Direct I/O open calls",
  "page_cache_tracker.n_open_flush": "Page Cache: #Open calls with cache flushed",
  "page_cache_tracker.n_remove": "Page Cache: #Evicted page cache entries",
  "path_cache.n_drop": "Path Cache: #Drops",
  "path_cache.n_forget": "Path Cache: #Forgets",
  "path_cache.n_hit": "Path Cache: #Hits",
  "path_cache.n_insert": "Path Cache: #Inserts",
  "path_cache.n_insert_negative": "Path Cache: #Negative inserts",
  "path_cache.n_miss": "Path Cache: #Misses",
  "path_cache.n_replace": "Path Cache: #Replaces",
  "path_cache.n_update": "Path Cache: #Updates",
  "path_cache.n_update_value": "Path Cache: #Value changes",
  "path_cache.sz_allocated": "Path Cache: #Allocated bytes ",
  "path_cache.sz_size": "Path Cache: Total size", # TODO which size? bytes?
  "pathstring.n_instances": "#Pathstring instances",
  "pathstring.n_overflows": "#Pathstring overflows",
  "sqlite.n_access": "Sqlite: #Access() calls",
  "sqlite.n_rand": "Sqlite: #Random() calls",
  "sqlite.n_read": "Sqlite: #Read() calls",
  "sqlite.n_sleep": "Sqlite: #Sleep() calls",
  "sqlite.n_time": "Sqlite: #Time() calls",
  "sqlite.no_open": "Sqlite: #Currently open files",
  "sqlite.sz_rand": "Sqlite: #Random bytes",
  "sqlite.sz_read": "Sqlite: #Total bytes read()",
  "sqlite.sz_sleep": "Sqlite: #Microseconds slept",
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

def getDistincBuilds(files):
  builds = []

  for filename in files:
    #remove dir
    tmp = str(filename.split("/")[-1])

    # split at first occurences of _
    tmp = tmp.split("_")[1]

    builds.append(tmp)

  return set(builds)

# filename layout
# dir/my-command_version-tag_option1_option2_123.csv
def getDistinctClientConfigs(files):
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
