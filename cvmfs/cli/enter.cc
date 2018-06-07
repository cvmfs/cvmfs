/**
 * This file is part of the CernVM File System.
 */

#include <errno.h>
#include <sched.h>
#include <unistd.h>

#include <cassert>
#include <string>
#include <vector>

#include "logging.h"
#include "options.h"
#include "platform.h"
#include "util/posix.h"

int main(int argc, char **argv) {
  std::string fqrn = "sft.cern.ch";
  if (argc > 1)
    fqrn = argv[1];
  std::string workspace = GetHomeDirectory() + "/.cvmfs";
  std::string workspace_fqrn = workspace + "/" + fqrn;
  std::string path_cache = workspace + "/cache";
  std::string path_config = workspace_fqrn + "/client.conf";
  std::string path_usyslog = workspace_fqrn + "/client.log";
  std::string path_mountpoint = workspace_fqrn + "/mount_rdonly";

  bool retval = MkdirDeep(workspace_fqrn, kPrivateDirMode);
  assert(retval);

  LogCvmfs(kLogCvmfs, kLogStdout, "Generating options for %s", fqrn.c_str());
  BashOptionsManager options_manager;
  options_manager.ParseDefault(fqrn);
  options_manager.SetValue("CVMFS_AUTO_UPDATE", "no");
  options_manager.SetValue("CVMFS_NFS_SOURCE", "no");
  options_manager.SetValue("CVMFS_HIDE_MAGIC_XATTRS", "yes");
  options_manager.SetValue("CVMFS_SERVER_CACHE_MODE", "yes");
  options_manager.SetValue("CVMFS_USYSLOG", path_usyslog);
  options_manager.SetValue("CVMFS_RELOAD_SOCKETS", path_cache);
  options_manager.SetValue("CVMFS_WORKSPACE", path_cache);
  options_manager.SetValue("CVMFS_CACHE_PRIMARY", "private");
  options_manager.SetValue("CVMFS_CACHE_private_TYPE", "posix");
  options_manager.SetValue("CVMFS_CACHE_private_BASE", path_cache);
  options_manager.SetValue("CVMFS_CACHE_private_SHARED", "on");
  options_manager.SetValue("CVMFS_CACHE_private_QUOTA_LIMIT", "4000");
  retval = SafeWriteToFile(options_manager.Dump(), path_config,
                           kPrivateFileMode);

  retval = MkdirDeep(path_cache, kPrivateDirMode);
  assert(retval);
  retval = MkdirDeep(path_mountpoint, kPrivateDirMode);
  assert(retval);
  //std::vector<std::string> cmd_line;
  //cmd_line.push_back("cvmfs2");
  //cmd_line.push_back("-oconfig=" + path_config);
  //cmd_line.push_back(fqrn);
  //cmd_line.push_back(path_mountpoint);
  //retval = ManagedExec(
  //  cmd_line,
  //  std::set<int>(),
  //  std::map<int, int>(),
  //  false /* drop_credentials */);
  //LogCvmfs(kLogCvmfs, kLogStdout, "RETVAL IS %d", retval);

  char buf[200];
  int sz = readlink("/proc/self/ns/mnt", buf, 200);
  buf[sz] = '\0';
  printf("MNT %s\n", buf);

  int rvi = unshare(CLONE_NEWUSER | CLONE_NEWNS);
  LogCvmfs(kLogCvmfs, kLogStdout, "unshare %d (%d), I am %d",
           rvi, errno, getuid());

  sz = readlink("/proc/self/ns/mnt", buf, 200);
  buf[sz] = '\0';
  printf("MNT %s\n", buf);

  return 0;
}
