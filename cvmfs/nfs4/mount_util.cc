/**
 * This file is part of the CernVM File System.
 */

#include "nfs4/mount_util.h"

#include <errno.h>
#include <spawn.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <sys/wait.h>

#include <cstdio>
#include <cstring>

#ifndef __APPLE__
#include <mntent.h>
#endif

extern char **environ;

namespace nfs4 {

int RunCommand(const std::vector<std::string> &args) {
  std::vector<char *> argv;
  for (unsigned i = 0; i < args.size(); ++i)
    argv.push_back(const_cast<char *>(args[i].c_str()));
  argv.push_back(NULL);
  pid_t pid;
  const int retval = posix_spawn(&pid, argv[0], NULL, NULL, &argv[0], environ);
  if (retval != 0)
    return -retval;
  int status;
  while (waitpid(pid, &status, 0) < 0) {
    if (errno != EINTR)
      return -errno;
  }
  return (WIFEXITED(status)) ? WEXITSTATUS(status) : -1;
}


bool IsMounted(const std::string &mountpoint) {
#ifdef __APPLE__
  struct statfs *mounts;
  const int n = getmntinfo(&mounts, MNT_NOWAIT);
  for (int i = 0; i < n; ++i) {
    if (mountpoint == mounts[i].f_mntonname
        && strcmp(mounts[i].f_fstypename, "nfs") == 0) {
      return true;
    }
  }
  return false;
#else
  FILE *f = setmntent("/proc/self/mounts", "r");
  if (f == NULL)
    return false;
  bool found = false;
  struct mntent *m;
  while ((m = getmntent(f)) != NULL) {
    if (mountpoint == m->mnt_dir && strncmp(m->mnt_type, "nfs", 3) == 0)
      found = true;
  }
  endmntent(f);
  return found;
#endif
}


bool UnmountNfs(const std::string &mountpoint) {
#ifdef __APPLE__
  if (unmount(mountpoint.c_str(), MNT_FORCE) == 0)
    return true;
#else
  if (umount2(mountpoint.c_str(), MNT_FORCE) == 0)
    return true;
  // Linux refuses forced NFS unmounts while files are open; detach instead
  if (errno == EBUSY && umount2(mountpoint.c_str(), MNT_DETACH) == 0)
    return true;
#endif
  std::vector<std::string> args;
  args.push_back("/sbin/umount");
  args.push_back("-f");
  args.push_back(mountpoint);
  return RunCommand(args) == 0;
}

}  // namespace nfs4
