/**
 * This file is part of the CernVM File System.
 */

#include "nfs4/mount_util.h"

#include <errno.h>
#include <spawn.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

#ifndef __APPLE__
#include <mntent.h>
#endif

#include "nfs4/logging.h"

extern char **environ;

namespace nfs4 {

ScopedRoot::ScopedRoot() : switched_(false), uid_(geteuid()), gid_(getegid()) {
  if (uid_ == 0 || getuid() != 0)
    return;
  if (seteuid(0) != 0)
    return;
  setegid(0);
  switched_ = true;
}

ScopedRoot::~ScopedRoot() {
  if (!switched_)
    return;
  setegid(gid_);
  seteuid(uid_);
}


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


#ifdef __APPLE__
static uint64_t PackFsid(const fsid_t &fsid) {
  return (static_cast<uint64_t>(static_cast<uint32_t>(fsid.val[0])) << 32)
         | static_cast<uint32_t>(fsid.val[1]);
}
#else
static bool HasPortOption(const char *options, uint16_t port) {
  char needle[16];
  snprintf(needle, sizeof(needle), "port=%u", port);
  const size_t len = strlen(needle);
  for (const char *p = strstr(options, needle); p; p = strstr(p + 1, needle)) {
    if ((p == options || p[-1] == ',') && (p[len] == ',' || p[len] == '\0'))
      return true;
  }
  // The kernel omits the default port
  return port == 2049 && strstr(options, "port=") == NULL;
}
#endif


uint64_t FindMount(const std::string &mountpoint, uint16_t port) {
#ifdef __APPLE__
  (void)port;
  struct statfs *mounts;
  const int n = getmntinfo(&mounts, MNT_NOWAIT);
  // The last entry is the topmost mount, i.e. the one just made
  for (int i = n - 1; i >= 0; --i) {
    if (mountpoint == mounts[i].f_mntonname
        && strcmp(mounts[i].f_fstypename, "nfs") == 0) {
      return PackFsid(mounts[i].f_fsid);
    }
  }
  return 0;
#else
  FILE *f = setmntent("/proc/self/mounts", "r");
  if (f == NULL)
    return 0;
  uint64_t id = 0;
  struct mntent *m;
  while ((m = getmntent(f)) != NULL) {
    if (mountpoint == m->mnt_dir && strncmp(m->mnt_type, "nfs", 3) == 0
        && HasPortOption(m->mnt_opts, port)) {
      id = port;
    }
  }
  endmntent(f);
  return id;
#endif
}


bool IsMounted(const std::string &mountpoint, uint64_t mount_id) {
  if (mount_id == 0)
    return false;
#ifdef __APPLE__
  struct statfs *mounts;
  const int n = getmntinfo(&mounts, MNT_NOWAIT);
  for (int i = 0; i < n; ++i) {
    if (mountpoint == mounts[i].f_mntonname
        && strcmp(mounts[i].f_fstypename, "nfs") == 0
        && PackFsid(mounts[i].f_fsid) == mount_id) {
      return true;
    }
  }
  return false;
#else
  return FindMount(mountpoint, static_cast<uint16_t>(mount_id)) != 0;
#endif
}


bool UnmountNfs(const std::string &mountpoint) {
  ScopedRoot const root;
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

std::string NfsMountOptions(uint16_t port) {
  char buf[256];
#ifdef __APPLE__
  // Not soft: the macOS client times out soft-mount READs after well under a
  // second under load, regardless of timeo.  A dead cvmfs2 is covered by the
  // guardian (see guardian.h), and deadtimeout lets the kernel give up on a
  // server that stays silent anyway.  rdonly is incompatible with soft but
  // not with hard mounts; the lock options are rejected for NFSv4.  Without
  // rsize, the client reads in 32 KiB requests.
  snprintf(buf, sizeof(buf),
           "vers=4,tcp,port=%u,rdonly,intr,deadtimeout=120,timeo=600,"
           "retrans=2,rsize=1048576,readahead=4,nobrowse,namedattr",
           port);
#else
  snprintf(buf, sizeof(buf),
           "vers=4.0,port=%u,ro,hard,timeo=600,retrans=2,local_lock=all",
           port);
#endif
  return buf;
}


bool MountNfs(const std::string &mountpoint, uint16_t port) {
  std::string options = NfsMountOptions(port);
  const char *extra = getenv("CVMFS_NFS_MOUNT_OPTIONS");
  if (extra && *extra)
    options += std::string(",") + extra;

#ifndef __APPLE__
  // cvmfs2 may run as CVMFS_USER with only CAP_SYS_ADMIN left, which
  // suffices for mount(2), but /bin/mount would treat it as a user mount.
  // The kernel takes the NFS options as text; it needs the server address.
  const std::string data = options + ",addr=127.0.0.1,clientaddr=127.0.0.1";
  if (mount("127.0.0.1:/", mountpoint.c_str(), "nfs4", MS_RDONLY,
            data.c_str()) == 0) {
    Log(kLogLevelInfo, "mounted %s with options %s",
              mountpoint.c_str(), data.c_str());
    return true;
  }
  Log(kLogLevelDebug, "mount(2) failed (%d), trying /bin/mount",
            errno);
#endif

  std::vector<std::string> args;
#ifdef __APPLE__
  args.push_back("/sbin/mount_nfs");
#else
  args.push_back("/bin/mount");
  args.push_back("-t");
  args.push_back("nfs4");
#endif
  args.push_back("-o");
  args.push_back(options);
  args.push_back("127.0.0.1:/");
  args.push_back(mountpoint);
  Log(kLogLevelInfo, "mounting %s with options %s",
            mountpoint.c_str(), options.c_str());
  int retval;
  {
    ScopedRoot const root;
    retval = RunCommand(args);
  }
  if (retval != 0) {
    Log(kLogLevelError, "%s failed (%d)", args[0].c_str(), retval);
    return false;
  }
  return true;
}

}  // namespace nfs4
