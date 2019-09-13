/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "namespace.h"

#include <fcntl.h>
#ifndef __APPLE__
#include <sched.h>
#include <sys/mount.h>
#endif
#include <sys/wait.h>

#include "util/posix.h"
#include "util/string.h"

// Might otherwise not compile on older Linux kernels
#ifndef CLONE_NEWUSER
#define CLONE_NEWUSER	0x10000000
#endif
#ifndef CLONE_NEWPID
#define CLONE_NEWPID 0x20000000
#endif


int CheckNamespaceFeatures() {
#ifdef __APPLE__
  return 0;
#else
  int result = kNsFeatureMount;  // available since kernel 2.4
  if (SymlinkExists("/proc/self/ns/pid")) result |= kNsFeaturePid;
  int fd = open("/proc/sys/kernel/unprivileged_userns_clone", O_RDONLY);
  if (fd < 0)
    return result;
  result |= kNsFeatureUserAvailable;
  char enabled = 0;
  SafeRead(fd, &enabled, 1);
  return (enabled != '1') ? result : (result | kNsFeatureUserEnabled);
#endif
}


bool CreateUserNamespace(uid_t map_uid_to, gid_t map_gid_to) {
#ifdef __APPLE__
  return false;
#else
  std::string uid_str = StringifyInt(geteuid());
  std::string gid_str = StringifyInt(geteuid());

  int rvi = unshare(CLONE_NEWUSER);
  if (rvi != 0) return false;

  bool rvb = SafeWriteToFile(StringifyInt(map_uid_to) + " " + uid_str + " 1",
                             "/proc/self/uid_map", kDefaultFileMode);
  if (!rvb) return false;
  rvb = SafeWriteToFile("deny", "/proc/self/setgroups", kDefaultFileMode);
  if (!rvb) return false;
  rvb = SafeWriteToFile(StringifyInt(map_gid_to) + " " + gid_str + " 1",
                        "/proc/self/gid_map", kDefaultFileMode);
  if (!rvb) return false;

  return true;
#endif
}


bool BindMount(const std::string &from, const std::string &to) {
#ifdef __APPLE__
  return false;
#else
  int rvi = mount(from.c_str(), to.c_str(), "", MS_BIND | MS_REC, NULL);
  return rvi == 0;
#endif
}


bool CreateMountNamespace() {
#ifdef __APPLE__
  return false;
#else
  std::string cwd = GetCurrentWorkingDirectory();

  int rvi = unshare(CLONE_NEWNS);
  if (rvi != 0) return false;

  rvi = chdir(cwd.c_str());
  return rvi == 0;
#endif
}


/**
 * The fd_parent file descriptor, if passed, is the read end of a pipe whose
 * write end is connected to the parent process.  This gives the namespace's
 * init process a means to detect when the parent process is terminated.
 */
bool CreatePidNamespace(int *fd_parent) {
#ifdef __APPLE__
  return false;
#else
  int rvi = unshare(CLONE_NEWPID);
  if (rvi != 0) return false;

  int pipe_parent[2];
  MakePipe(pipe_parent);

  int max_fd;
  int status;
  pid_t pid = fork();
  switch (pid) {
    case -1:
      abort();
    case 0:
      // New init process
      break;
    default:
      // Parent, wait for the namespace to exit

      // Close all file descriptors
      max_fd = sysconf(_SC_OPEN_MAX);
      for (int fd = 0; fd < max_fd; fd++) {
        if (fd != pipe_parent[1])
          close(fd);
      }

      char c = 'x';
      SafeWrite(pipe_parent[1], &c, 1);

      rvi = waitpid(pid, &status, 0);
      if (rvi >= 0) {
        if (WIFEXITED(status))
          exit(WEXITSTATUS(status));
      }
      exit(127);
  }
  close(pipe_parent[1]);
  if (fd_parent != NULL)
    *fd_parent = pipe_parent[0];

  // Note: only signals for which signal handlers are established can be sent
  // by other processes of this pid namespace to the init process

  rvi = mount("", "/proc", "proc", 0, NULL);
  return rvi == 0;
#endif
}
