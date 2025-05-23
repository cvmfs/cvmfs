/**
 * This file is part of the CernVM File System.
 */


#include "namespace.h"

#include <fcntl.h>
#include <signal.h>
#ifndef __APPLE__
#include <sched.h>
#include <sys/mount.h>
#endif
#include <sys/wait.h>

#include <cstring>

#include "util/posix.h"
#include "util/string.h"

// Might otherwise not compile on older Linux kernels or glibc versions
#ifndef CLONE_NEWUSER
#define CLONE_NEWUSER 0x10000000
#endif
#ifndef CLONE_NEWPID
#define CLONE_NEWPID 0x20000000
#endif
#ifndef MS_REC
#define MS_REC 0x4000
#endif

#ifndef __APPLE__
#define CVMFS_HAS_UNSHARE 1
#ifdef __GLIBC_MINOR__
#if __GLIBC_MINOR__ < 4
#undef CVMFS_HAS_UNSHARE
#endif
#endif
#endif


int CheckNamespaceFeatures() {
#ifdef __APPLE__
  return 0;
#else
  int result = kNsFeatureMount;  // available since kernel 2.4
  if (SymlinkExists("/proc/self/ns/pid"))
    result |= kNsFeaturePid;
  int fd = open("/proc/sys/kernel/unprivileged_userns_clone", O_RDONLY);
  if (fd < 0)
    return result;
  result |= kNsFeatureUserAvailable;
  char enabled = 0;
  SafeRead(fd, &enabled, 1);
  close(fd);
  return (enabled != '1') ? result : (result | kNsFeatureUserEnabled);
#endif
}


NamespaceFailures CreateUserNamespace(uid_t map_uid_to, gid_t map_gid_to) {
#ifdef CVMFS_HAS_UNSHARE
  std::string uid_str = StringifyInt(geteuid());
  std::string gid_str = StringifyInt(getegid());

  int rvi = unshare(CLONE_NEWUSER);
  if (rvi != 0)
    return kFailNsUnshare;

  std::string uid_map = StringifyInt(map_uid_to) + " " + uid_str + " 1";
  std::string gid_map = StringifyInt(map_gid_to) + " " + gid_str + " 1";

  int fd;
  ssize_t nbytes;
  fd = open("/proc/self/setgroups", O_WRONLY);
  if (fd < 0)
    return kFailNsSetgroupsOpen;
  nbytes = write(fd, "deny", 4);
  close(fd);
  if (nbytes != 4)
    return kFailNsSetgroupsWrite;

  fd = open("/proc/self/uid_map", O_WRONLY);
  if (fd < 0)
    return kFailNsMapUidOpen;
  nbytes = write(fd, uid_map.data(), uid_map.length());
  close(fd);
  if (nbytes != static_cast<ssize_t>(uid_map.length()))
    return kFailNsMapUidWrite;

  fd = open("/proc/self/gid_map", O_WRONLY);
  if (fd < 0)
    return kFailNsMapGidOpen;
  nbytes = write(fd, gid_map.data(), gid_map.length());
  close(fd);
  if (nbytes != static_cast<ssize_t>(gid_map.length()))
    return kFailNsMapGidWrite;

  return kFailNsOk;
#else
  return kFailNsUnsuppored;
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


bool ProcMount(const std::string &to) {
#ifdef __APPLE__
  return false;
#else
  int rvi = mount("proc", to.c_str(), "proc", 0, NULL);
  return rvi == 0;
#endif
}


bool CreateMountNamespace() {
#ifdef CVMFS_HAS_UNSHARE
  std::string cwd = GetCurrentWorkingDirectory();

  int rvi = unshare(CLONE_NEWNS);
  if (rvi != 0)
    return false;

  rvi = chdir(cwd.c_str());
  return rvi == 0;
#else
  return false;
#endif
}


#ifdef CVMFS_HAS_UNSHARE
namespace {

static void Reaper(int /*sig*/, siginfo_t * /*siginfo*/, void * /*context*/) {
  while (true) {
    pid_t retval = waitpid(-1, NULL, WNOHANG);
    if (retval <= 0)
      return;
  }
}

}  // anonymous namespace
#endif


/**
 * The fd_parent file descriptor, if passed, is the read end of a pipe whose
 * write end is connected to the parent process.  This gives the namespace's
 * init process a means to know its pid in the context of the parent namespace.
 */
bool CreatePidNamespace(int *fd_parent) {
#ifdef CVMFS_HAS_UNSHARE
  int rvi = unshare(CLONE_NEWPID);
  if (rvi != 0)
    return false;

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
      max_fd = static_cast<int>(sysconf(_SC_OPEN_MAX));
      for (int fd = 0; fd < max_fd; fd++) {
        if (fd != pipe_parent[1])
          close(fd);
      }

      pid_t parent_pid = getpid();
      SafeWrite(pipe_parent[1], &parent_pid, sizeof(parent_pid));
      SafeWrite(pipe_parent[1], &pid, sizeof(pid));

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
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_sigaction = Reaper;
  sa.sa_flags = SA_SIGINFO;
  sigfillset(&sa.sa_mask);
  rvi = sigaction(SIGCHLD, &sa, NULL);
  assert(rvi == 0);

  rvi = mount("", "/proc", "proc", 0, NULL);
  return rvi == 0;
#else
  return false;
#endif
}
