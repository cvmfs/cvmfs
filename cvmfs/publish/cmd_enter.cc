/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_enter.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/limits.h>
#include <poll.h>
#include <sched.h>
#include <signal.h>
#include <sys/mount.h>
#include <unistd.h>


#include <sys/wait.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "backoff.h"
#include "logging.h"
#include "options.h"
#include "platform.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "sanitizer.h"
#include "util/namespace.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace {

/**
 * The parent pid and init pid in the outer PID namespace
 */
struct AnchorPid {
  pid_t parent_pid;
  pid_t init_pid;
};

static AnchorPid EnterRootContainer() {
  bool rvb = CreateUserNamespace(0, 0);
  if (!rvb) throw publish::EPublish("cannot create root user namespace");
  rvb = CreateMountNamespace();
  if (!rvb) throw publish::EPublish("cannot create mount namespace");
  int fd;
  rvb = CreatePidNamespace(&fd);
  if (!rvb) throw publish::EPublish("cannot create pid namespace");
  AnchorPid anchor_pid;
  int rvi = SafeRead(fd, &anchor_pid.parent_pid, sizeof(pid_t));
  if (rvi != sizeof(pid_t))
    throw publish::EPublish("cannot initialize pid namespace");
  rvi = SafeRead(fd, &anchor_pid.init_pid, sizeof(pid_t));
  if (rvi != sizeof(pid_t))
    throw publish::EPublish("cannot initialize pid namespace");
  close(fd);
  return anchor_pid;
}

static void EnsureDirectory(const std::string &path) {
  bool rv = MkdirDeep(path, 0700, true /* veryfy_writable */);
  if (!rv)
    throw publish::EPublish("cannot create directory " + path);
}

static void RemoveSingle(const std::string &path) {
  platform_stat64 info;
  int retval = platform_lstat(path.c_str(), &info);
  if (retval != 0) {
    if (errno == ENOENT)
      return;
    throw publish::EPublish("cannot remove " + path);
  }

  int rv = 0;
  if (S_ISDIR(info.st_mode))
    rv = rmdir(path.c_str());
  else
    rv = unlink(path.c_str());

  if (rv == 0 || errno == ENOENT)
    return;

  throw publish::EPublish(
    "cannot remove " + path + " (" + StringifyInt(errno) + ")");
}


static void RemoveUnderlay(
  const std::string &path, const std::vector<std::string> &new_paths)
{
  std::vector<std::string> all_mounts = platform_mountlist();
  std::vector<std::string> umount_targets;
  for (unsigned i = 0; i < all_mounts.size(); ++i) {
    if (HasPrefix(all_mounts[i], path + "/", false /* ignore_case */))
      umount_targets.push_back(all_mounts[i]);
  }

  std::sort(umount_targets.begin(), umount_targets.end());
  std::vector<std::string>::reverse_iterator iter = umount_targets.rbegin();
  std::vector<std::string>::reverse_iterator iend = umount_targets.rend();
  for (; iter != iend; ++iter) {
    bool rvb = platform_umount_lazy(iter->c_str());
    // Some sub mounts, e.g. /sys/kernel/tracing, cannot be unmounted
    if (!rvb && errno != EINVAL && errno != EACCES) {
      throw publish::EPublish(
        "cannot unmount " + *iter + " (" + StringifyInt(errno) + ")");
    }
  }

  std::vector<std::string> sorted_new_paths(new_paths);
  std::sort(sorted_new_paths.begin(), sorted_new_paths.end());
  iter = sorted_new_paths.rbegin();
  iend = sorted_new_paths.rend();
  for (; iter != iend; ++iter) {
    std::string p = *iter;
    while (p.length() > path.length()) {
      assert(HasPrefix(p, path, false /*ignore_case*/));
      // The parent path might not be empty until all children are visited
      try {
        RemoveSingle(p);
      } catch (const publish::EPublish &e) {
        if (errno != ENOTEMPTY)
          throw;
      }
      p = GetParentPath(p);
    }
  }
  RemoveSingle(path);
}

}  // anonymous namespace


namespace publish {

void CmdEnter::CreateUnderlay(
  const std::string &source_dir,
  const std::string &dest_dir,
  const std::vector<std::string> &empty_dirs,
  std::vector<std::string> *new_paths)
{
  LogCvmfs(kLogCvmfs, kLogDebug, "underlay: entry %s --> %s",
           source_dir.c_str(), dest_dir.c_str());

  // For an empty directory /cvmfs/atlas.cern.ch, we are going to store "/cvmfs"
  std::vector<std::string> empty_toplevel_dirs;
  for (unsigned i = 0; i < empty_dirs.size(); ++i) {
    std::string toplevel_dir = empty_dirs[i];
    while (!GetParentPath(toplevel_dir).empty())
      toplevel_dir = GetParentPath(toplevel_dir);
    empty_toplevel_dirs.push_back(toplevel_dir);

    // We create $DEST/cvmfs (top-level dir)
    std::string dest_empty_dir = dest_dir + toplevel_dir;
    LogCvmfs(kLogCvmfs, kLogDebug, "underlay: mkdir %s",
             dest_empty_dir.c_str());
    EnsureDirectory(dest_empty_dir);
    new_paths->push_back(dest_empty_dir);
  }

  std::vector<std::string> names;
  std::vector<mode_t> modes;
  // In a recursive call, the source directory might not exist, which is fine
  std::string d = source_dir.empty() ? "/" : source_dir;
  if (DirectoryExists(d)) {
    bool rv = ListDirectory(d, &names, &modes);
    if (!rv)
      throw EPublish("cannot list directory " + d);
  }
  // We need to order creating the bindmounts such that the destination itself
  // is handled first; otherwise, the recursive bind mount to the session dir
  // creates all other sub bind mounts again.
  for (unsigned i = 0; i < names.size(); ++i) {
    std::string source = source_dir + "/" + names[i] + "/";
    std::string dest = dest_dir + "/" + names[i] + "/";
    if (HasPrefix(dest, source, false /* ignore_case */)) {
      iter_swap(names.begin(), names.begin() + i);
      iter_swap(modes.begin(), modes.begin() + i);
      break;
    }
  }

  // List the contents of the source directory
  //   1. Symlinks are created as they are
  //   2. Directories become empty directories and are bind-mounted
  //   3. File become empty regular files and are bind-mounted
  for (unsigned i = 0; i < names.size(); ++i) {
    if (std::find(empty_toplevel_dirs.begin(), empty_toplevel_dirs.end(),
        std::string("/") + names[i]) != empty_toplevel_dirs.end())
    {
      continue;
    }

    std::string source = source_dir + "/" + names[i];
    std::string dest = dest_dir + "/" + names[i];
    new_paths->push_back(dest);
    if (S_ISLNK(modes[i])) {
      char buf[PATH_MAX + 1];
      ssize_t nchars = readlink(source.c_str(), buf, PATH_MAX);
      if (nchars < 0)
        throw EPublish("cannot read symlink " + source);
      buf[nchars] = '\0';
      SymlinkForced(std::string(buf), dest);
    } else {
      if (S_ISDIR(modes[i])) {
        EnsureDirectory(dest);
      } else {
        CreateFile(dest, 0600, false /* ignore_failure */);
      }
      LogCvmfs(kLogCvmfs, kLogDebug, "underlay: %s --> %s",
               source.c_str(), dest.c_str());
      bool rv = BindMount(source, dest);
      if (!rv) {
        throw EPublish("cannot bind mount " + source + " --> " + dest +
                       " (" + StringifyInt(errno) + ")");
      }
    }
  }

  // Recurse into the directory trees containing empty directories
  // CreateUnderlay($SOURCE/cvmfs, $DEST/cvmfs, /atlas.cern.ch)
  for (unsigned i = 0; i < empty_toplevel_dirs.size(); ++i) {
    std::string toplevel_dir = empty_toplevel_dirs[i];
    std::vector<std::string> empty_sub_dir;
    empty_sub_dir.push_back(empty_dirs[i].substr(toplevel_dir.length()));
    if (!empty_sub_dir[0].empty()) {
      CreateUnderlay(source_dir + toplevel_dir,
                     dest_dir + toplevel_dir,
                     empty_sub_dir,
                     new_paths);
    }
  }
}

void CmdEnter::WriteCvmfsConfig() {
  BashOptionsManager options_manager;
  options_manager.set_taint_environment(false);
  options_manager.ParseDefault(fqrn_);
  options_manager.SetValue("CVMFS_MOUNT_DIR", lower_layer_);
  options_manager.SetValue("CVMFS_AUTO_UPDATE", "no");
  options_manager.SetValue("CVMFS_NFS_SOURCE", "no");
  options_manager.SetValue("CVMFS_HIDE_MAGIC_XATTRS", "yes");
  options_manager.SetValue("CVMFS_SERVER_CACHE_MODE", "yes");
  options_manager.SetValue("CVMFS_USYSLOG", usyslog_path_);
  options_manager.SetValue("CVMFS_RELOAD_SOCKETS", cache_dir_);
  options_manager.SetValue("CVMFS_WORKSPACE", cache_dir_);
  options_manager.SetValue("CVMFS_CACHE_PRIMARY", "private");
  options_manager.SetValue("CVMFS_CACHE_private_TYPE", "posix");
  options_manager.SetValue("CVMFS_CACHE_private_BASE", cache_dir_);
  options_manager.SetValue("CVMFS_CACHE_private_SHARED", "on");
  options_manager.SetValue("CVMFS_CACHE_private_QUOTA_LIMIT", "4000");

  bool rv = SafeWriteToFile(options_manager.Dump(), config_path_,
                            kPrivateFileMode);
  if (!rv)
    throw EPublish("cannot write client config to " + config_path_);
}


void CmdEnter::MountCvmfs() {
  if (FindExecutable(cvmfs2_binary_).empty()) {
    throw EPublish("cannot find executable " + cvmfs2_binary_,
                   EPublish::kFailMissingDependency);
  }

  int fd_stdout = open(stdout_path_.c_str(), O_CREAT | O_APPEND | O_WRONLY,
                       kPrivateFileMode);
  int fd_stderr = open(stderr_path_.c_str(), O_CREAT | O_APPEND | O_WRONLY,
                       kPrivateFileMode);

  std::vector<std::string> cmdline;
  cmdline.push_back(cvmfs2_binary_);
  cmdline.push_back("-o");
  cmdline.push_back("allow_other,config=" + config_path_);
  cmdline.push_back(fqrn_);
  cmdline.push_back(lower_layer_);
  std::set<int> preserved_fds;
  preserved_fds.insert(0);
  preserved_fds.insert(1);
  preserved_fds.insert(2);
  std::map<int, int> map_fds;
  map_fds[fd_stdout] = 1;
  map_fds[fd_stderr] = 2;
  pid_t pid_child;
  bool rvb = ManagedExec(cmdline, preserved_fds, map_fds,
                         false /* drop_credentials */, false /* clear_env */,
                         false /* double_fork */,
                         &pid_child);
  if (!rvb) {
    close(fd_stdout);
    close(fd_stderr);
    throw EPublish("cannot run " + cvmfs2_binary_);
  }
  int exit_code = WaitForChild(pid_child);
  close(fd_stdout);
  close(fd_stderr);
  if (exit_code != 0) {
    throw EPublish("cannot mount cvmfs read-only branch (" +
          StringifyInt(exit_code) + ")\n" +
          "  command: `" + JoinStrings(cmdline, " ").c_str() + "`");
  }
}


void CmdEnter::MountOverlayfs() {
  if (FindExecutable(overlayfs_binary_).empty()) {
    throw EPublish("cannot find executable " + overlayfs_binary_,
                   EPublish::kFailMissingDependency);
  }

  int fd_stdout = open(stdout_path_.c_str(), O_CREAT | O_APPEND | O_WRONLY,
                       kPrivateFileMode);
  int fd_stderr = open(stderr_path_.c_str(), O_CREAT | O_APPEND | O_WRONLY,
                       kPrivateFileMode);

  std::vector<std::string> cmdline;
  cmdline.push_back(overlayfs_binary_);
  cmdline.push_back("-o");
  cmdline.push_back(string("lowerdir=") + lower_layer_ +
                           ",upperdir=" + upper_layer_ +
                           ",workdir=" + ovl_workdir_);
  cmdline.push_back(rootfs_dir_ + target_dir_);
  std::set<int> preserved_fds;
  preserved_fds.insert(0);
  preserved_fds.insert(1);
  preserved_fds.insert(2);
  std::map<int, int> map_fds;
  map_fds[fd_stdout] = 1;
  map_fds[fd_stderr] = 2;
  pid_t pid_child;
  bool rvb = ManagedExec(cmdline, preserved_fds, map_fds,
                         true /* drop_credentials */, false /* clear_env */,
                         false /* double_fork */,
                         &pid_child);
  if (!rvb) {
    close(fd_stdout);
    close(fd_stderr);
    throw EPublish("cannot run " + overlayfs_binary_);
  }
  int exit_code = WaitForChild(pid_child);
  close(fd_stdout);
  close(fd_stderr);
  if (exit_code != 0) {
    throw EPublish("cannot mount overlay file system (" +
          StringifyInt(exit_code) + ")\n" +
          "  command: `" + JoinStrings(cmdline, " ").c_str() + "`");
  }
}


void CmdEnter::CleanupSession(
  bool keep_logs, const std::vector<std::string> &new_paths)
{
  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Cleaning out session directory... ");

  std::string pid_xattr;
  bool rvb = platform_getxattr(lower_layer_, "user.pid", &pid_xattr);
  if (!rvb)
    throw EPublish("cannot find CernVM-FS process");
  pid_t pid_cvmfs = String2Uint64(pid_xattr);

  rvb = platform_umount_lazy((rootfs_dir_ + target_dir_).c_str());
  if (!rvb)
    throw EPublish("cannot unmount overlayfs on " + rootfs_dir_ + target_dir_);
  rvb = platform_umount_lazy(lower_layer_.c_str());
  if (!rvb)
    throw EPublish("cannot unmount CernVM-FS on " + lower_layer_);

  BackoffThrottle backoff;
  while (ProcessExists(pid_cvmfs)) {
    backoff.Throttle();
  }

  RemoveSingle(lower_layer_);
  RemoveSingle(session_dir_ + "/sysdefault.conf");
  rvb = RemoveTree(ovl_workdir_);
  if (!rvb)
    throw EPublish("cannot remove " + ovl_workdir_);
  rvb = RemoveTree(upper_layer_);
  if (!rvb)
    throw EPublish("cannot remove " + upper_layer_);
  rvb = RemoveTree(cache_dir_);
  if (!rvb)
    throw EPublish("cannot remove " + cache_dir_);
  RemoveUnderlay(rootfs_dir_, new_paths);

  if (keep_logs) {
    LogCvmfs(kLogCvmfs, kLogStdout, "[logs available in %s]",
             (session_dir_ + "/logs").c_str());
    return;
  }

  rvb = RemoveTree(session_dir_ + "/logs");
  RemoveSingle(session_dir_);
  LogCvmfs(kLogCvmfs, kLogStdout, "[done]");
}


int CmdEnter::Main(const Options &options) {
  fqrn_ = options.plain_args()[0].value_str;
  sanitizer::RepositorySanitizer sanitizer;
  if (!sanitizer.IsValid(fqrn_)) {
    throw EPublish("malformed repository name: " + fqrn_);
  }

  if (options.Has("cvmfs2")) {
    cvmfs2_binary_ = options.GetString("cvmfs2");
    // Lucky guess: library in the same directory than the binary,
    // but don't overwrite an explicit setting
    setenv("CVMFS_LIBRARY_PATH", GetParentPath(cvmfs2_binary_).c_str(), 0);
  }

  target_dir_ = "/cvmfs/" + fqrn_;

  // Save context-sensitive directories before switching name spaces
  string cwd = GetCurrentWorkingDirectory();
  uid_t uid = geteuid();
  gid_t gid = getegid();
  string workspace = GetHomeDirectory() + "/.cvmfs/" + fqrn_;

  EnsureDirectory(workspace);
  session_dir_ = CreateTempDir(workspace + "/session");
  if (session_dir_.empty())
    throw EPublish("cannot create session directory in " + workspace);
  rootfs_dir_ = session_dir_ + "/rootfs";
  EnsureDirectory(rootfs_dir_);
  lower_layer_ = session_dir_ + "/lower_layer";
  EnsureDirectory(lower_layer_);
  upper_layer_ = session_dir_ + "/upper_layer";
  EnsureDirectory(upper_layer_);
  ovl_workdir_ = session_dir_ + "/ovl_workdir";
  EnsureDirectory(ovl_workdir_);
  cache_dir_ = session_dir_ + "/cache";
  EnsureDirectory(cache_dir_);
  config_path_ = session_dir_ + "/sysdefault.conf";
  EnsureDirectory(session_dir_ + "/logs");
  usyslog_path_ = session_dir_ + "/logs/cvmfs.log";
  stdout_path_ = session_dir_ + "/logs/stdout.log";
  stderr_path_ = session_dir_ + "/logs/stderr.log";

  LogCvmfs(kLogCvmfs, kLogStdout,
           "*** NOTE: This is currently an experimental CernVM-FS feature\n");
  LogCvmfs(kLogCvmfs, kLogStdout,
           "Entering ephemeral writable shell for %s... ", target_dir_.c_str());
  AnchorPid anchor_pid = EnterRootContainer();
  std::vector<std::string> empty_dirs;
  empty_dirs.push_back(target_dir_);
  empty_dirs.push_back("/proc");
  std::vector<std::string> new_paths;
  CreateUnderlay("", rootfs_dir_, empty_dirs, &new_paths);
  bool rvb = SymlinkForced(session_dir_, rootfs_dir_ + "/.cvmfsenter");
  if (!rvb)
    throw EPublish("cannot create marker file " + rootfs_dir_ + "/.cvmfsenter");
  new_paths.push_back(rootfs_dir_ + "/.cvmfsenter");
  rvb = ProcMount(rootfs_dir_ + "/proc");
  if (!rvb)
    throw EPublish("cannot mount " + rootfs_dir_ + "/proc");
  setenv("CVMFS_ENTER_SESSION_DIR", session_dir_.c_str(), 1 /* overwrite */);

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Mounting CernVM-FS read-only layer... ");
  WriteCvmfsConfig();
  if (options.Has("cvmfs-config"))
    config_path_ += std::string(":") + options.GetString("cvmfs-config");
  MountCvmfs();
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Mounting union file system... ");
  MountOverlayfs();
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  // Fork the inner process, the outer one is used later for cleanup
  pid_t pid = fork();
  if (pid < 0)
    throw EPublish("cannot create subshell");

  if (pid == 0) {
    if (!options.Has("root")) {
      rvb = CreateUserNamespace(uid, gid);
      if (!rvb) {
        throw EPublish(std::string("cannot create user namespace (") +
                       StringifyInt(uid) + ", " + StringifyInt(gid) + ")");
      }
    }

    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             "Switching to %s... ", rootfs_dir_.c_str());
    int rvi = chroot(rootfs_dir_.c_str());
    if (rvi != 0)
      throw EPublish("cannot chroot to " + rootfs_dir_);
    LogCvmfs(kLogCvmfs, kLogStdout, "done");
    // May fail if the working directory was invalid to begin with
    chdir(cwd.c_str());

    LogCvmfs(kLogCvmfs, kLogStdout, "\n"
      "You can attach to this shell from another another terminal with\n");
    if (options.Has("root")) {
      LogCvmfs(kLogCvmfs, kLogStdout,
               "    nsenter --preserve-credentials -U -p -m -t %u\n"
               "    chroot %s\n",
               anchor_pid.init_pid, rootfs_dir_.c_str());
    } else {
      LogCvmfs(kLogCvmfs, kLogStdout,
               "    nsenter --preserve-credentials -U -m -t %u\n"
               "    chroot %s\n"
               "    nsenter --preserve-credentials -S %u -G %u -U -p -t 1\n",
               anchor_pid.parent_pid, rootfs_dir_.c_str(), uid, gid);
    }

    std::vector<std::string> cmdline;
    // options.plain_args()[0] is the repository name
    for (unsigned i = 1; i < options.plain_args().size(); ++i) {
      cmdline.push_back(options.plain_args()[i].value_str);
    }
    if (cmdline.empty())
      cmdline.push_back(GetShell());
    std::set<int> preserved_fds;
    preserved_fds.insert(0);
    preserved_fds.insert(1);
    preserved_fds.insert(2);
    pid_t pid_child;
    rvb = ManagedExec(cmdline, preserved_fds, std::map<int, int>(),
                      false /* drop_credentials */, false /* clear_env */,
                      false /* double_fork */,
                      &pid_child);
    return WaitForChild(pid_child);
  }
  int exit_code = WaitForChild(pid);

  LogCvmfs(kLogCvmfs, kLogStdout, "Leaving CernVM-FS shell...");

  if (!options.Has("keep-session"))
    CleanupSession(options.Has("keep-logs"), new_paths);

  return exit_code;
}

}  // namespace publish
