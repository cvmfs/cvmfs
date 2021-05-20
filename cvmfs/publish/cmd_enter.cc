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
  uid_t euid = geteuid();
  uid_t egid = getegid();
  NamespaceFailures failure = CreateUserNamespace(0, 0);
  if (failure != kFailNsOk) {
    throw publish::EPublish("cannot create root user namespace (" +
      StringifyInt(failure) + " / " + StringifyInt(errno) + ") [euid=" +
      StringifyInt(euid) + ", egid=" + StringifyInt(egid) + "]");
  }

  bool rvb = CreateMountNamespace();
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

void CmdEnter::WriteCvmfsConfig(const std::string &extra_config) {
  BashOptionsManager options_manager(
    new DefaultOptionsTemplateManager(fqrn_));
  options_manager.ParseDefault(fqrn_);
  if (!extra_config.empty())
    options_manager.ParsePath(extra_config, false /* external */);

  options_manager.SetValue("CVMFS_MOUNT_DIR",
                           settings_spool_area_.readonly_mnt());
  options_manager.SetValue("CVMFS_AUTO_UPDATE", "no");
  options_manager.SetValue("CVMFS_NFS_SOURCE", "no");
  options_manager.SetValue("CVMFS_HIDE_MAGIC_XATTRS", "yes");
  options_manager.SetValue("CVMFS_SERVER_CACHE_MODE", "yes");
  options_manager.SetValue("CVMFS_CLAIM_OWNERSHIP", "yes");
  options_manager.SetValue("CVMFS_USYSLOG", settings_spool_area_.client_log());
  options_manager.SetValue("CVMFS_RELOAD_SOCKETS",
                           settings_spool_area_.cache_dir());
  options_manager.SetValue("CVMFS_WORKSPACE", settings_spool_area_.cache_dir());
  options_manager.SetValue("CVMFS_CACHE_PRIMARY", "private");
  options_manager.SetValue("CVMFS_CACHE_private_TYPE", "posix");
  options_manager.SetValue("CVMFS_CACHE_private_BASE",
                           settings_spool_area_.cache_dir());
  options_manager.SetValue("CVMFS_CACHE_private_SHARED", "on");
  options_manager.SetValue("CVMFS_CACHE_private_QUOTA_LIMIT", "4000");

  bool rv = SafeWriteToFile(options_manager.Dump(),
                            settings_spool_area_.client_config(),
                            kPrivateFileMode);
  if (!rv) {
    throw EPublish("cannot write client config to " +
                   settings_spool_area_.client_config());
  }
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

  std::string cvmfs_config = settings_spool_area_.client_config();

  std::vector<std::string> cmdline;
  cmdline.push_back(cvmfs2_binary_);
  cmdline.push_back("-o");
  cmdline.push_back("allow_other,config=" + cvmfs_config);
  cmdline.push_back(fqrn_);
  cmdline.push_back(settings_spool_area_.readonly_mnt());
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

  rvb = BindMount(settings_spool_area_.readonly_mnt(),
                  rootfs_dir_ + settings_spool_area_.readonly_mnt());
  if (!rvb)
    throw EPublish("cannot map CernVM-FS mount point");
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
  cmdline.push_back(string("lowerdir=") + settings_spool_area_.readonly_mnt() +
                           ",upperdir=" + settings_spool_area_.scratch_dir() +
                           ",workdir=" + settings_spool_area_.ovl_work_dir());
  cmdline.push_back(rootfs_dir_ + settings_spool_area_.union_mnt());
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


std::string CmdEnter::GetCvmfsXattr(const std::string &name) {
  std::string xattr;
  bool rvb = platform_getxattr(settings_spool_area_.readonly_mnt(),
                               std::string("user.") + name, &xattr);
  if (!rvb) {
    throw EPublish("cannot get extrended attribute " + name + " from " +
                   settings_spool_area_.readonly_mnt());
  }
  return xattr;
}


void CmdEnter::CleanupSession(
  bool keep_logs, const std::vector<std::string> &new_paths)
{
  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Cleaning out session directory... ");

  std::string pid_xattr;
  bool rvb = platform_getxattr(settings_spool_area_.readonly_mnt(),
                               "user.pid", &pid_xattr);
  if (!rvb)
    throw EPublish("cannot find CernVM-FS process");
  pid_t pid_cvmfs = String2Uint64(GetCvmfsXattr("pid"));

  const std::string union_mnt = rootfs_dir_ + settings_spool_area_.union_mnt();
  rvb = platform_umount_lazy(union_mnt.c_str());
  if (!rvb)
    throw EPublish("cannot unmount overlayfs on " + union_mnt);
  rvb = platform_umount_lazy(
    (rootfs_dir_ + settings_spool_area_.readonly_mnt()).c_str());
  if (!rvb) {
    throw EPublish("cannot unmount mapped CernVM-FS on " +
                   rootfs_dir_ + settings_spool_area_.readonly_mnt());
  }
  rvb = platform_umount_lazy(settings_spool_area_.readonly_mnt().c_str());
  if (!rvb) {
    throw EPublish("cannot unmount CernVM-FS on " +
                   settings_spool_area_.readonly_mnt());
  }

  BackoffThrottle backoff;
  while (ProcessExists(pid_cvmfs)) {
    backoff.Throttle();
  }

  RemoveSingle(settings_spool_area_.readonly_mnt());
  RemoveSingle(settings_spool_area_.client_config());
  RemoveSingle(env_conf_);
  rvb = RemoveTree(settings_spool_area_.ovl_work_dir());
  if (!rvb)
    throw EPublish("cannot remove " + settings_spool_area_.ovl_work_dir());
  rvb = RemoveTree(settings_spool_area_.scratch_base());
  if (!rvb)
    throw EPublish("cannot remove " + settings_spool_area_.scratch_base());
  rvb = RemoveTree(settings_spool_area_.cache_dir());
  if (!rvb)
    throw EPublish("cannot remove " + settings_spool_area_.cache_dir());
  RemoveUnderlay(rootfs_dir_, new_paths);
  rvb = RemoveTree(settings_spool_area_.tmp_dir());
  if (!rvb)
    throw EPublish("cannot remove " + settings_spool_area_.tmp_dir());

  if (keep_logs) {
    LogCvmfs(kLogCvmfs, kLogStdout, "[logs available in %s]",
             settings_spool_area_.log_dir().c_str());
    return;
  }

  rvb = RemoveTree(settings_spool_area_.log_dir());
  RemoveSingle(session_dir_);
  LogCvmfs(kLogCvmfs, kLogStdout, "[done]");
}


int CmdEnter::Main(const Options &options) {
  fqrn_ = options.plain_args()[0].value_str;
  sanitizer::RepositorySanitizer sanitizer;
  if (!sanitizer.IsValid(fqrn_)) {
    throw EPublish("malformed repository name: " + fqrn_);
  }

  bool rvb;

  // We cannot have any capabilities or else we are not allowed to write
  // to /proc/self/setgroups anc /proc/self/[u|g]id_map when creating a user
  // namespace
  Env::DropCapabilities();

  if (options.Has("cvmfs2")) {
    cvmfs2_binary_ = options.GetString("cvmfs2");
    // Lucky guess: library in the same directory than the binary,
    // but don't overwrite an explicit setting
    setenv("CVMFS_LIBRARY_PATH", GetParentPath(cvmfs2_binary_).c_str(), 0);
  }

  // Prepare the session directory
  std::string workspace = GetHomeDirectory() + "/.cvmfs/" + fqrn_;
  EnsureDirectory(workspace);
  session_dir_ = CreateTempDir(workspace + "/session");
  if (session_dir_.empty())
    throw EPublish("cannot create session directory in " + workspace);
  settings_spool_area_.SetUnionMount(std::string("/cvmfs/") + fqrn_);
  settings_spool_area_.SetSpoolArea(session_dir_);
  settings_spool_area_.EnsureDirectories();
  rootfs_dir_ = session_dir_ + "/rootfs";
  EnsureDirectory(rootfs_dir_);
  stdout_path_ = settings_spool_area_.log_dir() + "/stdout.log";
  stderr_path_ = settings_spool_area_.log_dir() + "/stderr.log";

  // Save process context information before switching namespaces
  string cwd = GetCurrentWorkingDirectory();
  uid_t uid = geteuid();
  gid_t gid = getegid();

  LogCvmfs(kLogCvmfs, kLogStdout,
           "*** NOTE: This is currently an experimental CernVM-FS feature\n");
  LogCvmfs(kLogCvmfs, kLogStdout,
           "Entering ephemeral writable shell for %s... ",
           settings_spool_area_.union_mnt().c_str());

  // Create root user namespace and rootfs underlay
  AnchorPid anchor_pid = EnterRootContainer();
  std::vector<std::string> empty_dirs;
  empty_dirs.push_back(settings_spool_area_.union_mnt());
  empty_dirs.push_back("/proc");
  std::vector<std::string> new_paths;
  CreateUnderlay("", rootfs_dir_, empty_dirs, &new_paths);
  // The .cvmfsenter marker file helps when attaching to the mount namespace
  // to distinguish it from the system root
  rvb = SymlinkForced(session_dir_, rootfs_dir_ + "/.cvmfsenter");
  if (!rvb)
    throw EPublish("cannot create marker file " + rootfs_dir_ + "/.cvmfsenter");
  new_paths.push_back(rootfs_dir_ + "/.cvmfsenter");
  rvb = ProcMount(rootfs_dir_ + "/proc");
  if (!rvb)
    throw EPublish("cannot mount " + rootfs_dir_ + "/proc");
  setenv("CVMFS_ENTER_SESSION_DIR", session_dir_.c_str(), 1 /* overwrite */);

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Mounting CernVM-FS read-only layer... ");
  // We mount in a sub process in order to avoid tainting the main process
  // with the CVMFS_* environment variables
  pid_t pid = fork();
  if (pid < 0)
    throw EPublish("cannot fork");
  if (pid == 0) {
    WriteCvmfsConfig(options.GetStringDefault("cvmfs-config", ""));
    MountCvmfs();
    _exit(0);
  }
  int exit_code = WaitForChild(pid);
  if (exit_code != 0)
    _exit(exit_code);
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  env_conf_ = session_dir_ + "/env.conf";
  rvb = SafeWriteToFile(std::string("CVMFS_FQRN=") + fqrn_ + "\n",
                        env_conf_, kPrivateFileMode);
  if (!rvb)
    throw EPublish("cannot create session environment file");

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Mounting union file system... ");
  MountOverlayfs();
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  // Fork the inner process, the outer one is used later for cleanup
  pid = fork();
  if (pid < 0)
    throw EPublish("cannot create subshell");

  if (pid == 0) {
    if (!options.Has("root")) {
      NamespaceFailures failure = CreateUserNamespace(uid, gid);
      if (failure != kFailNsOk) {
        throw publish::EPublish("cannot create user namespace for " +
          StringifyInt(uid) + ":" + StringifyInt(gid) + " (" +
          StringifyInt(failure) + " / " + StringifyInt(errno) + ") [euid=" +
          StringifyInt(geteuid()) + ", egid=" + StringifyInt(getegid()) + "]");
      }
    }

    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             "Switching to %s... ", rootfs_dir_.c_str());
    int rvi = chroot(rootfs_dir_.c_str());
    if (rvi != 0)
      throw EPublish("cannot chroot to " + rootfs_dir_);
    LogCvmfs(kLogCvmfs, kLogStdout, "done");
    // May fail if the working directory was invalid to begin with
    rvi = chdir(cwd.c_str());
    if (rvi != 0) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Warning: cannot chdir to %s",
               cwd.c_str());
    }

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
  exit_code = WaitForChild(pid);

  LogCvmfs(kLogCvmfs, kLogStdout, "Leaving CernVM-FS shell...");

  if (!options.Has("keep-session"))
    CleanupSession(options.Has("keep-logs"), new_paths);

  return exit_code;
}

}  // namespace publish
