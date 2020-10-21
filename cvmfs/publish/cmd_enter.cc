/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_enter.h"

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

#include "logging.h"
#include "options.h"
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

}  // anonymous namespace


namespace publish {

void CmdEnter::CreateUnderlay(
  const std::string &source_dir,
  const std::string &dest_dir,
  const std::vector<std::string> &empty_dirs)
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

    // And recurse into it, i.e.
    // CreateUnderlay($SOURCE/cvmfs, $DEST/cvmfs, /atlas.cern.ch)
    std::vector<std::string> empty_sub_dir;
    empty_sub_dir.push_back(empty_dirs[i].substr(toplevel_dir.length()));
    if (!empty_sub_dir[0].empty()) {
      CreateUnderlay(source_dir + toplevel_dir,
                     dest_dir + toplevel_dir,
                     empty_sub_dir);
    }
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
      if (!rv)
        throw EPublish("cannot bind mount " + source + " --> " + dest);
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

  std::vector<std::string> cmdline;
  cmdline.push_back(cvmfs2_binary_);
  cmdline.push_back("-o");
  cmdline.push_back("allow_other,config=" + config_path_);
  cmdline.push_back(fqrn_);
  cmdline.push_back(lower_layer_);
  std::set<int> preserved_fds;
  preserved_fds.insert(0);
  // TODO(jblomer): redirect to usyslog
  preserved_fds.insert(1);
  preserved_fds.insert(2);
  pid_t pid_child;
  bool rvb = ManagedExec(cmdline, preserved_fds, std::map<int, int>(),
                         false /* drop_credentials */, false /* clear_env */,
                         false /* double_fork */,
                         &pid_child);
  if (!rvb) throw EPublish("cannot run " + cvmfs2_binary_);
  int exit_code = WaitForChild(pid_child);
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

  std::vector<std::string> args;
  args.push_back("-o");
  args.push_back(string("lowerdir=") + lower_layer_ +
                 ",upperdir=" + upper_layer_ +
                 ",workdir=" + ovl_workdir_);
  args.push_back(rootfs_dir_ + target_dir_);
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  pid_t pid_ovl;
  bool rvb = ExecuteBinary(&fd_stdin, &fd_stdout, &fd_stderr, overlayfs_binary_,
                           args, false /* double_fork */, &pid_ovl);
  if (!rvb) EPublish("cannot run " + overlayfs_binary_);
  int exit_code = WaitForChild(pid_ovl);
  if (exit_code != 0) EPublish("cannot mount overlay file system");
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
  usyslog_path_ = session_dir_ + "/usyslog";

  LogCvmfs(kLogCvmfs, kLogStdout,
           "*** NOTE: This is currently an experimental CernVM-FS feature\n");
  LogCvmfs(kLogCvmfs, kLogStdout,
           "Entering ephemeral writable shell for %s... ", target_dir_.c_str());
  AnchorPid anchor_pid = EnterRootContainer();
  std::vector<std::string> empty_dirs;
  empty_dirs.push_back(target_dir_);
  empty_dirs.push_back("/proc");
  CreateUnderlay("", rootfs_dir_, empty_dirs);
  bool rvb = SymlinkForced(session_dir_, rootfs_dir_ + "/.cvmfsenter");
  if (!rvb)
    throw EPublish("cannot create marker file " + rootfs_dir_ + "/.cvmfsenter");
  rvb = ProcMount(rootfs_dir_ + "/proc");
  if (!rvb)
    throw EPublish("cannot mount " + rootfs_dir_ + "/proc");

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
  int exit_code = WaitForChild(pid_child);

  LogCvmfs(kLogCvmfs, kLogStdout, "Leaving CernVM-FS shell...");

  if (!options.Has("keep-session")) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             "Cleaning out session directory... ");
    LogCvmfs(kLogCvmfs, kLogStdout, "");
    //rvb = RemoveTree(session_dir_);
    //if (rvb) {
    //  LogCvmfs(kLogCvmfs, kLogStdout, "done");
    //} else {
    //  LogCvmfs(kLogCvmfs, kLogStdout, "failed!");
    //  exit_code |= 64;
    //}
  }

  return exit_code;
}

}  // namespace publish
