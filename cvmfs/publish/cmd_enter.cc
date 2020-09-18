/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_enter.h"

#include <errno.h>
#include <fcntl.h>
#include <linux/limits.h>
#include <sched.h>
#include <signal.h>
#include <sys/mount.h>
#include <unistd.h>

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

using namespace std;  // NOLINT

namespace {

static void EnterRootContainer() {
  bool rvb = CreateUserNamespace(0, 0);
  if (!rvb) throw publish::EPublish("cannot create root user namespace");
  rvb = CreateMountNamespace();
  if (!rvb) throw publish::EPublish("cannot create mount namespace");
  rvb = CreatePidNamespace(NULL);
  if (!rvb) throw publish::EPublish("cannot create pid namespace");
}

}  // anonymous namespace


namespace publish {

void CmdEnter::MountCvmfs(const SettingsPublisher &settings) {
  const SettingsSpoolArea &spool_area = settings.transaction().spool_area();
  BashOptionsManager options_manager;
  options_manager.ParseDefault(settings.fqrn());
  options_manager.SetValue("CVMFS_MOUNT_DIR", spool_area.readonly_mnt());
  options_manager.SetValue("CVMFS_AUTO_UPDATE", "no");
  options_manager.SetValue("CVMFS_NFS_SOURCE", "no");
  options_manager.SetValue("CVMFS_HIDE_MAGIC_XATTRS", "yes");
  options_manager.SetValue("CVMFS_SERVER_CACHE_MODE", "yes");
  options_manager.SetValue("CVMFS_USYSLOG", spool_area.client_log());
  options_manager.SetValue("CVMFS_RELOAD_SOCKETS", spool_area.cache_dir());
  options_manager.SetValue("CVMFS_WORKSPACE", spool_area.cache_dir());
  options_manager.SetValue("CVMFS_CACHE_PRIMARY", "private");
  options_manager.SetValue("CVMFS_CACHE_private_TYPE", "posix");
  options_manager.SetValue("CVMFS_CACHE_private_BASE", spool_area.cache_dir());
  options_manager.SetValue("CVMFS_CACHE_private_SHARED", "on");
  options_manager.SetValue("CVMFS_CACHE_private_QUOTA_LIMIT", "4000");
  options_manager.SetValue("CVMFS_NFILES", "65538");
  options_manager.SetValue("CVMFS_HTTP_PROXY", "DIRECT");
  options_manager.SetValue("CVMFS_SERVER_URL", settings.url());
  // TODO(jblomer)
  options_manager.SetValue("CVMFS_KEYS_DIR", GetCurrentWorkingDirectory());

  bool rvb = SafeWriteToFile(options_manager.Dump(), spool_area.client_config(),
                             kPrivateFileMode);
  if (!rvb) throw EPublish("cannot write client config");

  vector<string> args;
  args.push_back("-o");
  args.push_back("config=" + spool_area.client_config());
  args.push_back(settings.fqrn());
  args.push_back(spool_area.readonly_mnt());
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  pid_t pid_cvmfs;
  rvb = ExecuteBinary(&fd_stdin, &fd_stdout, &fd_stderr, cvmfs_binary_, args,
                      false /* double_fork */, &pid_cvmfs);
  if (!rvb) throw EPublish("cannot run " + cvmfs_binary_);

  int exit_code = WaitForChild(pid_cvmfs);
  if (exit_code != 0) throw EPublish("cannot mount cvmfs read-only branch");
}


void CmdEnter::MountOverlayfs(const SettingsPublisher &settings) {
  const SettingsSpoolArea &spool_area = settings.transaction().spool_area();
  vector<string> args;
  args.push_back("-o");
  args.push_back(string("lowerdir=") + spool_area.readonly_mnt() +
                 ",upperdir=" + spool_area.scratch_dir() +
                 ",workdir=" + spool_area.ovl_work_dir());
  args.push_back(spool_area.union_mnt() + "/" + settings.fqrn());
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  pid_t pid_ovl;
  bool rvb = ExecuteBinary(&fd_stdin, &fd_stdout, &fd_stderr, overlayfs_binary_,
                           args, false /* double_fork */, &pid_ovl);
  if (!rvb) EPublish("cannot run " + overlayfs_binary_);
  int exit_code = WaitForChild(pid_ovl);
  if (exit_code != 0) EPublish("cannot mount overlay file system");

  rvb = BindMount(spool_area.union_mnt(), "/cvmfs");
  if (!rvb) throw EPublish("cannot bind mount union file system to /cvmfs");
}


void CmdEnter::CreateUnderlay(
  const std::string &source_dir,
  const std::string &dest_dir,
  const std::vector<std::string> &empty_dirs)
{
  // For an empty directory /cvmfs/atlas.cern.ch, we are going to store "/cvmfs"
  std::vector<std::string> empty_toplevel_dirs;
  for (unsigned i = 0; i < empty_dirs.size(); ++i) {
    std::string toplevel_dir = empty_dirs[i];
    while (!GetParentPath(toplevel_dir).empty())
      toplevel_dir = GetParentPath(toplevel_dir);
    empty_toplevel_dirs.push_back(toplevel_dir);

    // We create $DEST/cvmfs (top-level dir)
    std::string dest_empty_dir = dest_dir + "/" + toplevel_dir;
    bool rv = MkdirDeep(dest_empty_dir, 0700, true /* veryfy_writable */);
    if (!rv)
      throw EPublish("cannot create directory " + dest_empty_dir);

    // And recurse into it, i.e.
    // CreateUnderlay($SOURCE/cvmfs, $DEST/cvmfs, /atlas.cern.ch)
    std::vector<std::string> empty_sub_dir;
    empty_sub_dir.push_back(empty_dirs[i].substr(toplevel_dir.length()));
    if (!empty_sub_dir[0].empty()) {
      CreateUnderlay(source_dir + "/" + toplevel_dir,
                     dest_dir + "/" + toplevel_dir,
                     empty_sub_dir);
    }
  }

  std::vector<std::string> names;
  std::vector<mode_t> modes;
  // In a recursive call, the source directory might not exist, which is fine
  if (DirectoryExists(source_dir)) {
    bool rv = ListDirectory(source_dir, &names, &modes);
    if (!rv)
      throw EPublish("cannot list directory " + source_dir);
  }

  // List the contents of the source directory
  //   1. Symlinks are created as they are
  //   2. Directories become empty directories and are bind-mounted
  //   3. File become empty regular files and are bind-mounted
  for (unsigned i = 0; i < names.size(); ++i) {
    if (std::find(empty_toplevel_dirs.begin(), empty_toplevel_dirs.end(),
        names[i]) != empty_toplevel_dirs.end())
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
        bool rv = MkdirDeep(dest, 0700, true /* verify_writable */);
        if (!rv)
          throw EPublish("cannot create directory " + dest);
      } else {
        CreateFile(dest, 0600, false /* ignore_failure */);
      }
      bool rv = BindMount(source, dest);
      if (!rv)
        throw EPublish("cannot bind mount " + source + " --> " + dest);
    }
  }
}


int CmdEnter::Main(const Options &options) {
  std::string fqrn = options.plain_args()[0].value_str;
  sanitizer::RepositorySanitizer sanitizer;
  if (!sanitizer.IsValid(fqrn)) {
    throw EPublish("malformed repository name: " + fqrn);
  }

  const std::string target = "/cvmfs/" + fqrn;

  // Save context-sensitive directories before switching name spaces
  string cwd = GetCurrentWorkingDirectory();
  string workspace = GetHomeDirectory() + "/.cvmfs/" + fqrn;

  bool retval_b = MkdirDeep(workspace, 0700, true /* verify_writable */);
  if (!retval_b)
    throw EPublish("cannot create workspace " + workspace);
  std::string rootfs = CreateTempDir(workspace + "/session");
  if (rootfs.empty())
    throw EPublish("cannot create session directory in " + workspace);

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Entering ephemeral writable shell for %s... ", target.c_str());
  EnterRootContainer();
  std::vector<std::string> empty_dirs;
  empty_dirs.push_back(target);
  CreateUnderlay("/", rootfs, empty_dirs);
  LogCvmfs(kLogCvmfs, kLogStdout, "done");


  //const SettingsSpoolArea &spool_area = settings.transaction().spool_area();
  //BashOptionsManager options_manager;
  //options_manager.ParseDefault(settings.fqrn());
  //options_manager.SetValue("CVMFS_MOUNT_DIR", spool_area.readonly_mnt());
  //options_manager.SetValue("CVMFS_AUTO_UPDATE", "no");
  //options_manager.SetValue("CVMFS_NFS_SOURCE", "no");
  //options_manager.SetValue("CVMFS_HIDE_MAGIC_XATTRS", "yes");
  //options_manager.SetValue("CVMFS_SERVER_CACHE_MODE", "yes");
  //options_manager.SetValue("CVMFS_USYSLOG", spool_area.client_log());
  //options_manager.SetValue("CVMFS_RELOAD_SOCKETS", spool_area.cache_dir());
  //options_manager.SetValue("CVMFS_WORKSPACE", spool_area.cache_dir());
  //options_manager.SetValue("CVMFS_CACHE_PRIMARY", "private");
  //options_manager.SetValue("CVMFS_CACHE_private_TYPE", "posix");
  //options_manager.SetValue("CVMFS_CACHE_private_BASE", spool_area.cache_dir());
  //options_manager.SetValue("CVMFS_CACHE_private_SHARED", "on");
  //options_manager.SetValue("CVMFS_CACHE_private_QUOTA_LIMIT", "4000");
  //options_manager.SetValue("CVMFS_NFILES", "65538");
  //options_manager.SetValue("CVMFS_HTTP_PROXY", "DIRECT");
  //options_manager.SetValue("CVMFS_SERVER_URL", settings.url());
  //// TODO(jblomer)
  //options_manager.SetValue("CVMFS_KEYS_DIR", GetCurrentWorkingDirectory());
//
  //bool rvb = SafeWriteToFile(options_manager.Dump(), spool_area.client_config(),
  //                           kPrivateFileMode);
  //if (!rvb) throw EPublish("cannot write client config");
//
  //vector<string> args;
  //args.push_back("-o");
  //args.push_back("config=" + spool_area.client_config());
  //args.push_back(settings.fqrn());
  //args.push_back(spool_area.readonly_mnt());
  //int fd_stdin;
  //int fd_stdout;
  //int fd_stderr;
  //pid_t pid_cvmfs;
  //rvb = ExecuteBinary(&fd_stdin, &fd_stdout, &fd_stderr, cvmfs_binary_, args,
  //                    false /* double_fork */, &pid_cvmfs);
  //if (!rvb) throw EPublish("cannot run " + cvmfs_binary_);
//
  //int exit_code = WaitForChild(pid_cvmfs);
  //if (exit_code != 0) throw EPublish("cannot mount cvmfs read-only branch");

  return 0;


  SettingsPublisher settings(fqrn);

  settings.SetOwner(geteuid(), getegid());
  if (options.Has("stratum0"))
    settings.SetUrl(options.GetString("stratum0"));
  settings.GetKeychain()->SetKeychainDir(".");
  settings.GetTransaction()->GetSpoolArea()->SetSpoolArea(workspace);
  // TODO(jblomer): Storage configuration must be gateway for the enter command
  settings.GetStorage()->MakeS3("../s3config",
                                settings.transaction().spool_area().tmp_dir());

  Publisher publisher(settings);
  publisher.Transaction();

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Entering publish container... ");
  EnterRootContainer();
  LogCvmfs(kLogCvmfs, kLogStdout, "done");



  // TODO(jblomer): set correct paths
  cvmfs_binary_ = "cvmfs2";
  overlayfs_binary_ = "fuse-overlayfs";

  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Mounting CernVM-FS read-only branch... ");
  MountCvmfs(settings);
  LogCvmfs(kLogCvmfs, kLogStdout, "done");
  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "Mounting union file system... ");
  MountOverlayfs(settings);
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  bool rvb = CreateUserNamespace(settings.owner_uid(), settings.owner_gid());
  if (!rvb) throw EPublish("cannot create user namespace");

  int rvi = setenv("CVMFS_PUBLISH", settings.fqrn().c_str(), 1 /*overwrite*/);
  assert(rvi == 0);
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

  if (exit_code == 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Publishing changeset...");
    publisher.Publish();
  } else {
    LogCvmfs(kLogCvmfs, kLogStdout, "Aborting transaction...");
    publisher.Abort();
  }

  return exit_code;
}

}  // namespace publish
