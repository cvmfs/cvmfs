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

#include <cassert>
#include <cstdio>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "logging.h"
#include "options.h"
#include "publish/except.h"
#include "publish/settings.h"
#include "sanitizer.h"
#include "util/namespace.h"
#include "util/posix.h"


using namespace std;  // NOLINT

namespace {

static void EnterRootContainer() {
  bool rvb = CreateUserNamespace(0, 0);
  if (!rvb) throw publish::EPublish("cannot create user namespace");
  rvb = CreateMountNamespace();
  if (!rvb) throw publish::EPublish("cannot create mount namespace");
  rvb = CreatePidNamespace(NULL);
  if (!rvb) throw publish::EPublish("cannot create pid namespace");
}

} // anonymous namespace


namespace publish {

int CmdEnter::Main(const Options &options) {
  std::string fqrn = options.plain_args()[0].value_str;
  sanitizer::RepositorySanitizer sanitizer;
  if (!sanitizer.IsValid(fqrn)) {
    throw EPublish("malformed repository name: " + fqrn);
  }
  SettingsPublisher settings(fqrn);

  string cwd = GetCurrentWorkingDirectory();
  string workspace = GetHomeDirectory() + "/.cvmfs/" + fqrn;

  settings.SetOwner(geteuid(), getegid());
  if (options.Has("stratum0"))
    settings.SetUrl(options.GetString("stratum0"));
  settings.GetKeychain()->SetKeychainDir(".");
  settings.GetTransaction()->GetSpoolArea()->SetSpoolArea(workspace);
  // TODO(jblomer): Storage configuration must be gateway for the enter command

  bool rvb;
  int rvi;

  const string path_usyslog = workspace + "/usyslog.log";
  const string path_cache = workspace + "/cache";
  const string path_config = workspace + "/client.config";
  const string path_mount_union = workspace + "/union";
  const string path_mount_rdonly = workspace + "/rdonly";
  const string path_mount_scratch = workspace + "/scratch";
  const string path_ovl_work = workspace + "/work";
  const string path_cvmfs2 =
    "/home/jakob/Documents/CERN/git/src/build-arch/cvmfs/cvmfs2";

  EnterRootContainer();

  LogCvmfs(kLogCvmfs, kLogStdout, "Create workspace %s", workspace.c_str());
  rvb = MkdirDeep(workspace, kPrivateDirMode);
  if (!rvb) throw EPublish("cannot create workspace " + workspace);
  rvb = MkdirDeep(path_cache, kPrivateDirMode);
  if (!rvb) throw EPublish("cannot create " + path_cache);
  rvb = MkdirDeep(path_mount_union, kPrivateDirMode);
  if (!rvb) throw EPublish("cannot create " + path_mount_union);
  rvb = MkdirDeep(path_mount_rdonly, kPrivateDirMode);
  if (!rvb) throw EPublish("cannot create " + path_mount_rdonly);
  rvb = MkdirDeep(path_mount_scratch, kPrivateDirMode);
  if (!rvb) throw EPublish("cannot create " + path_mount_scratch);
  rvb = MkdirDeep(path_ovl_work, kPrivateDirMode);
  if (!rvb) throw EPublish("cannot create " + path_ovl_work);

  LogCvmfs(kLogCvmfs, kLogStdout, "Generating options for %s", fqrn.c_str());
  BashOptionsManager options_manager;
  options_manager.ParseDefault(fqrn);
  options_manager.SetValue("CVMFS_MOUNT_DIR", path_mount_rdonly);
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
  options_manager.SetValue("CVMFS_NFILES", "65538");
  options_manager.SetValue("CVMFS_HTTP_PROXY", "auto");
  options_manager.SetValue("CVMFS_PAC_URLS", "http://wlcg-wpad.cern.ch/wpad.dat;http://wlcg-wpad.fnal.gov/wpad.dat");
  options_manager.SetValue("CVMFS_SERVER_URL", "http://cvmfs-stratum-one.cern.ch/cvmfs/@fqrn@");
  options_manager.SetValue("CVMFS_KEYS_DIR", "/home/jakob/Documents/CERN/git/src/mount/keys");
  rvb = SafeWriteToFile(options_manager.Dump(), path_config, kPrivateFileMode);

  vector<string> args;
  args.push_back("-o");
  args.push_back("config=" + path_config);
  args.push_back(fqrn);
  args.push_back(path_mount_rdonly);
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  pid_t pid_cvmfs;
  rvb = ExecuteBinary(&fd_stdin, &fd_stdout, &fd_stderr, path_cvmfs2, args,
                      false, &pid_cvmfs);
  if (!rvb) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to launch %s", path_cvmfs2.c_str());
    return 32;
  }
  close(fd_stdin);

  int exit_code = WaitForChild(pid_cvmfs);
  if (exit_code != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failure mounting");
    return 1;
  }
  LogCvmfs(kLogCvmfs, kLogStdout, "mounted read-only branch");


  LogCvmfs(kLogCvmfs, kLogStdout, "mounting union file system");
  vector<string> args_ovl;
  args_ovl.push_back("-o");
  args_ovl.push_back(string("lowerdir=") + path_mount_rdonly +
                     ",upperdir=" + path_mount_scratch +
                     ",workdir=" + path_ovl_work);
  args_ovl.push_back(path_mount_union);
  printf("OVL: %s\n", JoinStrings(args_ovl, " ").c_str());
  pid_t pid_ovl;
  rvb = ExecuteBinary(&fd_stdin, &fd_stdout, &fd_stderr,
                "/home/jakob/Documents/CERN/git/fuse-overlayfs/fuse-overlayfs",
                      args_ovl, false, &pid_ovl);
  if (!rvb) EPublish("failed to mount ovl");
  exit_code = WaitForChild(pid_ovl);
  if (exit_code != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failure mounting ovl");
    //return 1;
  }

  LogCvmfs(kLogCvmfs, kLogStdout, "union file system mounted");

  rvb = BindMount(path_mount_union, "/cvmfs");
  if (!rvb) throw EPublish("cannot bind mount to /cvmfs");

  rvb = CreateUserNamespace(settings.owner_uid(), settings.owner_gid());
  if (!rvb) throw EPublish("cannot create user namespace");

  rvi = setenv("PS1", "[CernVM-FS Transaction] ", 1 /* overwrite */);
  assert(rvi == 0);
  std::vector<std::string> cmdline;
  cmdline.push_back(GetShell());
  std::set<int> preserved_fds;
  preserved_fds.insert(0);
  preserved_fds.insert(1);
  preserved_fds.insert(2);
  pid_t pid_child;
  rvb = ManagedExec(cmdline, preserved_fds, std::map<int, int>(),
                    false /* drop_credentials */, false /* double_fork */,
                    &pid_child);
  exit_code = WaitForChild(pid_child);

  LogCvmfs(kLogCvmfs, kLogStdout,
           "exiting pid namespace including process cleanup (%d)", exit_code);

  return 0;
}

}  // namespace publish
