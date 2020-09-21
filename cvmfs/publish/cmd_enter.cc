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

static void EnterRootContainer() {
  bool rvb = CreateUserNamespace(0, 0);
  if (!rvb) throw publish::EPublish("cannot create root user namespace");
  rvb = CreateMountNamespace();
  if (!rvb) throw publish::EPublish("cannot create mount namespace");
  rvb = CreatePidNamespace(NULL);
  if (!rvb) throw publish::EPublish("cannot create pid namespace");
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
  LogCvmfs(kLogCvmfs, kLogStdout, "underlay: entry %s --> %s",
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
    LogCvmfs(kLogCvmfs, kLogStdout, "underlay: mkdir %s", dest_empty_dir.c_str());
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
      LogCvmfs(kLogCvmfs, kLogStdout, "underlay: %s --> %s",
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
  std::vector<std::string> cmdline;
  cmdline.push_back(cvmfs2_binary_);
  cmdline.push_back("-o");
  cmdline.push_back("config=" + config_path_);
  cmdline.push_back(fqrn_);
  cmdline.push_back(lower_layer_);
  std::set<int> preserved_fds;
  preserved_fds.insert(0);
  //preserved_fds.insert(1);
  preserved_fds.insert(2);
  pid_t pid_child;
  bool rvb = ManagedExec(cmdline, preserved_fds, std::map<int, int>(),
                         false /* drop_credentials */, false /* clear_env */,
                         false /* double_fork */,
                         &pid_child);
  if (!rvb) throw EPublish("cannot run " + cvmfs2_binary_);
  int exit_code = WaitForChild(pid_child);
  if (exit_code != 0) throw EPublish("cannot mount cvmfs read-only branch");
}


void CmdEnter::MountOverlayfs() {
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



struct ForkFailures {  // TODO(rmeusel): C++11 (type safe enum)
  enum Names {
    kSendPid,
    kUnknown,
    kFailDupFd,
    kFailGetMaxFd,
    kFailGetFdFlags,
    kFailSetFdFlags,
    kFailDropCredentials,
    kFailExec,
  };

  static std::string ToString(const Names name) {
    switch (name) {
      case kSendPid:
        return "Sending PID";

      default:
      case kUnknown:
        return "Unknown Status";
      case kFailDupFd:
        return "Duplicate File Descriptor";
      case kFailGetMaxFd:
        return "Read maximal File Descriptor";
      case kFailGetFdFlags:
        return "Read File Descriptor Flags";
      case kFailSetFdFlags:
        return "Set File Descriptor Flags";
      case kFailDropCredentials:
        return "Lower User Permissions";
      case kFailExec:
        return "Invoking execvp()";
    }
  }
};

/**
 * Execve to the given command line, preserving the given file descriptors.
 * If stdin, stdout, stderr should be preserved, add 0, 1, 2.
 * File descriptors from the parent process can also be mapped to the new
 * process (dup2) using map_fildes.  Can be useful for
 * stdout/in/err redirection.
 * NOTE: The destination fildes have to be preserved!
 * Does a double fork to detach child.
 * The command_line parameter contains the binary at index 0 and the arguments
 * in the rest of the vector.
 * Using the optional parameter *pid it is possible to retrieve the process ID
 * of the spawned process.
 */
bool MyExec(const std::vector<std::string>  &command_line,
                 const std::set<int>        &preserve_fildes,
                 const std::map<int, int>   &map_fildes,
                 const bool             drop_credentials,
                 const bool             clear_env,
                 const bool             double_fork,
                       pid_t           *child_pid)
{
  assert(command_line.size() >= 1);

  Pipe pipe_fork;
  pid_t pid = fork();
  assert(pid >= 0);
  if (pid == 0) {
    pid_t pid_grand_child;
    int max_fd;
    int fd_flags;
    ForkFailures::Names failed = ForkFailures::kUnknown;

    if (clear_env) {
#ifdef __APPLE__
      environ = NULL;
#else
      int retval = clearenv();
      assert(retval == 0);
#endif
    }

    const char *argv[command_line.size() + 1];
    for (unsigned i = 0; i < command_line.size(); ++i)
      argv[i] = command_line[i].c_str();
    argv[command_line.size()] = NULL;

    // Child, map file descriptors
    for (std::map<int, int>::const_iterator i = map_fildes.begin(),
         iEnd = map_fildes.end(); i != iEnd; ++i)
    {
      int retval = dup2(i->first, i->second);
      if (retval == -1) {
        failed = ForkFailures::kFailDupFd;
        goto fork_failure;
      }
    }

    // Child, close file descriptors
    max_fd = sysconf(_SC_OPEN_MAX);
    if (max_fd < 0) {
      failed = ForkFailures::kFailGetMaxFd;
      goto fork_failure;
    }
    for (int fd = 0; fd < max_fd; fd++) {
      if ((fd != pipe_fork.write_end) && (preserve_fildes.count(fd) == 0)) {
        close(fd);
      }
    }

    // Double fork to disconnect from parent
    if (double_fork) {
      pid_grand_child = fork();
      assert(pid_grand_child >= 0);
      if (pid_grand_child != 0) _exit(0);
    }

    fd_flags = fcntl(pipe_fork.write_end, F_GETFD);
    if (fd_flags < 0) {
      failed = ForkFailures::kFailGetFdFlags;
      goto fork_failure;
    }
    fd_flags |= FD_CLOEXEC;
    if (fcntl(pipe_fork.write_end, F_SETFD, fd_flags) < 0) {
      failed = ForkFailures::kFailSetFdFlags;
      goto fork_failure;
    }

#ifdef DEBUGMSG
    assert(setenv("__CVMFS_DEBUG_MODE__", "yes", 1) == 0);
#endif
    if (drop_credentials && !SwitchCredentials(geteuid(), getegid(), false)) {
      failed = ForkFailures::kFailDropCredentials;
      goto fork_failure;
    }

    // retrieve the PID of the new (grand) child process and send it to the
    // grand father
    pid_grand_child = getpid();
    pipe_fork.Write(ForkFailures::kSendPid);
    pipe_fork.Write(pid_grand_child);

    execvp(command_line[0].c_str(), const_cast<char **>(argv));

    failed = ForkFailures::kFailExec;

   fork_failure:
    pipe_fork.Write(failed);
    _exit(1);
  }
  if (double_fork) {
    int statloc;
    waitpid(pid, &statloc, 0);
  }

  close(pipe_fork.write_end);

  // Either the PID or a return value is sent
  ForkFailures::Names status_code;
  bool retcode = pipe_fork.Read(&status_code);
  assert(retcode);
  if (status_code != ForkFailures::kSendPid) {
    close(pipe_fork.read_end);
    LogCvmfs(kLogCvmfs, kLogDebug, "managed execve failed (%s)",
             ForkFailures::ToString(status_code).c_str());
    return false;
  }

  // read the PID of the spawned process if requested
  // (the actual read needs to be done in any case!)
  pid_t buf_child_pid = 0;
  retcode = pipe_fork.Read(&buf_child_pid);
  assert(retcode);
  if (child_pid != NULL)
    *child_pid = buf_child_pid;
  close(pipe_fork.read_end);
  LogCvmfs(kLogCvmfs, kLogDebug, "execve'd %s (PID: %d)",
           command_line[0].c_str(),
           static_cast<int>(buf_child_pid));
  return true;
}


pid_t MyShell() {
  std::vector<std::string> cmdline;
  cmdline.push_back(GetShell());
  std::set<int> preserved_fds;
  preserved_fds.insert(0);
  preserved_fds.insert(1);
  preserved_fds.insert(2);
  pid_t pid_child;
  bool rvb = MyExec(cmdline, preserved_fds, std::map<int, int>(),
                    false /* drop_credentials */, false /* clear_env */,
                    false /* double_fork */,
                    &pid_child);
  return pid_child;
}

pid_t CmdEnter::RunInteractiveShell() {
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  std::vector<std::string> args;
  // We disconnect the terminal and therefore need to force it to be an
  // interactive shell
  args.push_back("--norc");
  args.push_back("-i");
  pid_t pid;

  setenv("PS1", "[ ] ", 1);
  bool rv = ExecuteBinary(&fd_stdin, &fd_stdout, &fd_stderr,
                          GetShell(), args,
                          false /* double_fork */,
                          &pid);
  if (!rv)
    throw EPublish("cannot start shell " + GetShell());

  //std::string ps1_prefix = std::string("\\e[1;34m(") + fqrn_ + ")\\e[0m  ";
  //std::string prompt = "PS1='" + ps1_prefix + "'\n";
  //WritePipe(fd_stdin, prompt.data(), prompt.length());
  //std::string dummy;
  //GetLineFd(fd_stdout, &dummy);

  //Block2Nonblock(0);
  //Block2Nonblock(fd_stdout);
  //Block2Nonblock(fd_stderr);
  struct pollfd watch_fds[3];
  while (true) {
    char buf[1024];

    watch_fds[0].fd = 0;
    watch_fds[0].events = POLLIN | POLLPRI;
    watch_fds[0].revents = 0;
    watch_fds[1].fd = fd_stdout;
    watch_fds[1].events = POLLIN | POLLPRI;
    watch_fds[1].revents = 0;
    watch_fds[2].fd = fd_stderr;
    watch_fds[2].events = POLLIN | POLLPRI;
    watch_fds[2].revents = 0;
    int rv = poll(watch_fds, 3, -1);
    printf("POLL %d\n", rv);
    assert(rv != 0);
    if (rv < 0) {
      if (errno == EINTR)
        continue;
      break;
    }

    printf("POLLACT\n");

    if (watch_fds[0].revents) {
      if (watch_fds[0].revents & (POLLERR | POLLHUP)) {
        printf("fd 0 error");
        break;
      }
      ssize_t nbytes = read(0, buf, sizeof(buf));
      printf("GOT %d INPUT\n", nbytes);
      if (nbytes)
        WritePipe(fd_stdin, buf, nbytes);
    }

    if (watch_fds[1].revents) {
      if (watch_fds[1].revents & (POLLERR | POLLHUP)) {
        printf("OUT ERR\n");
        break;
      }

      //printf(" STDOUT\n");
      ssize_t nbytes = read(fd_stdout, buf, sizeof(buf));
      printf("GOT %d OUTPUT\n", nbytes);
      if (nbytes)
        WritePipe(1, buf, nbytes);
    }

    if (watch_fds[2].revents) {
      if (watch_fds[2].revents & (POLLERR | POLLHUP))
        break;

      //printf(" STDERR\n");
      ssize_t nbytes = read(fd_stderr, buf, sizeof(buf));
      printf("GOT %d ERR\n", nbytes);
      if (nbytes)
        WritePipe(2, buf, nbytes);
    }
  }

  printf("BYE!\n");

  close(fd_stdin);
  close(fd_stdout);
  close(fd_stderr);

  return pid;
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

  //pid_t pid_shell = MyShell();
  //int code = WaitForChild(pid_shell);
  //printf("EXIT CODE %d\n", code);
  //return 0;

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
           "Entering ephemeral writable shell for %s... ", target_dir_.c_str());
  EnterRootContainer();
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

  //pid_t pid_child = RunInteractiveShell();
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
  } else {
    LogCvmfs(kLogCvmfs, kLogStdout, "Aborting transaction...");
  }

  LogCvmfs(kLogCvmfs, kLogStdout, "Cleaning out session directory");

  return exit_code;
}

}  // namespace publish
