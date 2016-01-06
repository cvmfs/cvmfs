/**
 * This file is part of the CernVM File System.
 *
 * Implements the mount helper.
 * Note: meaning of return codes is speficied by mount command.
 */

#include <errno.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/wait.h>
#ifdef __APPLE__
#include <sys/sysctl.h>
#endif
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "options.h"
#include "platform.h"
#include "sanitizer.h"
#include "util.h"

using namespace std;  // NOLINT

BashOptionsManager options_manager_;

static void Usage(int output_dest) {
  LogCvmfs(kLogCvmfs, output_dest,
           "Mount helper for CernVM-FS.  Used my mount(8)\n"
           "Mandatory arguments:\n"
           "  repository name: <repository>\n"
           "  mountpoint of the repository: <mountpoint>\n"
           "Options:\n"
           "  -o <mount options>\n"
           "  -f dry run, just prints out the mount command\n"
           "  -h print this help");
}


static void AddMountOption(const string &option,
                           vector<string> *mount_options)
{
  mount_options->push_back(option);
}


static string MkFqrn(const string &repository) {
  const string::size_type idx = repository.find_last_of('.');
  if (idx == string::npos) {
    string domain;
    bool retval = options_manager_.GetValue("CVMFS_DEFAULT_DOMAIN", &domain);
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
               "CVMFS_DEFAULT_DOMAIN missing");
      abort();
    }
    return repository + "." + domain;
  }
  return repository;
}


static bool CheckFuse() {
  string fuse_device;
  int retval;
#ifdef __APPLE__
  if (FileExists("/Library/Filesystems/osxfuse.fs/Contents/Resources/"
                 "load_osxfuse"))
  {
    // OS X Fuse 3
    retval = system("/Library/Filesystems/osxfuse.fs/Contents/Resources/"
                    "load_osxfuse");
  } else {
    retval = system("/Library/Filesystems/osxfusefs.fs/Support/load_osxfusefs");
  }
  if (retval != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed loading OSX Fuse");
    return false;
  }
  fuse_device = "/dev/osxfuse0";
#else
  fuse_device = "/dev/fuse";
#endif
  platform_stat64 info;
  retval = platform_stat(fuse_device.c_str(), &info);
  if ((retval != 0) || !S_ISCHR(info.st_mode)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Fuse not loaded");
    return false;
  }
  return true;
}


static bool CheckStrictMount(const string &fqrn) {
  string param_strict_mount;
  if (options_manager_.GetValue("CVMFS_STRICT_MOUNT", &param_strict_mount) &&
      options_manager_.IsOn(param_strict_mount))
  {
    string repository_list;
    bool retval =
      options_manager_.GetValue("CVMFS_REPOSITORIES", &repository_list);
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogStderr, "CVMFS_REPOSITORIES missing");
      return false;
    }
    vector<string> repositories = SplitString(repository_list, ',');
    for (unsigned i = 0; i < repositories.size(); ++i) {
      if (MkFqrn(repositories[i]) == fqrn)
        return true;
    }
    string config_repository;
    retval =
      options_manager_.GetValue("CVMFS_CONFIG_REPOSITORY", &config_repository);
    if (retval && (config_repository == fqrn))
      return true;
    LogCvmfs(kLogCvmfs, kLogStderr, "Not allowed to mount %s, "
             "add it to CVMFS_REPOSITORIES", fqrn.c_str());
    return false;
  }
  return true;
}


static bool CheckProxy() {
  string param;
  int retval = options_manager_.GetValue("CVMFS_HTTP_PROXY", &param);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "CVMFS_HTTP_PROXY required");
    return false;
  }
  return true;
}


static bool CheckConcurrentMount(const string &fqrn, const string &cachedir) {
  // Try connecting to cvmfs_io socket
  int socket_fd = ConnectSocket(cachedir + "/cvmfs_io." + fqrn);
  if (socket_fd < 0)
    return true;

  // There is a repository mounted, find out the mount point
  SendMsg2Socket(socket_fd, "mountpoint");
  string mountpoint;
  char buf;
  while (read(socket_fd, &buf, 1) == 1) {
    mountpoint.push_back(buf);
  }
  close(socket_fd);
  int output_flags = kLogStderr;
  if (!mountpoint.empty() && (mountpoint[mountpoint.length()-1] == '\n'))
    output_flags |= kLogNoLinebreak;
  LogCvmfs(kLogCvmfs, output_flags, "Repository %s is already mounted on %s",
           fqrn.c_str(), mountpoint.c_str());
  return false;
}


static bool GetCacheDir(const string &fqrn, string *cachedir) {
  string param;
  int retval = options_manager_.GetValue("CVMFS_CACHE_BASE", &param);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "CVMFS_CACHE_BASE required");
    return false;
  }
  *cachedir = MakeCanonicalPath(param);
  if (options_manager_.GetValue("CVMFS_SHARED_CACHE", &param) &&
      options_manager_.IsOn(param))
  {
    *cachedir = *cachedir + "/shared";
  } else {
    *cachedir = *cachedir + "/" + fqrn;
  }
  return true;
}


static bool WaitForReload(const std::string &mountpoint) {
  string param;
  int retval = options_manager_.GetValue("CVMFS_RELOAD_SOCKETS", &param);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "CVMFS_RELOAD_SOCKETS required");
    return false;
  }
  string reload_guard = param + "/cvmfs.pause";
  // Deprecated, now a directory
  if (FileExists(reload_guard)) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Waiting for CernVM-FS reload...");
    while (FileExists(reload_guard))
      SafeSleepMs(250);
  }
  if (DirectoryExists(reload_guard)) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Waiting for CernVM-FS reload...");
    const string mountpoint_base64 = Base64(mountpoint);
    while (DirectoryExists(reload_guard)) {
      // We are in paused state but automounter unmounted the repo.
      // We need to allow to mount just to reload.
      if (FileExists(reload_guard + "/" + mountpoint_base64))
        break;
      SafeSleepMs(250);
    }
  }
  return true;
}


static bool GetCvmfsUser(string *cvmfs_user) {
  string param;
  int retval = options_manager_.GetValue("CVMFS_USER", &param);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "CVMFS_USER required");
    return false;
  }
  *cvmfs_user = param;  // No sanitation; due to PAM, username can be anything
  return true;
}


static std::string GetCvmfsBinary() {
  std::string result;
  vector<string> paths;
  paths.push_back("/usr/bin");

#ifdef __APPLE__
  int major, minor, patch;
  platform_get_os_version(&major, &minor, &patch);
  // OS X El Capitan came with SIP, forcing us to become relocatable. CVMFS
  // 2.2.0+ installs into /usr/local always
  if (major == 10 && minor >= 9) {
    paths.push_back("/usr/local/bin");
  }
#endif

  // TODO(reneme): C++11 range based for loop
        vector<string>::const_iterator i    = paths.begin();
  const vector<string>::const_iterator iend = paths.end();
  for (; i != iend; ++i) {
    const std::string cvmfs2 = *i + "/cvmfs2";
    if (FileExists(cvmfs2) || SymlinkExists(cvmfs2)) {
      result = cvmfs2;
      break;
    }
  }

  return result;
}


int main(int argc, char **argv) {
  bool dry_run = false;
  vector<string> mount_options;

  // Option parsing
  int c;
  while ((c = getopt(argc, argv, "vfnho:")) != -1) {
    switch (c) {
      case 'f':
        dry_run = true;
        break;
      case 'n':
        LogCvmfs(kLogCvmfs, kLogStdout, "Note: fusermount _does_ modify "
                 "/etc/mtab in case it is writable.");
        // Fall through
      case 'v':
        break;
      case 'h':
        Usage(kLogStdout);
        return 0;
      case 'o':
        AddMountOption(optarg, &mount_options);
        break;
      default:
        Usage(kLogStderr);
        return 1;
    }
  }
  if (optind+2 != argc) {
    Usage(kLogStderr);
    return 1;
  }

  string device = argv[optind];
  // For Ubuntu 8.04 automounter
  if (HasPrefix(device, "/cvmfs/", false))
    device = device.substr(7);
  sanitizer::RepositorySanitizer repository_sanitizer;
  if (!repository_sanitizer.IsValid(device)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Invalid repository: %s", device.c_str());
    return 1;
  }
  string mountpoint = argv[optind+1];

  options_manager_.ParseDefault("");
  const string fqrn = MkFqrn(device);
  options_manager_.ParseDefault(fqrn);

  int retval;
  int sysret;
  string cvmfs_user;
  string cachedir;
  // Environment checks
  retval = WaitForReload(mountpoint);
  if (!retval) return 1;
  retval = GetCacheDir(fqrn, &cachedir);
  if (!retval) return 1;
  retval = GetCvmfsUser(&cvmfs_user);
  if (!retval) return 1;
  retval = CheckFuse();
  if (!retval) return 1;
  retval = CheckStrictMount(fqrn);
  if (!retval) return 1;
  retval = CheckProxy();
  if (!retval) return 1;
  // This is not a sure thing.  When the CVMFS_CACHE_BASE parameter is changed
  // two repositories can get mounted concurrently (but that should not hurt).
  // If the same repository is mounted multiple times at the same time, there
  // is a race here.  Eventually, only one repository will be mounted while the
  // other cvmfs processes block on a file lock in the cache.
  retval = CheckConcurrentMount(fqrn, cachedir);
  if (!retval) return 1;

  // Retrieve cvmfs uid/gid and fuse gid if exists
  uid_t uid_cvmfs;
  gid_t gid_cvmfs;
  gid_t gid_fuse;
  bool has_fuse_group = false;
  retval = GetUidOf(cvmfs_user, &uid_cvmfs, &gid_cvmfs);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to find user %s in passwd database",
             cvmfs_user.c_str());
    return 1;
  }
  has_fuse_group = GetGidOf("fuse", &gid_fuse);

  // Prepare cache directory
  retval = MkdirDeep(cachedir, 0755, false);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to create cache directory %s",
             cachedir.c_str());
    return 1;
  }
  retval = MkdirDeep("/var/run/cvmfs", 0755);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to create socket directory");
    return 1;
  }
  sysret = chown(cachedir.c_str(), uid_cvmfs, getegid());
  if (sysret != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to transfer ownership of %s to %s",
             cachedir.c_str(), cvmfs_user.c_str());
    return 1;
  }
  sysret = chown("/var/run/cvmfs", uid_cvmfs, getegid());
  if (sysret != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to transfer ownership of %s to %s",
             "/var/run/cvmfs", cvmfs_user.c_str());
    return 1;
  }

  // Set maximum number of files
#ifdef __APPLE__
  string param;
  if (options_manager_.GetValue("CVMFS_NFILES", &param)) {
    sanitizer::IntegerSanitizer integer_sanitizer;
    if (!integer_sanitizer.IsValid(param)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Invalid CVMFS_NFILES: %s",
               param.c_str());
      return 1;
    }
    int nfiles = String2Uint64(param);
    if ((nfiles < 128) || (nfiles > 524288)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Invalid CVMFS_NFILES: %s",
               param.c_str());
      return 1;
    }
    int nfiles_all = nfiles + 512;
    int sys_nfiles, sys_nfiles_all;
    size_t len = sizeof(sys_nfiles);
    int mib[2];
    mib[0] = CTL_KERN;
    mib[1] = KERN_MAXFILESPERPROC;
    retval = sysctl(mib, 2, &sys_nfiles, &len, NULL, 0);
    if ((retval != 0) || (sys_nfiles < 0)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to get KERN_MAXFILESPERPROC");
      return 1;
    }
    if (sys_nfiles < nfiles) {
      mib[1] = KERN_MAXFILES;
      retval = sysctl(mib, 2, &sys_nfiles_all, &len, NULL, 0);
      if ((retval != 0) || (sys_nfiles_all < 0)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to get KERN_MAXFILES");
        return 1;
      }
      if (sys_nfiles_all < nfiles_all) {
        retval = sysctl(mib, 2, NULL, NULL, &nfiles_all, sizeof(nfiles_all));
        if (retval != 0) {
          LogCvmfs(kLogCvmfs, kLogStderr, "Failed to set KERN_MAXFILES");
          return 1;
        }
      }
      mib[1] = KERN_MAXFILESPERPROC;
      retval = sysctl(mib, 2, NULL, NULL, &nfiles_all, sizeof(nfiles_all));
      if (retval != 0) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to set KERN_MAXFILESPERPROC");
        return 1;
      }
    }
  }
#endif

  AddMountOption("fsname=cvmfs2", &mount_options);
  AddMountOption("allow_other", &mount_options);
  AddMountOption("grab_mountpoint", &mount_options);
  AddMountOption("uid=" + StringifyInt(uid_cvmfs), &mount_options);
  AddMountOption("gid=" + StringifyInt(gid_cvmfs), &mount_options);
  if (options_manager_.IsDefined("CVMFS_DEBUGLOG"))
    AddMountOption("debug", &mount_options);

  const string cvmfs_binary = GetCvmfsBinary();
  if (cvmfs_binary.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to locate the cvmfs2 binary");
    return 1;
  }

  // Dry run early exit
  if (dry_run) {
    string cmd = cvmfs_binary + " -o " + JoinStrings(mount_options, ",") +
                 " " + fqrn + " " + mountpoint;
    if (has_fuse_group) {
      cmd = "sg fuse -c \"" + cmd + "\"";
    }
    LogCvmfs(kLogCvmfs, kLogStdout, "%s", cmd.c_str());
    return 0;
  }

  // Real mount, add supplementary fuse group
  if (has_fuse_group) {
    retval = AddGroup2Persona(gid_fuse);
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to add fuse to "
               " the list of supplementary groups");
      return 1;
    }
  }

  // Exec cvmfs2, from here on errors return 32 (mount error)
  int fd_stdin, fd_stdout, fd_stderr;
  pid_t pid_cvmfs;
  vector<string> cvmfs_args;
  cvmfs_args.push_back("-o");
  cvmfs_args.push_back(JoinStrings(mount_options, ","));
  cvmfs_args.push_back(fqrn);
  cvmfs_args.push_back(mountpoint);
  retval = ExecuteBinary(&fd_stdin, &fd_stdout, &fd_stderr,
                         cvmfs_binary, cvmfs_args, false, &pid_cvmfs);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to launch %s",
             cvmfs_binary.c_str());
    return 32;
  }
  close(fd_stdin);

  // Print stdout / stderr and collect exit code
  int nfds = fd_stdout > fd_stderr ? fd_stdout + 1 : fd_stderr + 1;
  fd_set readfds;
  bool stdout_open = true;
  bool stderr_open = true;
  do {
    FD_ZERO(&readfds);
    if (stdout_open) FD_SET(fd_stdout, &readfds);
    if (stderr_open) FD_SET(fd_stderr, &readfds);
    do {
      retval = select(nfds, &readfds, NULL, NULL, NULL);
      if ((retval == -1) && (errno != EINTR)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to pipe stdout/stderr");
        return 32;
      }
      if (retval > 0)
        break;
    } while (true);
    char buf;
    int num_bytes;
    if (FD_ISSET(fd_stdout, &readfds)) {
      num_bytes = read(fd_stdout, &buf, 1);
      switch (num_bytes) {
        case 0:
          stdout_open = false;
          break;
        case 1:
          LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "%c", buf);
          break;
        default:
          LogCvmfs(kLogCvmfs, kLogStderr, "Failed reading stdout");
          return 32;
      }
    }
    if (FD_ISSET(fd_stderr, &readfds)) {
      num_bytes = read(fd_stderr, &buf, 1);
      switch (num_bytes) {
        case 0:
          stderr_open = false;
          break;
        case 1:
          LogCvmfs(kLogCvmfs, kLogStderr | kLogNoLinebreak, "%c", buf);
          break;
        default:
          LogCvmfs(kLogCvmfs, kLogStderr, "Failed reading stderr");
          return 32;
      }
    }
  } while (stdout_open || stderr_open);
  close(fd_stdout);
  close(fd_stderr);

  int status;
  retval = waitpid(pid_cvmfs, &status, 0);
  if (retval == -1) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed reading return code");
    return 32;
  }
  if (WIFEXITED(status) && (WEXITSTATUS(status) == 0))
    return 0;

  return 32;
}
