/**
 * This file is part of the CernVM File System.
 *
 * Runs mount/umount commands and removes scratch space on behalf of a
 * repository owner.  Server-only utility.
 *
 * This binary does not use the cvmfs infrastructure code to stay lean.
 */

// clang-format off
#include <sys/xattr.h>  // NOLINT
// clang-format on

#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "cvmfs_suid_util.h"
#include "sanitizer.h"
#include "util/platform.h"

using namespace std;  // NOLINT

const char *kSpoolArea = "/var/spool/cvmfs";

enum RemountType {
  kRemountRdonly,
  kRemountRw
};

static void GetCredentials(uid_t *calling_uid, uid_t *effective_uid) {
  *calling_uid = getuid();
  *effective_uid = geteuid();
}

static void ExecAsRoot(const char *binary, const char *arg1, const char *arg2,
                       const char *arg3, const char *arg4) {
  char *argv[] = {strdup(binary),
                  arg1 ? strdup(arg1) : NULL,
                  arg2 ? strdup(arg2) : NULL,
                  arg3 ? strdup(arg3) : NULL,
                  arg4 ? strdup(arg4) : NULL,
                  NULL};
  char *environ[] = {NULL};

  int retval = setuid(0);
  if (retval != 0) {
    fprintf(stderr, "failed to gain root privileges (%d)\n", errno);
    exit(1);
  }

  execve(binary, argv, environ);
  fprintf(stderr, "failed to run %s... (%d)\n", binary, errno);
  exit(1);
}

static void ForkAndExecAsRoot(const char *binary, const char *arg1,
                              const char *arg2, const char *arg3,
                              const char *arg4) {
  pid_t child = fork();
  if (child == -1) {
    fprintf(stderr, "failed to fork %s... (%d)\n", binary, errno);
    exit(1);
  } else if (child == 0) {
    ExecAsRoot(binary, arg1, arg2, arg3, arg4);
  } else {
    int wstatus;
    waitpid(child, &wstatus, 0);
    if (WIFSIGNALED(wstatus)) {
      exit(128 + WTERMSIG(wstatus));
    } else if (WIFEXITED(wstatus) && WEXITSTATUS(wstatus)) {
      exit(WEXITSTATUS(wstatus));
    }
  }
}

static void Remount(const string &path, const RemountType how) {
  string remount_option = "remount,";
  switch (how) {
    case kRemountRw:
      remount_option += "rw";
      break;
    case kRemountRdonly:
      remount_option += "ro";
      break;
    default:
      fprintf(stderr, "internal error\n");
      exit(1);
  }

  // Note: the device name "dev" doesn't matter. It won't change from the
  // "overlayfs_<fqrn>" name used when originally mounting the path. It is
  // a dummy argument to appease the mount command.
  ExecAsRoot("/bin/mount", "-o", remount_option.c_str(), "dev", path.c_str());
}

static void Mount(const string &path) {
  platform_stat64 info;
  int retval = platform_stat("/bin/systemctl", &info);
  if (retval == 0) {
    string systemd_unit = cvmfs_suid::EscapeSystemdUnit(path);
    // On newer versions of systemd, the mount unit is based on the fully
    // resolved path (discovered on Ubuntu 18.04, test 539)
    if (!cvmfs_suid::PathExists(string("/run/systemd/generator/")
                                + systemd_unit)) {
      string resolved_path = cvmfs_suid::ResolvePath(path);
      if (resolved_path.empty()) {
        fprintf(stderr, "cannot resolve %s\n", path.c_str());
        exit(1);
      }
      systemd_unit = cvmfs_suid::EscapeSystemdUnit(resolved_path);
    }
    ForkAndExecAsRoot("/bin/systemctl", "restart", systemd_unit.c_str(), NULL,
                      NULL);
    ExecAsRoot("/bin/systemctl", "reset-failed", systemd_unit.c_str(), NULL,
               NULL);
  } else {
    ExecAsRoot("/bin/mount", path.c_str(), NULL, NULL, NULL);
  }
}

static void Umount(const string &path) {
  ExecAsRoot("/bin/umount", path.c_str(), NULL, NULL, NULL);
}

static void LazyUmount(const string &path) {
  ExecAsRoot("/bin/umount", "-l", path.c_str(), NULL, NULL);
}

static void KillCvmfs(const string &fqrn) {
  // prevent exploitation like:
  // fqrn = ../../../../usr/home/file_with_xattr_user.pid
  if (fqrn.find("/") != string::npos || fqrn.find("\\") != string::npos) {
    exit(1);
  }
  string pid;
  const string mountpoint = string(kSpoolArea) + "/" + fqrn + "/rdonly";
  const bool retval = platform_getxattr(mountpoint.c_str(), "user.pid", &pid);
  if (!retval || pid.empty())
    exit(1);
  sanitizer::PositiveIntegerSanitizer pid_sanitizer;
  if (!pid_sanitizer.IsValid(pid))
    exit(1);
  ExecAsRoot("/bin/kill", "-9", pid.c_str(), NULL, NULL);
}

class ScopedWorkingDirectory {
 public:
  explicit ScopedWorkingDirectory(const string &path)
      : previous_path_(GetCurrentWorkingDirectory()), directory_handle_(NULL) {
    ChangeDirectory(path);
    directory_handle_ = opendir(".");
  }

  ~ScopedWorkingDirectory() {
    if (directory_handle_ != NULL) {
      closedir(directory_handle_);
    }
    ChangeDirectory(previous_path_);
  }

  operator bool() const { return directory_handle_ != NULL; }

  struct DirectoryEntry {
    bool is_directory;
    string name;
  };

  bool NextDirectoryEntry(DirectoryEntry *entry) {
    platform_dirent64 *dirent;
    while ((dirent = platform_readdir(directory_handle_)) != NULL
           && IsDotEntry(dirent)) {
    }
    if (dirent == NULL) {
      return false;
    }

    platform_stat64 info;
    if (platform_lstat(dirent->d_name, &info) != 0) {
      return false;
    }

    entry->is_directory = S_ISDIR(info.st_mode);
    entry->name = dirent->d_name;
    return true;
  }

 protected:
  string GetCurrentWorkingDirectory() {
    char path[PATH_MAX];
    const char *cwd = getcwd(path, PATH_MAX);
    assert(cwd == path);
    return string(cwd);
  }

  void ChangeDirectory(const string &path) {
    const int retval = chdir(path.c_str());
    assert(retval == 0);
  }

  bool IsDotEntry(const platform_dirent64 *dirent) {
    return (strcmp(dirent->d_name, ".") == 0)
           || (strcmp(dirent->d_name, "..") == 0);
  }

 private:
  const string previous_path_;
  DIR *directory_handle_;
};

static bool ClearDirectory(const string &path) {
  ScopedWorkingDirectory swd(path);
  if (!swd) {
    return false;
  }

  bool success = true;
  ScopedWorkingDirectory::DirectoryEntry dirent;
  while (success && swd.NextDirectoryEntry(&dirent)) {
    success = (dirent.is_directory) ? ClearDirectory(dirent.name)
                                          && (rmdir(dirent.name.c_str()) == 0)
                                    : (unlink(dirent.name.c_str()) == 0);
  }

  return success;
}

static int CleanupDirectory(const string &path) {
  if (!ClearDirectory(path)) {
    fprintf(stderr, "failed to clear %s\n", path.c_str());
    return 1;
  }
  return 0;
}

static int DoSynchronousScratchCleanup(const string &fqrn) {
  const string scratch = string(kSpoolArea) + "/" + fqrn + "/scratch/current";
  return CleanupDirectory(scratch);
}

static int DoAsynchronousScratchCleanup(const string &fqrn) {
  const string wastebin = string(kSpoolArea) + "/" + fqrn + "/scratch/wastebin";

  // double-fork to daemonize the process and redirect I/O to /dev/null
  pid_t pid;
  int statloc;
  if ((pid = fork()) == 0) {
    int retval = setsid();
    assert(retval != -1);
    if ((pid = fork()) == 0) {
      int null_read = open("/dev/null", O_RDONLY);
      int null_write = open("/dev/null", O_WRONLY);
      assert((null_read >= 0) && (null_write >= 0));
      retval = dup2(null_read, 0);
      assert(retval == 0);
      retval = dup2(null_write, 1);
      assert(retval == 1);
      retval = dup2(null_write, 2);
      assert(retval == 2);
      close(null_read);
      close(null_write);
    } else {
      assert(pid > 0);
      _exit(0);
    }
  } else {
    assert(pid > 0);
    waitpid(pid, &statloc, 0);
    _exit(0);
  }

  return CleanupDirectory(wastebin);
}

static void Usage(const string &exe, FILE *output) {
  fprintf(output,
          "Usage: %s lock|open|rw_mount|rw_umount|rdonly_mount|rdonly_umount|"
          "clear_scratch|clear_scratch_async|kill_cvmfs <fqrn>\n"
          "Example: %s rw_umount atlas.cern.ch\n"
          "This binary is typically called by cvmfs_server.\n",
          exe.c_str(), exe.c_str());
}


int main(int argc, char *argv[]) {
  umask(077);
  int retval;

  // Figure out real and effective uid
  uid_t calling_uid, effective_uid, repository_uid;
  GetCredentials(&calling_uid, &effective_uid);
  if (effective_uid != 0) {
    fprintf(stderr, "Needs to run as root\n");
    return 1;
  }

  // Arguments
  if (argc != 3) {
    Usage(argv[0], stderr);
    return 1;
  }
  const string command = argv[1];
  const string fqrn = argv[2];

  // Verify if repository exists
  platform_stat64 info;
  retval = platform_lstat((string(kSpoolArea) + "/" + fqrn).c_str(), &info);
  if (retval != 0) {
    fprintf(stderr, "unknown repository: %s\n", fqrn.c_str());
    return 1;
  }
  repository_uid = info.st_uid;

  // Verify if caller uid matches
  if ((calling_uid != 0) && (calling_uid != repository_uid)) {
    fprintf(stderr, "called as %d, repository owned by %d\n", calling_uid,
            repository_uid);
    return 1;
  }

  if (command == "lock") {
    Remount("/cvmfs/" + fqrn, kRemountRdonly);
  } else if (command == "open") {
    Remount("/cvmfs/" + fqrn, kRemountRw);
  } else if (command == "kill_cvmfs") {
    KillCvmfs(fqrn);
  } else if (command == "rw_mount") {
    Mount("/cvmfs/" + fqrn);
  } else if (command == "rw_umount") {
    Umount("/cvmfs/" + fqrn);
  } else if (command == "rw_lazy_umount") {
    LazyUmount("/cvmfs/" + fqrn);
  } else if (command == "rdonly_mount") {
    Mount(string(kSpoolArea) + "/" + fqrn + "/rdonly");
  } else if (command == "rdonly_umount") {
    Umount(string(kSpoolArea) + "/" + fqrn + "/rdonly");
  } else if (command == "rdonly_lazy_umount") {
    LazyUmount(string(kSpoolArea) + "/" + fqrn + "/rdonly");
  } else if (command == "clear_scratch") {
    return DoSynchronousScratchCleanup(fqrn);
  } else if (command == "clear_scratch_async") {
    return DoAsynchronousScratchCleanup(fqrn);
  } else {
    Usage(argv[0], stderr);
    return 1;
  }

  return 0;
}
