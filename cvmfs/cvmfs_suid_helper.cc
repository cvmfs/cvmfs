/**
 * This file is part of the CernVM File System.
 *
 * Runs mount/umount commands and removes scratch space on behalf of a
 * repository owner.  Server-only utility.
 *
 * This binary does not use the cvmfs infrastructure code to stay lean.
 */

#include <sys/xattr.h>  // NOLINT

#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "platform.h"
#include "sanitizer.h"

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

static void ExecAsRoot(const char *binary,
                       const char *arg1, const char *arg2, const char *arg3)
{
  char *argv[] = { strdup(binary),
                   arg1 ? strdup(arg1) : NULL,
                   arg2 ? strdup(arg2) : NULL,
                   arg3 ? strdup(arg3) : NULL,
                   NULL };
  char *environ[] = { NULL };

  int retval = setuid(0);
  if (retval != 0) {
    fprintf(stderr, "failed to gain root privileges (%d)\n", errno);
    exit(1);
  }

  execve(binary, argv, environ);
  fprintf(stderr, "failed to run %s... (%d)\n", binary, errno);
  exit(1);
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

  ExecAsRoot("/bin/mount", "-o", remount_option.c_str(), path.c_str());
}

static void Mount(const string &path) {
  ExecAsRoot("/bin/mount", path.c_str(), NULL, NULL);
}

static void Umount(const string &path) {
  ExecAsRoot("/bin/umount", path.c_str(), NULL, NULL);
}

static void LazyUmount(const string &path) {
  ExecAsRoot("/bin/umount", "-l", path.c_str(), NULL);
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
  ExecAsRoot("/bin/kill", "-9", pid.c_str(), NULL);
}

static bool ClearWorkingDir() {
  int retval;
  DIR *dirp = opendir(".");
  if (dirp == NULL)
    return false;
  platform_dirent64 *dirent;
  while ((dirent = platform_readdir(dirp)) != NULL) {
    if ((strcmp(dirent->d_name, ".") == 0) ||
        (strcmp(dirent->d_name, "..") == 0))
    {
      continue;
    }

    platform_stat64 info;
    retval = platform_lstat(dirent->d_name, &info);
    if (retval != 0) {
      closedir(dirp);
      return false;
    }

    if (S_ISDIR(info.st_mode)) {
      // Recursion
      retval = chdir(dirent->d_name);
      if (retval != 0) {
        closedir(dirp);
        return false;
      }
      retval = ClearWorkingDir();
      if (!retval) {
        closedir(dirp);
        return false;
      }
      retval = chdir("..");
      if (retval != 0) {
        closedir(dirp);
        return false;
      }
      retval = rmdir(dirent->d_name);
      if (retval != 0) {
        closedir(dirp);
        return false;
      }
    } else {
      retval = unlink(dirent->d_name);
      if (retval != 0) {
        closedir(dirp);
        return false;
      }
    }
  }
  closedir(dirp);
  return true;
}

static void Usage(const string &exe, FILE *output) {
  fprintf(output,
    "Usage: %s lock|open|rw_mount|rw_umount|rdonly_mount|rdonly_umount|"
      "clear_scratch|kill_cvmfs <fqrn>\n"
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
    fprintf(stderr, "called as %d, repository owned by %d\n",
            calling_uid, repository_uid);
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
    const string scratch_area = string(kSpoolArea) + "/" + fqrn + "/scratch";
    retval = chdir(scratch_area.c_str());
    if (retval != 0) {
      fprintf(stderr, "failed to chdir to %s\n", scratch_area.c_str());
      return 1;
    }
    retval = ClearWorkingDir();
    if (!retval) {
      fprintf(stderr, "failed to clear %s\n", scratch_area.c_str());
      return 1;
    }
  } else {
    Usage(argv[0], stderr);
    return 1;
  }

  return 0;
}
