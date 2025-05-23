/**
 * This file is part of the CernVM File System.
 */

#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdio>
#include <set>
#include <string>

const char *kVersion = "1.1";
const unsigned int kSymlinkBufSize = 512;

static std::string GetMntNamespace(const std::string &pid) {
  const std::string path = std::string("/proc/") + pid + "/ns/mnt";
  char symlink_buf[kSymlinkBufSize];
  ssize_t nbytes = readlink(path.c_str(), symlink_buf, kSymlinkBufSize - 1);
  if (nbytes < 0)
    return "";

  symlink_buf[nbytes] = '\0';
  return symlink_buf;
}


static std::string ExtractCvmfsRepoIfAny(const std::string &symlink) {
  char symlink_buf[kSymlinkBufSize];
  ssize_t nbytes = readlink(symlink.c_str(), symlink_buf, kSymlinkBufSize - 1);
  if (nbytes < 0)
    return "";

  symlink_buf[nbytes] = '\0';
  const std::string fd = symlink_buf;
  if (fd.length() < 7)
    return "";
  if (fd.substr(0, 7) != "/cvmfs/")
    return "";

  std::string repo;
  for (unsigned int i = 7; i < fd.length(); ++i) {
    if (fd[i] == '/')
      break;
    repo.push_back(fd[i]);
  }
  return repo;
}


static void Usage(char *progname) {
  printf("\nIdentifies processes and repositories that are used in detached\n");
  printf("mount namespaces.\n\n");
  printf("Usage: %s\n\n", progname);
}


int main(int argc, char **argv) {
  int c;
  while ((c = getopt(argc, argv, "vh")) != -1) {
    switch (c) {
      case 'v':
        printf("Version %s\n", kVersion);
        return 0;
      case 'h':
        Usage(argv[0]);
        return 0;
      default:
        Usage(argv[0]);
        return 1;
    }
  }

  std::string system_mnt_ns = GetMntNamespace("self");
  if (system_mnt_ns.empty()) {
    fprintf(stderr, "[ERR] cannot find own mount namespace\n");
    return 1;
  }
  printf("Current mount namespace is %s\n", system_mnt_ns.c_str());

  DIR *dirp = opendir("/proc");
  if (!dirp) {
    fprintf(stderr, "[ERR] cannot open /proc (%d)\n", errno);
    return 1;
  }
  dirent64 *dit;
  while ((dit = readdir64(dirp))) {
    const std::string name = dit->d_name;
    if (name == "." || name == "..")
      continue;
    const std::string path = std::string("/proc/") + name;

    struct stat64 info;
    int retval = stat64(path.c_str(), &info);
    if (retval != 0)
      continue;
    if (!S_ISDIR(info.st_mode))
      continue;

    bool is_pid_dir = true;
    for (unsigned i = 0; i < name.length(); ++i) {
      if ((name[i] < '0') || (name[i] > '9')) {
        is_pid_dir = false;
        break;
      }
    }
    if (!is_pid_dir)
      continue;

    std::string mnt_ns = GetMntNamespace(name);
    if (mnt_ns.empty())
      continue;
    if (mnt_ns == system_mnt_ns)
      continue;

    std::set<std::string> active_repositories;
    std::string repo = ExtractCvmfsRepoIfAny(path + "/cwd");
    if (!repo.empty())
      active_repositories.insert(repo);
    DIR *dirp_fd = opendir((path + "/fd").c_str());
    if (!dirp_fd)  // The process may have disappeared meanwhile
      continue;
    dirent64 *dit_fd;
    while ((dit_fd = readdir64(dirp_fd))) {
      std::string path_fd = path + "/fd/" + dit_fd->d_name;
      repo = ExtractCvmfsRepoIfAny(path_fd);
      if (repo.empty())
        continue;

      active_repositories.insert(repo);
    }
    closedir(dirp_fd);

    if (active_repositories.empty())
      continue;

    std::string proc = "(kernel)";
    char symlink_buf[kSymlinkBufSize];
    ssize_t nbytes = readlink((path + "/exe").c_str(), symlink_buf,
                              kSymlinkBufSize - 1);
    if (nbytes > 0) {
      symlink_buf[nbytes] = '\0';
      proc = symlink_buf;
    }

    std::set<std::string>::const_iterator iter = active_repositories.begin();
    for (; iter != active_repositories.end(); ++iter) {
      printf("@%s / %s (%s): %s\n", name.c_str(), mnt_ns.c_str(), proc.c_str(),
             iter->c_str());
    }
  }
  closedir(dirp);

  return 0;
}
