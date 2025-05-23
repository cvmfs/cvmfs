/**
 * This file is part of the CernVM File System.
 *
 * Utility to analyze the size of subtrees of a single catalog. The utility
 * subtree traversal stops at .cvmfscatalog files. The utility can be used
 * by repository administrators to identify run-away subtrees that are missing
 * a .cvmfscatalog marker.
 */

#include <dirent.h>
#include <sys/stat.h>

#include <cstdio>
#include <string>


void CountSubtree(const std::string &path, unsigned int *count) {
  struct stat info;
  int retval = lstat(path.c_str(), &info);
  if (retval != 0) {
    fprintf(stderr, "cannot access %s\n", path.c_str());
    return;
  }
  if (!S_ISDIR(info.st_mode))
    return;

  DIR *dirp = opendir(path.c_str());
  if (!dirp) {
    fprintf(stderr, "cannot open %s\n", path.c_str());
    return;
  }

  while (struct dirent *d = readdir(dirp)) {
    std::string name = d->d_name;
    if (name == "." || name == "..")
      continue;
    if (name == ".cvmfscatalog") {
      closedir(dirp);
      return;
    }
  }

  rewinddir(dirp);

  while (struct dirent *d = readdir(dirp)) {
    std::string name = d->d_name;
    if (name == "." || name == "..")
      continue;
    (*count)++;
    CountSubtree(path + "/" + name, count);
  }
  closedir(dirp);
}


int main(int argc, char **argv) {
  std::string base = ".";
  if (argc > 1)
    base = argv[1];

  printf("Calculating CernVM-FS subtree size at %s\n", base.c_str());

  DIR *dirp = opendir(base.c_str());
  if (!dirp) {
    fprintf(stderr, "cannot open %s\n", base.c_str());
    return 1;
  }

  int self = 0;
  while (struct dirent *d = readdir(dirp)) {
    std::string name = d->d_name;
    if (name == "." || name == "..")
      continue;
    unsigned int count = 0;
    std::string path = base + "/" + name;
    CountSubtree(path, &count);
    if (count)
      printf("%10u   %s\n", count, path.c_str());
    self++;
  }
  printf("%10u   .\n", self);

  closedir(dirp);
  return 0;
}
