/**
 * This file is part of the CernVM File System.
 */

#include <sys/xattr.h>  // NOLINT

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <stdio.h>
#include <stdlib.h>

static int Exists(char *path) {
  struct stat info;
  return stat(path, &info) == 0;
}

int main(int argc, char **argv) {
  if (argc < 2) {
    (void)fprintf(stderr, "Usage: %s /cvmfs/<path>\n", argv[0]);
    return 1;
  }

  char *path = argv[1];
  const int fd = open(path, O_RDONLY);
  if (fd < 0) {
    (void)fprintf(stderr, "cannot open %s\n", path);
    return 1;
  }

  while (!Exists("stop_topen")) {
    sleep(1);
  }

  printf("*** END %s %s\n", argv[0], path);

  return 0;
}
