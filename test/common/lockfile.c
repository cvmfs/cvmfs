/**
 * This file is part of the CernVM File System.
 *
 * Used in integration tests as a wrapper for flock()
 */

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <errno.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
  if (argc < 2) {
    printf("path missing\n");
    return 1;
  }
  char *path = argv[1];
  printf("locking %s\n", path);
  int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
  if (fd < 0) {
    printf("cannot open %s\n", path);
    return 1;
  }

  if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
    close(fd);
    if (errno == EWOULDBLOCK) {
      printf("already locked: %s\n", path);
      return 2;
    }
    printf("cannot lock %s\n", path);
    return 1;
  }

  pid_t pid;
  int statloc;
  if ((pid = fork()) == 0) {
    if ((pid = fork()) == 0) {
      printf("successfully locked %s, PID %d\n", path, getpid());
      char buffer[32];
      snprintf(buffer, sizeof(buffer), "%d\n", getpid());
      int len = strlen(buffer);
      write(fd, buffer, len);
      sleep(100000);
      return 0;
    } else {
      struct stat info;
      do {
        int retval = stat(path, &info);
        if (retval != 0)
          _exit(0);
      } while (info.st_size == 0);
      _exit(0);
    }
  }

  waitpid(pid, &statloc, 0);
  return 0;
}
