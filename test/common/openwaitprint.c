/**
 * This file is part of the CernVM File System.
 * Helper for cache manager integration tests, e.g. 690 (streaming cache manager)
 */

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

int fd = -1;

// The test shell opens this descriptor before launching the helper so that
// reporting an open failure does not require another open.
static const int kStatusFd = 3;

static int ReportStatus(char state) {
  ssize_t written;
  do {
    written = write(kStatusFd, &state, 1);
  } while (written < 0 && errno == EINTR);
  return written == 1;
}

static int ReturnError() {
  if (fd >= 0)
    close(fd);
  return 1;
}

int main(int argc, char **argv) {
  if (argc < 2)
    return 1;

  const int notify = argc == 3 && strcmp(argv[2], "--notify") == 0;
  int sigwait = SIGUSR1;

  sigset_t sigset;
  int retval = sigemptyset(&sigset);
  if (retval != 0)
    return ReturnError();
  retval = sigaddset(&sigset, sigwait);
  if (retval != 0)
    return ReturnError();
  retval = pthread_sigmask(SIG_BLOCK, &sigset, NULL);
  if (retval != 0)
    return ReturnError();

  fd = open(argv[1], O_RDONLY);
  if (notify) {
    const int reported = ReportStatus(fd >= 0 ? 'O' : 'F');
    close(kStatusFd);
    if (!reported)
      return ReturnError();
  }
  if (fd < 0)
    return ReturnError();

  do {
    retval = sigwaitinfo(&sigset, NULL);
  } while ((retval != sigwait) && (errno == EINTR));
  if (retval != sigwait)
    return ReturnError();

  int c;
  ssize_t nbytes;
  do {
    nbytes = read(fd, &c, 1);
    if (nbytes == 0)
      break;
    printf("%c", c);
  } while (nbytes == 1);
  if (nbytes < 0)
    return ReturnError();

  close(fd);
  return 0;
}
