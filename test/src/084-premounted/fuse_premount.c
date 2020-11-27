/*
 * Does a fuse mount first and then loads whatever fuse module
 *  is loaded by the following command, as long as that command starts a
 *  fuse filesystem and accepts the mountpoint as the last parameter.
 *  Replaces the mountpoint with /dev/fd/NN where NN is a file descriptor
 *  returned from opening /dev/fuse.  Requires libfuse >= 3.3.0.
 * To compile and make setuid as root:
 *  # cc -o fuse-premount fuse-premunt.c
 *  # chmod 4755 fuse-premount
 * Written by Dave Dykstra 29 March 2019
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

int main(int argc, char **argv) {
  char *mnt;
  char tmp[128];
  struct stat stbuf;
  int fd;
  int res;
  int i;

  printf("UID %d %d\n", geteuid(), getuid());

  if (geteuid() != 0 || getuid() == 0) {
    fprintf(stderr, "Not running as setuid root\n", argv[0]);
    return 1;
  }

  mnt = argv[argc-1];
  if (access(mnt, W_OK) != 0) {
    fprintf(stderr ,"%s: no write access mountpoint %s: %s\n",
                argv[0], mnt, strerror(errno));
    return 1;
  }

  /* based on https://github.com/libfuse/libfuse/blob/fuse-3.3.0/lib/mount.c */
  res = stat(mnt, &stbuf);
  if (res == -1) {
    fprintf(stderr ,"%s: failed to access mountpoint %s: %s\n",
                argv[0], mnt, strerror(errno));
    return 1;
  }

  fd = open("/dev/fuse", O_RDWR);
  if (fd == -1) {
    fprintf(stderr, "%s: failed to open /dev/fuse: %s\n",
                argv[0], strerror(errno));
    return 1;
  }

  snprintf(tmp, sizeof(tmp), "fd=%i,rootmode=%o,user_id=%u,group_id=%u",
        fd, stbuf.st_mode & S_IFMT, getuid(), getgid());

  res = mount("/dev/fuse", mnt, "fuse", 0, tmp);
  if (res == -1) {
    fprintf(stderr ,"%s: failed to mount -o %s /dev/fuse %s: %s\n",
                argv[0], tmp, mnt, strerror(errno));
    return 1;
  }

  if (setgid(getgid()) != 0) {
    fprintf(stderr ,"%s: failed to drop gid privilege: %s\n",
                argv[0], strerror(errno));
    return 1;
  }
  if (setuid(getuid()) != 0) {
    fprintf(stderr ,"%s: failed to drop gid privilege: %s\n",
                argv[0], strerror(errno));
    return 1;
  }
  if (setuid(0) == 0 || seteuid(0) == 0) {
    fprintf(stderr ,"%s: did not successfully drop root privilege\n", argv[0]);
    return 1;
  }

  snprintf(tmp, sizeof(tmp), "/dev/fd/%d", fd);

  argv[argc-1] = tmp;

  printf("+ ");
  for (i=1; i < argc; i++) printf("%s ", argv[i]);
  printf("\n");
  fflush(stdout);

  execvp(argv[1],&argv[1]);
  perror("failed to execvp");
  return(1);
}
