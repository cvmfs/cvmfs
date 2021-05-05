/**
 * C utility to test the behavior of an open file whose content changes
 */

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <stdio.h>


unsigned int Checksum(unsigned char *data, int count)
{
  unsigned int result = 0;
  for (int i = 0; i < count; ++i)
    result += data[i];
  return result;
}

void Usage(char *progname) {
  printf("%s <path>\n", progname);
}

int main(int argc, char **argv) {
  if (argc < 2)
    Usage(argv[0]);
  char *path = argv[1];

  int fd = open(path, O_RDONLY);
  if (fd < 0) {
    fprintf(stderr, "Cannot open %s: %d\n", path, errno);
    return 1;
  }

  printf("opened file %s on %d\n", path, fd);

  unsigned int checksum = 0;
  unsigned int trial = 0;
  while (1) {
    unsigned char buf[4096];
    ssize_t nbytes = read(fd, buf, 4096);
    checksum += Checksum(buf, nbytes);
    if (nbytes < 4096) {
      printf("trial %u: checksum %u\n", trial, checksum);
      trial++;
      checksum = 0;
      lseek(fd, 0, SEEK_SET);
      sleep(2);
    }
  }

  return 0;
}