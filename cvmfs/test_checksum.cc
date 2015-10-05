
/**
 * This is a small test utility which allows us to test the CRC32 checksums
 * independently of the rest of CVMFS.
 */

#include "checksum.h"
#include "hash.h"
#include "platform.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>

// Note we made READ_BUFFER unaligned with the size of the checksum blocks
// Hopefully this will help prevent against any alignment assumptions.
#define READ_BUFFER 4095

int main(int argc, char *argv[]) {
  if (argc != 3) {
    fprintf(stderr, "Usage: %s cvmfs_cache_path /cvmfs/repo/path\n", argv[0]);
    return 1;
  }
  std::string cache_path = argv[1];
  std::string file_path = argv[2];

  std::string hash_value;
  if (!platform_getxattr(file_path.c_str(), "user.hash", &hash_value))
  {
    fprintf(stderr, "Failed to determine file's (%s) hash: %s (errno=%d)\n",
            file_path.c_str(), strerror(errno), errno);
    return 1;
  }
  shash::Any hash(shash::kSha1, shash::HexPtr(hash_value));

  std::string path = cache_path + "/" + hash.MakePathWithoutSuffix();
  int fd = open(path.c_str(), O_RDWR);
  if (fd < 0) {
    fprintf(stderr, "Failed to open file %s for checksumming (errno=%d, %s)\n",
            path.c_str(), errno, strerror(errno));
    return errno;
  }
  struct stat st;
  if (-1 == stat(file_path.c_str(), &st)) {
    fprintf(stderr, "Failed to stat cache file (%s): %s (errno=%d).\n",
            file_path.c_str(), strerror(errno), errno);
    return errno;
  }

  checksum::ChecksumFileWriter writer(fd, st.st_size, false);

  int result;
  unsigned char buffer[READ_BUFFER];
  errno = 0;
  while (((result = read(fd, buffer, READ_BUFFER)) > 0) && (errno == EINTR)) {
    if (errno) {continue;}
    writer.stream(buffer, result);
  }
  if (result < 0) {
    fprintf(stderr, "Failed to read file %s for checksumming (errno=%d, %s)\n",
            path.c_str(), errno, strerror(errno));
    return errno;
  }
  uint32_t crc32;
  if (writer.finalize(crc32)) {
    fprintf(stderr, "Checksum calculation failed for %s\n", path.c_str());
    return 1;
  }
  close(fd);
  printf("Calculated CRC32 checksum: %u\n", crc32);
  return 0;
}

