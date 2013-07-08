
/**
 * This is a small test utility which allows us to test the CRC32 checksums
 * independently of the rest of CVMFS.
 */

#include "checksum.h"
#include "hash.h"

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>

std::string cache_path;

std::string
__attribute__((weak))
GetPathInCache(const hash::Any &hash) {
  return cache_path + "/" + hash.MakePath(1, 2);
}

// Note we made READ_BUFFER unaligned with the size of the checksum blocks
// Hopefully this will help prevent against any alignment assumptions.
#define READ_BUFFER 4095

int main(int argc, char *argv[]) {
  if (argc != 3) {
    fprintf(stderr, "Usage: %s cvmfs_cache_path hash\n", argv[0]);
    return 1;
  }
  cache_path = argv[1];

  hash::Any hash(hash::kSha1, hash::HexPtr(argv[2]));

  ChecksumFileWriter writer(hash, true);

  std::string path = GetPathInCache(hash);
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    fprintf(stderr, "Failed to open file %s for checksumming (errno=%d, %s)\n",
            path.c_str(), errno, strerror(errno));
    return errno;
  }
  int result;
  unsigned char buffer[READ_BUFFER];
  while ((result = read(fd, buffer, READ_BUFFER)) > 0) {
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
  printf("Calculated CRC32 checksum: %u\n", crc32);
  return 0;
}
