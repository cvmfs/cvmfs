
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

int verify(int fd, size_t size) {
  checksum::ChecksumFileReader reader(fd, size);

  int result;
  unsigned char buffer[READ_BUFFER];
  errno = 0;
  size_t to_read = size;
  size_t to_read_chunk = to_read > READ_BUFFER ? READ_BUFFER : to_read;
  off_t off = 0;
  while (to_read && (((result = read(fd, buffer, to_read_chunk)) > 0) ||
                     (errno == EINTR))) {
    if (errno) {continue;}
    int result2 = reader.verify(buffer, result, off);
    if (result2 < 0) {
      fprintf(stderr, "Failed to verify file: %s (errno=%d)\n",
             strerror(-result2), -result2);
      return -result2;
    }
    to_read -= result;
    off += result;
    to_read_chunk = to_read > READ_BUFFER ? READ_BUFFER : to_read;
  }
  if (result < 0) {
    fprintf(stderr, "Failed to read file for checksumming (errno=%d, %s)\n",
            errno, strerror(errno));
    return errno;
  }
  close(fd);
  return 0;
}

int main(int argc, char *argv[]) {
  if ((argc != 3) && (argc != 4)) {
    fprintf(stderr, "Usage: %s [-v] cvmfs_cache_path /cvmfs/repo/path\n", argv[0]);
    return 1;
  }

  unsigned next_arg = 1;
  bool do_verify = false;
  if (!strcmp(argv[1], "-v")) {
    next_arg++;
    do_verify = true;
  } else if (argc == 4) {
    fprintf(stderr, "Usage: %s [-v] cvmfs_cache_path /cvmfs/repo/path\n", argv[0]);
    return 1;
  }
  std::string cache_path = argv[next_arg++];
  std::string file_path = argv[next_arg++];

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

  if (do_verify) {
    return verify(fd, st.st_size);
  }

  checksum::ChecksumFileWriter writer(fd, st.st_size, false);

  int result;
  unsigned char buffer[READ_BUFFER];
  errno = 0;
  size_t to_read = st.st_size;
  size_t to_read_chunk = to_read > READ_BUFFER ? READ_BUFFER : to_read;
  while (to_read && (((result = read(fd, buffer, to_read_chunk)) > 0) || (errno == EINTR))) {
    if (errno) {continue;}
    writer.stream(buffer, result);
    to_read -= result;
    to_read_chunk = to_read > READ_BUFFER ? READ_BUFFER : to_read;
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
