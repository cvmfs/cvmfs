/**
 * This file is part of the CernVM File System.
 */

#include <stdint.h>

#include <google/dense_hash_map>

#include "util/murmur.hxx"

namespace compat {

template <typename hashed_type>
struct hash_murmur {
  size_t operator() (const hashed_type key) const {
#ifdef __x86_64__
    return MurmurHash64A(&key, sizeof(key), 0x9ce603115bba659bLLU);
#else
    return MurmurHash2(&key, sizeof(key), 0x07387a4f);
#endif
  }
};

struct DirectoryListingV1 {
  char *buffer;  /**< Filled by fuse_add_direntry */
  size_t size;
  size_t capacity;
};

class DirectoryHandlesV1 : public google::dense_hash_map<uint64_t,
                                                         DirectoryListingV1,
                                                         hash_murmur<uint64_t> >
{};

struct InodeGenerationInfoV1 {
  unsigned version;
  uint64_t initial_revision;
  uint32_t incarnation;
  uint32_t overflow_counter;  // not used any more
  uint64_t inode_generation;
};

struct FuseStateV1 {
  unsigned version;
  bool cache_symlinks;
  bool has_dentry_expire;
};

} // namespace compat
