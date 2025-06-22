/**
 * This file is part of the CernVM File System.
 *
 * Provides the LRU sub classes used for the file system client meta-data cache
 */

#ifndef CVMFS_LRU_MD_H_
#define CVMFS_LRU_MD_H_

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <stdint.h>

#include "crypto/hash.h"
#include "directory_entry.h"
#include "duplex_fuse.h"
#include "lru.h"
#include "shortstring.h"
#include "util/atomic.h"
#include "util/logging.h"
#include "util/murmur.hxx"


namespace lru {

// Hash functions
static inline uint32_t hasher_md5(const shash::Md5 &key) {
  // Don't start with the first bytes, because == is using them as well
  return static_cast<uint32_t>(
      *(reinterpret_cast<const uint32_t *>(key.digest) + 1));
}

static inline uint32_t hasher_inode(const fuse_ino_t &inode) {
  return MurmurHash2(&inode, sizeof(inode), 0x07387a4f);
}
// uint32_t hasher_md5(const shash::Md5 &key);
// uint32_t hasher_inode(const fuse_ino_t &inode);


class InodeCache : public LruCache<fuse_ino_t, catalog::DirectoryEntry> {
 public:
  explicit InodeCache(unsigned int cache_size, perf::Statistics *statistics)
      : LruCache<fuse_ino_t, catalog::DirectoryEntry>(
            cache_size, fuse_ino_t(-1), hasher_inode,
            perf::StatisticsTemplate("inode_cache", statistics)) { }

  bool Insert(const fuse_ino_t &inode, const catalog::DirectoryEntry &dirent) {
    LogCvmfs(kLogLru, kLogDebug, "insert inode --> dirent: %lu -> '%s'", inode,
             dirent.name().c_str());
    const bool result = LruCache<fuse_ino_t, catalog::DirectoryEntry>::Insert(
        inode, dirent);
    return result;
  }

  bool Lookup(const fuse_ino_t &inode, catalog::DirectoryEntry *dirent,
              bool update_lru = true) {
    const bool result = LruCache<fuse_ino_t, catalog::DirectoryEntry>::Lookup(
        inode, dirent);
    LogCvmfs(kLogLru, kLogDebug, "lookup inode --> dirent: %lu (%s)", inode,
             result ? "hit" : "miss");
    return result;
  }

  void Drop() {
    LogCvmfs(kLogLru, kLogDebug, "dropping inode cache");
    LruCache<fuse_ino_t, catalog::DirectoryEntry>::Drop();
  }
};  // InodeCache


class PathCache : public LruCache<fuse_ino_t, PathString> {
 public:
  explicit PathCache(unsigned int cache_size, perf::Statistics *statistics)
      : LruCache<fuse_ino_t, PathString>(
            cache_size, fuse_ino_t(-1), hasher_inode,
            perf::StatisticsTemplate("path_cache", statistics)) { }

  bool Insert(const fuse_ino_t &inode, const PathString &path) {
    LogCvmfs(kLogLru, kLogDebug, "insert inode --> path %lu -> '%s'", inode,
             path.c_str());
    const bool result = LruCache<fuse_ino_t, PathString>::Insert(inode, path);
    return result;
  }

  bool Lookup(const fuse_ino_t &inode, PathString *path,
              bool update_lru = true) {
    const bool found = LruCache<fuse_ino_t, PathString>::Lookup(inode, path);
    LogCvmfs(kLogLru, kLogDebug, "lookup inode --> path: %lu (%s)", inode,
             found ? "hit" : "miss");
    return found;
  }

  void Drop() {
    LogCvmfs(kLogLru, kLogDebug, "dropping path cache");
    LruCache<fuse_ino_t, PathString>::Drop();
  }
};  // PathCache


class Md5PathCache : public LruCache<shash::Md5, catalog::DirectoryEntry> {
 public:
  explicit Md5PathCache(unsigned int cache_size, perf::Statistics *statistics)
      : LruCache<shash::Md5, catalog::DirectoryEntry>(
            cache_size, shash::Md5(shash::AsciiPtr("!")), hasher_md5,
            perf::StatisticsTemplate("md5_path_cache", statistics)) {
    dirent_negative_ = catalog::DirectoryEntry(catalog::kDirentNegative);
  }

  bool Insert(const shash::Md5 &hash, const catalog::DirectoryEntry &dirent) {
    LogCvmfs(kLogLru, kLogDebug, "insert md5 --> dirent: %s -> '%s'",
             hash.ToString().c_str(), dirent.name().c_str());
    const bool result = LruCache<shash::Md5, catalog::DirectoryEntry>::Insert(
        hash, dirent);
    return result;
  }

  bool InsertNegative(const shash::Md5 &hash) {
    const bool result = Insert(hash, dirent_negative_);
    if (result)
      perf::Inc(counters_.n_insert_negative);
    return result;
  }

  bool Lookup(const shash::Md5 &hash, catalog::DirectoryEntry *dirent,
              bool update_lru = true) {
    const bool result = LruCache<shash::Md5, catalog::DirectoryEntry>::Lookup(
        hash, dirent);
    LogCvmfs(kLogLru, kLogDebug, "lookup md5 --> dirent: %s (%s)",
             hash.ToString().c_str(), result ? "hit" : "miss");
    return result;
  }

  bool Forget(const shash::Md5 &hash) {
    LogCvmfs(kLogLru, kLogDebug, "forget md5: %s", hash.ToString().c_str());
    return LruCache<shash::Md5, catalog::DirectoryEntry>::Forget(hash);
  }

  void Drop() {
    LogCvmfs(kLogLru, kLogDebug, "dropping md5path cache");
    LruCache<shash::Md5, catalog::DirectoryEntry>::Drop();
  }

 private:
  catalog::DirectoryEntry dirent_negative_;
};  // Md5PathCache

}  // namespace lru

#endif  // CVMFS_LRU_MD_H_
