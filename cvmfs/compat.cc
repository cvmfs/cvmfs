/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "compat.h"

#include <openssl/md5.h>

#include <cstdlib>
#include <cstring>

using namespace std;  // NOLINT

namespace compat {

namespace shash_v1 {

const char *kSuffixes[] = {"", "", "-rmd160", ""};

Md5::Md5(const char *chars, const unsigned length) {
  algorithm = kMd5;

  MD5_CTX md5_state;
  MD5_Init(&md5_state);
  MD5_Update(&md5_state, reinterpret_cast<const unsigned char *>(chars),
             length);
  MD5_Final(digest, &md5_state);
}

void MigrateAny(const Any *old_hash, shash::Any *new_hash) {
  memcpy(new_hash->digest, old_hash->digest, kDigestSizes[kAny]);
  new_hash->algorithm = shash::Algorithms(old_hash->algorithm);
  new_hash->suffix = shash::kSuffixNone;
}

}  // namespace shash_v1


//------------------------------------------------------------------------------


namespace shash_v2 {

const char *kSuffixes[] = {"", "", "-rmd160", ""};

void MigrateAny(const Any *old_hash, shash::Any *new_hash) {
  memcpy(new_hash->digest, old_hash->digest, kDigestSizes[kAny]);
  new_hash->algorithm = shash::Algorithms(old_hash->algorithm);
  new_hash->suffix = old_hash->suffix;
}

}  // namespace shash_v2


//------------------------------------------------------------------------------


namespace inode_tracker {

bool InodeContainer::ConstructPath(const uint64_t inode, PathString *path) {
  InodeMap::const_iterator needle = map_.find(inode);
  if (needle == map_.end())
    return false;

  if (needle->second.name.IsEmpty())
    return true;

  bool retval = ConstructPath(needle->second.parent_inode, path);
  path->Append("/", 1);
  path->Append(needle->second.name.GetChars(),
               needle->second.name.GetLength());
  assert(retval);
  return retval;
}


InodeTracker::~InodeTracker() {
  pthread_mutex_destroy(lock_);
  free(lock_);
}

void Migrate(InodeTracker *old_tracker, glue::InodeTracker *new_tracker) {
  InodeContainer::InodeMap::const_iterator i, iEnd;
  i = old_tracker->inode2path_.map_.begin();
  iEnd = old_tracker->inode2path_.map_.end();
  for (; i != iEnd; ++i) {
    uint64_t inode = i->first;
    uint32_t references = i->second.references;
    PathString path;
    old_tracker->inode2path_.ConstructPath(inode, &path);
    new_tracker->VfsGetBy(inode, references, path);
  }
}

}  // namespace inode_tracker


//------------------------------------------------------------------------------


namespace inode_tracker_v2 {

static uint32_t hasher_md5(const shash_v1::Md5 &key) {
  return (uint32_t) *((uint32_t *)key.digest + 1);  // NOLINT
}

static uint32_t hasher_inode(const uint64_t &inode) {
  return MurmurHash2(&inode, sizeof(inode), 0x07387a4f);
}

void Migrate(InodeTracker *old_tracker, glue::InodeTracker *new_tracker) {
  old_tracker->inode_map_.map_.hasher_ = hasher_inode;
  old_tracker->path_map_.map_.hasher_ = hasher_md5;

  SmallHashDynamic<uint64_t, uint32_t> *old_inodes =
    &old_tracker->inode_references_.map_;
  for (unsigned i = 0; i < old_inodes->capacity_; ++i) {
    const uint64_t inode = old_inodes->keys_[i];
    if (inode == 0) continue;

    const uint32_t references = old_inodes->values_[i];
    PathString path;
    bool retval = old_tracker->FindPath(inode, &path);
    assert(retval);
    new_tracker->VfsGetBy(inode, references, path);
  }
}

}  // namespace inode_tracker_v2


//------------------------------------------------------------------------------


namespace inode_tracker_v3 {

static uint32_t hasher_md5(const shash_v1::Md5 &key) {
  return (uint32_t) *((uint32_t *)key.digest + 1);  // NOLINT
}

static uint32_t hasher_inode(const uint64_t &inode) {
  return MurmurHash2(&inode, sizeof(inode), 0x07387a4f);
}

void Migrate(InodeTracker *old_tracker, glue::InodeTracker *new_tracker) {
  old_tracker->inode_map_.map_.SetHasher(hasher_inode);
  old_tracker->path_map_.map_.SetHasher(hasher_md5);
  old_tracker->path_map_.path_store_.map_.SetHasher(hasher_md5);

  SmallHashDynamic<uint64_t, uint32_t> *old_inodes =
    &old_tracker->inode_references_.map_;
  for (unsigned i = 0; i < old_inodes->capacity(); ++i) {
    const uint64_t inode = old_inodes->keys()[i];
    if (inode == 0) continue;

    const uint32_t references = old_inodes->values()[i];
    PathString path;
    bool retval = old_tracker->FindPath(inode, &path);
    assert(retval);
    new_tracker->VfsGetBy(inode, references, path);
  }
}

}  // namespace inode_tracker_v3


//------------------------------------------------------------------------------


namespace chunk_tables {

ChunkTables::~ChunkTables() {
  pthread_mutex_destroy(lock);
  free(lock);
  for (unsigned i = 0; i < kNumHandleLocks; ++i) {
    pthread_mutex_destroy(handle_locks.At(i));
    free(handle_locks.At(i));
  }
}

void Migrate(ChunkTables *old_tables, ::ChunkTables *new_tables) {
  new_tables->next_handle = old_tables->next_handle;
  new_tables->handle2fd = old_tables->handle2fd;
  new_tables->inode2references = old_tables->inode2references;

  SmallHashDynamic<uint64_t, FileChunkReflist> *old_inode2chunks =
    &old_tables->inode2chunks;
  for (unsigned keyno = 0; keyno < old_inode2chunks->capacity(); ++keyno) {
    const uint64_t inode = old_inode2chunks->keys()[keyno];
    if (inode == 0) continue;

    FileChunkReflist *old_reflist = &old_inode2chunks->values()[keyno];
    BigVector<FileChunk> *old_list = old_reflist->list;
    FileChunkList *new_list = new FileChunkList();
    for (unsigned i = 0; i < old_list->size(); ++i) {
      const FileChunk *old_chunk = old_list->AtPtr(i);
      off_t offset = old_chunk->offset();
      size_t size = old_chunk->size();
      shash::Any hash;
      shash_v1::MigrateAny(&old_chunk->content_hash_, &hash);
      new_list->PushBack(::FileChunk(hash, offset, size));
    }
    delete old_list;
    ::FileChunkReflist new_reflist(new_list, old_reflist->path,
                                   zlib::kZlibDefault, false);
    new_tables->inode2chunks.Insert(inode, new_reflist);
  }
}

}  // namespace chunk_tables


//------------------------------------------------------------------------------


namespace chunk_tables_v2 {

ChunkTables::~ChunkTables() {
  pthread_mutex_destroy(lock);
  free(lock);
  for (unsigned i = 0; i < kNumHandleLocks; ++i) {
    pthread_mutex_destroy(handle_locks.At(i));
    free(handle_locks.At(i));
  }
}

void Migrate(ChunkTables *old_tables, ::ChunkTables *new_tables) {
  new_tables->next_handle = old_tables->next_handle;
  new_tables->handle2fd = old_tables->handle2fd;
  new_tables->inode2references = old_tables->inode2references;

  SmallHashDynamic<uint64_t, FileChunkReflist> *old_inode2chunks =
    &old_tables->inode2chunks;
  for (unsigned keyno = 0; keyno < old_inode2chunks->capacity(); ++keyno) {
    const uint64_t inode = old_inode2chunks->keys()[keyno];
    if (inode == 0) continue;

    FileChunkReflist *old_reflist = &old_inode2chunks->values()[keyno];
    BigVector<FileChunk> *old_list = old_reflist->list;
    FileChunkList *new_list = new FileChunkList();
    for (unsigned i = 0; i < old_list->size(); ++i) {
      const FileChunk *old_chunk = old_list->AtPtr(i);
      off_t offset = old_chunk->offset();
      size_t size = old_chunk->size();
      shash::Any hash;
      shash_v2::MigrateAny(&old_chunk->content_hash_, &hash);
      new_list->PushBack(::FileChunk(hash, offset, size));
    }
    delete old_list;
    ::FileChunkReflist new_reflist(new_list, old_reflist->path,
                                   zlib::kZlibDefault, false);
    new_tables->inode2chunks.Insert(inode, new_reflist);
  }
}

}  // namespace chunk_tables_v2

}  // namespace compat
