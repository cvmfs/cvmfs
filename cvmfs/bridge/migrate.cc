/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "bridge/migrate.h"

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include "bridge/ds_stubs.h"
#include "bridge/marshal.h"
#include "util/smalloc.h"

namespace {

template <typename T>
size_t Serialize(const T &value, void *buffer) {
  assert(false);
  return 0;
}

template <>
size_t Serialize<bool>(const bool &value, void *buffer) {
  return cvm_bridge_write_bool(&value, buffer);
}

template <>
size_t __attribute__((used))
Serialize<int32_t>(const int32_t &value, void *buffer) {
  return cvm_bridge_write_int32(&value, buffer);
}

template <>
size_t __attribute__((used))
Serialize<uint32_t>(const uint32_t &value, void *buffer) {
  return cvm_bridge_write_uint32(&value, buffer);
}

template <>
size_t __attribute__((used))
Serialize<int64_t>(const int64_t &value, void *buffer) {
  return cvm_bridge_write_int64(&value, buffer);
}

template <>
size_t __attribute__((used))
Serialize<uint64_t>(const uint64_t &value, void *buffer) {
  return cvm_bridge_write_uint64(&value, buffer);
}

template <>
size_t Serialize<cvm_bridge_blob>(const cvm_bridge_blob &value, void *buffer) {
  return cvm_bridge_write_blob(&value, buffer);
}

template<unsigned char StackSize, char Type>
size_t SerializeShortstringV1(
  const compat::ShortStringV1<StackSize, Type> &value, void *buffer)
{
  if (buffer) {
    size_t length = value.GetLength();
    Serialize<size_t>(length, buffer);
    if (length > 0) {
      memcpy(reinterpret_cast<char *>(buffer) + sizeof(size_t),
             value.GetChars(), value.GetLength());
    }
  }
  return sizeof(size_t) + value.GetLength();
}

template <>
size_t Serialize<compat::NameStringV1>(
  const compat::NameStringV1 &value, void *buffer)
{
  return SerializeShortstringV1(value, buffer);
}

template <>
size_t Serialize<compat::DentryTrackerV1::Entry>(
  const compat::DentryTrackerV1::Entry &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == NULL) ? &buffer : reinterpret_cast<void**>(&pos);

  pos += Serialize<uint64_t>(value.expiry, *where);
  pos += Serialize<uint64_t>(value.inode_parent, *where);
  pos += Serialize<compat::NameStringV1>(value.name, *where);
  return pos - base;
}

template <class Item>
size_t SerializeBigQueueV1(const compat::BigQueueV1<Item> &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  size_t size = value.size();
  pos += Serialize<size_t>(size, *where);
  if (value.IsEmpty())
    return pos - base;

  assert(size > 0);
  Item *item_ptr;
  bool retval = value.Peek(&item_ptr);
  assert(retval);
  for (size_t i = 0; i < size; ++i) {
    pos += Serialize<Item>(*(item_ptr + i), *where);
  }
  return pos - base;
}

template <>
size_t Serialize<compat::InodeGenerationInfoV1>(
  const compat::InodeGenerationInfoV1 &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  pos += Serialize<unsigned int>(value.version, *where);
  pos += Serialize<uint64_t>(value.initial_revision, *where);
  pos += Serialize<uint32_t>(value.incarnation, *where);
  if (value.version < 2) {
    return pos - base;
  }

  pos += Serialize<uint32_t>(value.overflow_counter, *where);
  pos += Serialize<uint64_t>(value.inode_generation, *where);
  return pos - base;
}

template <>
size_t Serialize<compat::FuseStateV1>(
  const compat::FuseStateV1 &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  pos += Serialize<unsigned int>(value.version, *where);
  pos += Serialize<bool>(value.cache_symlinks, *where);
  pos += Serialize<bool>(value.has_dentry_expire, *where);
  return pos - base;
}

template <>
size_t Serialize<compat::DirectoryListingV1>(
  const compat::DirectoryListingV1 &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  cvm_bridge_blob blob;
  blob.buffer = value.buffer;
  blob.size = value.size;
  blob.is_mmapd = (value.capacity == 0);
  pos += Serialize<cvm_bridge_blob>(blob, *where);
  pos += Serialize<size_t>(value.size, *where);
  pos += Serialize<size_t>(value.capacity, *where);
  return pos - base;
}

template <>
size_t Serialize<compat::DirectoryHandlesV1>(
  const compat::DirectoryHandlesV1 &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  size_t size = value.size();
  pos += Serialize<size_t>(size, *where);
  for (compat::DirectoryHandlesV1::const_iterator it = value.begin(),
       itEnd = value.end(); it != itEnd; ++it)
  {
    pos += Serialize<uint64_t>(it->first, *where);
    pos += Serialize<compat::DirectoryListingV1>(it->second, *where);
  }
  return pos - base;
}

template <>
size_t Serialize<compat::DentryTrackerV1>(
  const compat::DentryTrackerV1 &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  value.Lock();
  pos += Serialize<unsigned>(value.version_, *where);
  pos += Serialize<int64_t>(value.statistics_.num_insert, *where);
  pos += Serialize<int64_t>(value.statistics_.num_remove, *where);
  pos += Serialize<int64_t>(value.statistics_.num_prune, *where);
  pos += Serialize<bool>(value.is_active_, *where);
  pos += SerializeBigQueueV1<compat::DentryTrackerV1::Entry>(
    value.entries_, *where);
  value.Unlock();

  return pos - base;
}

}  // anonymous namespace

void *cvm_bridge_migrate_directory_handles_v1v2s(void *v1) {
  compat::DirectoryHandlesV1 *h =
    reinterpret_cast<compat::DirectoryHandlesV1 *>(v1);

  size_t nbytes = Serialize<compat::DirectoryHandlesV1>(*h, NULL);
  void *v2s = smalloc(nbytes);
  Serialize<compat::DirectoryHandlesV1>(*h, v2s);
  return v2s;
}

void cvm_bridge_free_directory_handles_v1(void *v1) {
  compat::DirectoryHandlesV1 *h =
    reinterpret_cast<compat::DirectoryHandlesV1 *>(v1);
  for (compat::DirectoryHandlesV1::const_iterator i = h->begin(),
       iEnd = h->end(); i != iEnd; ++i)
  {
    if (i->second.buffer == NULL)
      continue;
    if (i->second.capacity == 0) {
      smunmap(i->second.buffer);
    } else {
      free(i->second.buffer);
    }
  }
  delete h;
}

void *cvm_bridge_migrate_nfiles_ctr_v1v2s(void *v1) {
  uint32_t *ctr = reinterpret_cast<uint32_t *>(v1);
  void *v2s = smalloc(4);
  Serialize<uint32_t>(*ctr, v2s);
  return v2s;
}

void cvm_bridge_free_nfiles_ctr_v1(void *v1) {
  delete reinterpret_cast<uint32_t *>(v1);
}

void *cvm_bridge_migrate_inode_generation_v1v2s(void *v1) {
  compat::InodeGenerationInfoV1 *info =
    reinterpret_cast<compat::InodeGenerationInfoV1 *>(v1);

  size_t nbytes = Serialize<compat::InodeGenerationInfoV1>(*info, NULL);

  void *v2s = smalloc(nbytes);
  Serialize<compat::InodeGenerationInfoV1>(*info, v2s);
  return v2s;
}

void cvm_bridge_free_inode_generation_v1(void *v1) {
  delete reinterpret_cast<compat::InodeGenerationInfoV1 *>(v1);
}

void *cvm_bridge_migrate_fuse_state_v1v2s(void *v1) {
  compat::FuseStateV1 *info = reinterpret_cast<compat::FuseStateV1 *>(v1);

  size_t nbytes = Serialize<compat::FuseStateV1>(*info, NULL);
  void *v2s = smalloc(nbytes);
  Serialize<compat::FuseStateV1>(*info, v2s);
  return v2s;
}

void cvm_bridge_free_fuse_state_v1(void *v1) {
  delete reinterpret_cast<compat::FuseStateV1 *>(v1);
}

void *cvm_bridge_migrate_dentry_tracker_v1v2s(void *v1) {
  compat::DentryTrackerV1 *info =
    reinterpret_cast<compat::DentryTrackerV1 *>(v1);

  size_t nbytes = Serialize<compat::DentryTrackerV1>(*info, NULL);
  void *v2s = smalloc(nbytes);
  Serialize<compat::DentryTrackerV1>(*info, v2s);
  return v2s;
}

void cvm_bridge_free_dentry_tracker_v1(void *v1) {
  delete reinterpret_cast<compat::DentryTrackerV1 *>(v1);
}
