/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "state.h"

#include "bridge/marshal.h"
#include "fuse_directory_handle.h"
#include "fuse_inode_gen.h"
#include "fuse_state.h"
#include "glue_buffer.h"

#include <cassert>

namespace {

template <typename T>
size_t Serialize(const T &value, void *buffer) {
  assert(false);
  return 0;
}

template <typename T>
size_t Deserialize(const void *buffer, T *value) {
  assert(false);
  return 0;
}

template <>
size_t Serialize<bool>(const bool &value, void *buffer) {
  return cvm_bridge_write_bool(&value, buffer);
}

template <>
size_t Deserialize<bool>(const void *buffer, bool *value) {
  return cvm_bridge_read_bool(buffer, value);
}

template <>
size_t __attribute__((used))
Serialize<int32_t>(const int32_t &value, void *buffer) {
  return cvm_bridge_write_int32(&value, buffer);
}

template <>
size_t __attribute__((used))
Deserialize<int32_t>(const void *buffer, int32_t *value) {
  return cvm_bridge_read_int32(buffer, value);
}

template <>
size_t __attribute__((used))
Serialize<uint32_t>(const uint32_t &value, void *buffer) {
  return cvm_bridge_write_uint32(&value, buffer);
}

template <>
size_t __attribute__((used))
Deserialize<uint32_t>(const void *buffer, uint32_t *value) {
  return cvm_bridge_read_uint32(buffer, value);
}

template <>
size_t __attribute__((used))
Serialize<int64_t>(const int64_t &value, void *buffer) {
  return cvm_bridge_write_int64(&value, buffer);
}

template <>
size_t __attribute__((used))
Deserialize<int64_t>(const void *buffer, int64_t *value) {
  return cvm_bridge_read_int64(buffer, value);
}

template <>
size_t __attribute__((used))
Serialize<uint64_t>(const uint64_t &value, void *buffer) {
  return cvm_bridge_write_uint64(&value, buffer);
}

template <>
size_t __attribute__((used))
Deserialize<uint64_t>(const void *buffer, uint64_t *value) {
  return cvm_bridge_read_uint64(buffer, value);
}

template <>
size_t Serialize<cvm_bridge_blob>(const cvm_bridge_blob &value, void *buffer) {
  return cvm_bridge_write_blob(&value, buffer);
}

template <>
size_t Deserialize<cvm_bridge_blob>(const void *buffer, cvm_bridge_blob *value)
{
  return cvm_bridge_read_blob(buffer, value);
}

template <>
size_t Serialize<cvmfs::DirectoryListing>(
  const cvmfs::DirectoryListing &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  cvm_bridge_blob blob;
  blob.buffer = value.buffer;
  blob.size = value.size;
  blob.is_mmapd = (value.capacity == 0);
  pos += Serialize<cvm_bridge_blob>(blob, buffer);
  pos += Serialize<size_t>(value.size, *where);
  pos += Serialize<size_t>(value.capacity, *where);
  return pos - base;
}

template <>
size_t Deserialize<cvmfs::DirectoryListing>(
  const void *buffer, cvmfs::DirectoryListing *value)
{
  const unsigned char *bytes = reinterpret_cast<const unsigned char *>(buffer);

  cvm_bridge_blob blob;
  bytes += Deserialize<cvm_bridge_blob>(bytes, &blob);
  value->buffer = reinterpret_cast<char *>(blob.buffer);
  bytes += Deserialize<size_t>(bytes, &(value->size));
  bytes += Deserialize<size_t>(bytes, &(value->capacity));
  return bytes - reinterpret_cast<const unsigned char *>(buffer);
}

template<unsigned char StackSize, char Type>
size_t SerializeShortstring(const ShortString<StackSize, Type> &value,
                            void *buffer)
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

template<unsigned char StackSize, char Type>
size_t DeserializeShortstring(const void *buffer,
                              ShortString<StackSize, Type> *value)
{
  size_t length;
  Deserialize<size_t>(buffer, &length);
  if (length > 0) {
    value->Assign(reinterpret_cast<const char *>(buffer) + sizeof(size_t),
                  length);
  } else {
    value->Clear();
  }
  return sizeof(size_t) + length;
}

template <>
size_t Serialize<NameString>(const NameString &value, void *buffer)
{
  return SerializeShortstring(value, buffer);
}

template <>
size_t Deserialize<NameString>(const void *buffer, NameString *value)
{
  return DeserializeShortstring(buffer, value);
}

template <>
size_t Serialize<glue::DentryTracker::Entry>(
  const glue::DentryTracker::Entry &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == NULL) ? &buffer : reinterpret_cast<void**>(&pos);

  pos += Serialize<uint64_t>(value.expiry, *where);
  pos += Serialize<uint64_t>(value.inode_parent, *where);
  pos += Serialize<NameString>(value.name, *where);
  return pos - base;
}

template <>
size_t Deserialize<glue::DentryTracker::Entry>(
  const void *buffer, glue::DentryTracker::Entry *value)
{
  const unsigned char *bytes = reinterpret_cast<const unsigned char *>(buffer);

  bytes += Deserialize<uint64_t>(bytes, &value->expiry);
  bytes += Deserialize<uint64_t>(bytes, &value->inode_parent);
  bytes += Deserialize<NameString>(bytes, &value->name);
  return bytes - reinterpret_cast<const unsigned char *>(buffer);
}

template <class Item>
size_t SerializeBigQueue(const BigQueue<Item> &value, void *buffer) {
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

template <class Item>
size_t DeserializeBigQueue(const void *buffer, BigQueue<Item> *value) {
  const unsigned char *bytes = reinterpret_cast<const unsigned char *>(buffer);

  value->Clear();
  size_t size;
  bytes += Deserialize<size_t>(bytes, &size);
  value->Reset(size);
  for (size_t i = 0; i < size; ++i) {
    Item item;
    bytes += Deserialize<Item>(bytes, &item);
    value->PushBack(item);
  }
  return bytes - reinterpret_cast<const unsigned char *>(buffer);
}

}  // anonymous namespace


size_t StateSerializer::SerializeDirectoryHandles(
  const cvmfs::DirectoryHandles &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  size_t size = value.size();
  pos += cvm_bridge_write_size(&size, *where);
  for (cvmfs::DirectoryHandles::const_iterator it = value.begin(),
       itEnd = value.end(); it != itEnd; ++it)
  {
    pos += Serialize<uint64_t>(it->first, *where);
    pos += Serialize<cvmfs::DirectoryListing>(it->second, *where);
  }
  return pos - base;
}

size_t StateSerializer::DeserializeDirectoryHandles(
  const void *buffer, cvmfs::DirectoryHandles *value)
{
  const unsigned char *bytes = reinterpret_cast<const unsigned char *>(buffer);

  size_t size;
  bytes += cvm_bridge_read_size(bytes, &size);
  for (size_t i = 0; i < size; ++i) {
    uint64_t key;
    cvmfs::DirectoryListing listing;
    bytes += Deserialize<uint64_t>(bytes, &key);
    bytes += Deserialize<cvmfs::DirectoryListing>(bytes, &listing);
    (*value)[key] = listing;
  }
  return bytes - reinterpret_cast<const unsigned char *>(buffer);
}

size_t StateSerializer::SerializeDentryTracker(const glue::DentryTracker &value,
                                               void *buffer)
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
  pos += SerializeBigQueue<glue::DentryTracker::Entry>(value.entries_, *where);
  value.Unlock();

  return pos - base;
}

size_t StateSerializer::DeserializeDentryTracker(const void *buffer,
                                                 glue::DentryTracker *value)
{
  const unsigned char *bytes = reinterpret_cast<const unsigned char *>(buffer);

  value->Lock();
  bytes += Deserialize<unsigned>(bytes, &value->version_);
  assert(value->version_ == 0);
  bytes += Deserialize<int64_t>(bytes, &value->statistics_.num_insert);
  bytes += Deserialize<int64_t>(bytes, &value->statistics_.num_remove);
  bytes += Deserialize<int64_t>(bytes, &value->statistics_.num_prune);
  bytes += Deserialize<bool>(bytes, &value->is_active_);
  bytes += DeserializeBigQueue<glue::DentryTracker::Entry>(bytes,
                                                           &value->entries_);
  value->Unlock();

  return bytes - reinterpret_cast<const unsigned char *>(buffer);
}

size_t StateSerializer::SerializeOpenFilesCounter(
 const uint32_t &value, void *buffer)
{
  return Serialize<uint32_t>(value, buffer);
}

size_t StateSerializer::DeserializeOpenFilesCounter(
 const void *buffer, uint32_t *value)
{
  return Deserialize<uint32_t>(buffer, value);
}

size_t StateSerializer::SerializeInodeGeneration(
  const cvmfs::InodeGenerationInfo &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  pos += Serialize<unsigned>(value.version, *where);
  pos += Serialize<uint64_t>(value.initial_revision, *where);
  pos += Serialize<uint32_t>(value.incarnation, *where);
  pos += Serialize<uint32_t>(value.overflow_counter, *where);
  pos += Serialize<uint64_t>(value.inode_generation, *where);
  return pos - base;
}

size_t StateSerializer::DeserializeInodeGeneration(
  const void *buffer, cvmfs::InodeGenerationInfo *value)
{
  const unsigned char *bytes = reinterpret_cast<const unsigned char *>(buffer);

  bytes += Deserialize<unsigned>(bytes, &(value->version));
  bytes += Deserialize<uint64_t>(bytes, &(value->initial_revision));
  bytes += Deserialize<uint32_t>(bytes, &(value->incarnation));
  if (value->version < 2) {
    return bytes - reinterpret_cast<const unsigned char *>(buffer);
  }

  bytes += Deserialize<uint32_t>(bytes, &(value->overflow_counter));
  bytes += Deserialize<uint64_t>(bytes, &(value->inode_generation));

  return bytes - reinterpret_cast<const unsigned char *>(buffer);
}

size_t StateSerializer::SerializeFuseState(
  const cvmfs::FuseState &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  pos += Serialize<unsigned>(value.version, *where);
  pos += Serialize<bool>(value.cache_symlinks, *where);
  pos += Serialize<bool>(value.has_dentry_expire, *where);
  return pos - base;
}

size_t StateSerializer::DeserializeFuseState(
  const void *buffer, cvmfs::FuseState *value)
{
  const unsigned char *bytes = reinterpret_cast<const unsigned char *>(buffer);

  bytes += Deserialize<unsigned>(bytes, &(value->version));
  bytes += Deserialize<bool>(bytes, &(value->cache_symlinks));
  bytes += Deserialize<bool>(bytes, &(value->has_dentry_expire));
  return bytes - reinterpret_cast<const unsigned char *>(buffer);
}
