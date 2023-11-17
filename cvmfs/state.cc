/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "state.h"

#include "bridge/marshal.h"
#include "fuse_directory_handle.h"
#include "fuse_inode_gen.h"
#include "fuse_state.h"

namespace {

static size_t SerializeDirectoryListing(
  const cvmfs::DirectoryListing &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  cvm_bridge_blob blob;
  blob.buffer = value.buffer;
  blob.size = value.size;
  blob.is_mmapd = (value.capacity == 0);
  pos += cvm_bridge_write_blob(&blob, *where);
  pos += cvm_bridge_write_size(&value.size, *where);
  pos += cvm_bridge_write_size(&value.capacity, *where);
  return pos - base;
}

static size_t DeserializeDirectoryListing(
  const void *buffer, cvmfs::DirectoryListing *value)
{
  const unsigned char *bytes = reinterpret_cast<const unsigned char *>(buffer);

  cvm_bridge_blob blob;
  bytes += cvm_bridge_read_blob(bytes, &blob);
  value->buffer = reinterpret_cast<char *>(blob.buffer);
  bytes += cvm_bridge_read_size(bytes, &(value->size));
  bytes += cvm_bridge_read_size(bytes, &(value->capacity));
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
    pos += cvm_bridge_write_uint64(&it->first, *where);
    pos += SerializeDirectoryListing(it->second, *where);
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
    bytes += cvm_bridge_read_uint64(bytes, &key);
    bytes += DeserializeDirectoryListing(bytes, &listing);
    (*value)[key] = listing;
  }
  return bytes - reinterpret_cast<const unsigned char *>(buffer);
}

size_t StateSerializer::SerializeOpenFilesCounter(
 const uint32_t &value, void *buffer)
{
  return cvm_bridge_write_uint32(&value, buffer);
}

size_t StateSerializer::DeserializeOpenFilesCounter(
 const void *buffer, uint32_t *value)
{
  return cvm_bridge_read_uint32(buffer, value);
}

size_t StateSerializer::SerializeInodeGeneration(
  const cvmfs::InodeGenerationInfo &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  pos += cvm_bridge_write_uint(&value.version, *where);
  pos += cvm_bridge_write_uint64(&value.initial_revision, *where);
  pos += cvm_bridge_write_uint32(&value.incarnation, *where);
  pos += cvm_bridge_write_uint32(&value.overflow_counter, *where);
  pos += cvm_bridge_write_uint64(&value.inode_generation, *where);
  return pos - base;
}

size_t StateSerializer::DeserializeInodeGeneration(
  const void *buffer, cvmfs::InodeGenerationInfo *value)
{
  const unsigned char *bytes = reinterpret_cast<const unsigned char *>(buffer);

  bytes += cvm_bridge_read_uint(bytes, &(value->version));
  bytes += cvm_bridge_read_uint64(bytes, &(value->initial_revision));
  bytes += cvm_bridge_read_uint32(bytes, &(value->incarnation));
  if (value->version < 2) {
    return bytes - reinterpret_cast<const unsigned char *>(buffer);
  }

  bytes += cvm_bridge_read_uint32(bytes, &(value->overflow_counter));
  bytes += cvm_bridge_read_uint64(bytes, &(value->inode_generation));

  return bytes - reinterpret_cast<const unsigned char *>(buffer);
}

size_t StateSerializer::SerializeFuseState(
  const cvmfs::FuseState &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  pos += cvm_bridge_write_uint(&value.version, *where);
  pos += cvm_bridge_write_bool(&value.cache_symlinks, *where);
  pos += cvm_bridge_write_bool(&value.has_dentry_expire, *where);
  return pos - base;
}

size_t StateSerializer::DeserializeFuseState(
  const void *buffer, cvmfs::FuseState *value)
{
  const unsigned char *bytes = reinterpret_cast<const unsigned char *>(buffer);

  bytes += cvm_bridge_read_uint(bytes, &(value->version));
  bytes += cvm_bridge_read_bool(bytes, &(value->cache_symlinks));
  bytes += cvm_bridge_read_bool(bytes, &(value->has_dentry_expire));
  return bytes - reinterpret_cast<const unsigned char *>(buffer);
}
