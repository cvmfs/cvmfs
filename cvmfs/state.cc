/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "state.h"

#include "bridge/marshal.h"
#include "fuse_inode_gen.h"

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
