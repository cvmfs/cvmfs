/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "bridge/migrate.h"

#include <cstdint>
#include <cstdlib>

#include "bridge/ds_stubs.h"
#include "bridge/marshal.h"
#include "util/smalloc.h"

namespace {

static size_t SerializeInodeGenerationV1(
  const compat::InodeGenerationInfoV1 &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  pos += cvm_bridge_write_uint(&value.version, *where);
  pos += cvm_bridge_write_uint64(&value.initial_revision, *where);
  pos += cvm_bridge_write_uint32(&value.incarnation, *where);
  if (value.version < 2) {
    return pos - base;
  }

  pos += cvm_bridge_write_uint32(&value.overflow_counter, *where);
  pos += cvm_bridge_write_uint64(&value.inode_generation, *where);
  return pos - base;
}

static size_t SerializeFuseStateV1(
  const compat::FuseStateV1 &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  pos += cvm_bridge_write_uint(&value.version, *where);
  pos += cvm_bridge_write_bool(&value.cache_symlinks, *where);
  pos += cvm_bridge_write_bool(&value.has_dentry_expire, *where);
  return pos - base;
}

static size_t SerializeDirectoryListingV1(
  const compat::DirectoryListingV1 &value, void *buffer)
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

static size_t SerializeDirectoryHandlesV1(
  const compat::DirectoryHandlesV1 &value, void *buffer)
{
  unsigned char *base = reinterpret_cast<unsigned char *>(buffer);
  unsigned char *pos = base;
  void** where = (buffer == nullptr) ? &buffer : reinterpret_cast<void**>(&pos);

  size_t size = value.size();
  pos += cvm_bridge_write_size(&size, *where);
  for (compat::DirectoryHandlesV1::const_iterator it = value.begin(),
       itEnd = value.end(); it != itEnd; ++it)
  {
    pos += cvm_bridge_write_uint64(&it->first, *where);
    pos += SerializeDirectoryListingV1(it->second, *where);
  }
  return pos - base;
}

}  // anonymous namespace

void *cvm_bridge_migrate_directory_handles_v1v2s(void *v1) {
  compat::DirectoryHandlesV1 *h =
    reinterpret_cast<compat::DirectoryHandlesV1 *>(v1);

  size_t nbytes = SerializeDirectoryHandlesV1(*h, NULL);
  void *v2s = smalloc(nbytes);
  SerializeDirectoryHandlesV1(*h, v2s);
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
  cvm_bridge_write_uint32(ctr, v2s);
  return v2s;
}

void cvm_bridge_free_nfiles_ctr_v1(void *v1) {
  delete reinterpret_cast<uint32_t *>(v1);
}

void *cvm_bridge_migrate_inode_generation_v1v2s(void *v1) {
  compat::InodeGenerationInfoV1 *info =
    reinterpret_cast<compat::InodeGenerationInfoV1 *>(v1);

  size_t nbytes = SerializeInodeGenerationV1(*info, NULL);

  void *v2s = smalloc(nbytes);
  SerializeInodeGenerationV1(*info, v2s);
  return v2s;
}

void cvm_bridge_free_inode_generation_v1(void *v1) {
  delete reinterpret_cast<compat::InodeGenerationInfoV1 *>(v1);
}

void *cvm_bridge_migrate_fuse_state_v1v2s(void *v1) {
  compat::FuseStateV1 *info = reinterpret_cast<compat::FuseStateV1 *>(v1);

  size_t nbytes = SerializeFuseStateV1(*info, NULL);
  void *v2s = smalloc(nbytes);
  SerializeFuseStateV1(*info, v2s);
  return v2s;
}

void cvm_bridge_free_fuse_state_v1(void *v1) {
  delete reinterpret_cast<compat::FuseStateV1 *>(v1);
}
