/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "bridge/migrate.h"

#include <cstdint>

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

}  // anonymous namespace

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
