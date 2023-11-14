/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "bridge/marshal.h"

size_t cvm_bridge_write_uint32(const uint32_t *value, void *buffer) {
  if (buffer != nullptr) {
    *reinterpret_cast<uint32_t *>(buffer) = *value;
  }
  return 4;
}

size_t cvm_bridge_read_uint32(const void *buffer, uint32_t *value) {
  *value = *reinterpret_cast<const uint32_t *>(buffer);
  return 4;
}
