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

size_t cvm_bridge_write_uint64(const uint64_t *value, void *buffer) {
  if (buffer != nullptr) {
    *reinterpret_cast<uint64_t *>(buffer) = *value;
  }
  return 8;
}

size_t cvm_bridge_read_uint64(const void *buffer, uint64_t *value) {
  *value = *reinterpret_cast<const uint64_t *>(buffer);
  return 8;
}

size_t cvm_bridge_write_uint(const unsigned *value, void *buffer) {
  if (buffer != nullptr) {
    *reinterpret_cast<unsigned *>(buffer) = *value;
  }
  return sizeof(unsigned);
}

size_t cvm_bridge_read_uint(const void *buffer, unsigned *value) {
  *value = *reinterpret_cast<const unsigned *>(buffer);
  return sizeof(unsigned);
}
