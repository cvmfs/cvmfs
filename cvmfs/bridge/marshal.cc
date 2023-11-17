/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "bridge/marshal.h"

#include "util/smalloc.h"

#include <cstring>

size_t cvm_bridge_write_uint32(const uint32_t *value, void *buffer) {
  if (buffer != nullptr) {
    memcpy(buffer, value, 4);
  }
  return 4;
}

size_t cvm_bridge_read_uint32(const void *buffer, uint32_t *value) {
  memcpy(value, buffer, 4);
  return 4;
}

size_t cvm_bridge_write_uint64(const uint64_t *value, void *buffer) {
  if (buffer != nullptr) {
    memcpy(buffer, value, 8);
  }
  return 8;
}

size_t cvm_bridge_read_uint64(const void *buffer, uint64_t *value) {
  memcpy(value, buffer, 8);
  return 8;
}

size_t cvm_bridge_write_uint(const unsigned *value, void *buffer) {
  if (buffer != nullptr) {
    memcpy(buffer, value, sizeof(unsigned));
  }
  return sizeof(unsigned);
}

size_t cvm_bridge_read_uint(const void *buffer, unsigned *value) {
  memcpy(value, buffer, sizeof(unsigned));
  return sizeof(unsigned);
}

size_t cvm_bridge_write_size(const size_t *value, void *buffer) {
  if (buffer != nullptr) {
    memcpy(buffer, value, sizeof(size_t));
  }
  return sizeof(size_t);
}

size_t cvm_bridge_read_size(const void *buffer, size_t *value) {
  memcpy(value, buffer, sizeof(size_t));
  return sizeof(size_t);
}

size_t cvm_bridge_write_bool(const bool *value, void *buffer) {
  if (buffer != nullptr) {
    *reinterpret_cast<bool *>(buffer) = *value;
  }
  return 1;
}

size_t cvm_bridge_read_bool(const void *buffer, bool *value) {
  *value = *reinterpret_cast<const bool *>(buffer);
  return 1;
}

size_t cvm_bridge_write_blob(const cvm_bridge_blob *value, void *buffer) {
  if (buffer != nullptr) {
    unsigned char *bytes = reinterpret_cast<unsigned char *>(buffer);
    bytes += cvm_bridge_write_size(&value->size, bytes);
    bytes += cvm_bridge_write_bool(&value->is_mmapd, bytes);
    if (value->size > 0)
      memcpy(bytes, value->buffer, value->size);
  }
  return sizeof(size_t) + 1 + value->size;
}

size_t cvm_bridge_read_blob(const void *buffer, cvm_bridge_blob *value) {
  const unsigned char *bytes = reinterpret_cast<const unsigned char *>(buffer);
  bytes += cvm_bridge_read_size(bytes, &value->size);
  bytes += cvm_bridge_read_bool(bytes, &value->is_mmapd);
  if (value->size == 0) {
    value->buffer = NULL;
    return sizeof(size_t) + 1;
  }
  if (value->is_mmapd) {
    value->buffer = smmap(value->size);
  } else {
    value->buffer = smalloc(value->size);
  }
  memcpy(value->buffer, bytes, value->size);
  return sizeof(size_t) + 1 + value->size;
}
