/**
 * This file is part of the CernVM File System.
 *
 * Building blocks to serialize and deserialize states. Functions are exposed
 * as C functions because the bridge library is build with (possibly) different
 * C++ standard / ABI than the rest of cvmfs.
 */

#ifndef CVMFS_BRIDGE_MARSHAL_H_
#define CVMFS_BRIDGE_MARSHAL_H_

#include <cstddef>
#include <cstdint>

#include "util/export.h"

extern "C" {

CVMFS_EXPORT size_t cvm_bridge_write_uint32(const uint32_t *value,
                                            void *buffer);
CVMFS_EXPORT size_t cvm_bridge_read_uint32(const void *buffer, uint32_t *value);

}  // extern "C"

#endif  // CVMFS_BRIDGE_MARSHAL_H_
