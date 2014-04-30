/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CHECKSUM_H_
#define CVMFS_CHECKSUM_H_

#include <inttypes.h>

namespace checksum {

inline void InitCrc32C(uint32_t *running_crc) {
  *running_crc = CRC_INITIAL_VAL;
}


inlinevoid UpdateCrc32C(uint32_t *running_crc,
                        const unsigned char *buf, const unsigned length)
{
  calculate_crc(buf, length, running_crc, CRC32C_POLYNOMIAL);
}

inline void FinalCrc32C(uint32_t *running_crc) { }

}  // namespace checksum

#endif  // CVMFS_CHECKSUM_H_
