/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef BULK_CRC32_H_INCLUDED
#define BULK_CRC32_H_INCLUDED

//
// This CRC32 implementation was taken from HDFS 2.0.3 sources.
// For benchmarks, see:
// https://issues.apache.org/jira/browse/HADOOP-7446?focusedCommentId=13084519&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13084519
// As of the time of writing, a new processor should be able to do 4KB per microsecond.
//

#include <stdint.h>
#include <unistd.h> /* for size_t */

// Constants for different CRC algorithms
#define CRC32C_POLYNOMIAL 1
#define CRC32_ZLIB_POLYNOMIAL 2

// Return codes for bulk_verify_crc
#define CHECKSUMS_VALID 0
#define INVALID_CHECKSUM_DETECTED -1
#define INVALID_CHECKSUM_TYPE -2

// Return type for bulk verification when verification fails
typedef struct crc32_error {
  uint32_t got_crc;
  uint32_t expected_crc;
  const uint8_t *bad_data; // pointer to start of data chunk with error
} crc32_error_t;


/**
 * Verify a buffer of data which is checksummed in chunks
 * of bytes_per_checksum bytes. The checksums are each 32 bits
 * and are stored in sequential indexes of the 'sums' array.
 *
 * @param data                  The data to checksum
 * @param dataLen               Length of the data buffer
 * @param sums                  (out param) buffer to write checksums into.
 *                              It must contain at least dataLen * 4 bytes.
 * @param checksum_type         One of the CRC32 algorithm constants defined 
 *                              above
 * @param bytes_per_checksum    How many bytes of data to process per checksum.
 * @param error_info            If non-NULL, will be filled in if an error
 *                              is detected
 *
 * @return                      0 for success, non-zero for an error, result codes
 *                              for which are defined above
 */
#ifdef __cplusplus
extern "C" { 
#endif
int bulk_verify_crc(const uint8_t *data, size_t data_len,
    const uint32_t *sums, int checksum_type,
    int bytes_per_checksum,
    crc32_error_t *error_info);

/**
 * Calculate checksums for some data.
 *
 * The checksums are each 32 bits and are stored in sequential indexes of the
 * 'sums' array.
 *
 * This function is not (yet) optimized.  It is provided for testing purposes
 * only.
 *
 * @param data                  The data to checksum
 * @param dataLen               Length of the data buffer
 * @param sums                  (out param) buffer to write checksums into.
 *                              It must contain at least dataLen * 4 bytes.
 * @param checksum_type         One of the CRC32 algorithm constants defined 
 *                              above
 * @param bytesPerChecksum      How many bytes of data to process per checksum.
 *
 * @return                      0 for success, non-zero for an error
 */
int bulk_calculate_crc(const uint8_t *data, size_t data_len,
                    uint32_t *sums, int checksum_type,
                    int bytes_per_checksum);
#ifdef __cplusplus
}
#endif

#endif
