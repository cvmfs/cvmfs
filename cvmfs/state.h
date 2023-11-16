/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_STATE_H_
#define CVMFS_STATE_H_

#include <cstddef>
#include <cstdint>

namespace cvmfs {
struct FuseState;
struct InodeGenerationInfo;
}

/**
 * Uses the marshalling functions from the bridge library to serialize and
 * deserialize state information that needs to survive a hotpatch/reload.
 *
 * See loader.h for a list of states.
 *
 * Note that states from before the serializer was introduced need to be
 * transformed first by the bridge library into the serialized format.
 */
class StateSerializer {
 public:
  static size_t SerializeOpenFilesCounter(const uint32_t &value, void *buffer);
  static size_t DeserializeOpenFilesCounter(const void *buffer,
                                            uint32_t *value);

  static size_t
  SerializeInodeGeneration(const cvmfs::InodeGenerationInfo &value,
                           void *buffer);
  static size_t DeserializeInodeGeneration(const void *buffer,
                                           cvmfs::InodeGenerationInfo *value);

  static size_t SerializeFuseState(const cvmfs::FuseState &value, void *buffer);
  static size_t DeserializeFuseState(const void *buffer,
                                     cvmfs::FuseState *value);
};  // class StateSerializer

#endif  // CVMFS_STATE_H_
