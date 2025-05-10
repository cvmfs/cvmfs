/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_UUID_H_
#define CVMFS_UTIL_UUID_H_

#include <inttypes.h>
#include <uuid/uuid.h>

#include <cstdint>
#include <string>

#include "util/export.h"

namespace cvmfs {

/**
 * Holds a unique identifies which is either read from a file or, if the file
 * does not yet exist, created and stored in a file.  This is how it is used to
 * identify a cvmfs cache directory.
 *
 * In order to create many UUIDs for short-lived objects, use the CreateOneTime
 * factory method.
 */
class CVMFS_EXPORT Uuid {
 public:
  static Uuid *Create(const std::string &store_path);
  static std::string CreateOneTime();
  std::string uuid() const { return uuid_; }
  const unsigned char *data() const {
    return reinterpret_cast<const unsigned char *>(&uuid_presentation_.uuid);
  }
  unsigned size() const { return sizeof(uuid_presentation_.uuid); }

 private:
  void MkUuid();
  Uuid();

  std::string uuid_;
  union {
    uuid_t uuid;
    struct __attribute__((__packed__)) {
      uint32_t a;
      uint16_t b;
      uint16_t c;
      uint16_t d;
      uint32_t e1;
      uint16_t e2;
    } split;
  } uuid_presentation_;
};

}  // namespace cvmfs

#endif  // CVMFS_UTIL_UUID_H_
