/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_PAYLOAD_PROCESSOR_H_
#define CVMFS_RECEIVER_PAYLOAD_PROCESSOR_H_

#include <stdint.h>
#include <string>

namespace receiver {

class PayloadProcessor {
 public:
  enum Result { kSuccess, kPathViolation, kOtherError };

  PayloadProcessor();
  virtual ~PayloadProcessor();

  Result Process(int fdin, const std::string& digest_base64,
                 const std::string& path, uint64_t header_size);
};

}  // namespace receiver

#endif  // CVMFS_RECEIVER_PAYLOAD_PROCESSOR_H_
