/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_PAYLOAD_PROCESSOR_H_
#define CVMFS_RECEIVER_PAYLOAD_PROCESSOR_H_

#include <stdint.h>
#include <string>

#include "pack.h"

namespace receiver {

class PayloadProcessor {
 public:
  enum Result { kSuccess, kPathViolation, kOtherError };

  PayloadProcessor();
  virtual ~PayloadProcessor();

  Result Process(int fdin, const std::string& digest_base64,
                 const std::string& path, uint64_t header_size);

  virtual void ConsumerEventCallback(const ObjectPackBuild::Event& event);

  int GetNumErrors() const { return num_errors_; }

 protected:
  virtual int WriteFile(int fd, const void* const buf, size_t buf_size);

 private:
  std::string current_repo_;
  int num_errors_;
};

}  // namespace receiver

#endif  // CVMFS_RECEIVER_PAYLOAD_PROCESSOR_H_
