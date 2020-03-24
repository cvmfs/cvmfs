/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_EXCEPT_H_
#define CVMFS_PUBLISH_EXCEPT_H_

#include <stdexcept>
#include <string>

namespace publish {

class EPublish : public std::runtime_error {
 public:
  explicit EPublish(const std::string& what)
    : std::runtime_error(what + "\n\nStacktrace:\n" + GetStacktrace())
  {}

 private:
  /**
   * Maximum number of frames in the stack trace
   */
  static const unsigned kMaxBacktrace = 64;
  static std::string GetStacktrace();
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_EXCEPT_H_
