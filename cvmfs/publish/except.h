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
  /**
   * Well-known exceptions that are usually caught and handled
   */
  enum EId {
    kIdUnspecified = 0,
    kIdTransactionLocked = 1,
  };

  explicit EPublish(const std::string& what, EId id = kIdUnspecified)
    : std::runtime_error(what + "\n\nStacktrace:\n" + GetStacktrace())
    , id_(id)
    , msg_(what)
  {}

  virtual ~EPublish() throw();

  EId id() const { return id_; }
  std::string msg() const { return msg_; }

 private:
  EId id_;
  std::string msg_;

  /**
   * Maximum number of frames in the stack trace
   */
  static const unsigned kMaxBacktrace = 64;
  static std::string GetStacktrace();
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_EXCEPT_H_
