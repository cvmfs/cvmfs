/**
 * This file is part of the CernVM File System.
 *
 * Private utility functions for libcvmfs_server
 */

#ifndef CVMFS_PUBLISH_REPOSITORY_UTIL_H_
#define CVMFS_PUBLISH_REPOSITORY_UTIL_H_

#include <string>

#include "hash.h"

namespace publish {

class CheckoutMarker {
 public:
  CheckoutMarker(
    const std::string &t, const std::string &b, const shash::Any &h)
    : tag_(t), branch_(b), hash_(h)
  {}
  static CheckoutMarker *CreateFrom(const std::string &path);
  void SaveAs(const std::string &path) const;

  std::string tag() const { return tag_; }
  std::string branch() const { return branch_; }
  shash::Any hash() const { return hash_; }

 private:
  std::string tag_;
  std::string branch_;
  shash::Any hash_;
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_REPOSITORY_UTIL_H_
