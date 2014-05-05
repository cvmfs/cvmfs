/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PATHSPEC_H_
#define CVMFS_PATHSPEC_H_

#include <string>
#include <vector>

#include "pathspec_pattern.h"

class Pathspec {
 private:
  static const char kNegator   = '!';
  static const char kSeparator = '/';

 public:
  Pathspec(const std::string &spec);

  bool IsMatching(const std::string &path) const;

  bool IsValid()    const { return valid_;    }
  bool IsAbsolute() const { return absolute_; }
  bool IsNegation() const { return negation_; }

 protected:
  void Parse(const std::string &spec);
  void ParsePath(             std::string::const_iterator  &itr,
                        const std::string::const_iterator  &end);
  void ParsePathElement(      std::string::const_iterator  &itr,
                        const std::string::const_iterator  &end);
  void SkipWhitespace(        std::string::const_iterator  &itr,
                        const std::string::const_iterator  &end) const;

  bool IsPathspecMatching(      std::string::const_iterator  &itr,
                          const std::string::const_iterator  &end) const;

 private:
  std::vector<PathspecElementPattern> patterns_;

  bool valid_;
  bool negation_;
  bool absolute_;
};

#endif  // CVMFS_PATHSPEC_H_
