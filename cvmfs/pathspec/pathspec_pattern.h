/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PATHSPEC_PATTERN_H_
#define CVMFS_PATHSPEC_PATTERN_H_

#include <string>

class PathspecElementPattern {
 public:
  PathspecElementPattern(const std::string::const_iterator   begin,
                         const std::string::const_iterator  &end);

 protected:
  void Parse(const std::string::const_iterator  &begin,
             const std::string::const_iterator  &end);
};

#endif  // CVMFS_PATHSPEC_PATTERN_H_
