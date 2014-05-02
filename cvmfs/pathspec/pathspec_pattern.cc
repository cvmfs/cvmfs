/**
 * This file is part of the CernVM File System.
 */

#include "pathspec_pattern.h"

#include <iostream> // TODO: remove me

PathspecElementPattern::PathspecElementPattern(
                                  const std::string::const_iterator   begin,
                                  const std::string::const_iterator  &end) {
  Parse(begin, end);
}


void PathspecElementPattern::Parse(const std::string::const_iterator  &begin,
                                   const std::string::const_iterator  &end) {
  std::string::const_iterator i = begin;
}
