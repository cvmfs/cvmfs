/**
 * This file is part of the CernVM File System.
 */

#include "pathspec.h"


Pathspec::Pathspec(const std::string &spec) : valid_(true),
                                              negation_(false),
                                              absolute_(false) {
  Parse(spec);
  if (patterns_.size() == 0) {
    valid_ = false;
  }
}


void Pathspec::Parse(const std::string &spec) {
        std::string::const_iterator itr = spec.begin();
  const std::string::const_iterator end = spec.end();

  SkipWhitespace(itr, end);
  if (*itr == kNegator) {
    negation_ = true;
    ++itr;
  }
  ParsePath(itr, end);
}

void Pathspec::ParsePath(      std::string::const_iterator  &itr,
                         const std::string::const_iterator  &end) {
  SkipWhitespace(itr, end);
  absolute_ = (*itr == kSeparator);

  while (itr != end) {
    if (*itr == kSeparator) {
      ++itr;
      continue;
    }
    ParsePathElement(itr, end);
  }
}

void Pathspec::ParsePathElement(      std::string::const_iterator  &itr,
                                const std::string::const_iterator  &end) {
  const std::string::const_iterator begin_element = itr;
  while (itr != end && *itr != kSeparator) {
    ++itr;
  }
  const std::string::const_iterator end_element = itr;
  patterns_.push_back(PathspecElementPattern(begin_element, end_element));
}

void Pathspec::SkipWhitespace(      std::string::const_iterator  &itr,
                              const std::string::const_iterator  &end) const {
  while (itr != end && *itr == ' ') {
    ++itr;
  }
}
