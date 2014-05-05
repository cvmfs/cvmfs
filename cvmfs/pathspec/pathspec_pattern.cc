/**
 * This file is part of the CernVM File System.
 */

#include "pathspec_pattern.h"

#include <cassert>

PathspecElementPattern::PathspecElementPattern(
                                  const std::string::const_iterator   begin,
                                  const std::string::const_iterator  &end) :
  valid_(true)
{
  Parse(begin, end);
}

PathspecElementPattern::PathspecElementPattern(
                                          const PathspecElementPattern& other) :
  valid_(other.valid_)
{
  subpatterns_.reserve(other.subpatterns_.size());
  SubPatterns::const_iterator i    = other.subpatterns_.begin();
  SubPatterns::const_iterator iend = other.subpatterns_.end();
  for (; i != iend; ++i) {
    subpatterns_.push_back((*i)->Clone());
  }
}


PathspecElementPattern::~PathspecElementPattern() {
  SubPatterns::const_iterator i    = subpatterns_.begin();
  SubPatterns::const_iterator iend = subpatterns_.end();
  for (; i != iend; ++i) {
    delete *i;
  }
  subpatterns_.clear();
}


void PathspecElementPattern::Parse(const std::string::const_iterator  &begin,
                                   const std::string::const_iterator  &end) {
  std::string::const_iterator i = begin;
  while (i != end) {
    SubPattern* next = (IsSpecialChar(*i))
                       ? ParseSpecialChar(i, end)
                       : ParsePlaintext(i, end);
    if (next->IsEmpty()) {
      valid_ = false;
      delete next;
    } else {
      subpatterns_.push_back(next);
    }
  }
}


PathspecElementPattern::SubPattern* PathspecElementPattern::ParsePlaintext(
                                            std::string::const_iterator  &i,
                                      const std::string::const_iterator  &end) {
  PlaintextSubPattern *pattern = new PlaintextSubPattern();
  bool next_is_escaped = false;

  while (i < end) {
    if (IsSpecialChar(*i) && !next_is_escaped) {
      break;
    }

    if (*i == kEscaper && !next_is_escaped) {
      next_is_escaped = true;
    } else if (next_is_escaped) {
      if (IsSpecialChar(*i) || *i == kEscaper) {
        pattern->AddChar(*i);
      } else {
        valid_ = false;
      }
    } else {
      assert (!IsSpecialChar(*i));
      pattern->AddChar(*i);
    }

    ++i;
  }

  return pattern;
}

PathspecElementPattern::SubPattern* PathspecElementPattern::ParseSpecialChar(
                                            std::string::const_iterator  &i,
                                      const std::string::const_iterator  &end) {
  assert (IsSpecialChar(*i));
  const char chr = *i;
  ++i;

  switch (chr) {
    case kWildcard:
      return new WildcardSubPattern();
    case kPlaceholder:
      return new PlaceholderSubPattern();
    default:
      assert (false && "unrecognized special character");
  }
}


bool PathspecElementPattern::Matches(const std::string::const_iterator   begin,
                                     const std::string::const_iterator  &end) const {
  return false;
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


void PathspecElementPattern::PlaintextSubPattern::AddChar(const char chr) {
  chars_.push_back(chr);
}
