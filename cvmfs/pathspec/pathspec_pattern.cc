/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "pathspec_pattern.h"

#include <cassert>

#include "pathspec.h"

PathspecElementPattern::PathspecElementPattern(
  const std::string::const_iterator begin,
  const std::string::const_iterator &end)
  : valid_(true)
{
  Parse(begin, end);
}

PathspecElementPattern::PathspecElementPattern(
  const PathspecElementPattern& other)
  : valid_(other.valid_)
{
  subpatterns_.reserve(other.subpatterns_.size());
  SubPatterns::const_iterator i    = other.subpatterns_.begin();
  SubPatterns::const_iterator iend = other.subpatterns_.end();
  for (; i != iend; ++i) {
    subpatterns_.push_back((*i)->Clone());
  }
}

PathspecElementPattern& PathspecElementPattern::operator=(
                                            const PathspecElementPattern& other)
{
  if (this != &other) {
    valid_ = other.valid_;
    subpatterns_.clear();
    subpatterns_.reserve(other.subpatterns_.size());
    SubPatterns::const_iterator i    = other.subpatterns_.begin();
    SubPatterns::const_iterator iend = other.subpatterns_.end();
    for (; i != iend; ++i) {
      subpatterns_.push_back((*i)->Clone());
    }
  }

  return *this;
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
    SubPattern* next = (Pathspec::IsSpecialChar(*i))
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
    if (Pathspec::IsSpecialChar(*i) && !next_is_escaped) {
      break;
    }

    if (*i == Pathspec::kEscaper && !next_is_escaped) {
      next_is_escaped = true;
    } else if (next_is_escaped) {
      if (Pathspec::IsSpecialChar(*i) || *i == Pathspec::kEscaper) {
        pattern->AddChar(*i);
        next_is_escaped = false;
      } else {
        valid_ = false;
      }
    } else {
      assert(!Pathspec::IsSpecialChar(*i));
      pattern->AddChar(*i);
    }

    ++i;
  }

  return pattern;
}

PathspecElementPattern::SubPattern* PathspecElementPattern::ParseSpecialChar(
  std::string::const_iterator  &i,
  const std::string::const_iterator  &end
) {
  assert(Pathspec::IsSpecialChar(*i));
  const char chr = *i;
  ++i;

  switch (chr) {
    case Pathspec::kWildcard:
      return new WildcardSubPattern();
    case Pathspec::kPlaceholder:
      return new PlaceholderSubPattern();
    default:
      assert(false && "unrecognized special character");
  }
}

std::string PathspecElementPattern::GenerateRegularExpression(
                                                  const bool is_relaxed) const {
  std::string result;
        SubPatterns::const_iterator i    = subpatterns_.begin();
  const SubPatterns::const_iterator iend = subpatterns_.end();
  for (; i != iend; ++i) {
    result += (*i)->GenerateRegularExpression(is_relaxed);
  }
  return result;
}

std::string PathspecElementPattern::GenerateGlobString() const {
  std::string result;
        SubPatterns::const_iterator i    = subpatterns_.begin();
  const SubPatterns::const_iterator iend = subpatterns_.end();
  for (; i != iend; ++i) {
    result += (*i)->GenerateGlobString();
  }
  return result;
}

bool PathspecElementPattern::operator== (
  const PathspecElementPattern &other) const
{
  if (subpatterns_.size() != other.subpatterns_.size() ||
      IsValid()           != other.IsValid()) {
    return false;
  }

        SubPatterns::const_iterator i    = subpatterns_.begin();
  const SubPatterns::const_iterator iend = subpatterns_.end();
        SubPatterns::const_iterator j    = other.subpatterns_.begin();
  const SubPatterns::const_iterator jend = other.subpatterns_.end();

  for (; i != iend && j != jend; ++i, ++j) {
    if (!(*i)->Compare(*j)) {
      return false;
    }
  }

  return true;
}


//------------------------------------------------------------------------------


void PathspecElementPattern::PlaintextSubPattern::AddChar(const char chr) {
  chars_.push_back(chr);
}

std::string
  PathspecElementPattern::PlaintextSubPattern::GenerateRegularExpression(
  const bool is_relaxed) const
{
  // Note: strict and relaxed regex are the same!
        std::string::const_iterator i    = chars_.begin();
  const std::string::const_iterator iend = chars_.end();
  std::string regex;
  for (; i != iend; ++i) {
    if (IsSpecialRegexCharacter(*i)) {
      regex += "\\";
    }
    regex += *i;
  }
  return regex;
}


std::string
  PathspecElementPattern::PlaintextSubPattern::GenerateGlobString() const
{
  std::string::const_iterator i = chars_.begin();
  const std::string::const_iterator iend = chars_.end();
  std::string glob_string;
  for (; i != iend; ++i) {
    if (Pathspec::IsSpecialChar(*i)) {
      glob_string += "\\";
    }
    glob_string += *i;
  }
  return glob_string;
}

bool PathspecElementPattern::PlaintextSubPattern::IsSpecialRegexCharacter(
  const char chr) const
{
  return (chr == '.'  ||
          chr == '\\' ||
          chr == '*'  ||
          chr == '?'  ||
          chr == '['  ||
          chr == ']'  ||
          chr == '('  ||
          chr == ')'  ||
          chr == '{'  ||
          chr == '}'  ||
          chr == '^'  ||
          chr == '$'  ||
          chr == '+');
}

bool PathspecElementPattern::PlaintextSubPattern::Compare(
  const SubPattern *other) const
{
  if (!other->IsPlaintext()) {
    return false;
  }

  const PlaintextSubPattern *pt_other =
                                dynamic_cast<const PlaintextSubPattern*>(other);
  assert(pt_other != NULL);
  return chars_ == pt_other->chars_;
}


std::string
  PathspecElementPattern::WildcardSubPattern::GenerateRegularExpression(
  const bool is_relaxed) const
{
  return (is_relaxed)
         ? std::string(".*")
         : std::string("[^") + Pathspec::kSeparator + "]*";
}


std::string
  PathspecElementPattern::WildcardSubPattern::GenerateGlobString() const
{
  return "*";
}


bool PathspecElementPattern::WildcardSubPattern::Compare(
  const SubPattern *other) const
{
  return other->IsWildcard();
}


std::string
  PathspecElementPattern::PlaceholderSubPattern::GenerateRegularExpression(
  const bool is_relaxed) const
{
  // Note: strict and relaxed regex are the same!
  return std::string("[^") + Pathspec::kSeparator + "]";
}

std::string
  PathspecElementPattern::PlaceholderSubPattern::GenerateGlobString() const
{
  return "?";
}

bool PathspecElementPattern::PlaceholderSubPattern::Compare(
  const SubPattern *other) const
{
  return other->IsPlaceholder();
}
