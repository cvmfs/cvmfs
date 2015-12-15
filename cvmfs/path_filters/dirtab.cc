/**
 * This file is part of the CernVM File System.
 */

#include "dirtab.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>

#include "../util.h"

namespace catalog {

Dirtab::Dirtab() : valid_(true) {}


bool Dirtab::Open(const std::string &dirtab_path) {
  if (!FileExists(dirtab_path)) {
    LogCvmfs(kLogCatalog, kLogStderr, "Cannot find dirtab at '%s'",
             dirtab_path.c_str());
    valid_ = false;
    return valid_;
  }

  FILE *dirtab_file = fopen(dirtab_path.c_str(), "r");
  if (dirtab_file == NULL) {
    LogCvmfs(kLogCatalog, kLogStderr, "Cannot open dirtab for reading at '%s' "
                                      "(errno: %d)",
             dirtab_path.c_str(), errno);
    valid_ = false;
    return valid_;
  }

  valid_ = Parse(dirtab_file);
  fclose(dirtab_file);
  return valid_;
}

bool Dirtab::Parse(const std::string &dirtab) {
  valid_ = true;
  off_t line_offset = 0;
  while (line_offset < static_cast<off_t>(dirtab.size())) {
    std::string line = GetLineMem(dirtab.c_str() + line_offset,
                                  dirtab.size() - line_offset);
    line_offset += line.size() + 1;  // +1 == skipped \n
    if (!ParseLine(line)) {
      valid_ = false;
    }
  }
  valid_ = valid_ && CheckRuleValidity();
  return valid_;
}


bool Dirtab::Parse(FILE *dirtab_file) {
  valid_ = true;
  std::string line;
  while (GetLineFile(dirtab_file, &line)) {
    if (!ParseLine(line)) {
      valid_ = false;
    }
  }
  valid_ = valid_ && CheckRuleValidity();
  return valid_;
}


bool Dirtab::ParseLine(const std::string &line) {
  // line parsing is done using std::string iterators. Each parsing method ex-
  // pects an iterator and the end iterator. While parsing itr is constantly
  // incremented to walk through the given .cvmfsdirtab line.
        std::string::const_iterator itr  = line.begin();
  const std::string::const_iterator iend = line.end();
  bool negation = false;

  // parse preamble
  SkipWhitespace(iend, &itr);
  if (*itr == Dirtab::kCommentMarker) {
    return true;
  } else if (*itr == Dirtab::kNegationMarker) {
    negation = true;
    ++itr;
    SkipWhitespace(iend, &itr);
  }

  // extract and parse pathspec
  std::string pathspec_str(itr, iend);
  return this->ParsePathspec(pathspec_str, negation);
}

bool Dirtab::ParsePathspec(const std::string &pathspec_str, bool negation) {
  if (pathspec_str.empty()) {
    return true;
  }
  Pathspec pathspec(pathspec_str);

  // all generated Pathspecs need to be valid and positive rules must be
  // absolute. Otherwise the .cvmfsdirtab is not valid.
  if ( !pathspec.IsValid() ||
      (!negation && !pathspec.IsAbsolute())) {
    return false;
  }

  // create a new dirtab rule
  const Rule rule(pathspec, negation);
  AddRule(rule);
  return true;
}


void Dirtab::AddRule(const Rule &rule) {
  if (rule.is_negation) {
    negative_rules_.push_back(rule);
  } else {
    positive_rules_.push_back(rule);
  }
}


bool Dirtab::CheckRuleValidity() const {
  // check if there are contradicting positive and negative rules
        Rules::const_iterator p    = positive_rules_.begin();
  const Rules::const_iterator pend = positive_rules_.end();
  for (; p != pend; ++p) {
    assert(!p->is_negation);
          Rules::const_iterator n    = negative_rules_.begin();
    const Rules::const_iterator nend = negative_rules_.end();
    for (; n != nend; ++n) {
      assert(n->is_negation);
      if (p->pathspec == n->pathspec) {
        return false;
      }
    }
  }

  return true;
}


bool Dirtab::IsMatching(const std::string &path) const {
  // check if path has a positive match
  bool has_positive_match = false;
        Rules::const_iterator p    = positive_rules_.begin();
  const Rules::const_iterator pend = positive_rules_.end();
  for (; p != pend; ++p) {
    assert(!p->is_negation);
    if (p->pathspec.IsMatching(path)) {
      has_positive_match = true;
      break;
    }
  }

  return has_positive_match && !IsOpposing(path);
}


bool Dirtab::IsOpposing(const std::string &path) const {
        Rules::const_iterator n    = negative_rules_.begin();
  const Rules::const_iterator nend = negative_rules_.end();
  for (; n != nend; ++n) {
    assert(n->is_negation);
    if (n->pathspec.IsMatchingRelaxed(path)) {
      return true;
    }
  }

  return false;
}

}  // namespace catalog
