/**
 * This file is part of the CernVM File System.
 */

#include "dirtab.h"

#include <fstream>
#include <sstream>

#include "util.h"

using namespace catalog;

Dirtab::Dirtab() : valid_(true) {}


Dirtab::Dirtab(const std::string &dirtab_path) {
  if (! FileExists(dirtab_path)) {
    LogCvmfs(kLogCatalog, kLogStderr, "Cannot find dirtab at '%s'",
             dirtab_path.c_str());
    valid_ = false;
    return;
  }

  std::ifstream dirtab(dirtab_path.c_str());
  if (!dirtab) {
    LogCvmfs(kLogCatalog, kLogStderr, "Cannot open dirtab for reading at '%s'",
             dirtab_path.c_str());
    valid_ = false;
    return;
  }

  valid_ = Parse(dirtab);
  dirtab.close();
}


bool Dirtab::Parse(const std::string &dirtab) {
  std::istringstream iss(dirtab);
  const bool parse_success = Parse(iss);
  valid_ = parse_success;
  return parse_success;
}


bool Dirtab::Parse(std::istream &dirtab) {
  std::string line;
  bool all_valid = true;
  while (std::getline(dirtab, line)) {
    if (! ParseLine(line)) {
      all_valid = false;
    }
  }
  return all_valid && CheckRuleValidity();
}


bool Dirtab::ParseLine(const std::string &line) {
        std::string::const_iterator itr  = line.begin();
  const std::string::const_iterator iend = line.end();
  bool negation = false;

  // parse preamble
  SkipWhitespace(itr, iend);
  if (*itr == Dirtab::kCommentMarker) {
    return true;
  } else if (*itr == Dirtab::kNegationMarker) {
    negation = true;
    ++itr;
    SkipWhitespace(itr, iend);
  }

  // extract and parse pathspec
  std::string pathspec_str(itr, iend);
  if (pathspec_str.empty()) {
    return true;
  }
  Pathspec pathspec(pathspec_str);
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
    assert (! p->is_negation);
          Rules::const_iterator n    = negative_rules_.begin();
    const Rules::const_iterator nend = negative_rules_.end();
    for (; n != nend; ++n) {
      assert (n->is_negation);
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
    assert (! p->is_negation);
    if (p->pathspec.IsMatching(path)) {
      has_positive_match = true;
      break;
    }
  }

  // no positive match, no match in general!
  if (! has_positive_match) {
    return false;
  }

  // check for negative matches (meaning no match in general)
        Rules::const_iterator n    = negative_rules_.begin();
  const Rules::const_iterator nend = negative_rules_.end();
  for (; n != nend; ++n) {
    assert (n->is_negation);
    if (n->pathspec.IsMatching(path)) {
      return false;
    }
  }

  // all good, path is matching dirtab
  return true;
}
