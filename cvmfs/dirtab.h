/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DIRTAB_H_
#define CVMFS_DIRTAB_H_

#include <string>
#include <istream>

#include "pathspec/pathspec.h"

namespace catalog {

class Dirtab {
 public:
  static const char kCommentMarker  = '#';
  static const char kNegationMarker = '!';

 public:
  struct Rule {
    Rule(const Pathspec &pathspec, const bool is_negation) :
      pathspec(pathspec), is_negation(is_negation) {}
    Pathspec  pathspec;
    bool      is_negation;
  };

  typedef std::vector<Rule> Rules;

 public:
  Dirtab();
  Dirtab(const std::string &dirtab_path);
  bool Parse(const std::string &dirtab);

  bool IsMatching(const std::string &path) const;
  bool IsOpposing(const std::string &path) const;

  const Rules& positive_rules() const { return positive_rules_; }
  const Rules& negative_rules() const { return negative_rules_; }

  size_t RuleCount() const { return NegativeRuleCount() + PositiveRuleCount(); }
  size_t NegativeRuleCount() const { return negative_rules_.size(); }
  size_t PositiveRuleCount() const { return positive_rules_.size(); }
  bool   IsValid() const { return valid_; }

 protected:
  bool Parse(std::istream &dirtab);
  bool ParseLine(const std::string &line);
  void AddRule(const Rule &rule);

 private:
  void SkipWhitespace(      std::string::const_iterator &itr,
                      const std::string::const_iterator &end) const {
    for (; itr != end && *itr == ' '; ++itr);
  }
  bool CheckRuleValidity() const;

 private:
  bool  valid_;
  Rules positive_rules_;
  Rules negative_rules_;
};

} // namespace catalog

#endif  // CVMFS_DIRTAB_H_

