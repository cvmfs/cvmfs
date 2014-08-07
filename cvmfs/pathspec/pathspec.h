/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PATHSPEC_H_
#define CVMFS_PATHSPEC_H_

#include <string>
#include <vector>
#include <regex.h>

#include "pathspec_pattern.h"

class Pathspec {
 public:
  static const char kSeparator   = '/';
  static const char kEscaper     = '\\';
  static const char kWildcard    = '*';
  static const char kPlaceholder = '?';

 protected:
  typedef std::vector<PathspecElementPattern> ElementPatterns;

 public:
  typedef std::vector<std::string> GlobStringSequence;

 public:
  Pathspec(const std::string &spec);
  Pathspec(const Pathspec &other);
  // TODO: C++11 move constructor
  ~Pathspec();

  bool IsMatching(const std::string &query_path) const;
  bool IsMatchingRelaxed(const std::string &query_path) const;
  bool IsValid()    const { return valid_;    }
  bool IsAbsolute() const { return absolute_; }

  const GlobStringSequence& GetGlobStringSequence() const;
  const std::string&        GetGlobString() const;

  Pathspec& operator=(const Pathspec &other);
  bool operator==(const Pathspec &other) const;
  bool operator!=(const Pathspec &other) const { return ! (*this == other); }

  static bool IsSpecialChar(const char chr) {
    return (chr == kWildcard || chr == kPlaceholder);
  }

 protected:
  void Parse(const std::string &spec);
  void ParsePathElement(      std::string::const_iterator  &itr,
                        const std::string::const_iterator  &end);

  bool IsPathspecMatching(const std::string &query_path) const;
  bool IsPathspecMatchingRelaxed(const std::string &query_path) const;

  bool ApplyRegularExpression(const std::string  &query_path,
                                    regex_t      *regex) const;

  regex_t* GetRegularExpression() const;
  regex_t* GetRelaxedRegularExpression() const;

  std::string GenerateRegularExpression(const bool is_relaxed = false) const;
  regex_t* CompileRegularExpression(const std::string &regex) const;

  void PrintRegularExpressionError(const int error_code) const;

  void GenerateGlobStringSequence() const;
  void GenerateGlobString() const;

  void DestroyRegularExpressions();

 private:
  ElementPatterns             patterns_;

  mutable bool                regex_compiled_;
  mutable regex_t            *regex_;

  mutable bool                relaxed_regex_compiled_;
  mutable regex_t            *relaxed_regex_;

  mutable bool                glob_string_compiled_;
  mutable std::string         glob_string_;

  mutable bool                glob_string_sequence_compiled_;
  mutable GlobStringSequence  glob_string_sequence_;

  bool valid_;
  bool absolute_;
};

#endif  // CVMFS_PATHSPEC_H_
