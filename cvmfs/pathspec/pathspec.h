/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PATHSPEC_PATHSPEC_H_
#define CVMFS_PATHSPEC_PATHSPEC_H_

#include <regex.h>

#include <string>
#include <vector>

#include "pathspec_pattern.h"

/**
 * A Pathspec is an abstract description of a file path pattern.
 * Examples (adding a space in front of * - silence compiler warning):
 *    /foo/bar/ *.txt  - matches .txt files in /foo/bar
 *    /kernel/2.6.?   - matches directories like: /kernel/2.6.[0-9a-z]
 *    /test/ *_debug/ * - matches all files in /test/cvmfs_debug/ for example
 *
 * We are supporting both the wildcard (i.e. *) and the placeholder (i.e. ?)
 * symbol. Furthermore Pathspecs can be absolute (starting with /) or relative.
 *
 * Pathspecs are similar to unix glob strings for file system paths and can be
 * transforms into such strings or sequences of these (cut at directory
 * boundaries). This comes in handy when searching CVMFS catalog entries with a
 * Pathspec.
 * Note: sophisticated Pathspec based catalog lookup was not needed, yet. But it
 *       is implemented and not merged (see: reneme/feature-lookup_pathspec).
 *
 * Also inverse matches are possible by transforming a Pathspec into a regular
 * expression and matching path strings with it. There are two matching modes:
 *   IsMatching()        - Matches the exact path
 *                         (wildcards don't span directory boundaries)
 *   IsMatchingRelaxed() - Matches the path more relaxed
 *                         (comparable to shell pattern matching, wildcards
 *                          match any character including /)
 *
 * Internally a Pathspec is broken up into PathspecElementPatterns at the
 * directory boundaries. Have a look there for further details.
 *
 * For matching, Pathspecs need to be transformed either into a regular expres-
 * sion or GlobString(Sequence). These transformations are done lazily on first
 * request.
 */
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
  /**
   * Create a new Pathspec that represents the pattern handed in as a parameter.
   * Note: The parser will determine if the given pattern is valid and set a
   *       flag. After creating a Pathspec it should be checked if .IsValid()
   *
   * @param spec  the pathspec pattern to be parsed
   */
  explicit Pathspec(const std::string &spec);
  Pathspec(const Pathspec &other);
  // TODO(rmeusel): C++11 move constructor
  ~Pathspec();

  /**
   * Matches an exact path string. Directory boundaries are taken into accound
   * Say: wildcards do not match beyond a singly directory tree level.
   *
   * @param query_path   the path to be matched against this Pathstring
   * @return             true if the path matches
   */
  bool IsMatching(const std::string &query_path) const;

  /**
   * Matches path strings similar to shell pattern matching (case...esac).
   * Say: wildcards match any character including / and therefore can span over
   *      directory boundaries.
   *
   * @param query_path   the path to be matched against this Pathstring
   * @return             true if the path matches
   */
  bool IsMatchingRelaxed(const std::string &query_path) const;

  /**
   * Checks if the parsed Pathspec is valid and can be used.
   * @return   true if this Pathspec is valid
   */
  bool IsValid()    const { return valid_;    }

  /**
   * Checks if this Pathspec is defining an absolute path (i.e. starts with /)
   * @return   true if this Pathspec is absolute
   */
  bool IsAbsolute() const { return absolute_; }

  /**
   * Generates an ordered list of unix-like glob strings split on directory
   * boundaries. Can be used to traverse down into a directory tree along a
   * given Pathspec.
   *
   * @return  an ordered list of unixoid glob strings (usable in glob())
   */
  const GlobStringSequence& GetGlobStringSequence() const;

  /**
   * Generates a single glob string out of this Pathspec. This string can be
   * used in glob()
   *
   * @return  a unix-compatible glob string
   */
  const std::string& GetGlobString() const;

  Pathspec& operator=(const Pathspec &other);
  bool operator== (const Pathspec &other) const;
  bool operator!= (const Pathspec &other) const { return !(*this == other); }

  static bool IsSpecialChar(const char chr) {
    return (chr == kWildcard || chr == kPlaceholder);
  }

 protected:
  void Parse(const std::string &spec);
  void ParsePathElement(const std::string::const_iterator &end,
                        std::string::const_iterator *itr);

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

#endif  // CVMFS_PATHSPEC_PATHSPEC_H_
