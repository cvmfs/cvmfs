/**
 * This file is part of the CernVM File System.
 */

#include "pathspec.h"

#include <iostream> // TODO: remove
#include <cassert>

#include "../smalloc.h"
#include "../logging.h"

Pathspec::Pathspec(const std::string &spec) :
  regex_compiled_(false),
  glob_string_compiled_(false),
  glob_string_sequence_compiled_(false),
  valid_(true),
  absolute_(false)
{
  Parse(spec);
  if (patterns_.size() == 0) {
    valid_ = false;
  }

        ElementPatterns::const_iterator i    = patterns_.begin();
  const ElementPatterns::const_iterator iend = patterns_.end();
  for (; i != iend; ++i) {
    if (!i->IsValid()) {
      valid_ = false;
    }
  }
}


void Pathspec::Parse(const std::string &spec) {
        std::string::const_iterator itr = spec.begin();
  const std::string::const_iterator end = spec.end();

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

bool Pathspec::IsMatching(const std::string &query_path) const {
  assert (IsValid());

  if (query_path.empty()) {
    return false;
  }

  const bool query_is_absolute = (query_path[0] == kSeparator);
  return (! query_is_absolute || this->IsAbsolute()) &&
         IsPathspecMatching(query_path);
}

bool Pathspec::IsPathspecMatching(const std::string &query_path) const {
  regex_t *regex = GetRegularExpression();
  const char *path = query_path.c_str();
  const int retval = regexec(regex, path, 0, NULL, 0);

  if (retval != 0 && retval != REG_NOMATCH) {
    PrintRegularExpressionError(retval);
  }

  return (retval == 0);
}

regex_t* Pathspec::GetRegularExpression() const {
  if (! regex_compiled_) {
    const std::string regex = GenerateRegularExpression();
    LogCvmfs(kLogPathspec, kLogDebug, "compiled regex: %s", regex.c_str());

    regex_ = (regex_t*)smalloc(sizeof(regex_t));
    const int flags = REG_NOSUB | REG_NEWLINE | REG_EXTENDED;
    const int retval = regcomp(regex_, regex.c_str(), flags);
    regex_compiled_ = true;

    if (retval != 0) {
      PrintRegularExpressionError(retval);
      assert (false && "failed to compile regex");
    }
  }

  return regex_;
}

std::string Pathspec::GenerateRegularExpression() const {
  // start matching at the first character
  std::string regex = "^";

  // absolute paths require a / in the beginning
  if (IsAbsolute()) {
    regex += kSeparator;
  }

  // concatenate the regular expressions of the compiled path elements
        ElementPatterns::const_iterator i    = patterns_.begin();
  const ElementPatterns::const_iterator iend = patterns_.end();
  for (; i != iend; ++i) {
    regex += i->GenerateRegularExpression();
    if (i + 1 != iend) {
      regex += kSeparator;
    }
  }

  // a path might end with a trailing slash
  // (pathspec does not distinguish files and directories)
  regex += kSeparator;
  regex += "?$";

  return regex;
}

bool Pathspec::operator==(const Pathspec &other) const {
  if (patterns_.size() != other.patterns_.size() ||
      IsValid()        != other.IsValid()        ||
      IsAbsolute()     != other.IsAbsolute()) {
    return false;
  }

        ElementPatterns::const_iterator i    = patterns_.begin();
  const ElementPatterns::const_iterator iend = patterns_.end();
        ElementPatterns::const_iterator j    = other.patterns_.begin();
  const ElementPatterns::const_iterator jend = other.patterns_.end();

  for (; i != iend && j != jend; ++i, ++j) {
    if (*i != *j) {
      return false;
    }
  }

  return true;
}

void Pathspec::PrintRegularExpressionError(const int error_code) const {
  assert (regex_compiled_);
  const size_t errbuf_size = 1024;
  char error[errbuf_size];
  regerror(error_code, regex_, error, errbuf_size);
  LogCvmfs(kLogPathspec, kLogStderr, "RegEx Error: %d - %s", error_code, error);
}

const Pathspec::GlobStringSequence& Pathspec::GetGlobStringSequence() const {
  if (! glob_string_sequence_compiled_) {
    GenerateGlobStringSequence();
    glob_string_sequence_compiled_ = true;
  }
  return glob_string_sequence_;
}


void Pathspec::GenerateGlobStringSequence() const {
  assert (glob_string_sequence_.empty());
        ElementPatterns::const_iterator i    = patterns_.begin();
  const ElementPatterns::const_iterator iend = patterns_.end();
  for (; i != iend; ++i) {
    const std::string glob_string = i->GenerateGlobString();
    glob_string_sequence_.push_back(glob_string);
  }
}


const std::string& Pathspec::GetGlobString() const {
  if (! glob_string_compiled_) {
    GenerateGlobString();
    glob_string_compiled_ = true;
  }
  return glob_string_;
}


void Pathspec::GenerateGlobString() const {
  assert (glob_string_.empty());

  bool is_first = true;
  const GlobStringSequence &seq = GetGlobStringSequence();
        GlobStringSequence::const_iterator i    = seq.begin();
  const GlobStringSequence::const_iterator iend = seq.end();
  for (; i != iend; ++i) {
    if (! is_first || IsAbsolute()) {
      glob_string_ += kSeparator;
    }
    glob_string_ += *i;
    is_first = false;
  }
}
