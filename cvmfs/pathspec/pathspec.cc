/**
 * This file is part of the CernVM File System.
 */

#include "pathspec.h"

#include <cassert>

#include "../logging.h"
#include "../smalloc.h"

Pathspec::Pathspec(const std::string &spec) :
  regex_compiled_(false),
  regex_(NULL),
  relaxed_regex_compiled_(false),
  relaxed_regex_(NULL),
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

Pathspec::Pathspec(const Pathspec &other) :
  patterns_(other.patterns_),
  regex_compiled_(false),          // compiled regex structure cannot be
  regex_(NULL),                    // duplicated and needs to be re-compiled
  relaxed_regex_compiled_(false),  // Note: the copy-constructed object will
  relaxed_regex_(NULL),            //       perform a lazy evaluation again
  glob_string_compiled_(other.glob_string_compiled_),
  glob_string_(other.glob_string_),
  glob_string_sequence_compiled_(other.glob_string_sequence_compiled_),
  glob_string_sequence_(other.glob_string_sequence_),
  valid_(other.valid_),
  absolute_(other.absolute_) {}

Pathspec::~Pathspec() {
  DestroyRegularExpressions();
}

Pathspec& Pathspec::operator=(const Pathspec &other) {
  if (this != &other) {
    DestroyRegularExpressions();  // see: copy c'tor for details
    patterns_ = other.patterns_;

    glob_string_compiled_ = other.glob_string_compiled_;
    glob_string_          = other.glob_string_;

    glob_string_sequence_compiled_ = other.glob_string_sequence_compiled_;
    glob_string_sequence_          = other.glob_string_sequence_;

    valid_    = other.valid_;
    absolute_ = other.absolute_;
  }

  return *this;
}


void Pathspec::Parse(const std::string &spec) {
  // parsing is done using std::string iterators to walk through the entire
  // pathspec parameter. Thus, all parsing methods receive references to these
  // iterators and increment itr as they pass along.
        std::string::const_iterator itr = spec.begin();
  const std::string::const_iterator end = spec.end();

  absolute_ = (*itr == kSeparator);
  while (itr != end) {
    if (*itr == kSeparator) {
      ++itr;
      continue;
    }
    ParsePathElement(end, &itr);
  }
}

void Pathspec::ParsePathElement(
  const std::string::const_iterator &end,
  std::string::const_iterator  *itr
) {
  // find the end of the current pattern element (next directory boundary)
  const std::string::const_iterator begin_element = *itr;
  while (*itr != end && **itr != kSeparator) {
    ++(*itr);
  }
  const std::string::const_iterator end_element = *itr;

  // create a PathspecElementPattern out of this directory description
  patterns_.push_back(PathspecElementPattern(begin_element, end_element));
}

bool Pathspec::IsMatching(const std::string &query_path) const {
  assert(IsValid());

  if (query_path.empty()) {
    return false;
  }

  const bool query_is_absolute = (query_path[0] == kSeparator);
  return (!query_is_absolute || this->IsAbsolute()) &&
         IsPathspecMatching(query_path);
}

bool Pathspec::IsMatchingRelaxed(const std::string &query_path) const {
  assert(IsValid());

  if (query_path.empty()) {
    return false;
  }

  return IsPathspecMatchingRelaxed(query_path);
}

bool Pathspec::IsPathspecMatching(const std::string &query_path) const {
  return ApplyRegularExpression(query_path, GetRegularExpression());
}

bool Pathspec::IsPathspecMatchingRelaxed(const std::string &query_path) const {
  return ApplyRegularExpression(query_path, GetRelaxedRegularExpression());
}

bool Pathspec::ApplyRegularExpression(const std::string  &query_path,
                                            regex_t      *regex) const {
  const char *path = query_path.c_str();
  const int retval = regexec(regex, path, 0, NULL, 0);

  if (retval != 0 && retval != REG_NOMATCH) {
    PrintRegularExpressionError(retval);
  }

  return (retval == 0);
}

regex_t* Pathspec::GetRegularExpression() const {
  if (!regex_compiled_) {
    const bool is_relaxed = false;
    const std::string regex = GenerateRegularExpression(is_relaxed);
    LogCvmfs(kLogPathspec, kLogDebug, "compiled regex: %s", regex.c_str());

    regex_ = CompileRegularExpression(regex);
    regex_compiled_ = true;
  }

  return regex_;
}

regex_t* Pathspec::GetRelaxedRegularExpression() const {
  if (!relaxed_regex_compiled_) {
    const bool is_relaxed = true;
    const std::string regex = GenerateRegularExpression(is_relaxed);
    LogCvmfs(kLogPathspec, kLogDebug, "compiled relaxed regex: %s",
             regex.c_str());

    relaxed_regex_ = CompileRegularExpression(regex);
    relaxed_regex_compiled_ = true;
  }

  return relaxed_regex_;
}

std::string Pathspec::GenerateRegularExpression(const bool is_relaxed) const {
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
    regex += i->GenerateRegularExpression(is_relaxed);
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

regex_t* Pathspec::CompileRegularExpression(const std::string &regex) const {
  regex_t *result = reinterpret_cast<regex_t *>(smalloc(sizeof(regex_t)));
  const int flags = REG_NOSUB | REG_NEWLINE | REG_EXTENDED;
  const int retval = regcomp(result, regex.c_str(), flags);

  if (retval != 0) {
    PrintRegularExpressionError(retval);
    assert(false && "failed to compile regex");
  }

  return result;
}

void Pathspec::DestroyRegularExpressions() {
  if (regex_compiled_) {
    assert(regex_ != NULL);
    regfree(regex_);
    regex_          = NULL;
    regex_compiled_ = false;
  }

  if (relaxed_regex_compiled_) {
    assert(relaxed_regex_ != NULL);
    regfree(relaxed_regex_);
    relaxed_regex_          = NULL;
    relaxed_regex_compiled_ = false;
  }
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
  assert(regex_compiled_);
  const size_t errbuf_size = 1024;
  char error[errbuf_size];
  regerror(error_code, regex_, error, errbuf_size);
  LogCvmfs(kLogPathspec, kLogStderr, "RegEx Error: %d - %s", error_code, error);
}

const Pathspec::GlobStringSequence& Pathspec::GetGlobStringSequence() const {
  if (!glob_string_sequence_compiled_) {
    GenerateGlobStringSequence();
    glob_string_sequence_compiled_ = true;
  }
  return glob_string_sequence_;
}


void Pathspec::GenerateGlobStringSequence() const {
  assert(glob_string_sequence_.empty());
        ElementPatterns::const_iterator i    = patterns_.begin();
  const ElementPatterns::const_iterator iend = patterns_.end();
  for (; i != iend; ++i) {
    const std::string glob_string = i->GenerateGlobString();
    glob_string_sequence_.push_back(glob_string);
  }
}


const std::string& Pathspec::GetGlobString() const {
  if (!glob_string_compiled_) {
    GenerateGlobString();
    glob_string_compiled_ = true;
  }
  return glob_string_;
}


void Pathspec::GenerateGlobString() const {
  assert(glob_string_.empty());

  bool is_first = true;
  const GlobStringSequence &seq = GetGlobStringSequence();
        GlobStringSequence::const_iterator i    = seq.begin();
  const GlobStringSequence::const_iterator iend = seq.end();
  for (; i != iend; ++i) {
    if (!is_first || IsAbsolute()) {
      glob_string_ += kSeparator;
    }
    glob_string_ += *i;
    is_first = false;
  }
}
