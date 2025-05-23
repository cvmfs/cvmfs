/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SANITIZER_H_
#define CVMFS_SANITIZER_H_

#include <string>
#include <vector>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace sanitizer {

class CharRange {
 public:
  CharRange(const char range_begin, const char range_end);
  explicit CharRange(const char single_char);
  bool InRange(const char c) const;

 private:
  char range_begin_;
  char range_end_;
};


class InputSanitizer {
 public:
  // whitelist is of the form "az AZ _ - 09"
  // Any other format will abort the program
  explicit InputSanitizer(const std::string &whitelist);
  InputSanitizer(const std::string &whitelist, int max_length);
  virtual ~InputSanitizer() { }

  std::string Filter(const std::string &input) const;
  bool IsValid(const std::string &input) const;

 protected:
  bool Sanitize(const std::string &input, std::string *filtered_output) const {
    return Sanitize(input.begin(), input.end(), filtered_output);
  }
  virtual bool Sanitize(std::string::const_iterator begin,
                        std::string::const_iterator end,
                        std::string *filtered_output) const;
  bool CheckRanges(const char chr) const;

 private:
  void InitValidRanges(const std::string &whitelist);

  int max_length_;
  std::vector<CharRange> valid_ranges_;
};


class AlphaNumSanitizer : public InputSanitizer {
 public:
  AlphaNumSanitizer() : InputSanitizer("az AZ 09") { }
};


class UuidSanitizer : public InputSanitizer {
 public:
  UuidSanitizer() : InputSanitizer("af AF 09 -") { }
};


class CacheInstanceSanitizer : public InputSanitizer {
 public:
  CacheInstanceSanitizer() : InputSanitizer("az AZ 09 _") { }
};


class RepositorySanitizer : public InputSanitizer {
 public:
  RepositorySanitizer() : InputSanitizer("az AZ 09 - _ .", 60) { }
};


class AuthzSchemaSanitizer : public InputSanitizer {
 public:
  AuthzSchemaSanitizer() : InputSanitizer("az AZ 09 - _ .") { }
};


// Also update is_valid_branch in cvmfs_server
class BranchSanitizer : public InputSanitizer {
 public:
  BranchSanitizer() : InputSanitizer("az AZ 09 - _ . @ /") { }
};


class TagSanitizer : public InputSanitizer {
 public:
  TagSanitizer() : InputSanitizer("az AZ 09 - _ . / :") { }
};


class IntegerSanitizer : public InputSanitizer {
 public:
  IntegerSanitizer() : InputSanitizer("09") { }

 protected:
  virtual bool Sanitize(std::string::const_iterator begin,
                        std::string::const_iterator end,
                        std::string *filtered_output) const;
};


class PositiveIntegerSanitizer : public IntegerSanitizer {
 public:
  PositiveIntegerSanitizer() : IntegerSanitizer() { }

 protected:
  virtual bool Sanitize(std::string::const_iterator begin,
                        std::string::const_iterator end,
                        std::string *filtered_output) const;
};


/**
 * Accepts both normal base64 and url conformant base64.
 */
class Base64Sanitizer : public InputSanitizer {
 public:
  Base64Sanitizer() : InputSanitizer("az AZ 09 + / - _ =") { }
};

/**
 * There could be more on the whitelist but this is already sufficient for the
 * octopus web service.  It includes the whitelist for valid repositories.
 */
class UriSanitizer : public InputSanitizer {
 public:
  UriSanitizer() : InputSanitizer("az AZ 09 . - _ /") { }
};

}  // namespace sanitizer

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_SANITIZER_H_
