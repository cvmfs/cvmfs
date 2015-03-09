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
  virtual ~InputSanitizer() { }

  std::string Filter(const std::string &input) const;
  bool IsValid(const std::string &input) const;
 private:
  virtual bool Sanitize(const std::string &input, std::string *filtered_output)
    const;
  std::vector<CharRange> valid_ranges_;
};


class AlphaNumSanitizer : public InputSanitizer {
 public:
  AlphaNumSanitizer() : InputSanitizer("az AZ 09") { }
  virtual ~AlphaNumSanitizer() { }
};


class RepositorySanitizer : public InputSanitizer {
 public:
  RepositorySanitizer() : InputSanitizer("az AZ 09 - _ .") { }
  virtual ~RepositorySanitizer() { }
};


class IntegerSanitizer : public InputSanitizer {
 public:
  IntegerSanitizer() : InputSanitizer("09") { }
  virtual ~IntegerSanitizer() { }
};

}  // namespace sanitizer

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_SANITIZER_H_
