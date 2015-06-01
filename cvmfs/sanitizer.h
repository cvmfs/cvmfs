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

 protected:
  bool Sanitize(const std::string &input, std::string *filtered_output) const {
    return Sanitize(input.begin(), input.end(), filtered_output);
  }
  virtual bool Sanitize(std::string::const_iterator   begin,
                        std::string::const_iterator   end,
                        std::string                  *filtered_output) const;
  bool CheckRanges(const char chr) const;

 private:
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


class PositiveIntegerSanitizer : public InputSanitizer {
 public:
  PositiveIntegerSanitizer() : InputSanitizer("09") { }

 protected:
  virtual bool Sanitize(std::string::const_iterator   begin,
                        std::string::const_iterator   end,
                        std::string                  *filtered_output) const;
};

class IntegerSanitizer : public PositiveIntegerSanitizer {
 public:
  IntegerSanitizer() : PositiveIntegerSanitizer() { }
  virtual ~IntegerSanitizer() { }

 protected:
  virtual bool Sanitize(std::string::const_iterator   begin,
                        std::string::const_iterator   end,
                        std::string                  *filtered_output) const;
};

}  // namespace sanitizer

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_SANITIZER_H_
