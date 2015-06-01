/**
 * This file is part of the CernVM File System.
 *
 * Provides input data sanitizer in the form of whitelist of character ranges.
 */

#include "cvmfs_config.h"
#include "sanitizer.h"

#include <cassert>

using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace sanitizer {

CharRange::CharRange(const char range_begin, const char range_end) {
  range_begin_ = range_begin;
  range_end_ = range_end;
}


CharRange::CharRange(const char single_char) {
  range_begin_ = range_end_ = single_char;
}


bool CharRange::InRange(const char c) const {
  return (c >= range_begin_) && (c <= range_end_);
}


//------------------------------------------------------------------------------


InputSanitizer::InputSanitizer(const string &whitelist) {
  // Parse the whitelist
  const unsigned length = whitelist.length();
  unsigned pickup_pos = 0;
  for (unsigned i = 0; i < length; ++i) {
    if ((i+1 >= length) || (whitelist[i+1] == ' ') || (i == length-1)) {
      const string range = whitelist.substr(pickup_pos, i-pickup_pos+1);
      switch (range.length()) {
        case 1:
          valid_ranges_.push_back(CharRange(range[0]));
          break;
        case 2:
          valid_ranges_.push_back(CharRange(range[0], range[1]));
          break;
        default:
          assert(false);
      }
      ++i;
      pickup_pos = i+1;
    }
  }
}


bool InputSanitizer::Sanitize(
                          std::string::const_iterator   begin,
                          std::string::const_iterator   end,
                          std::string                  *filtered_output) const {
  bool is_sane = true;
  for (; begin != end; ++begin) {
    if (CheckRanges(*begin))
      filtered_output->push_back(*begin);
    else
      is_sane = false;
  }
  return is_sane;
}


bool InputSanitizer::CheckRanges(const char chr) const {
  for (unsigned j = 0; j < valid_ranges_.size(); ++j) {
    if (valid_ranges_[j].InRange(chr)) {
      return true;
    }
  }
  return false;
}


string InputSanitizer::Filter(const std::string &input) const {
  string filtered_output;
  Sanitize(input, &filtered_output);
  return filtered_output;
}


bool InputSanitizer::IsValid(const std::string &input) const {
  string dummy;
  return Sanitize(input, &dummy);
}


bool IntegerSanitizer::Sanitize(
                          std::string::const_iterator   begin,
                          std::string::const_iterator   end,
                          std::string                  *filtered_output) const {
  if (std::distance(begin, end) == 0) {
    return false;
  }

  if (*begin == '-') {
    // minus is allowed as the first character!
    filtered_output->push_back('-');
    begin++;
  }

  return InputSanitizer::Sanitize(begin, end, filtered_output);
}


bool PositiveIntegerSanitizer::Sanitize(
                          std::string::const_iterator   begin,
                          std::string::const_iterator   end,
                          std::string                  *filtered_output) const {
  if (std::distance(begin, end) == 0) {
    return false;
  }

  return InputSanitizer::Sanitize(begin, end, filtered_output);
}

}  // namespace sanitizer

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
