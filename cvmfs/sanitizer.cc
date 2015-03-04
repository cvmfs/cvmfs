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


bool InputSanitizer::Sanitize(const std::string &input,
                              std::string *filtered_output) const
{
  *filtered_output = "";
  bool is_sane = true;
  for (unsigned i = 0; i < input.length(); ++i) {
    bool valid_char = false;
    for (unsigned j = 0; j < valid_ranges_.size(); ++j) {
      if (valid_ranges_[j].InRange(input[i])) {
        valid_char = true;
        break;
      }
    }
    if (valid_char)
      filtered_output->push_back(input[i]);
    else
      is_sane = false;
  }
  return is_sane;
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

}  // namespace sanitizer

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
