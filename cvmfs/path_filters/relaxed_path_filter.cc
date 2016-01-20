/**
 * This file is part of the CernVM File System.
 */

#include <string>

#include "relaxed_path_filter.h"

using namespace catalog;  // NOLINT

RelaxedPathFilter *RelaxedPathFilter::Create(const std::string &dirtab_path) {
  RelaxedPathFilter *dt = new RelaxedPathFilter();
  dt->Open(dirtab_path);
  return dt;
}


bool RelaxedPathFilter::IsMatching(const std::string &path) const {
  bool has_positive_match = Dirtab::IsMatching(path);
  if (!has_positive_match) {
    std::string current_path = path;
    while (current_path.length() > 0) {
      size_t new_length = current_path.find_last_of("/");
      current_path = current_path.substr(0, new_length);
      if (exact_dirtab_.IsMatching(current_path)) {
        has_positive_match = true;
        break;
      }
    }  // walk through sub paths
  }

  return has_positive_match && !IsOpposing(path);
}


bool RelaxedPathFilter::IsOpposing(const std::string &path) const {
  if (Dirtab::IsOpposing(path))
    return true;

  std::string current_path = path;
  while (current_path.length() > 0) {
    size_t new_length = current_path.find_last_of("/");
    current_path = current_path.substr(0, new_length);
    if (Dirtab::IsOpposing(current_path)) {
      return true;
    }
  }

  return false;
}


bool RelaxedPathFilter::Parse(const std::string &dirtab) {
  return Dirtab::Parse(dirtab) & exact_dirtab_.Parse(dirtab);
}


bool RelaxedPathFilter::ParsePathspec(const std::string &pathspec_str,
                                      bool negation) {
  if (negation) {
    return Dirtab::ParsePathspec(pathspec_str, true);
  }
  bool success = true;
  std::string current_pathspec_str(pathspec_str);
  while (current_pathspec_str.length() > 0) {
    if (!Dirtab::ParsePathspec(current_pathspec_str, false))
      success = false;
    size_t new_length = current_pathspec_str.find_last_of("/");
    current_pathspec_str = current_pathspec_str.substr(0, new_length);
  }

  return success;
}
