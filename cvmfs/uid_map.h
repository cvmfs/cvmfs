/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UID_MAP_H_
#define CVMFS_UID_MAP_H_

#include <cerrno>
#include <map>
#include <vector>
#include <sys/types.h>

#include "util.h"

template <typename T>
class IntegerMap {
 public:
  typedef T                                       key_type;
  typedef T                                       value_type;
  typedef typename std::map<key_type, value_type> map_type;

 public:
  IntegerMap()
    : valid_(true)
    , has_default_value_(false)
    , default_value_(T(0)) {}

  void Set(const T k, const T v) { map_[k] = v; }
  void SetDefault(const T v) {
    has_default_value_ = true;
    default_value_     = v;
  }

  bool Read(const std::string &path) {
    valid_ = ReadFromFile(path);
    return IsValid();
  }

  bool Contains(const T k) const {
    assert (IsValid());
    return map_.find(k) != map_.end();
  }

  T Map(const T k) const {
    assert (IsValid());
    typename map_type::const_iterator i = map_.find(k);
    if (i != map_.end()) {
      return i->second;
    }

    return (HasDefault())
      ? default_value_
      : T(0);
  }

  bool   IsValid()    const { return valid_;             }
  bool   HasDefault() const { return has_default_value_; }
  size_t RuleCount()  const { return map_.size();        }

 protected:
  bool ReadFromFile(const std::string &path) {
    FILE *fmap = fopen(path.c_str(), "r");
    if (!fmap) {
      LogCvmfs(kLogUtility, kLogDebug, "failed to open %s (errno: %d)",
               path.c_str(), errno);
      return false;
    }

    std::string line;
    unsigned int line_number = 0;
    while (GetLineFile(fmap, &line)) {
      ++line_number;
      line = Trim(line);
      if (line.empty() || line[0] == '#') {
        continue;
      }

      std::vector<std::string> components = SplitString(line, ' ');
      FilterEmptyStrings(components);
      if (components.size() != 2) {
        fclose(fmap);
        LogCvmfs(kLogUtility, kLogDebug, "failed to read line %d in %s",
                 line_number, path.c_str());
        return false;
      }

      value_type to = String2Uint64(components[1]);
      if (components[0] == "*") {
        SetDefault(to);
        continue;
      }

      key_type from = String2Uint64(components[0]);
      Set(from, to);
    }

    fclose(fmap);
    return true;
  }

  void FilterEmptyStrings(std::vector<std::string> &vec) const {
          std::vector<std::string>::iterator       i    = vec.begin();
    const std::vector<std::string>::const_iterator iend = vec.end();
    for (; i != iend ;) {
      i = (i->empty()) ? vec.erase(i) : i + 1;
    }
  }

 private:
  bool      valid_;
  map_type  map_;

  bool      has_default_value_;
  T         default_value_;
};

typedef IntegerMap<uid_t> UidMap;
typedef IntegerMap<gid_t> GidMap;

#endif // CVMFS_UTIL_H_
