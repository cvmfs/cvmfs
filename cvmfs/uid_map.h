/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UID_MAP_H_
#define CVMFS_UID_MAP_H_

#include <sys/types.h>

#include <cerrno>
#include <map>
#include <string>
#include <vector>

#include "logging.h"
#include "sanitizer.h"
#include "util.h"

/**
 * This reads a mapping file of (roughly) the following format:
 * +-------------------------------------------------------------+
 * | user_id.map                                                 |
 * | ~~~~~~~~~~~                                                 |
 * |                                                             |
 * | # map UIDs 137 and 138 to 1000 (I am a comment by the way)  |
 * | 137  1000                                                   |
 * | 138  1000                                                   |
 * |                                                             |
 * | # swap two UIDs                                             |
 * | 101  5                                                      |
 * | 5    101                                                    |
 * |                                                             |
 * | # map everything else to root (wildcard)                    |
 * | * 0                                                         |
 * +-------------------------------------------------------------+
 *
 * These files are intended for the definition of UID and GID mappings both on
 * the client and the server side of CernVM-FS.
 *
 * The class takes care of managing these mappings and can be initialised via
 * a file read from disk or programmatically through the class's public API.
 * When reading from a file, simple consistency checks are performed to ensure
 * proper functionality.
 */
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

  /**
   * Define a mapping from k to v
   * @param k  map the given value to v
   * @param v  the value given in k is mapped to this
   **/
  void Set(const T k, const T v) { map_[k] = v; }

  /**
   * Sets a default (or fallback) value to be used if no other mapping rule fits
   * Note: A previously defined default value is overwritten.
   * @param v  the value to be used as a fallback in Map()
   */
  void SetDefault(const T v) {
    has_default_value_ = true;
    default_value_     = v;
  }

  /**
   * Reads mapping rules from a provided file path. The file format is discussed
   * in the class description above.
   * Note: If a read failure occurs the IntegerMap<> is declared invalid and
   *       must not be used anymore.
   *
   * @param path  the file path to be read
   * @return      true if the file was successfully read
   */
  bool Read(const std::string &path) {
    valid_ = ReadFromFile(path);
    return IsValid();
  }

  /**
   * Checks if a mapping rule for a given value is available
   * @param k  the value to be checked
   * @return   true if a mapping rule for k exists
   */
  bool Contains(const T k) const {
    assert(IsValid());
    return map_.find(k) != map_.end();
  }

  /**
   * Applies the mapping rules inside this IntegerMap<> to the given value.
   * @param k  the value to be mapped
   * @return   the result of the mapping rule application (might be the default)
   */
  T Map(const T k) const {
    assert(IsValid());
    typename map_type::const_iterator i = map_.find(k);
    if (i != map_.end()) {
      return i->second;
    }

    return (HasDefault())
      ? default_value_
      : k;
  }

  bool HasEffect() const {
    return (map_.size() != 0) || has_default_value_;
  }

  bool   IsEmpty()    const { return map_.size() == 0;   }
  bool   IsValid()    const { return valid_;             }
  bool   HasDefault() const { return has_default_value_; }
  size_t RuleCount()  const { return map_.size();        }

  T GetDefault() const { assert(has_default_value_); return default_value_; }
  const map_type& GetRuleMap() const { return map_; }

 protected:
  bool ReadFromFile(const std::string &path) {
    FILE *fmap = fopen(path.c_str(), "r");
    if (!fmap) {
      LogCvmfs(kLogUtility, kLogDebug, "failed to open %s (errno: %d)",
               path.c_str(), errno);
      return false;
    }

    sanitizer::IntegerSanitizer int_sanitizer;

    std::string line;
    unsigned int line_number = 0;
    while (GetLineFile(fmap, &line)) {
      ++line_number;
      line = Trim(line);
      if (line.empty() || line[0] == '#') {
        continue;
      }

      std::vector<std::string> components = SplitString(line, ' ');
      FilterEmptyStrings(&components);
      if (components.size() != 2                ||
          !int_sanitizer.IsValid(components[1]) ||
          (components[0] != "*" && !int_sanitizer.IsValid(components[0]))) {
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

  void FilterEmptyStrings(std::vector<std::string> *vec) const {
          std::vector<std::string>::iterator       i    = vec->begin();
    const std::vector<std::string>::const_iterator iend = vec->end();
    for (; i != iend ;) {
      i = (i->empty()) ? vec->erase(i) : i + 1;
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

#endif  // CVMFS_UID_MAP_H_
