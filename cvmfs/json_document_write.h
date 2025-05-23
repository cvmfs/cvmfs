/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_JSON_DOCUMENT_WRITE_H_
#define CVMFS_JSON_DOCUMENT_WRITE_H_

#include <cstdio>
#include <string>
#include <vector>

#include "util/exception.h"
#include "util/string.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * This class is used for marshalling JSON objects to strings.
 *
 * When creating simple objects is sufficient to call the `Add()` methods to add
 * new key - values to the final JSON.
 *
 * When creating complex objects, (an object that contains another object) is
 * necessary to create first the nested object and then add it to the final
 * object with the `AddJsonObject`. This will take care of all the escaping.
 */
class JsonStringGenerator {
  enum JsonVariant {
    kString,
    kInteger,
    kFloat,
    kJsonObject
  };

  struct JsonEntry {
    JsonVariant variant;
    std::string key_escaped;
    std::string str_val_escaped;
    int64_t int_val;
    float float_val;

    JsonEntry(const std::string &key_escaped, const std::string &val)
        : variant(kString)
        , key_escaped(key_escaped)
        , str_val_escaped(val)
        , int_val(0)
        , float_val(0.0) { }

    JsonEntry(const std::string &key_escaped, const std::string &val,
              const JsonVariant variant)
        : variant(variant)
        , key_escaped(key_escaped)
        , str_val_escaped(val)
        , int_val(0)
        , float_val(0.0) { }

    JsonEntry(const std::string &key_escaped, const int val)
        : variant(kInteger)
        , key_escaped(key_escaped)
        , str_val_escaped()
        , int_val(val)
        , float_val(0.0) { }

    JsonEntry(const std::string &key_escaped, const float val)
        : variant(kFloat)
        , key_escaped(key_escaped)
        , str_val_escaped()
        , int_val(0)
        , float_val(val) { }

    JsonEntry(const std::string &key_escaped, const int64_t val)
        : variant(kInteger)
        , key_escaped(key_escaped)
        , str_val_escaped()
        , int_val(val)
        , float_val(0.0) { }

    std::string Format() const {
      switch (variant) {
        case kString:
          return "\"" + key_escaped + "\":\"" + str_val_escaped + "\"";
        case kInteger:
          return "\"" + key_escaped + "\":" + StringifyInt(int_val);
        case kFloat:
          return "\"" + key_escaped + "\":" + StringifyDouble(float_val);
        case kJsonObject:
          return "\"" + key_escaped + "\":" + str_val_escaped;
        default:
          PANIC(kLogStdout | kLogStderr, "JSON creation failed");
      }
    }
  };

 public:
  void Add(const std::string &key, const std::string &val) {
    const JsonEntry entry(Escape(key), Escape(val));
    entries.push_back(entry);
  }

  void Add(const std::string &key, const int val) {
    const JsonEntry entry(Escape(key), val);
    entries.push_back(entry);
  }

  void Add(const std::string &key, const float val) {
    const JsonEntry entry(Escape(key), val);
    entries.push_back(entry);
  }

  void Add(const std::string &key, const int64_t val) {
    const JsonEntry entry(Escape(key), val);
    entries.push_back(entry);
  }

  void AddJsonObject(const std::string &key, const std::string &json) {
    // we **do not escape** the value here
    const JsonEntry entry(Escape(key), json, kJsonObject);
    entries.push_back(entry);
  }

  std::string GenerateString() const {
    std::string output;

    output += "{";
    for (size_t i = 0u; i < this->entries.size(); ++i) {
      output += this->entries[i].Format();
      if (i < this->entries.size() - 1) {
        output += ',';
      }
    }
    output += std::string("}");
    return output;
  }

  void Clear() { entries.clear(); }

 private:
  // this escape procedure is not as complete as it should be.
  // we should manage ALL control chars from '\x00' to '\x1f'
  // however this are the one that we can expect to happen
  // More info: https://stackoverflow.com/a/33799784/869271
  const std::string Escape(const std::string &input) const {
    std::string result;
    result.reserve(input.size());
    for (size_t i = 0; i < input.size(); i++) {
      switch (input[i]) {
        case '"':
          result.append("\\\"");
          break;
        case '\\':
          result.append("\\\\");
          break;
        case '\b':
          result.append("\\b");
          break;
        case '\f':
          result.append("\\f");
          break;
        case '\n':
          result.append("\\n");
          break;
        case '\r':
          result.append("\\r");
          break;
        case '\t':
          result.append("\\t");
          break;
        default:
          result.push_back(input[i]);
          break;
      }
    }
    return result;
  }

  std::vector<JsonEntry> entries;
};

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_JSON_DOCUMENT_WRITE_H_
