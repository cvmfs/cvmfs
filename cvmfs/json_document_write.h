/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_JSON_DOCUMENT_WRITE_H_
#define CVMFS_JSON_DOCUMENT_WRITE_H_

#include <string>
#include <vector>

/**
 * This class is used for marshalling JSON objects to strings.
 * When adding an object, use the Quoted variant when adding a
 * string and the Unquoted variant when adding anything else.
 */
class JsonStringGenerator {
  enum JsonVariant { String, Integer, Float, JsonObject };

  struct JsonEntry {
    JsonVariant variant;
    std::string key;
    std::string str_val;
    int64_t int_val;
    float float_val;

    JsonEntry(std::string key, std::string val)
        : variant(String),
          key(key),
          str_val(val),
          int_val(0),
          float_val(0) {}

    JsonEntry(std::string key, int val)
        : variant(Integer),
          key(key),
          str_val(),
          int_val(val),
          float_val(0) {}

    JsonEntry(std::string key, float val)
        : variant(Float),
          key(key),
          str_val(),
          int_val(0),
          float_val(val) {}

    JsonEntry(std::string key, int64_t val)
        : variant(Integer),
          key(key),
          str_val(),
          int_val(val),
          float_val(0) {}

    const std::string format() const {
      char buffer[64];
      int rc = -1;
      switch (variant) {
        case String:
          return "\"" + key + "\":\"" + str_val + "\"";
        case Integer:
          rc = snprintf(buffer, sizeof(buffer), "%ld", int_val);
          assert(rc > 0);
          return "\"" + key + "\":" + std::string(buffer);
        case Float:
          rc = snprintf(buffer, sizeof(buffer), "%f", float_val);
          assert(rc > 0);
          return "\"" + key + "\":" + std::string(buffer);
        case JsonObject:
          return "\"" + key + "\":" + str_val;
        default:
          return "";
      }
    }
  };

 public:
  void Add(std::string key, std::string val) {
    JsonEntry entry(Escape(key), Escape(val));
    entries.push_back(entry);
  }

  void Add(std::string key, int val) {
    JsonEntry entry(Escape(key), val);
    entries.push_back(entry);
  }

  void Add(std::string key, float val) {
    JsonEntry entry(Escape(key), val);
    entries.push_back(entry);
  }

  void Add(std::string key, int64_t val) {
    JsonEntry entry(Escape(key), val);
    entries.push_back(entry);
  }

  void AddJsonObject(std::string key, std::string json) {
    // we **do not escape** the value here
    JsonEntry entry(Escape(key), json);
    entry.variant = JsonObject;
    entries.push_back(entry);
  }

  std::string GenerateString() const {
    std::string output;

    output += "{";
    for (size_t i = 0u; i < this->entries.size(); ++i) {
      output += this->entries[i].format();
      if (i < this->entries.size() - 1) {
        output += ',';
      }
    }
    output += std::string("}");
    return output;
  }

  void Clear() {
    entries.clear();
  }

 private:
  // this escape procedure is not as complete as it should be.
  // we should manage ALL control chars from '\x00' to '\x1f'
  // however this are the one that we can expect to happen
  // More info: https://stackoverflow.com/a/33799784/869271
  std::string Escape(const std::string input) const {
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

#endif  // CVMFS_JSON_DOCUMENT_WRITE_H_
