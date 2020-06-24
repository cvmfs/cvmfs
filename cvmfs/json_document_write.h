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
  enum JsonVariant { String, Integer, Float };

  struct JsonEntry {
    JsonVariant variant;
    std::string key;
    std::string str_val;
    int int_val;
    float float_val;
    bool is_quoted;

    JsonEntry()
        : variant(JsonVariant::String),
          int_val(0),
          float_val(0),
          is_quoted(true) {}
  };

 public:
  void AddQuoted(std::string key, std::string val) {
    this->Add(key, val, true);
  }

  void AddUnquoted(std::string key, std::string val) {
    this->Add(key, val, false);
  }

  std::string GenerateString() const {
    std::string output;

    output += "{";
    for (size_t i = 0u; i < this->entries.size(); ++i) {
      output += std::string("\"") + this->entries[i].key + "\":";
      if (this->entries[i].is_quoted) {
        output += std::string("\"") + this->entries[i].str_val + "\"";
      } else {
        output += this->entries[i].str_val;
      }
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
  void Add(std::string key, std::string val, bool quoted = true) {
    JsonEntry entry;
    entry.key = Escape(key);
    entry.str_val = Escape(val);
    entry.is_quoted = quoted;
    entries.push_back(entry);
  }

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
