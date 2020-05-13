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
  struct JsonStringEntry {
    std::string key;
    std::string val;
    bool is_quoted;

    JsonStringEntry() : is_quoted(true) {}
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
        output += std::string("\"") + this->entries[i].val + "\"";
      } else {
        output += this->entries[i].val;
      }
      if (i < this->entries.size() - 1) {
        output += ',';
      }
    }
    output += std::string("}");
    return output;
  }

 private:
  void Add(std::string key, std::string val, bool quoted = true) {
    JsonStringEntry entry;
    entry.key = key;
    entry.val = val;
    entry.is_quoted = quoted;
    entries.push_back(entry);
  }

  std::vector<JsonStringEntry> entries;
};

#endif  // CVMFS_JSON_DOCUMENT_WRITE_H_
