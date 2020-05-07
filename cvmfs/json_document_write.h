/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_JSON_DOCUMENT_WRITE_H_
#define CVMFS_JSON_DOCUMENT_WRITE_H_

// This class is used for marshalling JSON objects to strings.
// When adding an object, use quoted = true for strings and
// quoted = false for numbers, nested objects, etc.

class JsonStringGenerator {
  struct JsonStringEntry {
    std::string key;
    std::string val;
    bool quoted;

    JsonStringEntry() {
      quoted = true;
    }
  };

 public:
  void PushBack(std::string key, std::string val, bool quoted = true) {
    JsonStringEntry entry;
    entry.key = key;
    entry.val = val;
    entry.quoted = quoted;
    entries.push_back(entry);
  }

  std::string GenerateString() const {
    std::string output;

    output += "{";
    for (size_t i = 0u; i < this->entries.size(); ++i) {
      output += std::string("\"") + this->entries[i].key + "\":";
      if (this->entries[i].quoted) {
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
  std::vector<JsonStringEntry> entries;
};

#endif  // CVMFS_JSON_DOCUMENT_H_
