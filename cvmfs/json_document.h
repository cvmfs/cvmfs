/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_JSON_DOCUMENT_H_
#define CVMFS_JSON_DOCUMENT_H_

#include <string>
#include <utility>
#include <vector>

#include "json.h"
#include "util/single_copy.h"

typedef struct json_value JSON;

class JsonDocument : SingleCopy {
 public:
  static JsonDocument *Create(const std::string &text);
  ~JsonDocument();

  std::string PrintCanonical();
  std::string PrintPretty();

  inline const JSON *root() const { return root_; }
  inline bool IsValid() const { return root_ != NULL; }

  static std::string EscapeString(const std::string &input);
  static JSON *SearchInObject(const JSON *json_object, const std::string &name,
                              const json_type type);

 private:
  static const unsigned kDefaultBlockSize = 2048;  // 2kB

  struct PrintOptions {
    PrintOptions() : with_whitespace(false), num_indent(0) { }
    bool with_whitespace;
    unsigned num_indent;
  };

  JsonDocument();
  bool Parse(const std::string &text);
  std::string PrintValue(JSON *value, PrintOptions print_options);
  std::string PrintArray(JSON *first_child, PrintOptions print_options);
  std::string PrintObject(JSON *first_child, PrintOptions print_options);

  block_allocator allocator_;
  JSON *root_;
  char *raw_text_;
};

template<typename T>
bool GetFromJSON(const JSON *object, const std::string &name, T *value);

#endif  // CVMFS_JSON_DOCUMENT_H_
