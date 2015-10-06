/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_JSON_DOCUMENT_H_
#define CVMFS_JSON_DOCUMENT_H_

#include <string>

#include "json.h"
#include "util/single_copy.h"

typedef struct json_value JSON;

class JsonDocument : SingleCopy {
 public:
  static JsonDocument *Create(const std::string &text);

  std::string PrintCanonical();
  std::string PrintPretty();

  inline const JSON* root() const { return root_; }
  inline bool IsValid() const { return root_ != NULL; }

 private:
  static const unsigned kDefaultBlockSize = 2048;  // 2kB
  static const unsigned kMaxTextSize = 1024*1024;  // 1MB

  JsonDocument();
  bool Parse(const std::string &text);

  block_allocator  allocator_;
  JSON            *root_;
};

#endif  // CVMFS_JSON_DOCUMENT_H_
