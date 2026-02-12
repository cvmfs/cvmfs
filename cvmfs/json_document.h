/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_JSON_DOCUMENT_H_
#define CVMFS_JSON_DOCUMENT_H_

#include <nlohmann/json.hpp>
#include <string>

#include "util/single_copy.h"

typedef nlohmann::json JSON;

class JsonDocument : SingleCopy {
 public:
  static JsonDocument *Create(const std::string &text);
  ~JsonDocument() { }

  std::string PrintCanonical();

  inline const JSON *root() const { return &root_; }
  inline bool IsValid() const { return !root_.is_null(); }

  static const JSON *SearchInObject(const JSON *json_object,
                                    const std::string &name,
                                    const nlohmann::json::value_t type);

 private:
  JsonDocument();
  bool Parse(const std::string &text);

  JSON root_;
};

template<typename T>
bool GetFromJSON(const JSON *object, const std::string &name, T *value);

#endif  // CVMFS_JSON_DOCUMENT_H_
