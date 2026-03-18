/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_JSON_DOCUMENT_H_
#define CVMFS_JSON_DOCUMENT_H_

#include <nlohmann/json.hpp>
#include <string>

#include "util/single_copy.h"

typedef nlohmann::json JSON;

static constexpr JSON::value_t JSON_NULL = JSON::value_t::null;
static constexpr JSON::value_t JSON_OBJECT = JSON::value_t::object;
static constexpr JSON::value_t JSON_ARRAY = JSON::value_t::array;
static constexpr JSON::value_t JSON_STRING = JSON::value_t::string;
static constexpr JSON::value_t JSON_INT = JSON::value_t::number_integer;
static constexpr JSON::value_t JSON_FLOAT = JSON::value_t::number_float;
static constexpr JSON::value_t JSON_BOOL = JSON::value_t::boolean;

class JsonDocument : SingleCopy {
 public:
  static JsonDocument *Create(const std::string &text);
  ~JsonDocument() { }

  std::string PrintCanonical();

  inline const JSON *root() const { return &root_; }
  inline bool IsValid() const { return !root_.is_null(); }

  static const JSON *SearchInObject(const JSON *json_object,
                                    const std::string &name,
                                    const JSON::value_t type);

 private:
  JsonDocument();
  bool Parse(const std::string &text);

  JSON root_;
};

template<typename T>
bool GetFromJSON(const JSON *object, const std::string &name, T *value);

#endif  // CVMFS_JSON_DOCUMENT_H_
