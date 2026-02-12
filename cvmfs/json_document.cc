/**
 * This file is part of the CernVM File System.
 */

#include "json_document.h"

#include <nlohmann/json.hpp>

#include "util/logging.h"
#include "util/pointer.h"

// TODO: fix json key ordering, use nlohmann::ordered_json instead
// fix floating point formatting. update test/force precision
// strict parsing: update unit tests to stop using {} without key value pair

using namespace std;  // NOLINT
using json = nlohmann::json;

JsonDocument *JsonDocument::Create(const string &text) {
  UniquePtr<JsonDocument> json(new JsonDocument());
  if (!json->Parse(text))
    return NULL;
  return json.Release();
}

JsonDocument::JsonDocument() : root_(json::value_t::null) { }

bool JsonDocument::Parse(const string &text) {
  root_ = json::parse(text, nullptr, false);

  if (root_.is_discarded()) {
    LogCvmfs(kLogUtility, kLogDebug, "Failed to parse JSON string.");
    root_ = json(json::value_t::null);
    return false;
  }
  return true;
}

string JsonDocument::PrintCanonical() {
  if (root_.is_null())
    return "";
  return root_.dump();
}

const JSON *JsonDocument::SearchInObject(const JSON *json_object,
                                         const string &name,
                                         const json::value_t type) {
  if (!json_object || !json_object->is_object())
    return NULL;

  auto it = json_object->find(name);
  if (it != json_object->end() && it->type() == type) {
    return &(*it);
  }
  return NULL;
}

template<>
bool GetFromJSON<string>(const JSON *object,
                         const string &name,
                         string *value) {
  const JSON *o = JsonDocument::SearchInObject(
      object, name, json::value_t::string);
  if (!o)
    return false;

  if (value) {
    const string *s = o->get_ptr<const string *>();
    if (s) {
      *value = *s;
      return true;
    }
    return false;
  }
  return true;
}

template<>
bool GetFromJSON<int>(const JSON *object,
                      const string &name,
                      int *value) {
  const JSON *o = JsonDocument::SearchInObject(
      object, name, json::value_t::number_integer);

  if (!o) {
    o = JsonDocument::SearchInObject(
        object, name, json::value_t::number_unsigned);
  }

  if (!o || !value)
    return false;

  if (auto p = o->get_ptr<const json::number_integer_t *>()) {
    *value = static_cast<int>(*p);
    return true;
  } else if (auto p = o->get_ptr<const json::number_unsigned_t *>()) {
    *value = static_cast<int>(*p);
    return true;
  }

  return false;
}

template<>
bool GetFromJSON<float>(const JSON *object,
                        const string &name,
                        float *value) {
  const JSON *o = JsonDocument::SearchInObject(
      object, name, json::value_t::number_float);
  if (!o || !value)
    return false;

  if (auto p = o->get_ptr<const json::number_float_t *>()) {
    *value = static_cast<float>(*p);
    return true;
  }
  return false;
}
