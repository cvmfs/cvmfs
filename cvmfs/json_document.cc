/**
 * This file is part of the CernVM File System.
 */

#include "json_document.h"

#include "util/logging.h"
#include <memory>

using namespace std;  // NOLINT

JsonDocument *JsonDocument::Create(const string &text) {
  std::unique_ptr<JsonDocument> json(new JsonDocument());
  if (!json->Parse(text))
    return NULL;
  return json.Release();
}

JsonDocument::JsonDocument() : root_(JSON::value_t::null) { }

bool JsonDocument::Parse(const string &text) {
  root_ = JSON::parse(text, nullptr, false);

  if (root_.is_discarded()) {
    LogCvmfs(kLogUtility, kLogDebug, "Failed to parse JSON string.");
    root_ = JSON(JSON::value_t::null);
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
                                         const JSON::value_t type) {
  if (!json_object || !json_object->is_object())
    return NULL;

  auto it = json_object->find(name);
  if (it != json_object->end()) {
    // if we asked for an int, accept signed or unsigned
    if (type == JSON::value_t::number_integer) {
      if (it->is_number_integer() || it->is_number_unsigned()) {
        return &(*it);
      }
    }
    // otherwise, stick to strict matching for strings, objects, etc.
    else if (it->type() == type) {
      return &(*it);
    }
  }
  return NULL;
}

template<>
bool GetFromJSON<string>(const JSON *object,
                         const string &name,
                         string *value) {
  const JSON *o = JsonDocument::SearchInObject(
      object, name, JSON::value_t::string);
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
bool GetFromJSON<int>(const JSON *object, const string &name, int *value) {
  const JSON *o = JsonDocument::SearchInObject(
      object, name, JSON::value_t::number_integer);

  if (!o) {
    o = JsonDocument::SearchInObject(
        object, name, JSON::value_t::number_unsigned);
  }

  if (!o || !value)
    return false;

  if (auto p = o->get_ptr<const JSON::number_integer_t *>()) {
    *value = static_cast<int>(*p);
    return true;
  } else if (auto p = o->get_ptr<const JSON::number_unsigned_t *>()) {
    *value = static_cast<int>(*p);
    return true;
  }

  return false;
}

template<>
bool GetFromJSON<float>(const JSON *object, const string &name, float *value) {
  const JSON *o = JsonDocument::SearchInObject(
      object, name, JSON::value_t::number_float);
  if (!o || !value)
    return false;

  if (auto p = o->get_ptr<const JSON::number_float_t *>()) {
    *value = static_cast<float>(*p);
    return true;
  }
  return false;
}
