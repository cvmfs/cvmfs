/**
 * This file is part of the CernVM File System.
 */

#include "json_document.h"

#include <cassert>
#include <cstdlib>
#include <cstring>

#include "util/exception.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/string.h"

using namespace std;  // NOLINT

JsonDocument *JsonDocument::Create(const string &text) {
  UniquePtr<JsonDocument> json(new JsonDocument());
  bool retval = json->Parse(text);
  if (!retval)
    return NULL;
  return json.Release();
}

string JsonDocument::EscapeString(const string &input) {
  string escaped;
  escaped.reserve(input.length());

  for (unsigned i = 0, s = input.length(); i < s; ++i) {
    if (input[i] == '\\') {
      escaped.push_back('\\');
      escaped.push_back('\\');
    } else if (input[i] == '"') {
      escaped.push_back('\\');
      escaped.push_back('"');
    } else {
      escaped.push_back(input[i]);
    }
  }
  return escaped;
}

JsonDocument::JsonDocument()
    : allocator_(kDefaultBlockSize), root_(NULL), raw_text_(NULL) { }

JsonDocument::~JsonDocument() {
  if (raw_text_)
    free(raw_text_);
}

/**
 * Parses a JSON string in text.
 *
 * @return        true if parsing was successful
 */
bool JsonDocument::Parse(const string &text) {
  assert(root_ == NULL);

  // The used JSON library 'vjson' is a destructive parser and therefore
  // alters the content of the provided buffer.  The buffer must persist as
  // name and string values from JSON nodes just point into it.
  raw_text_ = strdup(text.c_str());

  char *error_pos = 0;
  char *error_desc = 0;
  int error_line = 0;
  JSON *root = json_parse(raw_text_, &error_pos, &error_desc, &error_line,
                          &allocator_);

  // check if the json string was parsed successfully
  if (!root) {
    LogCvmfs(kLogUtility, kLogDebug,
             "Failed to parse json string. Error at line %d: %s (%s)",
             error_line, error_desc, error_pos);
    return false;
  }

  root_ = root;
  return true;
}

string JsonDocument::PrintArray(JSON *first_child, PrintOptions print_options) {
  string result = "[";
  if (print_options.with_whitespace) {
    result += "\n";
    print_options.num_indent += 2;
  }
  JSON *value = first_child;
  if (value != NULL) {
    result += PrintValue(value, print_options);
    value = value->next_sibling;
  }
  while (value != NULL) {
    result += print_options.with_whitespace ? ",\n" : ",";
    result += PrintValue(value, print_options);
    value = value->next_sibling;
  }
  if (print_options.with_whitespace) {
    result += "\n";
    for (unsigned i = 2; i < print_options.num_indent; ++i)
      result.push_back(' ');
  }
  return result + "]";
}

/**
 * JSON string in a canonical format:
 *   - No whitespaces
 *   - Variable names and strings in quotes
 *
 * Can be used as a canonical representation to sign or encrypt a JSON text.
 */
string JsonDocument::PrintCanonical() {
  if (!root_)
    return "";
  PrintOptions print_options;
  return PrintObject(root_->first_child, print_options);
}

string JsonDocument::PrintObject(JSON *first_child,
                                 PrintOptions print_options) {
  string result = "{";
  if (print_options.with_whitespace) {
    result += "\n";
    print_options.num_indent += 2;
  }
  JSON *value = first_child;
  if (value != NULL) {
    result += PrintValue(value, print_options);
    value = value->next_sibling;
  }
  while (value != NULL) {
    result += print_options.with_whitespace ? ",\n" : ",";
    result += PrintValue(value, print_options);
    value = value->next_sibling;
  }
  if (print_options.with_whitespace) {
    result += "\n";
    for (unsigned i = 2; i < print_options.num_indent; ++i)
      result.push_back(' ');
  }
  return result + "}";
}

/**
 * JSON string for humans.
 */
string JsonDocument::PrintPretty() {
  if (!root_)
    return "";
  PrintOptions print_options;
  print_options.with_whitespace = true;
  return PrintObject(root_->first_child, print_options);
}

std::string JsonDocument::PrintValue(JSON *value, PrintOptions print_options) {
  assert(value);

  string result;
  for (unsigned i = 0; i < print_options.num_indent; ++i)
    result.push_back(' ');
  if (value->name) {
    result += "\"" + EscapeString(value->name) + "\":";
    if (print_options.with_whitespace)
      result += " ";
  }
  switch (value->type) {
    case JSON_NULL:
      result += "null";
      break;
    case JSON_OBJECT:
      result += PrintObject(value->first_child, print_options);
      break;
    case JSON_ARRAY:
      result += PrintArray(value->first_child, print_options);
      break;
    case JSON_STRING:
      result += "\"" + EscapeString(value->string_value) + "\"";
      break;
    case JSON_INT:
      result += StringifyInt(value->int_value);
      break;
    case JSON_FLOAT:
      result += StringifyDouble(value->float_value);
      break;
    case JSON_BOOL:
      result += value->int_value ? "true" : "false";
      break;
    default:
      PANIC(NULL);
  }
  return result;
}

JSON *JsonDocument::SearchInObject(const JSON *json_object, const string &name,
                                   const json_type type) {
  if (!json_object || (json_object->type != JSON_OBJECT))
    return NULL;

  JSON *walker = json_object->first_child;
  while (walker != NULL) {
    if (string(walker->name) == name) {
      return (walker->type == type) ? walker : NULL;
    }
    walker = walker->next_sibling;
  }
  return NULL;
}

template<>
bool GetFromJSON<std::string>(const JSON *object, const std::string &name,
                              std::string *value) {
  const JSON *o = JsonDocument::SearchInObject(object, name, JSON_STRING);

  if (o == NULL) {
    return false;
  }

  if (value) {
    *value = o->string_value;
  }

  return true;
}

template<>
bool GetFromJSON<int>(const JSON *object, const std::string &name, int *value) {
  const JSON *o = JsonDocument::SearchInObject(object, name, JSON_INT);

  if (o == NULL || value == NULL) {
    return false;
  }

  *value = o->int_value;

  return true;
}

template<>
bool GetFromJSON<float>(const JSON *object, const std::string &name,
                        float *value) {
  const JSON *o = JsonDocument::SearchInObject(object, name, JSON_FLOAT);

  if (o == NULL || value == NULL) {
    return false;
  }

  *value = o->float_value;

  return true;
}
