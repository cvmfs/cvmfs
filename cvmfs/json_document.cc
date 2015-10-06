/**
 * This file is part of the CernVM File System.
 */

#include "json_document.h"

#include <cassert>

#include "logging.h"

using namespace std;  // NOLINT


JsonDocument *JsonDocument::Create(const string &text) {
  UniquePtr<JsonDocument> json(new JsonDocument());
  bool retval = json->Parse(text);
  if (!retval)
    return NULL;
  return json.Release();
}


JsonDocument::JsonDocument() :
  allocator_(kDefaultBlockSize),
  root_(NULL) {}

/**
 * Parses a JSON string in text.  The text should not be very long because it
 * needs to be copied to the stack.
 *
 * @return        true if parsing was successful
 */
bool JsonDocument::Parse(const string &text) {
  assert(root_ == NULL);

  // The used JSON library 'vjson' is a destructive parser and therefore
  // alters the content of the provided buffer!
  if (text.length() > kMaxTextSize)
    return false;
  char *buffer = strdupa(text.c_str());

  char *error_pos  = 0;
  char *error_desc = 0;
  int   error_line = 0;
  JSON *root = json_parse(buffer,
                          &error_pos,
                          &error_desc,
                          &error_line,
                          &allocator_);

  // check if the json string was parsed successfully
  if (!root) {
    LogCvmfs(kLogUtility, kLogDebug,
             "Failed to parse Riak configuration json string. "
             "Error at line %d: %s (%s)", error_line, error_desc, error_pos);
    return false;
  }

  root_ = root;
  return true;
}


/**
 * JSON string in a canonical format:
 *   - No whitespaces
 *   - Variable names in quotes
 *
 * Can be used as a cacnonical representation to sign or encrypt a JSON text.
 */
string JsonDocument::PrintCanonical() {
  if (!root_)
    return "";
}


/**
 * JSON string for humans.
 */
string JsonDocument::PrintPretty() {
  if (!root_)
    return "";
}
