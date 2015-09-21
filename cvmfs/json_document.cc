/**
 * This file is part of the CernVM File System.
 */

// TODO(rmeusel): Move to Riak specific source files

#include "json_document.h"

JsonDocument::JsonDocument() :
  allocator_(1 << 10),
  root_(NULL) {}


bool JsonDocument::Parse(char *buffer) {
  assert(root_ == NULL);

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
    LogCvmfs(kLogUtility, kLogStderr, "Failed to parse Riak configuration json "
                                      "string.\n"
                                      "Error at line %d: %s\n"
                                      "%s",
             error_line, error_desc, error_pos);
    return false;
  }

  root_ = root;
  return true;
}
