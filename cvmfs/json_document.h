/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_JSON_DOCUMENT_H_
#define CVMFS_JSON_DOCUMENT_H_

#include "json.h"
#include "util.h"

typedef struct json_value JSON;

class JsonDocument : SingleCopy {
 public:
  JsonDocument();

  /**
   * Parses a JSON string in buffer.
   * Note: the used JSON library 'vjson' is a destructive parser and therefore
   *       alters the content of the provided buffer!
   *
   * @param buffer  pointer to the buffer containing the JSON string to parse
   * @return        true if parsing was successful
   */
  bool Parse(char *buffer);

  inline const JSON* root() const { return root_; }

 private:
  block_allocator  allocator_;
  JSON            *root_;
};

#endif  // CVMFS_JSON_DOCUMENT_H_
