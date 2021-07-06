/**
 * This file is part of the CernVM File System.
 */

#include "bundle.h"

#include <string>

#include "json_document.h"
#include "pack.h"
#include "util/exception.h"
#include "util/posix.h"
#include "util/string.h"

ObjectPack *Bundle::CreateBundle(const JSON *json_obj) {
  const JSON *value = json_obj;
  if (value->type != JSON_OBJECT) {
    PANIC(kLogStderr, "JSON object not found");
  } else {
    // create an ObjectPack
    ObjectPack *op = new ObjectPack();

    if (op == NULL) {
      PANIC(kLogStderr, "Insufficient memory");
    }

    value = (value->first_child);  // bundleid
    value = (value->next_sibling);  // JSON array
    if (value->first_child) {
      value = value->first_child;
      std::string filepath = std::string(value->string_value);

      // if file doesn't exist then bundle is not created
      if (!FileExists(filepath)) {
        PrintWarning("File not found: " + filepath);
        delete op;
        return NULL;
      }

      // if the file size exceeds kMaxFileSize then bundle is not created
      int64_t currentFileSize = GetFileSize(filepath);
      if (currentFileSize > kMaxFileSize) {
        PrintWarning("Large file found: " + filepath + " of size: "
          + StringifyInt(currentFileSize));
        delete op;
        return NULL;
      }

      if (!AddFileToObjectPack(op, filepath)) {
        delete op;
        return NULL;
      }

      while (value->next_sibling) {
        value = value->next_sibling;
        filepath = std::string(value->string_value);

        // if file doesn't exist then bundle is not created
        if (!FileExists(filepath)) {
          PrintWarning("File not found: " + filepath);
          delete op;
          return NULL;
        }

        // if the file size exceeds kMaxFileSize then bundle is not created
        currentFileSize = GetFileSize(filepath);
        if (currentFileSize > kMaxFileSize) {
          PrintWarning("Large file found: " + filepath + " of size: "
                + StringifyInt(currentFileSize));
          delete op;
          return NULL;
        }

        if (!AddFileToObjectPack(op, filepath)) {
          delete op;
          return NULL;
        }
      }
    } else {
      PrintWarning("Empty bundle not created");
      return NULL;
    }

    return op;
  }
}
