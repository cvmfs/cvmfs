/**
 * This file is part of the CernVM File System.
 */

#include "bundle.h"

#include <string>

#include "json_document.h"
#include "pack.h"
#include "util/exception.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

UniquePtr<ObjectPack> *Bundle::CreateBundle(const JSON *json_obj) {
  const JSON *value = json_obj;
  if (value->type != JSON_OBJECT) {
    PANIC(kLogStderr, "JSON object not found");
  } else {
    // create an ObjectPack
    UniquePtr<ObjectPack>* op = new UniquePtr<ObjectPack>(new ObjectPack());

    if (!op->IsValid()) {
      PANIC(kLogStderr, "Insufficient memory");
    }

    value = (value->first_child);  // bundleid
    value = (value->next_sibling);  // JSON array
    if (!value->first_child) {
      PrintWarning("Empty bundle not created");
      return op;
    } else {
      value = value->first_child;
      std::string filepath;

      do {
        filepath = std::string(value->string_value);

        // if file doesn't exist then skip it
        if (!FileExists(filepath)) {
          PrintWarning("Could not add file to the bundle: " + filepath
                + " not found");
        } else {
          // if the file size exceeds kMaxFileSize then bundle is not created
          int64_t currentFileSize = GetFileSize(filepath);
          if (currentFileSize > kMaxFileSize) {
            PrintWarning("Large file found: " + filepath + " of size: "
                  + StringifyInt(currentFileSize));
            op->Release();
            return op;
          }

          if (!AddFileToObjectPack(op, filepath)) {
            op->Release();
            return op;
          }
        }
      } while (value->next_sibling);
    }

    return op;
  }
}
