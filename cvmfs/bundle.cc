/**
 * This file is part of the CernVM File System.
 */

#include "bundle.h"

#include <fcntl.h>

#include <set>
#include <string>

#include "json_document.h"
#include "pack.h"
#include "util/exception.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

UniquePtr<ObjectPack> * Bundle::CreateBundle(std::set<std::string> filepaths) {
    // create an ObjectPack
    UniquePtr<ObjectPack> *op = new UniquePtr<ObjectPack>(new ObjectPack());
    if (!op->IsValid()) {
      PANIC(kLogStderr, "Insufficient memory");
    }

    for (std::set<std::string>::iterator it = filepaths.begin();
        it != filepaths.end(); it++) {
      std::string path = *it;

      // open the file for reading
      int fd = open(path.c_str(), O_RDONLY);
      if (fd < 0) {
        PANIC(kLogStderr, "Could not open file: %s for reading", path.c_str());
      }

      // read the file contents into a buffer
      int64_t file_size = GetFileSize(path);
      UniquePtr<unsigned char> buffer(new unsigned char[file_size]);
      if (!buffer.IsValid()) {
        PANIC(kLogStderr, "Insufficient memory");
      }
      ssize_t nbytes = SafeRead(fd, buffer, file_size);
      if (nbytes < file_size) {
        PANIC(kLogStderr, "Incomplete file read: %s", path.c_str());
      }

      // add buffer to Bucket
      ObjectPack::BucketHandle bucket_handle = (*op)->NewBucket();
      bucket_handle->name = path;
      ObjectPack::AddToBucket(buffer, file_size, bucket_handle);

      // commit Bucket
      shash::Any id(shash::kSha1);
      shash::HashMem(buffer, file_size, &id);
      if (!((*op)->CommitBucket(ObjectPack::kNamed, id, bucket_handle,
                                path))) {
        PANIC(kLogStderr, "Could not commit bucket");
      }

      // cleanup
      buffer.Release();
      close(fd);
    }
    return op;
}

std::set<std::string> Bundle::ParseBundleSpec(const JSON *json_obj) {
  std::set<std::string> filepath_set;
  const JSON *value = json_obj;
  if (value->type != JSON_OBJECT) {
    PANIC(kLogStderr, "JSON object not found");
  }

  value = (value->first_child);  // bundleid
  value = (value->next_sibling);  // JSON array
  if (value->first_child) {
    value = value->first_child;
    std::string filepath;
    do {
      filepath = std::string(value->string_value);
      if (filepath_set.find(filepath) != filepath_set.end()) {
        PANIC(kLogStderr, "Duplicate filepath found: %s", filepath.c_str());
      }
      filepath_set.insert(filepath);

      value = value->next_sibling;
    } while (value);
  }

  return filepath_set;
}
