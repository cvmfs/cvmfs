/**
 * This file is part of the CernVM File System.
 */

#include "bundle.h"

#include <fcntl.h>

#include <set>
#include <string>
#include <vector>

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

UniquePtr<std::vector<FilepathSet>> *ParseBundleSpecFile(
    std::string bundle_spec_path) {
  // open the bundle specification file for reading
  int fd = open(bundle_spec_path.c_str(), O_RDONLY);
  if (fd < 0) {
    PANIC(kLogStderr, "Could not open bundle specification file: %s",
          bundle_spec_path.c_str());
  }

  // copy JSON contents to the string
  std::string json_string;
  if (!SafeReadToString(fd, &json_string)) {
    PANIC(kLogStderr, "Could not read contents of file: %s",
          bundle_spec_path.c_str());
  }

  // create JsonDocument from the JSON string
  UniquePtr<JsonDocument> json(JsonDocument::Create(json_string));

  UniquePtr<std::vector<FilepathSet>> *all_filepaths =
      new UniquePtr<std::vector<FilepathSet>>(new std::vector<FilepathSet>());

  // parse the array of bundle spec JSON objects
  const JSON *value = json->root();
  if (value->type != JSON_ARRAY) {
    PANIC(kLogStderr, "JSON array not found");
  }

  if (!value->first_child) {
    PANIC(kLogStderr, "No bundle specifications found");
  }
  value = value->first_child;
  do {
    if (value->type != JSON_OBJECT) {
      PANIC(kLogStderr, "Malformed bundle specification");
    }

    FilepathSet filepath_set;
    const JSON *p = value;

    if (!p->first_child) {
      PANIC(kLogStderr, "Malformed bundle specification");
    }

    // bundle name
    p = p->first_child;
    if (std::string(p->name) != "bundle_name") {
      PANIC(kLogStderr, "Malformed bundle specification: no bundle name found");
    }
    std::string bundle_name = std::string(p->string_value);

    // filepaths
    p = p->next_sibling;
    if (std::string(p->name) != "filepaths") {
      PANIC(kLogStderr, "Malformed bundle specification: "
            "no filepaths array found");
    }
    if (p->first_child) {
      p = p->first_child;
      std::string filepath;
      do {
        filepath = std::string(p->string_value);
        if (filepath_set.find(filepath) != filepath_set.end()) {
          PANIC(kLogStderr, "Duplicate filepath found: %s", filepath.c_str());
        }
        for (std::vector<FilepathSet>::iterator it = (*all_filepaths)->begin();
          it != (*all_filepaths)->end(); it++) {
          if (it->find(filepath) != it->end()) {
            PANIC(kLogStderr, "Duplicate filepath found: %s already exists in"
                  " %s", filepath.c_str(), bundle_name.c_str());
          }
        }
        filepath_set.insert(filepath);

        p = p->next_sibling;
      } while (p);
    }

    (*all_filepaths)->push_back(filepath_set);

    value = value->next_sibling;
  } while (value);

  return all_filepaths;
}
