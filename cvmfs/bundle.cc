/**
 * This file is part of the CernVM File System.
 */

#include "bundle.h"

#include <fcntl.h>

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "json_document.h"
#include "pack.h"
#include "util/exception.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;

UniquePtr<ObjectPack> * Bundle::CreateBundle(
    const FilepathSet &filepaths) {
  // create an ObjectPack
  UniquePtr<ObjectPack> *op = new UniquePtr<ObjectPack>(new ObjectPack());
  if (!op->IsValid()) {
    PANIC(kLogStderr, "Insufficient memory");
  }

  for (FilepathSet::iterator it = filepaths.begin();
      it != filepaths.end(); it++) {
    string path = *it;

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
    ssize_t nbytes = SafeRead(fd, buffer.weak_ref(), file_size);
    if (nbytes < file_size) {
      PANIC(kLogStderr, "Incomplete file read: %s", path.c_str());
    }

    // add buffer to Bucket
    ObjectPack::BucketHandle bucket_handle = (*op)->NewBucket();
    bucket_handle->name = path;
    ObjectPack::AddToBucket(buffer.weak_ref(), file_size, bucket_handle);

    // commit Bucket
    shash::Any id(shash::kSha1);
    shash::HashMem(buffer.weak_ref(), file_size, &id);
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

UniquePtr<vector<pair<string, FilepathSet>>> *Bundle::ParseBundleSpecFile(
    string bundle_spec_path) {
  // open the bundle specification file for reading
  int fd = open(bundle_spec_path.c_str(), O_RDONLY);
  if (fd < 0) {
    PANIC(kLogStderr, "Could not open bundle specification file: %s",
          bundle_spec_path.c_str());
  }

  // copy JSON contents to the string
  string json_string;
  if (!SafeReadToString(fd, &json_string)) {
    PANIC(kLogStderr, "Could not read contents of file: %s",
          bundle_spec_path.c_str());
  }

  // create JsonDocument from the JSON string
  UniquePtr<JsonDocument> json(JsonDocument::Create(json_string));

  UniquePtr<vector<pair<string, FilepathSet>>> *all_filepaths =
      new UniquePtr<vector<pair<string, FilepathSet>>>(
          new vector<pair<string, FilepathSet>>());

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
    if (string(p->name) != "bundle_name") {
      PANIC(kLogStderr, "Malformed bundle specification: no bundle name found");
    }
    string bundle_name = string(p->string_value);

    // filepaths
    p = p->next_sibling;
    if (string(p->name) != "filepaths") {
      PANIC(kLogStderr, "Malformed bundle specification: "
            "no filepaths array found");
    }
    if (p->first_child) {
      p = p->first_child;
      string filepath;
      do {
        filepath = string(p->string_value);

        // if the file size exceeds kMaxFileSize then the file is skipped
        int64_t currentFileSize = GetFileSize(filepath);
        if (currentFileSize > kMaxFileSize) {
          PrintWarning("Large file skipped: " + filepath + " of size: "
                + StringifyInt(currentFileSize) + " bytes");
        } else {
          if (filepath_set.find(filepath) != filepath_set.end()) {
            PANIC(kLogStderr, "Duplicate filepath found: %s", filepath.c_str());
          }
          for (vector<pair<string, FilepathSet>>::iterator it =
               (*(*all_filepaths)).begin();
               it != (*(*all_filepaths)).end(); it++) {
            if ((it->second).find(filepath) != (it->second).end()) {
              PANIC(kLogStderr, "Duplicate filepath found: %s already exists in"
                    " %s", filepath.c_str(), bundle_name.c_str());
            }
          }
          filepath_set.insert(filepath);
        }

        p = p->next_sibling;
      } while (p);
    }

    if (filepath_set.size() == 0) {
      PrintWarning("Empty bundle: " + bundle_name + " will not be created");
    } else {
      (*(*all_filepaths)).push_back(make_pair(bundle_name, filepath_set));
    }

    value = value->next_sibling;
  } while (value);

  return all_filepaths;
}
