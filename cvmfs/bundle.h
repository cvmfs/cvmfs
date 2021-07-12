/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_BUNDLE_H_
#define CVMFS_BUNDLE_H_

#include <string>

#include "json_document.h"
#include "pack.h"
#include "util/pointer.h"

/**
 * A bundle has the binary format of object packs used as
 * a form to concatenate multiple objects in a single BLOB.
 * Bundle files can be chunked the same way regular large files get chunked.
 * Chunked and non-chunked bundle files get a B as a hash suffix
 *
 * The bundle files are created according to the contents of
 * .cvmfsbundles file.
 */
class Bundle {
 public:
  const int64_t kMaxFileSize = 1024 * 1024 * 100;

  /**
   * Creates a ObjectPack bundle of one or more regular files
   * The bundle is not created if any of the files to be included is not found or
   * if the file size of any of the files exceeds a maximum value kMaxFileSize
   *
   * Returns a pointer to the created ObjectPack on success, NULL
   * otherwise
   *
   * The `json_obj` must be a JSON Object having a bundle ID and a JSON array
   * of paths to regular files. The bundle ID is given as a key-value pair where
   * the key must be `bundle_id` and the value must be a unique bundle name over
   * all the other bundles. Each of the file paths must be a string.
   *
   * An example JSON Object:
   * {
   *   "bundle_id": "bundle1",
   *   "filepaths": [
   *     "/cvmfs/test.cern.ch/example1.txt",
   *     "/cvmfs/test.cern.ch/a/example2.txt",
   *     ...
   *   ]
   * }
   */
  UniquePtr<ObjectPack> *CreateBundle(const JSON *json_obj);
};

#endif  // CVMFS_BUNDLE_H_
