/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_BUNDLE_H_
#define CVMFS_BUNDLE_H_

#include <set>
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
   *
   * Returns a UniquePtr to the created ObjectPack on success, an invalid
   * UniquePtr otherwise
   */
  UniquePtr<ObjectPack> *CreateBundle(std::set<std::string> filepaths);
  std::set<std::string> ParseBundleSpec(const JSON *json_obj);
};

#endif  // CVMFS_BUNDLE_H_
