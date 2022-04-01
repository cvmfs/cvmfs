/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_BUNDLE_H_
#define CVMFS_BUNDLE_H_

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "json_document.h"
#include "pack.h"
#include "util/pointer.h"

using namespace std;  // NOLINT

typedef set<string> FilepathSet;

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
  static const int64_t kMaxFileSize = 1024 * 1024 * 100;

  /**
   * Creates a ObjectPack bundle of one or more regular files
   *
   * Returns a UniquePtr to the created ObjectPack on success, an invalid
   * UniquePtr otherwise
   */
  UniquePtr<ObjectPack> *CreateBundle(const FilepathSet &filepaths);

  /**
   * Parses a bundle specification file
   *
   * The bundle specification file contains a JSON array of one or more bundle
   * specifications. Each bundle specification is a JSON object containing the
   * bundle name and the list of filepaths.
   *
   * The bundle name must be unique among all the bundle names in the specification
   * file. Each file specified in the filepaths array, must exist, and can belong
   * to atmost one bundle.
   *
   * Bundle specification JSON format:
   *  [
   *    {
   *      "bundle_name": "examplebundle",
   *      "filepaths": ["filepath1", ... "filepathN"]
   *    },
   *    ...
   *  ]
   */
  static UniquePtr<vector<pair<string, FilepathSet>>> *ParseBundleSpecFile(
    string bundle_spec_path);
};

struct BundleEntry {
  int64_t id;
  string name;
  shash::Any hash;
  int64_t size;
  FilepathSet filepath_set;
};

#endif  // CVMFS_BUNDLE_H_
