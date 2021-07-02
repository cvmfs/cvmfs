/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_BUNDLE_H_
#define CVMFS_BUNDLE_H_

/**
 * A bundle has the binary format of object packs used by the gateway as
 * a form to concatenate multiple objects in a single BLOB.
 * Bundle files can be chunked the same way regular large files get chunked.
 * Chunked and non-chunked bundle files get a B as a hash suffix
 *
 * The bundle files are created according to the contents of
 * .cvmfsbundles file.
 */
class Bundle {

};

#endif  // CVMFS_BUNDLE_H_
