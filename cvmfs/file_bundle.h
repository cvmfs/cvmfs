/**
 * This file is part of the CernVM File System.
 *
 * This class implements the format for .cvmfsbundle files
 */

#ifndef CVMFS_FILE_BUNDLE_H_
#define CVMFS_FILE_BUNDLE_H_

#include "shortstring.h"  // PathString


/*

The .cvmfsbundle file servers both as a file list and as a trigger for loading a
bundle. The convention is to call it .cvmfsbundle.<filename>, where <filename>
should trigger the bundle.

? The content could be structured in json.

The file format should be versioned, with the header:

#%CVMFS_BUNDLE version=1 encoding=UTF-8

? end marker

*/

class BundleFileMgr {
 public:
  // TODO(christge): this is to be reverted. It's the basic interface needed to
  // interact with the file bundle. Now there are some mocks for prototyping
  BundleFileMgr(const PathString &bf) { }
  PathString GetNext() { return PathString{}; };
};

#endif

