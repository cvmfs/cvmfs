/**
 * This file is part of the CernVM File System.
 *
 * This class implements the format for .cvmfsbundle files
 */

#ifndef CVMFS_FILE_BUNDLE_H_
#define CVMFS_FILE_BUNDLE_H_

#include "cache.h"         // LabeledObject
#include "shortstring.h"   // PathString
#include "util/pointer.h"  // UniquePtr

/*

The .cvmfsbundle file serves both as a file list and as a trigger for loading a
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
  virtual ~BundleFileMgr() = default;
  virtual UniquePtr<CacheManager::LabeledObject> GetNext() const {
    // TODO(christge): return actual labled objects
    CacheManager::Label label;
    label.path = std::string{};
    label.size = sizeof(shash::Any);
    label.zip_algorithm = zlib::kZlibDefault;
    return UniquePtr<CacheManager::LabeledObject>(
        new CacheManager::LabeledObject(shash::Any{}, label));
  };

  virtual size_t Size() const { return size_; }

 private:
  size_t size_ = 42;
};

#endif

