/**
 * This file is part of the CernVM File System.
 *
 * This class implements the format for .cvmfsbundle files
 */

#ifndef CVMFS_FILE_BUNDLE_H_
#define CVMFS_FILE_BUNDLE_H_

#include "cache.h"
#include "json_document.h"
#include "shortstring.h"
#include "util/pointer.h"
#include "util/single_copy.h"

/*

The .cvmfsbundle file servers both as a file list and as a trigger for loading a
bundle. The convention is to call it .cvmfsbundle.<filename>, where <filename>
should trigger the bundle.

? The content could be structured in json.

The file format should be versioned, with the header:

#%CVMFS_BUNDLE version=1 encoding=UTF-8

? end marker

*/

class BundleFileMgr : SingleCopy {
 public:
  BundleFileMgr() = default;
  BundleFileMgr(const PathString &bf) : file_content_(nullptr) { }
  BundleFileMgr(UniquePtr<JsonDocument> &fc) : file_content_(fc.Release()) { }
  BundleFileMgr(JsonDocument *fc) : file_content_(fc) { fc = nullptr; }

  void Manage(UniquePtr<JsonDocument> &fc) {
    ReleaseDocument();
    file_content_ = fc.Release();
  }
  void Manage(JsonDocument *fc) {
    ReleaseDocument();
    file_content_ = fc;
  }

  virtual ~BundleFileMgr() { ReleaseDocument(); }

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

  operator bool() const {
    return (file_content_ != nullptr) ? file_content_->IsValid() : false;
  }

 private:
  void ReleaseDocument() {
    if (file_content_ != nullptr) {
      delete file_content_;
      file_content_ = nullptr;
    }
  }
  size_t size_ = 42;
  JsonDocument *file_content_ = nullptr;
};

#endif

