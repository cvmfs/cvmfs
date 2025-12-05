/**
 * This file is part of the CernVM File System.
 *
 * This class implements the format for .cvmfsbundle files
 */

#ifndef CVMFS_FILE_BUNDLE_H_
#define CVMFS_FILE_BUNDLE_H_

#include <json.h>

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

The json file should contain an array of labeled objects as follows:
{
"labeled_objects":[{"id":{"algorithm":%d,"suffix":"%c","digest":"%s"},"label":{"flags":%d,"size":%PRIu64,"zip_algorithm":%d,"range_offset":%d,"path":"%s"}}]
}
*/

class BundleFileMgr : SingleCopy {
 public:
  BundleFileMgr() = default;
  BundleFileMgr(const PathString &bf) : file_content_(nullptr) { }
  BundleFileMgr(UniquePtr<JsonDocument> &fc) : file_content_(fc.Release()) { }
  BundleFileMgr(JsonDocument *fc) : file_content_(fc) { fc = nullptr; }

  void Manage(UniquePtr<JsonDocument> &fc) { Manage(fc.Release()); }
  void Manage(JsonDocument *fc) {
    ReleaseDocument();
    file_content_ = fc;
    ResetSize();
    ResetCurrentLabeledObject();
  }

  virtual ~BundleFileMgr() { ReleaseDocument(); }

  virtual UniquePtr<CacheManager::LabeledObject> GetNext() {
    if (not current_labeled_object_)
      return UniquePtr<CacheManager::LabeledObject>(nullptr);

    auto *id_ptr = JsonDocument::SearchInObject(current_labeled_object_, "id",
                                                JSON_OBJECT);
    assert(id_ptr != nullptr);
    int algorithm;
    GetFromJSON<int>(id_ptr, "algorithm", &algorithm);
    std::string suffix;
    GetFromJSON<std::string>(id_ptr, "suffix", &suffix);
    std::string digest;
    GetFromJSON<std::string>(id_ptr, "digest", &digest);
    shash::Any id = shash::MkFromHexPtr(shash::HexPtr(digest), suffix[0]);
    id.algorithm = static_cast<shash::Algorithms>(algorithm);
    auto label_ptr = JsonDocument::SearchInObject(current_labeled_object_,
                                                  "label", JSON_OBJECT);
    assert(label_ptr != nullptr);

    int flags;
    GetFromJSON<int>(label_ptr, "flags", &flags);
    int size;  ///< unzipped size, if known
    GetFromJSON<int>(label_ptr, "size", &size);
    int zip_algorithm;
    GetFromJSON<int>(label_ptr, "zip_algorithm", &zip_algorithm);
    int range_offset;
    GetFromJSON<int>(label_ptr, "range_offset", &range_offset);
    std::string path;
    GetFromJSON<std::string>(label_ptr, "path", &path);

    CacheManager::Label label{};
    label.flags = flags;
    label.size = size;
    label.zip_algorithm = static_cast<zlib::Algorithms>(zip_algorithm);
    label.range_offset = range_offset;
    label.path = path;

    current_labeled_object_ = current_labeled_object_->next_sibling;
    return UniquePtr<CacheManager::LabeledObject>(
        new CacheManager::LabeledObject(id, label));
  };

  virtual size_t Size() const { return size_; }

  operator bool() const {
    return (file_content_ != nullptr) ? file_content_->IsValid() : false;
  }

 private:
  void ResetCurrentLabeledObject() {
    auto *ptr = JsonDocument::SearchInObject(file_content_->root(),
                                             "labeled_objects", JSON_ARRAY);
    if (not ptr) {
      current_labeled_object_ = nullptr;
    } else {
      current_labeled_object_ = ptr->first_child;
    }
  }
  void ResetSize() {
    if (not file_content_) {
      size_ = 0;
    } else {
      auto *ptr = JsonDocument::SearchInObject(file_content_->root(),
                                               "labeled_objects", JSON_ARRAY);
      size_ = 0;
      for (ptr = ptr->first_child; ptr != nullptr; ptr = ptr->next_sibling) {
        ++size_;
      }
    }
  }
  void ReleaseDocument() {
    if (file_content_ != nullptr) {
      delete file_content_;
      file_content_ = nullptr;
    }
  }
  size_t size_ = 0;
  JsonDocument *file_content_ = nullptr;
  JSON *current_labeled_object_ = nullptr;
};

#endif

