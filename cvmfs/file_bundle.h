/**
 * This file is part of the CernVM File System.
 *
 * This class implements the format for .cvmfsbundle files
 */

#ifndef CVMFS_FILE_BUNDLE_H_
#define CVMFS_FILE_BUNDLE_H_

#include <json.h>

#include <cassert>
#include <fstream>
#include <sstream>

#include "json_document.h"
#include "shortstring.h"
#include "util/single_copy.h"

/*

The .cvmfsbundle file serves both as a file list and as a trigger for loading a
bundle. The convention is to call it .cvmfsbundle.<filename>, where <filename>
should trigger the bundle.

? The content could be structured in json.

The file format should be versioned, with the header:

#%CVMFS_BUNDLE version=1 encoding=UTF-8

? end marker

The json file should contain an array of labeled objects as follows:
{
"name":"CVMFS_BUNDLE",
"version":"1.0.0",
"encoding":"UTF-8",
"dependencies":["/absolute/path/to/file/from/repositories/root"]
}

*/

class BundleFileMgr : SingleCopy {
 public:
  BundleFileMgr(const PathString &bundle_file_path) {
    std::ifstream file(bundle_file_path.ToString());
    if (!file.is_open()) {
      bundle_doc_ = nullptr;
    } else {
      std::stringstream rbuf;
      rbuf << file.rdbuf();
      JsonDocument *json = JsonDocument::Create(rbuf.str());
      Manage(json);
    }
  }

  BundleFileMgr(JsonDocument *fc) { Manage(fc); }

  virtual ~BundleFileMgr() { ReleaseDocument(); }

  std::string GetVersion() const {
    auto *ptr = JsonDocument::SearchInObject(bundle_doc_->root(), "version",
                                             JSON_STRING);
    return (ptr) ? ptr->string_value : std::string();
  }

  std::string GetEncoding() const {
    auto *ptr = JsonDocument::SearchInObject(bundle_doc_->root(), "encoding",
                                             JSON_STRING);
    return (ptr) ? ptr->string_value : std::string();
  }

  virtual PathString GetNext() {
    if (current_entry_ == nullptr)
      // This should be handled as an error code. Path string should never be
      // empty;
      return PathString();
    assert(current_entry_->type == JSON_STRING);
    const PathString result = static_cast<PathString>(
        current_entry_->string_value);
    current_entry_ = current_entry_->next_sibling;
    return (result.IsEmpty()) ? GetNext() : result;
  };

  virtual size_t Size() const { return size_; }

  operator bool() const {
    return (bundle_doc_ != nullptr) ? bundle_doc_->IsValid() : false;
  }

 private:
  void Manage(JsonDocument *fc) {
    bundle_doc_ = fc;
    ResetSize();
    ResetCurrentEntry();
  }

  void ResetCurrentEntry() {
    auto *ptr = JsonDocument::SearchInObject(bundle_doc_->root(),
                                             "dependencies", JSON_ARRAY);
    current_entry_ = (ptr) ? ptr->first_child : ptr;
  }

  void ResetSize() {
    if (not bundle_doc_) {
      size_ = 0;
    } else {
      auto *ptr = JsonDocument::SearchInObject(bundle_doc_->root(),
                                               "dependencies", JSON_ARRAY);
      size_ = 0;
      for (ptr = ptr->first_child; ptr != nullptr; ptr = ptr->next_sibling) {
        ++size_;
      }
    }
  }

  void ReleaseDocument() {
    if (bundle_doc_ != nullptr) {
      delete bundle_doc_;
      bundle_doc_ = nullptr;
    }
  }
  size_t size_ = 0;
  JsonDocument *bundle_doc_ = nullptr;
  JSON *current_entry_ = nullptr;
};

#endif

