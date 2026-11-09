/**
 * This file is part of the CernVM File System.
 *
 * This class implements the format for .cvmfsbundle files
 */

#ifndef CVMFS_FILE_BUNDLE_H_
#define CVMFS_FILE_BUNDLE_H_

#include <cassert>
#include <fstream>
#include <sstream>
#include <string>

#include "json_document.h"
#include "shortstring.h"
#include "util/single_copy.h"

/*

The .cvmfsbundle file serves both as a file list and as a trigger for loading a
bundle. The convention is to call it .cvmfsbundle-<filename>[.cvmfsbundle],
where <filename> should trigger the bundle. The optional .cvmfsbundle suffix
keeps tools that scan by extension (e.g. Lmod for *.lua) from picking up the
spec. Both forms for the same <filename> must not coexist. A trigger whose own
name ends in .cvmfsbundle needs the suffixed form.

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

namespace bundle {
const char kSpecPrefix[] = ".cvmfsbundle-";
const char kSpecSuffix[] = ".cvmfsbundle";

/** Spec file name with the optional suffix, looked up first. */
inline std::string SuffixedSpecName(const std::string &trigger_name) {
  return std::string(kSpecPrefix) + trigger_name + kSpecSuffix;
}

inline std::string PlainSpecName(const std::string &trigger_name) {
  return std::string(kSpecPrefix) + trigger_name;
}

/** Trigger file name for a spec file name; empty if there is none. */
inline std::string TriggerName(const std::string &spec_name) {
  const size_t np = sizeof(kSpecPrefix) - 1;
  const size_t ns = sizeof(kSpecSuffix) - 1;
  if (spec_name.compare(0, np, kSpecPrefix) != 0)
    return "";
  std::string name = spec_name.substr(np);
  if (name.size() > ns && name.compare(name.size() - ns, ns, kSpecSuffix) == 0)
    name.resize(name.size() - ns);
  return name;
}
}  // namespace bundle

class BundleFileMgr : SingleCopy {
 public:
  BundleFileMgr(const PathString &bundle_file_path) {
    std::ifstream file(bundle_file_path.ToString());
    if (!file.is_open()) {
      bundle_doc_ = nullptr;
    } else {
      std::stringstream rbuf;
      rbuf << file.rdbuf();
      Manage(JsonDocument::Create(rbuf.str()));
    }
  }

  BundleFileMgr(JsonDocument *fc) { Manage(fc); }

  virtual ~BundleFileMgr() { ReleaseDocument(); }

  std::string GetVersion() const {
    const JSON *ptr = JsonDocument::SearchInObject(
        bundle_doc_->root(), "version", JSON_STRING);
    return (ptr) ? ptr->get<std::string>() : std::string();
  }

  std::string GetEncoding() const {
    const JSON *ptr = JsonDocument::SearchInObject(
        bundle_doc_->root(), "encoding", JSON_STRING);
    return (ptr) ? ptr->get<std::string>() : std::string();
  }

  virtual PathString GetNext() {
    const JSON *deps = Dependencies();
    if (deps == nullptr || current_index_ >= deps->size()) {
      // This should be handled as an error code. Path string should never be
      // empty;
      return PathString();
    }
    const JSON &entry = (*deps)[current_index_++];
    assert(entry.is_string());
    const PathString result = static_cast<PathString>(
        entry.get<std::string>());
    return (result.IsEmpty()) ? GetNext() : result;
  }

  virtual size_t Size() const { return size_; }

  operator bool() const {
    return (bundle_doc_ != nullptr) ? bundle_doc_->IsValid() : false;
  }

 private:
  void Manage(JsonDocument *fc) {
    bundle_doc_ = fc;
    current_index_ = 0;
    ResetSize();
  }

  const JSON *Dependencies() const {
    if (bundle_doc_ == nullptr)
      return nullptr;
    return JsonDocument::SearchInObject(
        bundle_doc_->root(), "dependencies", JSON_ARRAY);
  }

  void ResetSize() {
    const JSON *deps = Dependencies();
    size_ = (deps != nullptr) ? deps->size() : 0;
  }

  void ReleaseDocument() {
    if (bundle_doc_ != nullptr) {
      delete bundle_doc_;
      bundle_doc_ = nullptr;
    }
  }
  size_t size_ = 0;
  size_t current_index_ = 0;
  JsonDocument *bundle_doc_ = nullptr;
};

#endif
