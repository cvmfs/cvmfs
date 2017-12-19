/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "swissknife_diff.h"
#include "cvmfs_config.h"

#include <inttypes.h>
#include <stdint.h>

#include <algorithm>
#include <cassert>
#include <string>
#include <vector>

#include "catalog.h"
#include "catalog_counters.h"
#include "catalog_mgr_ro.h"
#include "download.h"
#include "statistics.h"
#include "swissknife_assistant.h"
#include "swissknife_diff_tool.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace swissknife {
CommandDiff::~CommandDiff() {}

ParameterList CommandDiff::GetParams() const {
  swissknife::ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository url"));
  r.push_back(Parameter::Mandatory('n', "repository name"));
  r.push_back(Parameter::Mandatory('k', "public key of the repository / dir"));
  r.push_back(Parameter::Mandatory('t', "directory for temporary files"));
  r.push_back(Parameter::Optional('s', "'from' tag name"));
  r.push_back(Parameter::Optional('d', "'to' tag name"));
  r.push_back(Parameter::Switch('m', "machine readable output"));
  r.push_back(Parameter::Switch('h', "show header"));
  // Used for testing
  r.push_back(Parameter::Switch('i', "ignore time stamp differences"));
  r.push_back(Parameter::Switch('L', "follow HTTP redirects"));
  return r;
}


history::History::Tag CommandDiff::GetTag(const string &tag_name) {
  history::History::Tag tag;
  if (tag_name.empty())
    return tag;

  if (tag_name[0] == kRawHashSymbol) {
    tag.name = tag_name.substr(1);
    tag.root_hash =
      shash::MkFromHexPtr(shash::HexPtr(tag.name), shash::kSuffixCatalog);
  } else {
    bool retval = history_->GetByName(tag_name, &tag);
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogStderr, "unknown tag: s", tag_name.c_str());
    }
  }

  return tag;
}


int swissknife::CommandDiff::Main(const swissknife::ArgumentList &args) {
  const string fqrn = MakeCanonicalPath(*args.find('n')->second);
  const string tmp_dir = MakeCanonicalPath(*args.find('t')->second);
  const string repository = MakeCanonicalPath(*args.find('r')->second);
  const bool show_header = args.count('h') > 0;
  const bool machine_readable = args.count('m') > 0;
  const bool ignore_timediff = args.count('i') > 0;
  const bool follow_redirects = args.count('L') > 0;
  string pubkey_path = *args.find('k')->second;
  if (DirectoryExists(pubkey_path))
    pubkey_path = JoinStrings(FindFilesBySuffix(pubkey_path, ".pub"), ":");
  string tagname_from = "trunk-previous";
  string tagname_to = "trunk";
  if (args.count('s') > 0) tagname_from = *args.find('s')->second;
  if (args.count('d') > 0) tagname_to = *args.find('d')->second;

  bool retval = this->InitDownloadManager(follow_redirects);
  assert(retval);
  if (!InitVerifyingSignatureManager(pubkey_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Error calling InitVerifyingSignatureManager");
    return 1;
  }
  UniquePtr<manifest::Manifest> manifest(FetchRemoteManifest(repository, fqrn));
  assert(manifest.IsValid());

  Assistant assistant(download_manager(), manifest, repository, tmp_dir);
  history_ = assistant.GetHistory(Assistant::kOpenReadOnly);
  assert(history_.IsValid());
  history::History::Tag tag_from = GetTag(tagname_from);
  if (tag_from.root_hash.IsNull()) return 1;
  history::History::Tag tag_to = GetTag(tagname_to);
  if (tag_to.root_hash.IsNull()) return 1;

  DiffTool diff_tool(repository, tag_from, tag_to, tmp_dir, download_manager(),
                     statistics(), machine_readable, ignore_timediff);
  if (!diff_tool.Init()) {
    return 1;
  }

  if (show_header) diff_tool.ReportHeader();
  diff_tool.ReportStats();
  diff_tool.Run(PathString(""));

  return 0;
}

}  // namespace swissknife
