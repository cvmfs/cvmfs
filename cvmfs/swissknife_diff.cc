/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "swissknife_diff.h"

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
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace swissknife {


CommandDiff::~CommandDiff() {
  delete catalog_mgr_from_;
  delete catalog_mgr_to_;
}


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
  r.push_back(Parameter::Switch('L', "follow HTTP redirects"));
  return r;
}


void CommandDiff::AppendFirstEntry(catalog::DirectoryEntryList *entry_list) {
  catalog::DirectoryEntry empty_entry;
  entry_list->push_back(empty_entry);
}


void CommandDiff::AppendLastEntry(catalog::DirectoryEntryList *entry_list) {
  assert(!entry_list->empty());
  catalog::DirectoryEntry last_entry;
  last_entry.set_inode(kLastInode);
  entry_list->push_back(last_entry);
}


bool CommandDiff::IsSmaller(
  const catalog::DirectoryEntry &a,
  const catalog::DirectoryEntry &b)
{
  bool a_is_first = (a.inode() == catalog::DirectoryEntryBase::kInvalidInode);
  bool a_is_last = (a.inode() == kLastInode);
  bool b_is_first = (b.inode() == catalog::DirectoryEntryBase::kInvalidInode);
  bool b_is_last = (b.inode() == kLastInode);

  if (a_is_last || b_is_first)
    return false;
  if (a_is_first)
    return !b_is_first;
  if (b_is_last)
    return !a_is_last;
  return a.name() < b.name();
}



void CommandDiff::FindDiff(const PathString &base_path) {
  bool retval;
  catalog::DirectoryEntryList listing_from;
  catalog::DirectoryEntryList listing_to;
  AppendFirstEntry(&listing_from);
  AppendFirstEntry(&listing_to);
  retval = catalog_mgr_from_->Listing(base_path, &listing_from);
  assert(retval);
  retval = catalog_mgr_to_->Listing(base_path, &listing_to);
  assert(retval);
  sort(listing_from.begin(), listing_from.end(), IsSmaller);
  sort(listing_to.begin(), listing_to.end(), IsSmaller);
  AppendLastEntry(&listing_from);
  AppendLastEntry(&listing_to);

  unsigned i_from = 0, size_from = listing_from.size();
  unsigned i_to = 0, size_to = listing_to.size();
  while ((i_from < size_from) || (i_to < size_to)) {
    catalog::DirectoryEntry entry_from = listing_from[i_from];
    catalog::DirectoryEntry entry_to = listing_to[i_to];

    PathString path_from(base_path);
    path_from.Append("/", 1);
    path_from.Append(entry_from.name().GetChars(),
                     entry_from.name().GetLength());
    PathString path_to(base_path);
    path_to.Append("/", 1);
    path_to.Append(entry_to.name().GetChars(), entry_to.name().GetLength());

    if (IsSmaller(entry_to, entry_from)) {
      i_to++;
      ReportAddition(path_to, entry_to);
      continue;
    } else if (IsSmaller(entry_from, entry_to)) {
      i_from++;
      ReportRemoval(path_from, entry_from);
      continue;
    }

    assert(path_from == path_to);
    i_from++;
    i_to++;
    ReportModification(path_from, entry_from, entry_to);
    if (!entry_from.IsDirectory() || !entry_to.IsDirectory())
      continue;

    // Recursion
    catalog::DirectoryEntryBase::Differences diff =
      entry_from.CompareTo(entry_to);
    if ((diff == catalog::DirectoryEntryBase::Difference::kIdentical) &&
        entry_from.IsNestedCatalogMountpoint())
    {
      // Early recursion stop if nested catalogs are identical
      shash::Any id_nested_from, id_nested_to;
      id_nested_from = catalog_mgr_from_->GetNestedCatalogHash(path_from);
      id_nested_to = catalog_mgr_to_->GetNestedCatalogHash(path_to);
      assert(!id_nested_from.IsNull() && !id_nested_to.IsNull());
      if (id_nested_from == id_nested_to)
        continue;
    }
    FindDiff(path_from);
  }
}


string CommandDiff::PrintEntryType(const catalog::DirectoryEntry &entry) {
  if (entry.IsRegular())
    return machine_readable_ ? "F" : "file";
  else if (entry.IsLink())
    return machine_readable_ ? "S" : "symlink";
  else if (entry.IsDirectory())
    return machine_readable_ ? "D" : "directory";
  else
    return machine_readable_ ? "U" : "unknown";
}


string CommandDiff::PrintDifferences(
  catalog::DirectoryEntryBase::Differences diff)
{
  vector<string> result_list;
  if (diff & catalog::DirectoryEntryBase::Difference::kName)
    result_list.push_back(machine_readable_ ? "N" : "name");
  if (diff & catalog::DirectoryEntryBase::Difference::kLinkcount)
    result_list.push_back(machine_readable_ ? "I" : "link-count");
  if (diff & catalog::DirectoryEntryBase::Difference::kSize)
    result_list.push_back(machine_readable_ ? "S" : "size");
  if (diff & catalog::DirectoryEntryBase::Difference::kMode)
    result_list.push_back(machine_readable_ ? "M" : "mode");
  if (diff & catalog::DirectoryEntryBase::Difference::kMtime)
    result_list.push_back(machine_readable_ ? "T" : "timestamp");
  if (diff & catalog::DirectoryEntryBase::Difference::kSymlink)
    result_list.push_back(machine_readable_ ? "L" : "symlink-target");
  if (diff & catalog::DirectoryEntryBase::Difference::kChecksum)
    result_list.push_back(machine_readable_ ? "C" : "content");
  if (diff & catalog::DirectoryEntryBase::Difference::kHardlinkGroup)
    result_list.push_back(machine_readable_ ? "G" : "hardlink-group");
  if (diff &
      catalog::DirectoryEntryBase::Difference::kNestedCatalogTransitionFlags)
  {
    result_list.push_back(machine_readable_ ? "N" : "nested-catalog");
  }
  if (diff & catalog::DirectoryEntryBase::Difference::kChunkedFileFlag)
    result_list.push_back(machine_readable_ ? "P" : "chunked-file");
  if (diff & catalog::DirectoryEntryBase::Difference::kHasXattrsFlag)
    result_list.push_back(machine_readable_ ? "X" : "xattrs");
  if (diff & catalog::DirectoryEntryBase::Difference::kExternalFileFlag)
    result_list.push_back(machine_readable_ ? "E" : "external-file");
  if (diff & catalog::DirectoryEntryBase::Difference::kBindMountpointFlag)
    result_list.push_back(machine_readable_ ? "B" : "bind-mountpoint");
  if (diff & catalog::DirectoryEntryBase::Difference::kHiddenFlag)
    result_list.push_back(machine_readable_ ? "H" : "hidden");

  return machine_readable_ ? ("[" + JoinStrings(result_list, "") + "]") :
                             (" [" + JoinStrings(result_list, ", ") + "]");
}


void CommandDiff::ReportAddition(
  const PathString &path,
  const catalog::DirectoryEntry &entry)
{
  string operation = machine_readable_ ? "A" : "add";
  if (machine_readable_) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s",
             operation.c_str(), PrintEntryType(entry).c_str(), path.c_str());
  } else {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s",
             path.c_str(), operation.c_str(), PrintEntryType(entry).c_str());
  }
}


void CommandDiff::ReportRemoval(
  const PathString &path,
  const catalog::DirectoryEntry &entry)
{
  string operation = machine_readable_ ? "R" : "remove";
  if (machine_readable_) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s",
             operation.c_str(), PrintEntryType(entry).c_str(), path.c_str());
  } else {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s",
             path.c_str(), operation.c_str(), PrintEntryType(entry).c_str());
  }
}


void CommandDiff::ReportModification(
  const PathString &path,
  const catalog::DirectoryEntry &entry_from,
  const catalog::DirectoryEntry &entry_to)
{
  catalog::DirectoryEntryBase::Differences diff =
    entry_from.CompareTo(entry_to);
  if (diff == catalog::DirectoryEntryBase::Difference::kIdentical)
    return;

  string type_from = PrintEntryType(entry_from);
  string type_to = PrintEntryType(entry_to);
  string type = type_from;
  if (type_from != type_to) {
    type += machine_readable_ ? type_to : ("->" + type_to);
  }

  string operation = machine_readable_ ? "M" : "modify";
  if (machine_readable_) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %s %s", operation.c_str(),
             PrintDifferences(diff).c_str(), type.c_str(), path.c_str());
  } else {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s%s", path.c_str(),
             operation.c_str(), type.c_str(), PrintDifferences(diff).c_str());
  }
}


void CommandDiff::ReportStats() {
  catalog::Catalog *catalog_from = catalog_mgr_from_->GetRootCatalog();
  catalog::Catalog *catalog_to = catalog_mgr_to_->GetRootCatalog();
  const catalog::Counters counters_from = catalog_from->GetCounters();
  const catalog::Counters counters_to = catalog_to->GetCounters();
  catalog::DeltaCounters counters_diff =
    catalog::Counters::Diff(counters_from, counters_to);
  string operation = machine_readable_ ? "S " : "d(";
  string type_file = machine_readable_ ? "F" : "# regular files):";
  string type_symlink = machine_readable_ ? "S" : "# symlinks):";
  string type_directory = machine_readable_ ? "D" : "# directories):";
  string type_catalog = machine_readable_ ? "N" : "# catalogs):";
  int64_t diff_file = counters_diff.self.regular_files +
                      counters_diff.subtree.regular_files;
  int64_t diff_symlink = counters_diff.self.symlinks +
                         counters_diff.subtree.symlinks;
  int64_t diff_catalog = counters_diff.self.nested_catalogs +
                         counters_diff.subtree.nested_catalogs;
  // Nested catalogs make internally two directory entries
  int64_t diff_directory = counters_diff.self.directories +
                           counters_diff.subtree.directories -
                           diff_catalog;
  LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %" PRId64,
           operation.c_str(), type_file.c_str(), diff_file);
  LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %" PRId64,
           operation.c_str(), type_symlink.c_str(), diff_symlink);
  LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %" PRId64,
           operation.c_str(), type_directory.c_str(), diff_directory);
  LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %" PRId64,
           operation.c_str(), type_catalog.c_str(), diff_catalog);
}


void CommandDiff::ReportHeader() {
  if (machine_readable_) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "# line descriptor: A - add, R - remove, M - modify, "
             "S - statistics; modify flags: S - size, M - mode, T - timestamp, "
             "C - content, L - symlink target; entry types: F - regular file, "
             "S - symbolic link, D - directory, N - nested catalog");
  } else {
    LogCvmfs(kLogCvmfs, kLogStdout, "DELTA: %s/r%u (%s) --> %s/r%u (%s)",
             tag_from_.name.c_str(), tag_from_.revision,
             StringifyTime(tag_from_.timestamp, true).c_str(),
             tag_to_.name.c_str(), tag_to_.revision,
             StringifyTime(tag_to_.timestamp, true).c_str());
  }
}


int swissknife::CommandDiff::Main(const swissknife::ArgumentList &args) {
  const string fqrn = MakeCanonicalPath(*args.find('n')->second);
  const string tmp_dir = MakeCanonicalPath(*args.find('t')->second);
  const string repository = MakeCanonicalPath(*args.find('r')->second);
  const bool show_header = args.count('h') > 0;
  machine_readable_ = args.count('m') > 0;
  const bool follow_redirects = args.count('L') > 0;
  string pubkey_path = *args.find('k')->second;
  if (DirectoryExists(pubkey_path))
    pubkey_path = JoinStrings(FindFiles(pubkey_path, ".pub"), ":");
  string tagname_from = "trunk-previous";
  string tagname_to = "trunk";
  if (args.count('s') > 0)
    tagname_from = *args.find('s')->second;
  if (args.count('d') > 0)
    tagname_to = *args.find('d')->second;

  bool retval = this->InitDownloadManager(follow_redirects);
  assert(retval);
  InitVerifyingSignatureManager(pubkey_path);
  UniquePtr<manifest::Manifest> manifest(FetchRemoteManifest(repository, fqrn));
  assert(manifest.IsValid());

  Assistant assistant(download_manager(), manifest, repository, tmp_dir);
  UniquePtr<history::History> history(
    assistant.GetHistory(Assistant::kOpenReadOnly));
  assert(history.IsValid());
  retval = history->GetByName(tagname_from, &tag_from_);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "unknown tag: s", tagname_from.c_str());
    return 1;
  }
  retval = history->GetByName(tagname_to, &tag_to_);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "unknown tag: s", tagname_to.c_str());
    return 1;
  }

  perf::Statistics statistics_from;
  catalog_mgr_from_ = new catalog::SimpleCatalogManager(
    tag_from_.root_hash,
    repository,
    tmp_dir,
    download_manager(),
    &statistics_from,
    true /* manage catalog files */);
  catalog_mgr_from_->Init();

  perf::Statistics statistics_to;
  catalog_mgr_to_ = new catalog::SimpleCatalogManager(
    tag_to_.root_hash,
    repository,
    tmp_dir,
    download_manager(),
    &statistics_to,
    true /* manage catalog files */);
  catalog_mgr_to_->Init();

  if (show_header)
    ReportHeader();
  ReportStats();
  FindDiff(PathString(""));

  return 0;
}

}  // namespace swissknife
