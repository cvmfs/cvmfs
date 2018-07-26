/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "swissknife_diff_tool.h"

#include <inttypes.h>
#include <stdint.h>
#include "cvmfs_config.h"

#include <vector>

#include "directory_entry.h"
#include "download.h"
#include "statistics.h"

namespace swissknife {

DiffTool::DiffTool(const std::string &repo_path,
                   const history::History::Tag &old_tag,
                   const history::History::Tag &new_tag,
                   const std::string &temp_dir,
                   download::DownloadManager *download_manager,
                   perf::Statistics *statistics,
                   bool machine_readable,
                   bool ignore_timediff)
    : CatalogDiffTool<catalog::SimpleCatalogManager>(
          repo_path, old_tag.root_hash, new_tag.root_hash, temp_dir,
          download_manager),
      old_tag_(old_tag),
      new_tag_(new_tag),
      machine_readable_(machine_readable),
      ignore_timediff_(ignore_timediff)
{
    // Created here but ownership goes to the Statistics class
    // owned by the server tool.
    counter_total_added_ = statistics->Register("difftool.added_bytes",
                                                "Total number of bytes added");
    counter_total_removed_ = statistics->Register("difftool.removed_bytes",
                                                  "Total number of bytes "
                                                  "removed");
}

DiffTool::~DiffTool() {}

std::string DiffTool::PrintEntryType(const catalog::DirectoryEntry &entry) {
  if (entry.IsRegular())
    return machine_readable_ ? "F" : "file";
  else if (entry.IsLink())
    return machine_readable_ ? "S" : "symlink";
  else if (entry.IsDirectory())
    return machine_readable_ ? "D" : "directory";
  else
    return machine_readable_ ? "U" : "unknown";
}

std::string DiffTool::PrintDifferences(
    catalog::DirectoryEntryBase::Differences diff) {
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
      catalog::DirectoryEntryBase::Difference::kNestedCatalogTransitionFlags) {
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

  return machine_readable_ ? ("[" + JoinStrings(result_list, "") + "]")
                           : (" [" + JoinStrings(result_list, ", ") + "]");
}

void DiffTool::ReportHeader() {
  if (machine_readable_) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "# line descriptor: A - add, R - remove, M - modify, "
             "S - statistics; modify flags: S - size, M - mode, T - timestamp, "
             "C - content, L - symlink target; entry types: F - regular file, "
             "S - symbolic link, D - directory, N - nested catalog");
  } else {
    LogCvmfs(kLogCvmfs, kLogStdout, "DELTA: %s/r%u (%s) --> %s/r%u (%s)",
             old_tag_.name.c_str(), old_tag_.revision,
             StringifyTime(old_tag_.timestamp, true).c_str(),
             new_tag_.name.c_str(), new_tag_.revision,
             StringifyTime(new_tag_.timestamp, true).c_str());
  }
}

void DiffTool::ReportAddition(const PathString &path,
                              const catalog::DirectoryEntry &entry,
                              const XattrList & /*xattrs*/,
                              const FileChunkList& /*chunks*/) {
  // XXX careful we're casting silently from usint64 to int64 there...
  counter_total_added_->Xadd(entry.size());
  string operation = machine_readable_ ? "A" : "add";
  if (machine_readable_) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s +%" PRIu64, operation.c_str(),
             PrintEntryType(entry).c_str(), path.c_str(), entry.size());
  } else {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s +%" PRIu64 " bytes",
             path.c_str(), operation.c_str(),
             PrintEntryType(entry).c_str(), entry.size());
  }
}

void DiffTool::ReportRemoval(const PathString &path,
                             const catalog::DirectoryEntry &entry) {
  // XXX careful we're casting silently from usint64 to int64 there...
  counter_total_removed_->Xadd(entry.size());
  string operation = machine_readable_ ? "R" : "remove";
  if (machine_readable_) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s -%" PRIu64, operation.c_str(),
             PrintEntryType(entry).c_str(), path.c_str(), entry.size());
  } else {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s -%" PRIu64 " bytes",
             path.c_str(), operation.c_str(),
             PrintEntryType(entry).c_str(), entry.size());
  }
}

void DiffTool::ReportModification(const PathString &path,
                                  const catalog::DirectoryEntry &entry_from,
                                  const catalog::DirectoryEntry &entry_to,
                                  const XattrList & /*xattrs*/,
                                  const FileChunkList& /*chunks*/) {
  catalog::DirectoryEntryBase::Differences diff =
      entry_from.CompareTo(entry_to);
  if (ignore_timediff_) {
    diff = diff & ~catalog::DirectoryEntryBase::Difference::kMtime;
    if (diff == 0)
      return;
  }

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

void DiffTool::ReportStats() {
  const catalog::Catalog *catalog_from = GetOldCatalog();
  const catalog::Catalog *catalog_to = GetNewCatalog();
  const catalog::Counters counters_from = catalog_from->GetCounters();
  const catalog::Counters counters_to = catalog_to->GetCounters();
  catalog::DeltaCounters counters_diff =
      catalog::Counters::Diff(counters_from, counters_to);
  string operation = machine_readable_ ? "S " : "d(";
  string type_file = machine_readable_ ? "F" : "# regular files):";
  string type_symlink = machine_readable_ ? "S" : "# symlinks):";
  string type_directory = machine_readable_ ? "D" : "# directories):";
  string type_catalog = machine_readable_ ? "N" : "# catalogs):";
  int64_t diff_file =
      counters_diff.self.regular_files + counters_diff.subtree.regular_files;
  int64_t diff_symlink =
      counters_diff.self.symlinks + counters_diff.subtree.symlinks;
  int64_t diff_catalog = counters_diff.self.nested_catalogs +
                         counters_diff.subtree.nested_catalogs;
  // Nested catalogs make internally two directory entries
  int64_t diff_directory = counters_diff.self.directories +
                           counters_diff.subtree.directories - diff_catalog;
  LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %" PRId64, operation.c_str(),
           type_file.c_str(), diff_file);
  LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %" PRId64, operation.c_str(),
           type_symlink.c_str(), diff_symlink);
  LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %" PRId64, operation.c_str(),
           type_directory.c_str(), diff_directory);
  LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %" PRId64, operation.c_str(),
           type_catalog.c_str(), diff_catalog);
}

}  // namespace swissknife
