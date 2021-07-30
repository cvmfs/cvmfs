/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_diff.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <cassert>
#include <string>
#include <vector>

#include "catalog_counters.h"
#include "directory_entry.h"
#include "history.h"
#include "logging.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "sanitizer.h"
#include "util/pointer.h"
#include "util/string.h"

namespace {

class DiffReporter : public publish::DiffListener {
 public:
  DiffReporter(bool show_header, bool machine_readable, bool ignore_timediff)
    : show_header_(show_header)
    , machine_readable_(machine_readable)
    , ignore_timediff_(ignore_timediff)
  {}
  virtual ~DiffReporter() {}


  virtual void OnInit(const history::History::Tag &from_tag,
                      const history::History::Tag &to_tag)
  {
    if (!show_header_)
      return;

    if (machine_readable_) {
      LogCvmfs(kLogCvmfs, kLogStdout,
             "# line descriptor: A - add, R - remove, M - modify, "
             "S - statistics; modify flags: S - size, M - mode, T - timestamp, "
             "C - content, L - symlink target; entry types: F - regular file, "
             "S - symbolic link, D - directory, N - nested catalog");
    } else {
      LogCvmfs(kLogCvmfs, kLogStdout, "DELTA: %s/r%u (%s) --> %s/r%u (%s)",
               from_tag.name.c_str(), from_tag.revision,
               StringifyTime(from_tag.timestamp, true).c_str(),
               to_tag.name.c_str(), to_tag.revision,
               StringifyTime(to_tag.timestamp, true).c_str());
    }
  }


  virtual void OnStats(const catalog::DeltaCounters &delta) {
    std::string operation = machine_readable_ ? "S " : "d(";
    std::string type_file = machine_readable_ ? "F" : "# regular files):";
    std::string type_symlink = machine_readable_ ? "S" : "# symlinks):";
    std::string type_directory = machine_readable_ ? "D" : "# directories):";
    std::string type_catalog = machine_readable_ ? "N" : "# catalogs):";
    int64_t diff_file = delta.self.regular_files + delta.subtree.regular_files;
    int64_t diff_symlink = delta.self.symlinks + delta.subtree.symlinks;
    int64_t diff_catalog = delta.self.nested_catalogs +
                           delta.subtree.nested_catalogs;
    // Nested catalogs make internally two directory entries
    int64_t diff_directory = delta.self.directories +
                             delta.subtree.directories - diff_catalog;
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %" PRId64, operation.c_str(),
             type_file.c_str(), diff_file);
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %" PRId64, operation.c_str(),
             type_symlink.c_str(), diff_symlink);
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %" PRId64, operation.c_str(),
             type_directory.c_str(), diff_directory);
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %" PRId64, operation.c_str(),
             type_catalog.c_str(), diff_catalog);
  }


  virtual void OnAdd(const std::string &path,
                     const catalog::DirectoryEntry &entry)
  {
    std::string operation = machine_readable_ ? "A" : "add";
    if (machine_readable_) {
      LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s +%" PRIu64, operation.c_str(),
               PrintEntryType(entry).c_str(), path.c_str(), entry.size());
    } else {
      LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s +%" PRIu64 " bytes",
               path.c_str(), operation.c_str(),
               PrintEntryType(entry).c_str(), entry.size());
    }
  }


  virtual void OnRemove(const std::string &path,
                        const catalog::DirectoryEntry &entry)
  {
    std::string operation = machine_readable_ ? "R" : "remove";
    if (machine_readable_) {
      LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s -%" PRIu64, operation.c_str(),
               PrintEntryType(entry).c_str(), path.c_str(), entry.size());
    } else {
      LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s -%" PRIu64 " bytes",
               path.c_str(), operation.c_str(),
               PrintEntryType(entry).c_str(), entry.size());
    }
  }


  virtual void OnModify(const std::string &path,
                        const catalog::DirectoryEntry &entry_from,
                        const catalog::DirectoryEntry &entry_to)
  {
    catalog::DirectoryEntryBase::Differences diff =
      entry_from.CompareTo(entry_to);
    if (ignore_timediff_) {
      diff = diff & ~catalog::DirectoryEntryBase::Difference::kMtime;
      if (diff == 0)
        return;
    }

    std::string type_from = PrintEntryType(entry_from);
    std::string type_to = PrintEntryType(entry_to);
    std::string type = type_from;
    if (type_from != type_to) {
      type += machine_readable_ ? type_to : ("->" + type_to);
    }

    std::string operation = machine_readable_ ? "M" : "modify";
    if (machine_readable_) {
      LogCvmfs(kLogCvmfs, kLogStdout, "%s%s %s %s", operation.c_str(),
               PrintDifferences(diff).c_str(), type.c_str(), path.c_str());
    } else {
      LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %s%s", path.c_str(),
               operation.c_str(), type.c_str(), PrintDifferences(diff).c_str());
    }
  }

 private:
  std::string PrintDifferences(catalog::DirectoryEntryBase::Differences diff) {
    std::vector<std::string> result_list;
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
    if (diff & catalog::DirectoryEntryBase::Difference::kDirectIoFlag)
      result_list.push_back(machine_readable_ ? "D" : "direct-io");

    return machine_readable_ ? ("[" + JoinStrings(result_list, "") + "]")
                             : (" [" + JoinStrings(result_list, ", ") + "]");
  }

  std::string PrintEntryType(const catalog::DirectoryEntry &entry) {
    if (entry.IsRegular()) return machine_readable_ ? "F" : "file";
    else if (entry.IsLink()) return machine_readable_ ? "S" : "symlink";
    else if (entry.IsDirectory()) return machine_readable_ ? "D" : "directory";
    else
      return machine_readable_ ? "U" : "unknown";
  }

  bool show_header_;
  bool machine_readable_;
  bool ignore_timediff_;
};  // class DiffReporter

}  // anonymous namespace


namespace publish {

int CmdDiff::Main(const Options &options) {
  SettingsBuilder builder;

  if (options.Has("worktree")) {
    UniquePtr<SettingsPublisher> settings(builder.CreateSettingsPublisher(
      options.plain_args().empty() ? "" : options.plain_args()[0].value_str));
    settings->SetIsSilent(true);
    settings->GetTransaction()->SetDryRun(true);
    settings->GetTransaction()->SetPrintChangeset(true);
    Publisher publisher(*settings);
    publisher.Sync();
    return 0;
  }

  SettingsRepository settings = builder.CreateSettingsRepository(
    options.plain_args().empty() ? "" : options.plain_args()[0].value_str);

  std::string from = options.GetStringDefault("from", "trunk-previous");
  std::string to = options.GetStringDefault("to", "trunk");

  if (options.Has("keychain")) {
    settings.GetKeychain()->SetKeychainDir(options.GetString("keychain"));
  }
  Repository repository(settings);

  DiffReporter diff_reporter(options.Has("header"),
                             options.Has("machine-readable"),
                             options.Has("ignore-timediff"));
  repository.Diff(from, to, &diff_reporter);

  return 0;
}

}  // namespace publish
