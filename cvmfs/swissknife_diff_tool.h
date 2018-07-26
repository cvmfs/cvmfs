/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_DIFF_TOOL_H_
#define CVMFS_SWISSKNIFE_DIFF_TOOL_H_

#include <string>

#include "catalog_diff_tool.h"
#include "catalog_mgr_ro.h"
#include "file_chunk.h"
#include "history.h"
#include "shortstring.h"

namespace perf {
class Statistics;
}

namespace swissknife {

class DiffTool : public CatalogDiffTool<catalog::SimpleCatalogManager> {
 public:
  DiffTool(const std::string &repo_path, const history::History::Tag &old_tag,
           const history::History::Tag &new_tag, const std::string &temp_dir,
           download::DownloadManager *download_manager,
           perf::Statistics *statistics,
           bool machine_readable, bool ignore_timediff);

  virtual ~DiffTool();

  void ReportHeader();
  void ReportStats();

 private:
  std::string PrintEntryType(const catalog::DirectoryEntry &entry);

  std::string PrintDifferences(catalog::DirectoryEntryBase::Differences diff);

  void ReportAddition(const PathString &path,
                      const catalog::DirectoryEntry &entry,
                      const XattrList & /*xattrs*/,
                      const FileChunkList& /*chunks*/);
  void ReportRemoval(const PathString &path,
                     const catalog::DirectoryEntry &entry);
  void ReportModification(const PathString &path,
                          const catalog::DirectoryEntry &entry_from,
                          const catalog::DirectoryEntry &entry_to,
                          const XattrList & /*xattrs*/,
                          const FileChunkList& /*chunks*/);

  history::History::Tag old_tag_;
  history::History::Tag new_tag_;
  bool machine_readable_;
  bool ignore_timediff_;
  perf::Counter *counter_total_added_;
  perf::Counter *counter_total_removed_;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_DIFF_TOOL_H_
