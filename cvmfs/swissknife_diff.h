/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_DIFF_H_
#define CVMFS_SWISSKNIFE_DIFF_H_

#include <stdint.h>

#include "directory_entry.h"
#include "shortstring.h"
#include "swissknife.h"

namespace catalog {
class SimpleCatalogManager;
}

namespace swissknife {

class CommandDiff : public Command {
 public:
  CommandDiff() : catalog_mgr_from_(NULL), catalog_mgr_to_(NULL),
                  machine_readable_(false) { }
  ~CommandDiff();
  virtual std::string GetName() const { return "diff"; }
  virtual std::string GetDescription() const {
    return "Show changes between two revisions";
  }
  ParameterList GetParams() const;
  int Main(const ArgumentList &args);

 private:
  /**
   * Used for an artificall directory entry that is always considered the
   * last entry.
   */
  static const uint64_t kLastInode = uint64_t(-1);
  static bool IsSmaller(const catalog::DirectoryEntry &a,
                        const catalog::DirectoryEntry &b);

  catalog::SimpleCatalogManager *catalog_mgr_from_;
  catalog::SimpleCatalogManager *catalog_mgr_to_;
  bool machine_readable_;

  std::string PrintEntryType(const catalog::DirectoryEntry &entry);
  std::string PrintDifferences(catalog::DirectoryEntryBase::Differences diff);

  void ReportAddition(const PathString &path,
                      const catalog::DirectoryEntry &entry);
  void ReportRemoval(const PathString &path,
                     const catalog::DirectoryEntry &entry);
  void ReportModification(const PathString &path,
                          const catalog::DirectoryEntry &entry_from,
                          const catalog::DirectoryEntry &entry_to);

  void AppendFirstEntry(catalog::DirectoryEntryList *entry_list);
  void AppendLastEntry(catalog::DirectoryEntryList *entry_list);
  void FindDiff(const PathString &base_path);
};  // class CommandDiff

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_DIFF_H_
