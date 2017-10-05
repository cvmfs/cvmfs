/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_TEST_TOOLS_H_
#define CVMFS_CATALOG_TEST_TOOLS_H_

#include <set>
#include <string>
#include <vector>

#include "catalog_mgr_rw.h"
#include "directory_entry.h"
#include "manifest.h"
#include "server_tool.h"
#include "statistics.h"
#include "upload.h"

/**
 * Multiple DirSpecItem objects make up a DirSpec object
 *
 * The DirSpecItem encapsulates the DirectoryEntry, XattrList and parent
 * directory associated with a DirSpec item.
 */
struct DirSpecItem {
  catalog::DirectoryEntry entry_;
  XattrList xattrs_;
  std::string parent_;

  DirSpecItem(const catalog::DirectoryEntry& entry, const XattrList& xattrs,
              const std::string& parent)
      : entry_(entry), xattrs_(xattrs), parent_(parent) {}


  const catalog::DirectoryEntryBase& entry_base() const {
    return static_cast<const catalog::DirectoryEntryBase&>(entry_);
  }
  const XattrList& xattrs() const { return xattrs_; }
  const std::string& parent() const { return parent_; }
};

/**
 * Declarative specification of the state of a repository
 *
 * The DirSpec is used to specify the contents of a repository. Currently,
 * the items of a DirSpec can either be directories or files.
 *
 * The DirSpec object is usually given as input to the CatalogTestTool, which
 * will build a catalog with the specified contents. Additionally, the
 * CatalogTestTool is also able to synthetise a DirSpec based on the contents
 * of an arbitrary catalog, referenced by a root hash.
 */
class DirSpec {
 public:
  typedef std::map<std::string, DirSpecItem> ItemList;

  DirSpec();

  bool AddFile(const std::string& name,
               const std::string& parent,
               const std::string& digest,
               const size_t size,
               const XattrList& xattrs = XattrList(),
               shash::Suffix suffix = shash::kSha1);
  bool AddDirectory(const std::string& name,
                    const std::string& parent,
                    const size_t size);

  bool AddDirectoryEntry(const catalog::DirectoryEntry& entry,
                         const XattrList& xattrs,
                         const std::string& parent);

  void ToString(std::string* out);

  const ItemList& items() const { return items_; }

  size_t NumItems() const { return items_.size(); }

  const DirSpecItem* Item(const std::string& full_path) const;

  void SetItem(const DirSpecItem& item, const std::string& full_path) {
    ItemList::iterator it = items_.find(full_path);
    if (it != items_.end()) {
      it->second = item;
    }
  }

  void RemoveItemRec(const std::string& full_path);

  std::vector<std::string> GetDirs() const;

 private:
  bool AddDir(const std::string& name, const std::string& parent);
  bool RmDir(const std::string& name, const std::string& parent);
  bool HasDir(const std::string& name) const;

  ItemList items_;
  std::set<std::string> dirs_;
};

/**
 * A helper class creating catalog instances for testing
 *
 * The CatalogTestTool is a helper class which can create create
 * repository catalogs based on an input specification (DirSpec).
 *
 * A given DirSpec is realized with the Apply(...) method.
 *
 * The history() method returns a list of root hashes of all the
 * realized catalogs.
 *
 * Finally, the class is also able to construct a DirSpec object
 * based on the state at a given root hash.
 */
class CatalogTestTool : public ServerTool {
 public:
  typedef std::vector<std::pair<std::string, shash::Any> > History;

  CatalogTestTool(const std::string& name);
  ~CatalogTestTool();

  bool Init();
  bool Apply(const std::string& id, const DirSpec& spec);

  bool DirSpecAtRootHash(const shash::Any& root_hash, DirSpec* spec);

  manifest::Manifest* manifest() { return manifest_.weak_ref(); }

  History history() { return history_; }

 private:
  static upload::Spooler* CreateSpooler(const std::string& config);

  static manifest::Manifest* CreateRepository(const std::string& dir,
                                              upload::Spooler* spooler);

  static catalog::WritableCatalogManager* CreateCatalogMgr(
      const shash::Any& root_hash, const std::string stratum0,
      const std::string& temp_dir, upload::Spooler* spooler,
      download::DownloadManager* dl_mgr, perf::Statistics* stats);

  const std::string name_;

  std::string stratum0_;
  std::string temp_dir_;

  UniquePtr<manifest::Manifest> manifest_;
  UniquePtr<upload::Spooler> spooler_;
  History history_;
};

#endif  //  CVMFS_CATALOG_TEST_TOOLS_H_
