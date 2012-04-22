/**
 * This file is part of the CernVM File System
 *
 * The WritableCatalog class is derived from Catalog.
 * It is used by the WritableCatalogManager on the server side.
 *
 * Catalogs are meant to be thread safe.
 *
 * The main functionality is in:
 *  - AddEntry
 *  - TouchEntry
 *  - RemoveEntry
 */

#ifndef CVMFS_CATALOG_RW_H_
#define CVMFS_CATALOG_RW_H_

#include <vector>
#include <string>

#include "catalog.h"

namespace catalog {

class WritableCatalog : public Catalog {
  friend class WritableCatalogManager;

 public:
  WritableCatalog(const std::string &path, Catalog *parent);
  virtual ~WritableCatalog();
  static bool CreateDatabase(const std::string &file_path,
                             const DirectoryEntry &root_entry,
                             const std::string &root_entry_parent_path,
                             const bool root_catalog);

  void Transaction();
  void Commit();

  inline bool IsDirty() const { return dirty_; }
  inline bool IsWritable() const { return true; }
  int GetMaxLinkId() const;

  bool AddEntry(const DirectoryEntry &entry, const std::string &entry_path,
                const std::string &parent_path);
  bool TouchEntry(const DirectoryEntry &entry, const std::string &entry_path);
  bool RemoveEntry(const std::string &entry_path);

  // Creation and removal of catalogs
  bool Partition(WritableCatalog *new_nested_catalog);
  bool MergeIntoParent();

  // Nested catalog references
  bool InsertNestedCatalog(const std::string &mountpoint,
                           Catalog *attached_reference,
                           const hash::Any content_hash);
  bool UpdateNestedCatalog(const std::string &path, const hash::Any &hash);
  bool RemoveNestedCatalog(const std::string &mountpoint,
                           Catalog **attached_reference);

  bool UpdateLastModified();
  bool IncrementRevision();
  bool SetPreviousRevision(const hash::Any &hash);

 protected:
  inline int DatabaseOpenFlags() const {
    return SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE;
  }

  inline bool AddEntry(const DirectoryEntry &entry, const std::string &path) {
    return AddEntry(entry, path, GetParentPath(path));
  }

  bool UpdateEntry(const DirectoryEntry &entry, const hash::Md5 &path_hash);
  inline bool UpdateEntry(const DirectoryEntry &entry, const std::string &path)
  {
    return UpdateEntry(entry, hash::Md5(hash::AsciiPtr(path)));
  }

  void InitPreparedStatements();
  void FinalizePreparedStatements();

  inline WritableCatalog* GetWritableParent() const {
    Catalog *parent = this->parent();
    assert(parent->IsWritable());
    return static_cast<WritableCatalog *>(parent);
  }

 private:
  InsertDirectoryEntrySqlStatement   *sql_insert_;
  TouchSqlStatement                  *sql_touch_;
  UnlinkSqlStatement                 *sql_unlink_;
  UpdateDirectoryEntrySqlStatement   *sql_update_;
  GetMaximalHardlinkGroupIdStatement *sql_max_link_id_;

  bool dirty_;  /**< Indicates if the catalog has been changed */

  inline void SetDirty() {
    if (!dirty_)
      Transaction();
    dirty_ = true;
  }
  static bool CreateSchema(const std::string &file_path);

  // Helpers for nested catalog creation and removal
  bool MakeTransitionPoint(const std::string &mountpoint);
  bool MakeNestedRoot();
  inline bool MoveToNested(const std::string dir_structure_root,
                           WritableCatalog *new_nested_catalog,
                           std::vector<std::string> *grand_child_mountpoints) {
    return MoveToNestedRecursively(dir_structure_root,
                                   new_nested_catalog,
                                   grand_child_mountpoints);
  }
  bool MoveToNestedRecursively(const std::string dir_structure_root,
                             WritableCatalog *new_nested_catalog,
                             std::vector<std::string> *grand_child_mountpoints);
  bool MoveCatalogsToNested(const std::vector<std::string> &nested_catalogs,
                            WritableCatalog *new_nested_catalog);

  bool CopyToParent();
  bool CopyCatalogsToParent();
};  // class WritableCatalog

typedef std::vector<WritableCatalog *> WritableCatalogList;

}  // namespace catalog

#endif  // CVMFS_CATALOG_RW_H_
