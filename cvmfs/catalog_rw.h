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
 *  - UpdateEntry
 *  - RemoveEntry
 */

#ifndef CVMFS_CATALOG_RW_H_
#define CVMFS_CATALOG_RW_H_

#include <stdint.h>

#include <vector>
#include <string>

#include "catalog.h"

namespace catalog {

class WritableCatalogManager;

class WritableCatalog : public Catalog {
  friend class WritableCatalogManager;

 public:
  WritableCatalog(const std::string &path, Catalog *parent);
  virtual ~WritableCatalog();

  void Transaction();
  void Commit();

  inline bool IsDirty() const { return dirty_; }
  inline bool IsWritable() const { return true; }
  uint32_t GetMaxLinkId() const;

  void AddEntry(const DirectoryEntry &entry, const std::string &entry_path,
                const std::string &parent_path);
  void TouchEntry(const DirectoryEntryBase &entry, const hash::Md5 &path_hash);
  inline void TouchEntry(const DirectoryEntryBase &entry, const std::string &path) {
    TouchEntry(entry, hash::Md5(hash::AsciiPtr(path)));
  }
  void RemoveEntry(const std::string &entry_path);
  void IncLinkcount(const std::string &path_within_group, const int delta);

  // Creation and removal of catalogs
  void Partition(WritableCatalog *new_nested_catalog);
  void MergeIntoParent();

  // Nested catalog references
  void InsertNestedCatalog(const std::string &mountpoint,
                           Catalog *attached_reference,
                           const hash::Any content_hash);
  void UpdateNestedCatalog(const std::string &path, const hash::Any &hash);
  void RemoveNestedCatalog(const std::string &mountpoint,
                           Catalog **attached_reference);

  void UpdateLastModified();
  void IncrementRevision();
  void SetPreviousRevision(const hash::Any &hash);

 protected:
  Database::OpenMode DatabaseOpenMode() const {
    return Database::kOpenReadWrite;
  }

  void UpdateEntry(const DirectoryEntry &entry, const hash::Md5 &path_hash);
  inline void UpdateEntry(const DirectoryEntry &entry, const std::string &path) {
    UpdateEntry(entry, hash::Md5(hash::AsciiPtr(path)));
  }

  inline void AddEntry(const DirectoryEntry &entry, const std::string &path) {
    AddEntry(entry, path, GetParentPath(path));
  }

  void InitPreparedStatements();
  void FinalizePreparedStatements();

  inline WritableCatalog* GetWritableParent() const {
    Catalog *parent = this->parent();
    assert(parent->IsWritable());
    return static_cast<WritableCatalog *>(parent);
  }

 private:
  SqlDirentInsert     *sql_insert_;
  SqlDirentUnlink     *sql_unlink_;
  SqlDirentTouch      *sql_touch_;
  SqlDirentUpdate     *sql_update_;
  SqlMaxHardlinkGroup *sql_max_link_id_;
  SqlIncLinkcount     *sql_inc_linkcount_;

  bool dirty_;  /**< Indicates if the catalog has been changed */

  DeltaCounters delta_counters_;

  inline void SetDirty() {
    if (!dirty_)
      Transaction();
    dirty_ = true;
  }

  // Helpers for nested catalog creation and removal
  void MakeTransitionPoint(const std::string &mountpoint);
  void MakeNestedRoot();
  inline void MoveToNested(const std::string dir_structure_root,
                           WritableCatalog *new_nested_catalog,
                           std::vector<std::string> *grand_child_mountpoints) {
    MoveToNestedRecursively(dir_structure_root,
                            new_nested_catalog,
                            grand_child_mountpoints);
  }
  void MoveToNestedRecursively(const std::string dir_structure_root,
                             WritableCatalog *new_nested_catalog,
                             std::vector<std::string> *grand_child_mountpoints);
  void MoveCatalogsToNested(const std::vector<std::string> &nested_catalogs,
                            WritableCatalog *new_nested_catalog);

  void CopyToParent();
  void CopyCatalogsToParent();

  void UpdateCounters();
};  // class WritableCatalog

typedef std::vector<WritableCatalog *> WritableCatalogList;

}  // namespace catalog

#endif  // CVMFS_CATALOG_RW_H_
