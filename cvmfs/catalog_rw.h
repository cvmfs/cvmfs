/**
 * This file is part of the CernVM File System
 *
 * The WritableCatalog class is derived from Catalog. It is used by the
 * WritableCatalogManager on the server side.
 *
 * The main functionality is in:
 *  - AddEntry
 *  - UpdateEntry
 *  - RemoveEntry
 *
 * Catalogs not thread safe.
 */

#ifndef CVMFS_CATALOG_RW_H_
#define CVMFS_CATALOG_RW_H_

#include <stdint.h>

#include <string>
#include <vector>

#include "catalog.h"

class XattrList;

namespace swissknife {
class CommandMigrate;
}

namespace catalog {

class WritableCatalogManager;

class WritableCatalog : public Catalog {
  friend class WritableCatalogManager;
  friend class swissknife::CommandMigrate;  // needed for catalog migrations

 public:
  WritableCatalog(const std::string &path,
                  const shash::Any  &catalog_hash,
                        Catalog     *parent,
                  const bool         is_not_root = false);
  virtual ~WritableCatalog();

  static WritableCatalog *AttachFreely(const std::string &root_path,
                                       const std::string &file,
                                       const shash::Any  &catalog_hash,
                                             Catalog     *parent      = NULL,
                                       const bool         is_not_root = false);

  void Transaction();
  void Commit();

  inline bool IsDirty() const { return dirty_; }
  inline bool IsWritable() const { return true; }
  uint32_t GetMaxLinkId() const;

  void AddEntry(const DirectoryEntry &entry,
                const XattrList &xattr,
                const std::string &entry_path,
                const std::string &parent_path);
  void TouchEntry(const DirectoryEntryBase &entry, const shash::Md5 &path_hash);
  inline void TouchEntry(
    const DirectoryEntryBase &entry,
    const std::string &path)
  {
    TouchEntry(entry, shash::Md5(shash::AsciiPtr(path)));
  }
  void RemoveEntry(const std::string &entry_path);
  void IncLinkcount(const std::string &path_within_group, const int delta);
  void AddFileChunk(const std::string &entry_path, const FileChunk &chunk);
  void RemoveFileChunks(const std::string &entry_path);

  // Creation and removal of catalogs
  void Partition(WritableCatalog *new_nested_catalog);
  void MergeIntoParent();

  // Nested catalog references
  void InsertNestedCatalog(const std::string &mountpoint,
                           Catalog *attached_reference,
                           const shash::Any content_hash,
                           const uint64_t size);
  void UpdateNestedCatalog(const std::string &path,
                           const shash::Any &hash, const uint64_t size);
  void RemoveNestedCatalog(const std::string &mountpoint,
                           Catalog **attached_reference);

  void UpdateLastModified();
  void IncrementRevision();
  void SetRevision(const uint64_t new_revision);
  void SetPreviousRevision(const shash::Any &hash);
  void SetTTL(const uint64_t new_ttl);
  bool SetVOMSAuthz(const std::string &voms_authz);

 protected:
  static const double kMaximalFreePageRatio   = 0.20;
  static const double kMaximalRowIdWasteRatio = 0.25;

  CatalogDatabase::OpenMode DatabaseOpenMode() const {
    return CatalogDatabase::kOpenReadWrite;
  }

  void UpdateEntry(const DirectoryEntry &entry, const shash::Md5 &path_hash);
  inline void UpdateEntry(
    const DirectoryEntry &entry,
    const std::string &path)
  {
    UpdateEntry(entry, shash::Md5(shash::AsciiPtr(path)));
  }

  inline void AddEntry(
    const DirectoryEntry &entry,
    const XattrList &xattrs,
    const std::string &path)
  {
    AddEntry(entry, xattrs, path, GetParentPath(path));
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
  SqlChunkInsert      *sql_chunk_insert_;
  SqlChunksRemove     *sql_chunks_remove_;
  SqlChunksCount      *sql_chunks_count_;
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
  inline void MoveToNested(
    const std::string dir_structure_root,
    WritableCatalog *new_nested_catalog,
    std::vector<std::string> *grand_child_mountpoints)
  {
    MoveToNestedRecursively(dir_structure_root,
                            new_nested_catalog,
                            grand_child_mountpoints);
  }
  void MoveToNestedRecursively(
    const std::string dir_structure_root,
    WritableCatalog *new_nested_catalog,
    std::vector<std::string> *grand_child_mountpoints);
  void MoveCatalogsToNested(const std::vector<std::string> &nested_catalogs,
                            WritableCatalog *new_nested_catalog);
  void MoveFileChunksToNested(const std::string       &full_path,
                              const shash::Algorithms  algorithm,
                              WritableCatalog         *new_nested_catalog);

  void CopyToParent();
  void CopyCatalogsToParent();

  void UpdateCounters();
  void VacuumDatabaseIfNecessary();
};  // class WritableCatalog

typedef std::vector<WritableCatalog *> WritableCatalogList;

}  // namespace catalog

#endif  // CVMFS_CATALOG_RW_H_
