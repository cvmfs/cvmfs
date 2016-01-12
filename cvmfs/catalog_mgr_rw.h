/**
 * This file is part of the CernVM File System.
 *
 * A WritableCatalogManager adds write functionality to the catalog
 * manager.  It is used for the server side of CVMFS
 * All nested catalog loading functionality is inherited from
 * AbstractCatalogManager.
 *
 * The WritableCatalogManager is provided with DirectoryEntryBase objects from
 * the underlying sync infrastructure (namely SyncMediator) on the server side
 * of CVMFS. In contrast to a full DirectoryEntry object DirectoryEntryBase con-
 * tains only pure file system specific meta data (i.e. mtime, mode, filename).
 * WritableCatalogManager is responsible for the addition and management of all
 * CVMFS-specific meta data in a full DirectoryEntry, which is then saved into
 * the actual Catalog databases.
 *
 * The inode assignment is based on the fact that the number of entries in a
 * catalog do not change (expect on reload). As we do exactly that with the
 * WritableCatalogManager here, inode numbers derived from WritableCatalogs
 * and the WritableCatalogManager may (and will) be screwed.  This is not an
 * issue in the current implementation, as they are not used in the synching
 * process.  Just keep in mind.
 *
 * The WritableCatalogManager starts with a base repository (given by the
 * root hash), and downloads and uncompresses all required catalogs into
 * temporary storage.
 */

#ifndef CVMFS_CATALOG_MGR_RW_H_
#define CVMFS_CATALOG_MGR_RW_H_

#include <pthread.h>
#include <stdint.h>

#include <set>
#include <string>

#include "catalog_mgr_ro.h"
#include "catalog_rw.h"
#include "upload_spooler_result.h"
#include "xattr.h"

class XattrList;
namespace upload {
class Spooler;
}

namespace download {
class DownloadManager;
}

namespace manifest {
class Manifest;
}

namespace perf {
class Statistics;
}

namespace catalog {
template <class CatalogMgrT>
class CatalogBalancer;
}

namespace catalog {

class WritableCatalogManager : public SimpleCatalogManager {
 public:
  friend class CatalogBalancer<WritableCatalogManager>;
  WritableCatalogManager(const shash::Any  &base_hash,
                         const std::string &stratum0,
                         const std::string &dir_temp,
                         upload::Spooler   *spooler,
                         download::DownloadManager *download_manager,
                         uint64_t catalog_entry_warn_threshold,
                         perf::Statistics *statistics,
                         bool is_balanceable,
                         unsigned max_weight,
                         unsigned min_weight);
  ~WritableCatalogManager();
  static manifest::Manifest *CreateRepository(const std::string &dir_temp,
                                              const bool volatile_content,
                                              const std::string &voms_authz,
                                              upload::Spooler   *spooler);

  // DirectoryEntry handling
  void AddFile(const DirectoryEntryBase &entry,
               const XattrList &xattrs,
               const std::string &parent_directory)
  {
    AddFile(DirectoryEntry(entry), xattrs, parent_directory);
  }
  void AddChunkedFile(const DirectoryEntryBase &entry,
                      const XattrList &xattrs,
                      const std::string &parent_directory,
                      const FileChunkList &file_chunks);
  void RemoveFile(const std::string &file_path);

  void AddDirectory(const DirectoryEntryBase &entry,
                    const std::string &parent_directory);
  void TouchDirectory(const DirectoryEntryBase &entry,
                      const std::string &directory_path);
  void RemoveDirectory(const std::string &directory_path);

  // Hardlink group handling
  void AddHardlinkGroup(const DirectoryEntryBaseList &entries,
                        const XattrList &xattrs,
                        const std::string &parent_directory);
  void ShrinkHardlinkGroup(const std::string &remove_path);

  // Nested catalog handling
  void CreateNestedCatalog(const std::string &mountpoint);
  void RemoveNestedCatalog(const std::string &mountpoint);
  bool IsTransitionPoint(const std::string &path);

  inline bool IsBalanceable() const { return is_balanceable_; }
  /**
   * TODO
   */
  void PrecalculateListings();

  void SetTTL(const uint64_t new_ttl);
  bool SetVOMSAuthz(const std::string &voms_authz);
  bool Commit(const bool           stop_for_tweaks,
              const uint64_t       manual_revision,
              manifest::Manifest  *manifest);
  void Balance() {
      if (IsBalanceable()) {
          DoBalance();
      } else {
          LogCvmfs(kLogCatalog, kLogVerboseMsg, "Not balancing the catalog "
                  "manager because it is not balanceable");
      }
  }

 protected:
  void EnforceSqliteMemLimit() { }

  Catalog *CreateCatalog(const PathString &mountpoint,
                         const shash::Any &catalog_hash,
                         Catalog *parent_catalog);
  void ActivateCatalog(Catalog *catalog);

  void AddFile(const DirectoryEntry  &entry,
               const XattrList       &xattrs,
               const std::string     &parent_directory);

 private:
  bool FindCatalog(const std::string &path, WritableCatalog **result);
  void DoBalance();
  void FixWeight(WritableCatalog *catalog);

  /**
   * Traverses all open catalogs and determines which catalogs need updated
   * snapshots.
   * @param[out] result the list of catalogs to snapshot
   */
  void GetModifiedCatalogs(WritableCatalogList *result) const {
    const unsigned int number_of_dirty_catalogs =
      GetModifiedCatalogsRecursively(GetRootCatalog(), result);
    assert(number_of_dirty_catalogs <= result->size());
  }
  int GetModifiedCatalogsRecursively(const Catalog *catalog,
                                     WritableCatalogList *result) const;

  shash::Any SnapshotCatalog(WritableCatalog *catalog) const;
  void CatalogUploadCallback(const upload::SpoolerResult &result);

 private:
  inline void SyncLock() { pthread_mutex_lock(sync_lock_); }
  inline void SyncUnlock() { pthread_mutex_unlock(sync_lock_); }

  // defined in catalog_mgr_rw.cc
  static const std::string kCatalogFilename;

  // private lock of WritableCatalogManager
  pthread_mutex_t *sync_lock_;
  upload::Spooler *spooler_;

  uint64_t catalog_entry_warn_threshold_;

  /**
   * Directories don't have extended attributes at this point.
   */
  XattrList empty_xattrs;

  /**
   * It indicates whether this catalog manager supports balancing operations
   */
  const bool is_balanceable_;

  /**
   * Defines the maximum weight an autogenerated catalog can have. If after a
   * publishing operation the catalog's weight is greater than this threshold it
   * will be considered overflowed and will automatically be split in different
   * sub-catalogs.
   */
  const unsigned max_weight_;

  /**
   * Defines the minimum weight an autogenerated catalog can have. If after a
   * publishing operation the catalog's weight is lesser than this threshold it
   * will be considered underflowed and will automatically be merged with its
   * parent.
   * This last operation can provoke an overflow in the parent, though.
   */
  const unsigned min_weight_;

  /**
   * Defines the threshold that will be used to balance a catalog that has been
   * overflowed. Its value should be lesser than max_weight_ and greater than
   * min_weight. By default it is set to max_weight / 2.
   */
  const unsigned balance_weight_;
};  // class WritableCatalogManager

}  // namespace catalog

#endif  // CVMFS_CATALOG_MGR_RW_H_
