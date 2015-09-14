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
                         bool is_balanced,
                         unsigned max_weight,
                         unsigned min_weight);
  ~WritableCatalogManager();
  static manifest::Manifest *CreateRepository(const std::string &dir_temp,
                                              const bool volatile_content,
                                              const bool garbage_collectable,
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

  inline bool IsBalanced() const { return is_balanced_; }
  /**
   * TODO
   */
  void PrecalculateListings();

  manifest::Manifest *Commit(const bool     stop_for_tweaks,
                             const uint64_t manual_revision);
  void Balance() {
      if (IsBalanced()) {
          DoBalance();
      } else {
          LogCvmfs(kLogCatalog, kLogVerboseMsg, "Not balancing the catalog "
                  "manager because it is not balanced");
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
   * It indicates whether this catalog supports balancing operations
   */
  const bool is_balanced_;
  
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



/**
 * This class is in charge of "balancing" a catalog embedded in a
 * WritableCatalogManager. The process of balancing consists of keeping the
 * number of entries in each catalog between a maximum and minimum threshold so
 * that all of them can be easily manipulable. This way there won't be catalogs
 * that take a lot of time being downloaded, or their SQL operations will take
 * a reasonable amount of time.
 * 
 * The CatalogBalancer uses the following WritableCatalogManager attributes:
 * - max_weight_: maximum number of entries in a catalog
 * - min_weight_: minimum number of entries in a catalog
 * - balance_weight_: maximum number of entries during the balancing process
 * 
 * The balancing process is done traversing the entire catalog tree in
 * postorder, so that the deepest catalogs are analyzed before their parents.
 * Each catalog is then analyzed individually, existing three different
 * scenarios:
 * 
 * a) The number of entries of the catalog is between max_weight_ and 
 * min_weight_: nothing happens, the catalog remains untouched.
 * 
 * b) The number of entries of the catalog is greater than max_weight_: the
 * catalog gets split in smaller catalogs. One of the new generated catalogs
 * will be mounted in the original mount point.
 * 
 * c) The number of entries of the catalog is lesser than min_weight_: the
 * catalog gets merged with its father (except the root catalog, obviously).
 */
template <class CatalogMgrT>
class CatalogBalancer {
 public:
  typedef typename CatalogMgrT::catalog_t catalog_t;
  explicit CatalogBalancer(CatalogMgrT *catalog_mgr)
    : catalog_mgr_(catalog_mgr) { }

  /**
   * This method balances a catalog. A catalog is considered overflowed if
   * its weight (the number of entries in the catalog database) is greater than
   * the stablish threshold defined by the max_weight_ variable. If a catalog is
   * overflowed it will be split in a number of new catalogs, meeting the
   * following properties:
   * - There will be a catalog in the same place where the overflowed catalog
   * was at the beginning of the process.
   * - New generated catalogs will have a maximum weight defined by the
   * attribute balance_weight_, which will always be lesser than max_weight_.
   * - The number of new generated catalogs is unknown.
   * 
   * A catalog is considered underflowed if its weight is lesser than the
   * minimum threshold, defined by the min_weight_ variable. If a catalog is
   * underflowed it will be merged with the father catalog. However, it is
   * remarkable that such an operation can provoke an overflow in the father,
   * which will be threaten separately.
   * 
   * If a catalog is not overflowed or underflowed no changes will be applied
   * to it.
   * 
   * For a WritableCatalogManager to be balanced it requires the is_balanced_
   * flag to be set to true.
   * 
   * @param catalog the catalog to be balanced, or NULL to balance all catalogs
   * in the WritableCatalogManager
   */
  void Balance(catalog_t *catalog);

 private:
     /**
      * A VirtualNode is the abstract representation of an entry in a catalog.
      * It is used by the CatalogBalancer to "virtually" represent the
      * file-system tree of a concrete catalog and spot the nodes where
      * a new catalog should be created.
      * 
      * One of its main functions is to keep track of the current weight of a
      * file or directory, i.e., the number of entries it contains. Concretely:
      * - Normal files and symlinks: it is always one.
      * - Normal directories: one plus the weight of each node it contains.
      * - Directories which are catalog mount points: it is always one
      */
  struct VirtualNode {
    vector<VirtualNode> children;
    unsigned weight;
    DirectoryEntry dirent;
    string path;
    bool is_new_nested_catalog;

    void ExtractChildren(CatalogMgrT *catalog_mgr);
    void FixWeight();
    VirtualNode(const string &path, CatalogMgrT *catalog_mgr)
      : children(), weight(1), dirent(), path(path),
        is_new_nested_catalog(false) {
      catalog_mgr->LookupPath(path, kLookupSole, &dirent);
    }
    VirtualNode(const string &path, const DirectoryEntry &dirent,
                CatalogMgrT *catalog_mgr)
      : children(), weight(1), dirent(dirent), path(path),
        is_new_nested_catalog(false) {
      if (!IsCatalog() && IsDirectory())
        ExtractChildren(catalog_mgr);
    }
    bool IsDirectory() { return dirent.IsDirectory(); }
    bool IsCatalog() { return is_new_nested_catalog ||
        dirent.IsNestedCatalogMountpoint(); }
  };
  typedef typename CatalogBalancer<CatalogMgrT>::VirtualNode virtual_node_t;

 private:
  void PartitionOptimally(VirtualNode *virtual_node);
  void AddCatalogMarker(string path);
  DirectoryEntryBase MakeEmptyDirectoryEntryBase(string name,
                                                 uid_t uid,
                                                 gid_t gid);
  static VirtualNode *MaxChild(VirtualNode *virtual_node);
  void AddCatalog(VirtualNode *child_node);

 private:
  CatalogMgrT *catalog_mgr_;
};


}  // namespace catalog


#include "catalog_mgr_rw_impl.h"

#endif  // CVMFS_CATALOG_MGR_RW_H_
