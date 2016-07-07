/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_BALANCER_H_
#define CVMFS_CATALOG_BALANCER_H_

#include <string>
#include <vector>

#include "catalog_mgr.h"
#include "directory_entry.h"


namespace catalog {

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
    std::vector<VirtualNode> children;
    unsigned weight;
    DirectoryEntry dirent;
    std::string path;
    bool is_new_nested_catalog;

   /**
    * Extracts not only the direct children of this VirtualNode, but
    * recursively all the VirtualNodes of this catalog. When a VirtualNode that
    * is the root of a nested catalog is created, it won't be expanded. In order
    * to actually expand that node it will be necessary to manually call this
    * method on it.
    *
    * @param catalog_mgr catalog manager that contains the file system tree
    */
    void ExtractChildren(CatalogMgrT *catalog_mgr);
    void FixWeight();
    VirtualNode(const std::string &path, CatalogMgrT *catalog_mgr)
      : children(), weight(1), dirent(), path(path),
        is_new_nested_catalog(false) {
      catalog_mgr->LookupPath(path, kLookupSole, &dirent);
    }
    VirtualNode(const std::string &path, const DirectoryEntry &dirent,
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

  void PartitionOptimally(VirtualNode *virtual_node);
  void AddCatalogMarker(std::string path);
  DirectoryEntryBase MakeEmptyDirectoryEntryBase(std::string name,
                                                 uid_t uid,
                                                 gid_t gid);
  static VirtualNode *MaxChild(VirtualNode *virtual_node);
  void AddCatalog(VirtualNode *child_node);

  CatalogMgrT *catalog_mgr_;
};

}  // namespace catalog

#include "catalog_balancer_impl.h"

#endif  // CVMFS_CATALOG_BALANCER_H_

