/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_HISTORY_FS_H_
#define CVMFS_HISTORY_FS_H_

namespace catalog {
class WritableCatalogManager;
}
namespace history {
class History;
}
struct SyncParameters;

class TagFolderCreator {
 public:
  TagFolderCreator(
    history::History *h,
    catalog::WritableCatalogManager *c,
    SyncParameters *p)
    : catalog_mgr_(c), history_(h), sync_params_(p) { }

   void Generate();

 private:
  catalog::WritableCatalogManager *catalog_mgr_;
  history::History *history_;
  SyncParameters *sync_params_;
};

#endif  // CVMFS_HISTORY_FS_H_
