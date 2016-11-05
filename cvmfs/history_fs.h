/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_HISTORY_FS_H_
#define CVMFS_HISTORY_FS_H_

#include "swissknife_assistant.h"

namespace catalog {
class WritableCatalogManager;
}
namespace download {
class DownloadManager;
}
namespace manifest {
class Manifest;
}
struct SyncParameters;


class TagFolderGenerator {
 public:
  TagFolderGenerator(manifest::Manifest *m,
                     download::DownloadManager *d,
                     catalog::WritableCatalogManager *c,
                     SyncParameters *p);
  void Generate();

 private:
  catalog::WritableCatalogManager *catalog_mgr_;
  swissknife::Assistant assistant_;
};  // class TagFolderGenerator

#endif  // CVMFS_HISTORY_FS_H_
