/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_CATALOG_VIRTUAL_H_
#define CVMFS_CATALOG_VIRTUAL_H_

#include "swissknife_assistant.h"

#include <string.h>

namespace catalog {
class WritableCatalog;
class WritableCatalogManager;
}
namespace download {
class DownloadManager;
}
namespace manifest {
class Manifest;
}
struct SyncParameters;


namespace catalog {

class VirtualCatalog {
 public:
  VirtualCatalog(manifest::Manifest *m,
                 download::DownloadManager *d,
                 catalog::WritableCatalogManager *c,
                 SyncParameters *p);
  void GenerateSnapshots();

 private:
  static const std::string kVirtualPath;

  WritableCatalog *GetCatalog();
  void CreateCatalog();

  catalog::WritableCatalogManager *catalog_mgr_;
  swissknife::Assistant assistant_;
};  // class VirtualCatalog

}  // namespace catalog

#endif  // CVMFS_CATALOG_VIRTUAL_H_
