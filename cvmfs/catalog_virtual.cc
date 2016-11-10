/**
 * This file is part of the CernVM File System.
 */
#include "cvmfs_config.h"
#include "catalog_virtual.h"

#include <cassert>

#include "catalog_mgr_rw.h"
#include "catalog_rw.h"
#include "logging.h"
#include "history.h"
#include "swissknife_sync.h"

using namespace std;  // NOLINT

namespace catalog {

const string VirtualCatalog::kVirtualPath = "/.cvmfs";


WritableCatalog *VirtualCatalog::GetCatalog() {
  DirectoryEntry entry;
  bool retval = catalog_mgr_->LookupPath(kVirtualPath, kLookupSole, &entry);
  if (!retval)
    CreateCatalog();
  assert(catalog_mgr_->IsTransitionPoint(kVirtualPath));
  return NULL;
}


void VirtualCatalog::CreateCatalog() {
  DirectoryEntryBase entry;
}


void VirtualCatalog::GenerateSnapshots() {
}


VirtualCatalog::VirtualCatalog(
  manifest::Manifest *m,
  download::DownloadManager *d,
  catalog::WritableCatalogManager *c,
  SyncParameters *p)
  : catalog_mgr_(c)
  , assistant_(d, m, p->stratum0, p->dir_temp)
{ }

}  // namespace catalog
