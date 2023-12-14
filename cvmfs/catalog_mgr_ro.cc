/**
 * This file is part of the CernVM file system.
 */

#include "cvmfs_config.h"
#include "catalog_mgr_ro.h"

#include "compression.h"
#include "network/download.h"
#include "util/exception.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace catalog {

// TODO(herethebedragons) correct return value and root_ctlg_location?
LoadReturn SimpleCatalogManager::GetNewRootCatalogContext(
                                                       CatalogContext *result) {
  if (result->hash().IsNull()) {
    result->SetHash(base_hash_);
  }
  result->SetRootCtlgLocation(kCtlgLocationServer);
  result->SetMountpoint(PathString("", 0));

  return kLoadNew;
}


/**
 * Loads a catalog via HTTP from Statum 0 into a temporary file.
 * See CatalogContext class description for correct usage
 * 
 * @return kLoadNew on success
 */
LoadReturn SimpleCatalogManager::LoadCatalogByHash(
                                                 CatalogContext *ctlg_context) {
  const shash::Any effective_hash = ctlg_context->hash();
  assert(shash::kSuffixCatalog == effective_hash.suffix);
  const string url = stratum0_ + "/data/" + effective_hash.MakePath();

  std::string tmp;

  FILE *fcatalog = CreateTempFile(dir_temp_ + "/catalog", 0666, "w", &tmp);
  ctlg_context->SetSqlitePath(tmp);
  if (!fcatalog) {
    PANIC(kLogStderr, "failed to create temp file when loading %s",
          url.c_str());
  }

  cvmfs::FileSink filesink(fcatalog);
  download::JobInfo download_catalog(&url, true, false,
                                    &effective_hash, &filesink);
  const download::Failures retval = download_manager_->Fetch(&download_catalog);
  fclose(fcatalog);

  if (retval != download::kFailOk) {
    unlink(ctlg_context->GetSqlitePathPtr()->c_str());
    PANIC(kLogStderr, "failed to load %s from Stratum 0 (%d - %s)",
                      url.c_str(), retval, download::Code2Ascii(retval));
  }

  return kLoadNew;
}


Catalog* SimpleCatalogManager::CreateCatalog(const PathString  &mountpoint,
                                             const shash::Any  &catalog_hash,
                                             Catalog           *parent_catalog)
{
  Catalog *new_catalog = new Catalog(mountpoint, catalog_hash, parent_catalog);
  if (manage_catalog_files_) {
    new_catalog->TakeDatabaseFileOwnership();
  }

  return new_catalog;
}

}  // namespace catalog
