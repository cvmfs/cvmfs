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
  LoadReturn SimpleCatalogManager::GetNewRootCatalogInfo(CatalogInfo *result) {
    LogCvmfs(kLogCache, kLogDebug, "SimpleCatalogManager::GetNewRootCatalogInfo %s",
           base_hash_.ToString().c_str());
    result->hash = base_hash_;
    result->root_ctlg_location = kServer;
    result->root_ctlg_timestamp = (uint64_t)-1;

    return kLoadUp2Date;
  }

  LoadReturn SimpleCatalogManager::LoadCatalogByHash(CatalogInfo *ctlg_info) {
    LogCvmfs(kLogCache, kLogDebug, "SimpleCatalogManager::LoadCatalogByHash %s",
           base_hash_.ToString().c_str());
    LogCvmfs(kLogCache, kLogDebug, "SimpleCatalogManager::LoadCatalogByHash %s",
           ctlg_info->sql_catalog_handle.c_str());
    // shash::Any effective_hash = hash_to_load.IsNull() ? rootInfo->hash : hash_to_load;
    shash::Any effective_hash = ctlg_info->hash;
    assert(shash::kSuffixCatalog == effective_hash.suffix);
    const string url = stratum0_ + "/data/" + effective_hash.MakePath();

    std::string tmp;
    
    FILE *fcatalog = CreateTempFile(dir_temp_ + "/catalog", 0666, "w", &tmp);
    LogCvmfs(kLogCache, kLogDebug, "SimpleCatalogManager::LoadCatalogByHash TMP %s", tmp);
    ctlg_info->sql_catalog_handle.assign(tmp);
    if (!fcatalog) {
      PANIC(kLogStderr, "failed to create temp file when loading %s",
            url.c_str());
    }

  cvmfs::FileSink filesink(fcatalog);
  download::JobInfo download_catalog(&url, true, false,
                                     &effective_hash, &filesink);
  download::Failures retval = download_manager_->Fetch(&download_catalog);
  fclose(fcatalog);

    if (retval != download::kFailOk) {
      unlink(ctlg_info->sql_catalog_handle.c_str());
      PANIC(kLogStderr, "failed to load %s from Stratum 0 (%d - %s)", url.c_str(),
            retval, download::Code2Ascii(retval));
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
