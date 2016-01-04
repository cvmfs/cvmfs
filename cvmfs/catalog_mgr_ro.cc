/**
 * This file is part of the CernVM file system.
 */

#include "cvmfs_config.h"
#include "catalog_mgr_ro.h"

#include "compression.h"
#include "download.h"
#include "util.h"

using namespace std;  // NOLINT

namespace catalog {

/**
 * Loads a catalog via HTTP from Statum 0 into a temporary file.
 * @param url_path the url of the catalog to load
 * @param mount_point the file system path where the catalog should be mounted
 * @param catalog_file a pointer to the string containing the full qualified
 *                     name of the catalog afterwards
 * @return 0 on success, different otherwise
 */
LoadError SimpleCatalogManager::LoadCatalog(const PathString  &mountpoint,
                                            const shash::Any  &hash,
                                            std::string       *catalog_path,
                                            shash::Any        *catalog_hash)
{
  shash::Any effective_hash = hash.IsNull() ? base_hash_ : hash;
  assert(shash::kSuffixCatalog == effective_hash.suffix);
  const string url = stratum0_ + "/data/" + effective_hash.MakePath();
  FILE *fcatalog = CreateTempFile(dir_temp_ + "/catalog", 0666, "w",
                                  catalog_path);
  if (!fcatalog) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "failed to create temp file when loading %s", url.c_str());
    assert(false);
  }

  download::JobInfo download_catalog(&url, true, false, fcatalog,
                                     &effective_hash);
  download::Failures retval = download_manager_->Fetch(&download_catalog);
  fclose(fcatalog);

  if (retval != download::kFailOk) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "failed to load %s from Stratum 0 (%d - %s)", url.c_str(),
             retval, download::Code2Ascii(retval));
    unlink(catalog_path->c_str());
    assert(false);
  }

  *catalog_hash = effective_hash;
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
