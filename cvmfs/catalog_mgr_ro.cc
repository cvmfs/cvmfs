/**
 * This file is part of the CernVM file system.
 */

#include "cvmfs_config.h"
#include "catalog_mgr_ro.h"

#include "compression/compression.h"
#include "network/download.h"
#include "util/exception.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace catalog {

SimpleCatalogManager::SimpleCatalogManager(
                       const shash::Any           &base_hash,
                       const std::string          &stratum0,
                       const std::string          &dir_temp,
                       download::DownloadManager  *download_manager,
                       perf::Statistics           *statistics,
                       const bool                  manage_catalog_files,
                       const std::string           &dir_cache,
                       const bool                  copy_to_tmp_dir)
                     : AbstractCatalogManager<Catalog>(statistics)
                     , local_cache_dir_(dir_cache)
                     , copy_to_tmp_dir_(copy_to_tmp_dir)
                     , base_hash_(base_hash)
                     , stratum0_(stratum0)
                     , dir_temp_(dir_temp)
                     , download_manager_(download_manager)
                     , manage_catalog_files_(manage_catalog_files) {
  if (!dir_cache.empty()) {
    const bool success = MakeCacheDirectories(local_cache_dir_, 0755);

    if (!success) {
      LogCvmfs(kLogCatalog, kLogStdout | kLogSyslog,
              "Failure during creation of local cache directory for server."
              "Continue, but no local cache will be used.");
      local_cache_dir_ = "";
      copy_to_tmp_dir_ = false;
    }
  } else {
    copy_to_tmp_dir_ = false;
  }
}

LoadReturn SimpleCatalogManager::GetNewRootCatalogContext(
                                                       CatalogContext *result) {
  if (result->hash().IsNull()) {
    result->SetHash(base_hash_);
  }
  result->SetRootCtlgLocation(kCtlgLocationServer);
  result->SetMountpoint(PathString("", 0));

  return kLoadNew;
}

std::string SimpleCatalogManager::CopyCatalogToTempFile(
                                                const std::string &cache_path) {
  std::string tmp_path;
  FILE *fcatalog = CreateTempFile(dir_temp_ + "/catalog", 0666, "w", &tmp_path);
  if (!fcatalog) {
    PANIC(kLogStderr, "failed to create temp file when loading %s",
                      cache_path.c_str());
  }

  const bool retval = CopyPath2File(cache_path, fcatalog);
  if (!retval) {
    PANIC(kLogStderr, "failed to read %s", cache_path.c_str());
  }
  fclose(fcatalog);

  return tmp_path;
}

/**
 * Loads a catalog via HTTP from Statum 0 into a temporary file.
 * See CatalogContext class description for correct usage
 *
 * Depending on the initialization of SimpleCatalogManager, it can locally
 * cache catalogs.
 *
 * Independent of the catalog being downloaded or being already locally cached,
 * for WriteableCatalog it creates a new copy of the catalog in a tmp dir.
 * This is due to write actions having to be transaction-based and therefore
 * cannot work on standard file locations for cvmfs - someone else could try to
 * access them in a non-clean state.
 *
 * @return kLoadNew on success
 */
LoadReturn SimpleCatalogManager::LoadCatalogByHash(
                                                 CatalogContext *ctlg_context) {
  const shash::Any effective_hash = ctlg_context->hash();
  assert(shash::kSuffixCatalog == effective_hash.suffix);
  const string url = stratum0_ + "/data/" + effective_hash.MakePath();

  FILE *fcatalog;

  if (useLocalCache()) {
    std::string tmp_path = local_cache_dir_ + "/"
                           + effective_hash.MakePathWithoutSuffix();

    ctlg_context->SetSqlitePath(tmp_path);

    // catalog is cached in "cache_dir/" + standard cvmfs file hierarchy
    if (FileExists(tmp_path.c_str())) {
      if (!copy_to_tmp_dir_) {
        return kLoadNew;
      } else {  // for writable catalog create copy in dir_temp_
        const std::string cache_path = tmp_path;

        tmp_path = CopyCatalogToTempFile(cache_path);
        ctlg_context->SetSqlitePath(tmp_path);

        return kLoadNew;
      }
    }

    // file not cached yet
    // open file to download into "cache_dir/" + standard cvmfs file hierarchy
    fcatalog = fopen(ctlg_context->sqlite_path().c_str(), "w");
    if (!fcatalog) {
      PANIC(kLogStderr, "failed to create file in cache.server when loading %s",
                        url.c_str());
    }
  } else {  // no local cache; just create a random tmp file for download
    std::string tmp_path;
    fcatalog = CreateTempFile(dir_temp_ + "/catalog", 0666, "w", &tmp_path);
    if (!fcatalog) {
      PANIC(kLogStderr, "failed to create temp file when loading %s",
                        url.c_str());
    }
    ctlg_context->SetSqlitePath(tmp_path);
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

  // for writable catalog make copy in dir_temp_ that can be modified
  if (useLocalCache() && copy_to_tmp_dir_) {
    const std::string cache_path = ctlg_context->sqlite_path();
    const std::string tmp_path = CopyCatalogToTempFile(cache_path);
    ctlg_context->SetSqlitePath(tmp_path);
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
