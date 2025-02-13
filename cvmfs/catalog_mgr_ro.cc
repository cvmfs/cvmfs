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
                                            shash::Any        *catalog_hash, uint64_t *manifest_age)
{
  shash::Any effective_hash = hash.IsNull() ? base_hash_ : hash;
  assert(shash::kSuffixCatalog == effective_hash.suffix);
  const string url = stratum0_ + "/data/" + effective_hash.MakePath();

  std::string tmp_path = "";

  FILE *fcatalog=NULL;

  if (useLocalCache()) {
    tmp_path = local_cache_dir_ + "/"
                           + effective_hash.MakePathWithoutSuffix();
    *catalog_path = tmp_path;
    *catalog_hash = hash;
    // catalog is cached in "cache_dir/" + standard cvmfs file hierarchy
    if (FileExists(tmp_path.c_str())) {
#ifndef BUILD_INGESTSQL
      LogCvmfs(kLogCvmfs, kLogSyslog, "LoadCatalog: serving catalog %s from cache", effective_hash.ToString().c_str() );
#endif
        const std::string cache_path = tmp_path;
        const std::string tmp_path = CopyCatalogToTempFile(cache_path);
        *catalog_path=tmp_path;
        return kLoadNew;
    }
    // file not cached yet
    // open file to download into "cache_dir/" + standard cvmfs file hierarchy
    // open temporary file to write it to, then atomically rename to destination
    fcatalog = CreateTempFile(dir_temp_ + "/catalog", 0666, "w", &tmp_path);
    if (!fcatalog) {
      PANIC(kLogStderr, "failed to create file in cache.server when loading %s",
                        url.c_str());
    }
  } else {  // no local cache; just create a random tmp file for download
    fcatalog = CreateTempFile(dir_temp_ + "/catalog", 0666, "w", &tmp_path);
    if (!fcatalog) {
      PANIC(kLogStderr, "failed to create temp file when loading %s",
                        url.c_str());
    }
    *catalog_path=tmp_path;
  }

  time_t t1=tick();
  cvmfs::FileSink filesink(fcatalog);
  download::JobInfo download_catalog(&url, true, false,
                                     &effective_hash, &filesink);

  if(getenv("_CVMFS_DEVEL_IGNORE_SIGNATURE_FAILURES") ) {
    LogCvmfs(kLogCatalog, kLogSyslog | kLogDebug, "Ignoring signature for catalog %s", effective_hash.ToString().c_str() );
    download_catalog.SetExpectedHash(NULL);
  }

  download::Failures retval = download_manager_->Fetch(&download_catalog);
  fclose(fcatalog);


  if (retval != download::kFailOk) {
    unlink(catalog_path->c_str());
    PANIC(kLogStderr, "failed to load %s from Stratum 0 (%d - %s)", url.c_str(),
          retval, download::Code2Ascii(retval));
  }

  if(useLocalCache()) {
    assert(tmp_path!="");
    int ret = rename( tmp_path.c_str(), catalog_path->c_str() );
    if (ret!=0) {
      PANIC(kLogStderr, "failed to rename %s to %s: errno= %d", tmp_path.c_str(), catalog_path->c_str(), errno );
    }
  }

  tock(t1, ("Wait on download of " + effective_hash.ToString() ).c_str());

  // for writable catalog make copy in dir_temp_ that can be modified
  if (useLocalCache()) {
    const std::string cache_path = *catalog_path;
    const std::string tmp_path = CopyCatalogToTempFile(cache_path);
    *catalog_path=tmp_path;
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
  (void) fclose(fcatalog);

  return tmp_path;
}
}  // namespace catalog
