/**
 * This file is part of the CernVM file system.
 */

#include "cvmfs_config.h"
#include "catalog_mgr_client.h"

#include "cache.h"
#include "download.h"
#include "fetch.h"
#include "manifest.h"
#include "quota.h"
#include "signature.h"
#include "statistics.h"

using namespace std;  // NOLINT

namespace catalog {

/**
 * Triggered when the catalog is attached (db file opened)
 */
void ClientCatalogManager::ActivateCatalog(Catalog *catalog) {
  const Counters &counters = const_cast<const Catalog*>(catalog)->GetCounters();
  if (catalog->IsRoot()) {
    all_inodes_ = counters.GetAllEntries();
  }
  loaded_inodes_ += counters.GetSelfEntries();
}


ClientCatalogManager::ClientCatalogManager(
  const string &repo_name,
  cvmfs::Fetcher *fetcher,
  signature::SignatureManager *signature_mgr,
  perf::Statistics *statistics)
  : AbstractCatalogManager<Catalog>(statistics)
  , repo_name_(repo_name)
  , fetcher_(fetcher)
  , signature_mgr_(signature_mgr)
  , offline_mode_(false)
  , all_inodes_(0)
  , loaded_inodes_(0)
  , fixed_alt_root_catalog_(false)
{
  LogCvmfs(kLogCatalog, kLogDebug, "constructing client catalog manager");
  n_certificate_hits_ = statistics->Register("cache.n_certificate_hits",
    "Number of certificate hits");
  n_certificate_misses_ = statistics->Register("cache.n_certificate_misses",
    "Number of certificate misses");
}


ClientCatalogManager::~ClientCatalogManager() {
  LogCvmfs(kLogCache, kLogDebug, "unpinning / unloading all catalogs");

  for (map<PathString, shash::Any>::iterator i = mounted_catalogs_.begin(),
       iend = mounted_catalogs_.end(); i != iend; ++i)
  {
    fetcher_->cache_mgr()->quota_mgr()->Unpin(i->second);
  }
}


Catalog *ClientCatalogManager::CreateCatalog(
  const PathString  &mountpoint,
  const shash::Any  &catalog_hash,
  catalog::Catalog  *parent_catalog
) {
  mounted_catalogs_[mountpoint] = loaded_catalogs_[mountpoint];
  loaded_catalogs_.erase(mountpoint);
  return new Catalog(mountpoint, catalog_hash, parent_catalog);
}


shash::Any ClientCatalogManager::GetRootHash() {
  ReadLock();
  shash::Any result = mounted_catalogs_[PathString("", 0)];
  Unlock();
  return result;
}


/**
 * Specialized initialization that uses a fixed root hash.
 */
bool ClientCatalogManager::InitFixed(
  const shash::Any &root_hash,
  bool alternative_path)
{
  LogCvmfs(kLogCatalog, kLogDebug, "Initialize catalog with root hash %s",
           root_hash.ToString().c_str());
  WriteLock();
  fixed_alt_root_catalog_ = alternative_path;
  bool attached = MountCatalog(PathString("", 0), root_hash, NULL);
  Unlock();

  if (!attached) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to initialize root catalog");
  }

  return attached;
}


LoadError ClientCatalogManager::LoadCatalog(
  const PathString  &mountpoint,
  const shash::Any  &hash,
  std::string *catalog_path,
  shash::Any *catalog_hash)
{
  string cvmfs_path = "file catalog at " + repo_name_ + ":" +
    (mountpoint.IsEmpty() ?
      "/" : string(mountpoint.GetChars(), mountpoint.GetLength()));
  bool retval;

  // send the catalog hash to a blind memory position if it zero (save some ifs)
  shash::Any blind_hash;
  if (catalog_hash == NULL) {
    catalog_hash = &blind_hash;
  }

  // Load a particular catalog
  if (!hash.IsNull()) {
    cvmfs_path += " (" + hash.ToString() + ")";
    string alt_catalog_path = "";
    if (mountpoint.IsEmpty() && fixed_alt_root_catalog_)
      alt_catalog_path = hash.MakeAlternativePath();
    LoadError load_error =
      LoadCatalogCas(hash, cvmfs_path, alt_catalog_path, catalog_path);
    if (load_error == catalog::kLoadNew)
      loaded_catalogs_[mountpoint] = hash;
    *catalog_hash = hash;
    return load_error;
  }

  // Happens only on init/remount, i.e. quota won't delete a cached catalog
  string checksum_dir = ".";
  // TODO(jblomer): find a way to remove this hack
  if (!FileExists("cvmfschecksum." + repo_name_)) {
    if (fetcher_->cache_mgr()->id() == cache::kPosixCacheManager) {
      cache::PosixCacheManager *cache_mgr =
        reinterpret_cast<cache::PosixCacheManager *>(fetcher_->cache_mgr());
      if (cache_mgr->alien_cache())
        checksum_dir = cache_mgr->cache_path();
    }
  }
  shash::Any cache_hash(shash::kSha1, shash::kSuffixCatalog);
  uint64_t cache_last_modified = 0;

  retval = manifest::Manifest::ReadChecksum(
    repo_name_, checksum_dir, &cache_hash, &cache_last_modified);
  if (retval) {
    LogCvmfs(kLogCache, kLogDebug, "cached copy publish date %s",
             StringifyTime(cache_last_modified, true).c_str());
  } else {
    LogCvmfs(kLogCache, kLogDebug, "unable to read local checksum");
  }

  // Load and verify remote checksum
  manifest::Failures manifest_failure;
  CachedManifestEnsemble ensemble(fetcher_->cache_mgr(), this);
  manifest_failure = manifest::Fetch("", repo_name_, cache_last_modified,
                                     &cache_hash, signature_mgr_,
                                     fetcher_->download_mgr(),
                                     &ensemble);
  if (manifest_failure != manifest::kFailOk) {
    LogCvmfs(kLogCache, kLogDebug, "failed to fetch manifest (%d - %s)",
             manifest_failure, manifest::Code2Ascii(manifest_failure));

    if (catalog_path) {
      LoadError error =
        LoadCatalogCas(cache_hash, cvmfs_path, "", catalog_path);
      if (error != catalog::kLoadNew)
        return error;
    }
    loaded_catalogs_[mountpoint] = cache_hash;
    *catalog_hash = cache_hash;
    offline_mode_ = true;
    return catalog::kLoadUp2Date;
  }

  offline_mode_ = false;
  cvmfs_path += " (" + ensemble.manifest->catalog_hash().ToString() + ")";
  LogCvmfs(kLogCache, kLogDebug, "remote checksum is %s",
           ensemble.manifest->catalog_hash().ToString().c_str());

  // Short way out, use cached copy
  if (ensemble.manifest->catalog_hash() == cache_hash) {
    if (catalog_path) {
      LoadError error =
        LoadCatalogCas(cache_hash, cvmfs_path, "", catalog_path);
      if (error == catalog::kLoadNew) {
        loaded_catalogs_[mountpoint] = cache_hash;
        *catalog_hash = cache_hash;
        return catalog::kLoadUp2Date;
      }
      LogCvmfs(kLogCache, kLogDebug,
               "unable to open catalog from local checksum, downloading");
    } else {
      loaded_catalogs_[mountpoint] = cache_hash;
      *catalog_hash = cache_hash;
      return catalog::kLoadUp2Date;
    }
  }
  if (!catalog_path)
    return catalog::kLoadNew;

  // Load new catalog
  catalog::LoadError load_retval =
    LoadCatalogCas(ensemble.manifest->catalog_hash(),
                   cvmfs_path,
                   ensemble.manifest->has_alt_catalog_path() ?
                     ensemble.manifest->MakeCatalogPath() : "",
                   catalog_path);
  if (load_retval != catalog::kLoadNew)
    return load_retval;
  loaded_catalogs_[mountpoint] = ensemble.manifest->catalog_hash();
  *catalog_hash = ensemble.manifest->catalog_hash();

  // Store new manifest and certificate
  fetcher_->cache_mgr()->CommitFromMem(ensemble.manifest->certificate(),
                                       ensemble.cert_buf, ensemble.cert_size,
                                       "certificate for " + repo_name_);
  ensemble.manifest->ExportChecksum(".", 0600);
  return catalog::kLoadNew;
}


LoadError ClientCatalogManager::LoadCatalogCas(
  const shash::Any &hash,
  const string &name,
  const std::string &alt_catalog_path,
  string *catalog_path)
{
  assert(hash.suffix == shash::kSuffixCatalog);
  int fd = fetcher_->Fetch(hash, cache::CacheManager::kSizeUnknown, name,
    zlib::kZlibDefault, cache::CacheManager::kTypeCatalog, alt_catalog_path);
  if (fd >= 0) {
    *catalog_path = "@" + StringifyInt(fd);
    return kLoadNew;
  }

  if (fd == -ENOSPC)
    return kLoadNoSpace;

  return kLoadFail;
}


void ClientCatalogManager::UnloadCatalog(const Catalog *catalog) {
  LogCvmfs(kLogCache, kLogDebug, "unloading catalog %s",
           catalog->path().c_str());

  map<PathString, shash::Any>::iterator iter =
    mounted_catalogs_.find(catalog->path());
  assert(iter != mounted_catalogs_.end());
  fetcher_->cache_mgr()->quota_mgr()->Unpin(iter->second);
  mounted_catalogs_.erase(iter);
  const catalog::Counters &counters = catalog->GetCounters();
  loaded_inodes_ -= counters.GetSelfEntries();
}


//------------------------------------------------------------------------------


void CachedManifestEnsemble::FetchCertificate(const shash::Any &hash) {
  uint64_t size;
  bool retval = cache_mgr_->Open2Mem(hash, &cert_buf, &size);
  cert_size = size;
  if (retval)
    perf::Inc(catalog_mgr_->n_certificate_hits_);
  else
    perf::Inc(catalog_mgr_->n_certificate_misses_);
}

}  // namespace catalog
