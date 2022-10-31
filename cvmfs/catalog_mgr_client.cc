/**
 * This file is part of the CernVM file system.
 */

#include "cvmfs_config.h"
#include "catalog_mgr_client.h"

#include <string>
#include <vector>

#include "cache_posix.h"
#include "crypto/signature.h"
#include "fetch.h"
#include "manifest.h"
#include "mountpoint.h"
#include "network/download.h"
#include "quota.h"
#include "statistics.h"
#include "util/posix.h"
#include "util/string.h"

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


ClientCatalogManager::ClientCatalogManager(MountPoint *mountpoint)
  : AbstractCatalogManager<Catalog>(mountpoint->statistics())
  , last_root_catalog_timestamp_(0)
  , repo_name_(mountpoint->fqrn())
  , fetcher_(mountpoint->fetcher())
  , signature_mgr_(mountpoint->signature_mgr())
  , workspace_(mountpoint->file_system()->workspace())
  , offline_mode_(false)
  , all_inodes_(0)
  , loaded_inodes_(0)
  , fixed_alt_root_catalog_(false)
  , root_fd_(-1)
{
  LogCvmfs(kLogCatalog, kLogDebug, "constructing client catalog manager");
  n_certificate_hits_ = mountpoint->statistics()->Register(
    "cache.n_certificate_hits", "Number of certificate hits");
  n_certificate_misses_ = mountpoint->statistics()->Register(
    "cache.n_certificate_misses", "Number of certificate misses");
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

LoadReturn ClientCatalogManager::GetNewRootCatalogInfo(CatalogInfo *result) {
  // 1) Get local (alien) cache root catalog

  // Happens only on init/remount, i.e. quota won't delete a cached catalog
  shash::Any breadcrumb_hash(shash::kSha1, shash::kSuffixCatalog);
  uint64_t breadcrumb_timestamp = 0;

  manifest::Breadcrumb breadcrumb =
    fetcher_->cache_mgr()->LoadBreadcrumb(repo_name_);
  if (breadcrumb.IsValid()) {
    breadcrumb_hash = breadcrumb.catalog_hash;
    breadcrumb_timestamp = breadcrumb.timestamp;
    LogCvmfs(kLogCache, kLogDebug, "cached copy publish date %s (hash %s)",
             StringifyTime(breadcrumb_timestamp, true).c_str(),
             breadcrumb_hash.ToString().c_str());
  } else {
    LogCvmfs(kLogCache, kLogDebug, "unable to read local checksum");
  }

  // 2) Select local newest catalog
  
  shash::Any local_newest_hash = breadcrumb_hash;
  uint64_t local_newest_timestamp = breadcrumb_timestamp;
  result->root_ctlg_location = RootCatalogLocation::kBreadcrumb;
  LoadReturn success_code = catalog::kLoadNew;

  // We only fetch currently loaded catalog if the timestamp is newer then
  // the breadcrumb timestamp. As such the assumption that the root catalog
  // is already loaded will always be true.
  if (last_root_catalog_timestamp_ != 0 
      && breadcrumb_timestamp <= last_root_catalog_timestamp_) {
    auto curr_hash_itr = mounted_catalogs_.find(PathString("", 0));
    local_newest_hash = curr_hash_itr->second;
    local_newest_timestamp = last_root_catalog_timestamp_;
    result->root_ctlg_location = RootCatalogLocation::kMounted;
    success_code = catalog::kLoadUp2Date;
  } 

  // 3) Get remote root catalog (fails if remote catalog is older)
  manifest::Failures manifest_failure;
  CachedManifestEnsemble ensemble(fetcher_->cache_mgr(), this);
  manifest_failure = manifest::Fetch("", repo_name_, local_newest_timestamp,
                                     &local_newest_hash, signature_mgr_,
                                     fetcher_->download_mgr(),
                                     &ensemble);
  if (manifest_failure == manifest::kFailOk 
      && ensemble.manifest->publish_timestamp() > local_newest_timestamp) {
    result->hash = ensemble.manifest->catalog_hash();
    result->root_ctlg_timestamp = ensemble.manifest->publish_timestamp();
    result->root_ctlg_location = RootCatalogLocation::kServer;
    offline_mode_ = false;

    return catalog::kLoadNew;
  }
  LogCvmfs(kLogCache, kLogDebug, "failed to fetch manifest from server (%d - %s)",
             manifest_failure, manifest::Code2Ascii(manifest_failure));


  offline_mode_ = true;
  result->hash = local_newest_hash;
  result->root_ctlg_timestamp = local_newest_timestamp;

  return success_code;
}

LoadReturn ClientCatalogManager::LoadCatalogByHash(CatalogInfo *ctlg_info) { //output
 LogCvmfs(kLogCache, kLogDebug, "LoadCatalogByHash start");

  string catalog_descr = "file catalog at " + repo_name_ + ":" +
    (ctlg_info->mountpoint.IsEmpty() ?
      "/" : string(ctlg_info->mountpoint.GetChars(),
                   ctlg_info->mountpoint.GetLength()));

  catalog_descr += " (" + ctlg_info->hash.ToString() + ")";
  string alt_root_catalog_path = "";

  // root catalog needs special handling because of alt_root_catalog_path
  CachedManifestEnsemble ensemble(fetcher_->cache_mgr(), this);
  if (ctlg_info->mountpoint.IsEmpty()) {
    LogCvmfs(kLogCache, kLogDebug, "LoadCatalogByHash root mountpoint");
    if ( fixed_alt_root_catalog_) {
      alt_root_catalog_path = ctlg_info->hash.MakeAlternativePath();
    }

    // get manifest from server and double check if we have newest hash
    if (ctlg_info->root_ctlg_location == RootCatalogLocation::kServer) {
      LogCvmfs(kLogCache, kLogDebug, "LoadCatalogByHash root from server");
      manifest::Failures manifest_failure;
      manifest_failure = manifest::Fetch("", repo_name_, 
                                        ctlg_info->root_ctlg_timestamp,
                                        &ctlg_info->hash,
                                        signature_mgr_,
                                        fetcher_->download_mgr(),
                                        &ensemble);
      if (manifest_failure != manifest::kFailOk) {
        LogCvmfs(kLogCache, kLogDebug, "failed to fetch manifest (%d - %s)",
                manifest_failure, manifest::Code2Ascii(manifest_failure));
        return kLoadFail;
      }

      ctlg_info->hash = ensemble.manifest->catalog_hash();
      ctlg_info->root_ctlg_timestamp = ensemble.manifest->publish_timestamp();
    }
  }

  LogCvmfs(kLogCache, kLogDebug, "LoadCatalogByHash Server (%s)",
                ctlg_info->hash.ToString().c_str());
  
  // TODO fetch should return if fetch from cache or from remote 
  // would save us the if in L223
  LoadReturn load_ret = FetchCatalogByHash( ctlg_info->hash, catalog_descr,
                                            alt_root_catalog_path, 
                                            &ctlg_info->sql_catalog_handle);
  // *catalog_hash = ctlg_info->hash;
  if (load_ret == catalog::kLoadNew) {
    loaded_catalogs_[ctlg_info->mountpoint] = ctlg_info->hash;

    if (ctlg_info->mountpoint.IsEmpty()) { // root catalog
    LogCvmfs(kLogCache, kLogDebug, "LoadCatalogByHash set root catalog variables");
      if(ctlg_info->root_ctlg_location == RootCatalogLocation::kMounted) {
        return LoadReturn::kLoadUp2Date;
      }
      // set timestamp
      last_root_catalog_timestamp_ = ctlg_info->root_ctlg_timestamp;

      // if coming from server: update breadcrumb
      if (ctlg_info->root_ctlg_location == RootCatalogLocation::kServer) {
          // Store new manifest and certificate
          LogCvmfs(kLogCache, kLogDebug, "LoadCatalogByHash write manifest");
          CacheManager::Label label;
          label.path = repo_name_;
          label.flags |= CacheManager::kLabelCertificate;
          fetcher_->cache_mgr()->CommitFromMem(
                  CacheManager::LabeledObject(ensemble.manifest->certificate(),
                                              label),
                  ensemble.cert_buf, ensemble.cert_size);
          fetcher_->cache_mgr()->StoreBreadcrumb(*ensemble.manifest);

      }
    }
  }
  
  return load_ret;  
}


LoadError ClientCatalogManager::FetchCatalogByHash(
  const shash::Any &hash,
  const string &name,
  const std::string &alt_root_catalog_path,
  std::string *sql_catalog_handle)
{
  LogCvmfs(kLogCatalog, kLogDebug, "FetchCatalogByHash");
  assert(hash.suffix == shash::kSuffixCatalog);
  CacheManager::Label label;
  label.path = name;
  label.flags = CacheManager::kLabelCatalog;
  int fd = fetcher_->Fetch(CacheManager::LabeledObject(hash, label),
                           alt_root_catalog_path);
  if (fd >= 0) {
    if (root_fd_ < 0) {
      root_fd_ = fd;
    }

    LogCvmfs(kLogCatalog, kLogDebug,
           "FetchCatalogByHash filedescriptor %d", fd);
    // sql_catalog_handle->assign("@" + StringifyInt(fd));
    *sql_catalog_handle = "@" + StringifyInt(fd);
    return kLoadNew;
  }

  if (fd == -ENOSPC)
    return kLoadNoSpace;

  return kLoadFail;
}


void ClientCatalogManager::UnloadCatalog(const Catalog *catalog) {
  LogCvmfs(kLogCache, kLogDebug, "unloading catalog %s",
           catalog->mountpoint().c_str());

  map<PathString, shash::Any>::iterator iter =
    mounted_catalogs_.find(catalog->mountpoint());
  assert(iter != mounted_catalogs_.end());
  fetcher_->cache_mgr()->quota_mgr()->Unpin(iter->second);
  mounted_catalogs_.erase(iter);
  const catalog::Counters &counters = catalog->GetCounters();
  loaded_inodes_ -= counters.GetSelfEntries();
}


/**
 * Checks if the current repository revision is blacklisted.  The format
 * of the blacklist lines is '<REPO N' where REPO is the repository name,
 * N is the revision number, and the two parts are separated by whitespace.
 * Any revision of REPO less than N is blacklisted.
 * Note: no extra characters are allowed after N, not even whitespace.
 * @return true if it is blacklisted, false otherwise
 */
bool ClientCatalogManager::IsRevisionBlacklisted() {
  uint64_t revision = GetRevision();

  LogCvmfs(kLogCache, kLogDebug, "checking if %s revision %u is blacklisted",
           repo_name_.c_str(), revision);

  vector<string> blacklist = signature_mgr_->GetBlacklist();
  for (unsigned i = 0; i < blacklist.size(); ++i) {
    std::string line = blacklist[i];
    if (line[0] != '<')
      continue;
    unsigned idx = repo_name_.length() + 1;
    if (line.length() <= idx)
      continue;
    if ((line[idx] != ' ') && (line[idx] != '\t'))
      continue;
    if (line.substr(1, idx - 1) != repo_name_)
      continue;
    ++idx;
    while ((line[idx] == ' ') || (line[idx] == '\t'))
      ++idx;
    if (idx >= line.length())
      continue;
    uint64_t rev;
    if (!String2Uint64Parse(line.substr(idx), &rev))
      continue;
    if (revision < rev)
      return true;
  }

  return false;
}


//------------------------------------------------------------------------------


void CachedManifestEnsemble::FetchCertificate(const shash::Any &hash) {
  CacheManager::Label label;
  label.flags |= CacheManager::kLabelCertificate;
  label.path = catalog_mgr_->repo_name();
  uint64_t size;
  bool retval = cache_mgr_->Open2Mem(CacheManager::LabeledObject(hash, label),
                                     &cert_buf, &size);
  cert_size = size;
  if (retval)
    perf::Inc(catalog_mgr_->n_certificate_hits_);
  else
    perf::Inc(catalog_mgr_->n_certificate_misses_);
}

}  // namespace catalog
