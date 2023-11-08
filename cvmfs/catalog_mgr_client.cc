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
  , repo_name_(mountpoint->fqrn())
  , fetcher_(mountpoint->fetcher())
  , signature_mgr_(mountpoint->signature_mgr())
  , workspace_(mountpoint->file_system()->workspace())
  , offline_mode_(false)
  , all_inodes_(0)
  , loaded_inodes_(0)
  , fixed_root_catalog_()
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
 *
 * @returns true  - root catalog was successfully mounted
 *          false - otherwise
 */
bool ClientCatalogManager::InitFixed(
  const shash::Any &root_hash,
  bool alternative_path)
{
  LogCvmfs(kLogCatalog, kLogDebug, "Initialize catalog with fixed root hash %s",
           root_hash.ToString().c_str());
  WriteLock();
  fixed_alt_root_catalog_ = alternative_path;
  fixed_root_catalog_ = root_hash;

  bool attached = MountCatalog(PathString("", 0), root_hash, NULL);
  Unlock();

  if (!attached) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to initialize fixed root catalog");
  }

  return attached;
}

/**
 * Gets information about the most recent root catalog.
 *
 * Checks the locations: mounted, alien cache and remote (server) and sets the
 * fields of variable "result". For the most recent catalog the location, hash
 * and revision number are set.
 *
 * @param [out] result All fields but sqlite_path will be set:
 *                     mountpoint, root_ctl_location, root_ctlg_revision, hash
 * @return kLoadUp2Date - if most recent root catalog is already mounted
 *         kLoadNew     - otherwise
 */
LoadReturn ClientCatalogManager::GetNewRootCatalogContext(
                                                       CatalogContext *result) {
  result->SetMountpoint(PathString("", 0));

  // 1) Get alien cache root catalog (local)

  // Happens only on init/remount, i.e. quota won't delete a cached catalog
  shash::Any breadcrumb_hash(shash::kSha1, shash::kSuffixCatalog);
  uint64_t breadcrumb_timestamp = 0;
  uint64_t breadcrumb_revision = -1ul;

  // TODO(heretherebedragons) how to handle old breadcrumbs that are loaded by
  // default with revision number 0?
  manifest::Breadcrumb breadcrumb =
                              fetcher_->cache_mgr()->LoadBreadcrumb(repo_name_);
  if (breadcrumb.IsValid()) {
    breadcrumb_hash = breadcrumb.catalog_hash;
    breadcrumb_timestamp = breadcrumb.timestamp;
    breadcrumb_revision = breadcrumb.revision;
    LogCvmfs(kLogCache, kLogDebug,
        "cached copy publish date %s (hash %s, revision %lu)",
        StringifyTime(static_cast<int64_t>(breadcrumb_timestamp), true).c_str(),
        breadcrumb_hash.ToString().c_str(), breadcrumb.revision);
  } else {
    LogCvmfs(kLogCache, kLogDebug, "unable to read local checksum");
  }

  // 2) Select local newest catalog: mounted vs alien

  shash::Any local_newest_hash = breadcrumb_hash;
  uint64_t local_newest_revision = breadcrumb_revision;
  result->SetRootCtlgLocation(kCtlgLocationBreadcrumb);
  LoadReturn success_code = catalog::kLoadNew;

  // We only look for currently loaded catalog if the revision is newer than
  // the breadcrumb revision and both revision numbers are valid (!= -1ul).
  if (breadcrumb_revision <= GetRevisionNoLock()
     || breadcrumb_revision == -1ul) {
    const std::map<PathString, shash::Any>::iterator curr_hash_itr =
                                      mounted_catalogs_.find(PathString("", 0));
    local_newest_hash = curr_hash_itr->second;
    local_newest_revision = GetRevisionNoLock();
    result->SetRootCtlgLocation(kCtlgLocationMounted);
    success_code = catalog::kLoadUp2Date;
  }

  // 3) Get remote root catalog (fails if remote catalog is older)
  manifest::Failures manifest_failure;
  UniquePtr<CachedManifestEnsemble> ensemble(
                       new CachedManifestEnsemble(fetcher_->cache_mgr(), this));
  // TODO(heretherebedragons)
  // breadcrumb_timestamp: does breadcrumb timestamp and revision always go hand
  // in hand?
  // can it be that the timestamp is older on the server but the revision higher
  // than the breadcrumb?
  manifest_failure = manifest::Fetch("", repo_name_, breadcrumb_timestamp,
                                     &local_newest_hash, signature_mgr_,
                                     fetcher_->download_mgr(),
                                     ensemble.weak_ref());

  if (manifest_failure == manifest::kFailOk) {
    // fixed catalog: just check if server is reachable but use local version
    // TODO(heretherebedragons) OK that this could be in theory the breadcrumb?
    if (fixed_root_catalog_ != shash::Any()) {
      offline_mode_ = false;
      result->SetHash(fixed_root_catalog_);
      result->SetRootCtlgRevision(local_newest_revision);
      return success_code;
    }
    // server has newest revision or no valid local revision
    if (ensemble->manifest->revision() > local_newest_revision
          || local_newest_revision == -1ul
          // if revision is 0 both local and server, load catalog from server
          // as local is most likely just "initialized" without valid value
          || (ensemble->manifest->revision() == 0 &&
              local_newest_revision == 0)) {
    result->SetHash(ensemble->manifest->catalog_hash());
    result->SetRootCtlgRevision(ensemble->manifest->revision());
    result->SetRootCtlgLocation(kCtlgLocationServer);
    if (ensemble->manifest->has_alt_catalog_path()) {
      fixed_alt_root_catalog_ = true;
    } else {
      fixed_alt_root_catalog_ = false;
    }

    result->TakeManifestEnsemble(
                  static_cast<manifest::ManifestEnsemble*>(ensemble.Release()));
    offline_mode_ = false;

    return catalog::kLoadNew;
    }
  }
  LogCvmfs(kLogCache, kLogDebug,
            "failed fetch manifest from server: "
            "manifest too old or server unreachable (%d - %s)",
            manifest_failure, manifest::Code2Ascii(manifest_failure));

  // corner case: server not reachable, local revision is 0. We can not
  // distinguish if local revision number is valid or invalid. Most likely
  // it is invalid - so fail
  if (local_newest_revision == 0 && !manifest_failure == manifest::kFailOk) {
    return catalog::kLoadFail;
  }

  if (manifest_failure == manifest::kFailOk
      && ensemble->manifest->revision() == local_newest_revision) {
      offline_mode_ = false;
  } else {
    offline_mode_ = true;
  }
  result->SetHash(local_newest_hash);
  result->SetRootCtlgRevision(local_newest_revision);

  return success_code;
}

/**
 * Loads (and fetches) a catalog by hash for a given mountpoint.
 *
 * Special case for root catalog: ctlg_context->root_ctlg_location must be given.
 * If it is kCtlgLocationServer than another check for the most recent
 * manifest is done.
 *
 * @param [in, out] ctlg_context mandatory fields (input): mountpoint, hash
 *         additional mandatory fields for root catalog: root_ctlg_location
 *         output: sqlite_path is set if catalog fetch successful
 * @return kLoadUp2Date for root catalog that is already mounted
 *         kLoadNew for any other successful load
 *         kLoadFail on failure
 */
LoadReturn ClientCatalogManager::LoadCatalogByHash(
                                                 CatalogContext *ctlg_context) {
  string catalog_descr = "file catalog at " + repo_name_ + ":" +
    (ctlg_context->IsRootCatalog() ?
      "/" : string(ctlg_context->mountpoint().GetChars(),
                   ctlg_context->mountpoint().GetLength()));

  catalog_descr += " (" + ctlg_context->hash().ToString() + ")";
  string alt_root_catalog_path = "";

  // root catalog needs special handling because of alt_root_catalog_path
  if (ctlg_context->IsRootCatalog() && fixed_alt_root_catalog_) {
    alt_root_catalog_path = ctlg_context->hash().MakeAlternativePath();
  }

  // TODO(heretherebedragons) could help: fetch should return if fetch from
  // cache or from remote would save us the if in L283
  const LoadReturn load_ret = FetchCatalogByHash(ctlg_context->hash(),
                                           catalog_descr, alt_root_catalog_path,
                                           ctlg_context->GetSqlitePathPtr());
  if (load_ret == catalog::kLoadNew) {
    loaded_catalogs_[ctlg_context->mountpoint()] = ctlg_context->hash();

    if (ctlg_context->IsRootCatalog()) {  // root catalog
      if (ctlg_context->root_ctlg_location() == kCtlgLocationMounted) {
        return kLoadUp2Date;
      }

      // if coming from server: update breadcrumb
      if (ctlg_context->root_ctlg_location() == kCtlgLocationServer) {
          // Store new manifest and certificate
          CacheManager::Label label;
          label.path = repo_name_;
          label.flags |= CacheManager::kLabelCertificate;
          fetcher_->cache_mgr()->CommitFromMem(
                CacheManager::LabeledObject(ctlg_context->manifest_ensemble()->
                                                        manifest->certificate(),
                                            label),
                                  ctlg_context->manifest_ensemble()->cert_buf,
                                  ctlg_context->manifest_ensemble()->cert_size);
          fetcher_->cache_mgr()->StoreBreadcrumb(
                                  *ctlg_context->manifest_ensemble()->manifest);
      }
    }
  }

  return load_ret;
}

/**
 * Fetch a catalog by hash either from cache or from remote.
 * Successful load always returns kLoadNew (independent of the location) and
 * sets the sqlite_path variable.
 *
 * @param [out] sqlite_path of the catalog if successfully fetched
 * @return kLoadNew on success
 *         kLoadNoSpace out of space, no room on the device to open the catalog
 *         kLoadFail on all other failures
 */
LoadReturn ClientCatalogManager::FetchCatalogByHash(
  const shash::Any &hash,
  const string &name,
  const std::string &alt_root_catalog_path,
  std::string *sqlite_path)
{
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
    *sqlite_path = "@" + StringifyInt(fd);
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
