/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_CATALOG_MGR_IMPL_H_
#define CVMFS_CATALOG_MGR_IMPL_H_

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "cvmfs_config.h"

#include <cassert>
#include <string>
#include <vector>

#include "logging.h"
#include "shortstring.h"
#include "statistics.h"
#include "xattr.h"

using namespace std;  // NOLINT

namespace catalog {

template <class CatalogT>
AbstractCatalogManager<CatalogT>::AbstractCatalogManager(
    perf::Statistics *statistics) :
  statistics_(statistics) {
  inode_watermark_status_ = 0;
  inode_gauge_ = AbstractCatalogManager<CatalogT>::kInodeOffset;
  revision_cache_ = 0;
  catalog_watermark_ = 0;
  volatile_flag_ = false;
  has_authz_cache_ = false;
  inode_annotation_ = NULL;
  incarnation_ = 0;
  rwlock_ =
    reinterpret_cast<pthread_rwlock_t *>(smalloc(sizeof(pthread_rwlock_t)));
  int retval = pthread_rwlock_init(rwlock_, NULL);
  assert(retval == 0);
  retval = pthread_key_create(&pkey_sqlitemem_, NULL);
  assert(retval == 0);
}

template <class CatalogT>
AbstractCatalogManager<CatalogT>::~AbstractCatalogManager() {
  DetachAll();
  pthread_key_delete(pkey_sqlitemem_);
  pthread_rwlock_destroy(rwlock_);
  free(rwlock_);
}

template <class CatalogT>
void AbstractCatalogManager<CatalogT>::SetInodeAnnotation(
    InodeAnnotation *new_annotation)
{
  assert(catalogs_.empty() || (new_annotation == inode_annotation_));
  inode_annotation_ = new_annotation;
}

template <class CatalogT>
void AbstractCatalogManager<CatalogT>::SetOwnerMaps(const OwnerMap &uid_map,
                                          const OwnerMap &gid_map)
{
  uid_map_ = uid_map;
  gid_map_ = gid_map;
}

template <class CatalogT>
void AbstractCatalogManager<CatalogT>::SetCatalogWatermark(unsigned limit) {
  catalog_watermark_ = limit;
}

template <class CatalogT>
void AbstractCatalogManager<CatalogT>::CheckInodeWatermark() {
  if (inode_watermark_status_ > 0)
    return;

  uint64_t highest_inode = inode_gauge_;
  if (inode_annotation_)
    highest_inode += inode_annotation_->GetGeneration();
  uint64_t uint32_border = 1;
  uint32_border = uint32_border << 32;
  if (highest_inode >= uint32_border) {
    LogCvmfs(kLogCatalog, kLogDebug | kLogSyslogWarn, "inodes exceed 32bit");
    inode_watermark_status_++;
  }
}


/**
 * Initializes the CatalogManager and loads and attaches the root entry.
 * @return true on successful init, otherwise false
 */
template <class CatalogT>
bool AbstractCatalogManager<CatalogT>::Init() {
  LogCvmfs(kLogCatalog, kLogDebug, "Initialize catalog");
  WriteLock();
  bool attached = MountCatalog(PathString("", 0), shash::Any(), NULL);
  Unlock();

  if (!attached) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to initialize root catalog");
  }

  return attached;
}


/**
 * Remounts the root catalog if necessary.  If a newer root catalog exists,
 * it is mounted and replaces the currently mounted tree (all existing catalogs
 * are detached)
 */
template <class CatalogT>
LoadError AbstractCatalogManager<CatalogT>::Remount(const bool dry_run) {
  LogCvmfs(kLogCatalog, kLogDebug,
           "remounting repositories (dry run %d)", dry_run);
  if (dry_run)
    return LoadCatalog(PathString("", 0), shash::Any(), NULL, NULL);

  WriteLock();

  string     catalog_path;
  shash::Any catalog_hash;
  const LoadError load_error = LoadCatalog(PathString("", 0),
                                           shash::Any(),
                                           &catalog_path,
                                           &catalog_hash);
  if (load_error == kLoadNew) {
    inode_t old_inode_gauge = inode_gauge_;
    DetachAll();
    inode_gauge_ = AbstractCatalogManager<CatalogT>::kInodeOffset;

    CatalogT *new_root = CreateCatalog(PathString("", 0), catalog_hash, NULL);
    assert(new_root);
    bool retval = AttachCatalog(catalog_path, new_root);
    assert(retval);

    if (inode_annotation_) {
      inode_annotation_->IncGeneration(old_inode_gauge);
    }
  }
  CheckInodeWatermark();
  Unlock();

  return load_error;
}

/**
 * Remounts to the given hash
 */
template <class CatalogT>
LoadError AbstractCatalogManager<CatalogT>::ChangeRoot(
  const shash::Any &root_hash)
{
  LogCvmfs(kLogCatalog, kLogDebug,
           "switching to root hash %s", root_hash.ToString().c_str());

  WriteLock();

  string     catalog_path;
  shash::Any catalog_hash;
  const LoadError load_error = LoadCatalog(PathString("", 0),
                                           root_hash,
                                           &catalog_path,
                                           &catalog_hash);
  if (load_error == kLoadNew) {
    inode_t old_inode_gauge = inode_gauge_;
    DetachAll();
    inode_gauge_ = AbstractCatalogManager<CatalogT>::kInodeOffset;

    CatalogT *new_root = CreateCatalog(PathString("", 0), catalog_hash, NULL);
    assert(new_root);
    bool retval = AttachCatalog(catalog_path, new_root);
    assert(retval);

    if (inode_annotation_) {
      inode_annotation_->IncGeneration(old_inode_gauge);
    }
  }
  CheckInodeWatermark();
  Unlock();

  return load_error;
}


/**
 * Detaches everything except the root catalog
 */
template <class CatalogT>
void AbstractCatalogManager<CatalogT>::DetachNested() {
  WriteLock();
  if (catalogs_.empty()) {
    Unlock();
    return;
  }

  typename CatalogList::const_iterator i;
  typename CatalogList::const_iterator iend;
  CatalogList catalogs_to_detach = GetRootCatalog()->GetChildren();
  for (i = catalogs_to_detach.begin(), iend = catalogs_to_detach.end();
       i != iend; ++i)
  {
    DetachSubtree(*i);
  }

  Unlock();
}


/**
 * Returns the NULL hash if the nested catalog is not found.
 */
template <class CatalogT>
shash::Any AbstractCatalogManager<CatalogT>::GetNestedCatalogHash(
  const PathString &mountpoint)
{
  assert(!mountpoint.IsEmpty());
  CatalogT *catalog = FindCatalog(mountpoint);
  assert(catalog != NULL);
  if (catalog->mountpoint() == mountpoint) {
    catalog = catalog->parent();
    assert(catalog != NULL);
  }
  shash::Any result;
  uint64_t size;
  catalog->FindNested(mountpoint, &result, &size);
  return result;
}


/**
 * Perform a lookup for a specific DirectoryEntry in the catalogs.
 * @param path      the path to find in the catalogs
 * @param options   whether to perform another lookup to get the parent entry,
 *                  too
 * @param dirent    the resulting DirectoryEntry, or special Negative entry
 *                  Note: can be set to zero if the result is not important
 * @return true if lookup succeeded otherwise false
 */
template <class CatalogT>
bool AbstractCatalogManager<CatalogT>::LookupPath(const PathString &path,
                                        const LookupOptions options,
                                        DirectoryEntry *dirent)
{
  // initialize as non-negative
  assert(dirent);
  *dirent = DirectoryEntry();

  // create a dummy negative directory entry
  const DirectoryEntry dirent_negative =
    DirectoryEntry(catalog::kDirentNegative);

  EnforceSqliteMemLimit();
  ReadLock();

  CatalogT *best_fit = FindCatalog(path);
  assert(best_fit != NULL);

  perf::Inc(statistics_.n_lookup_path);
  LogCvmfs(kLogCatalog, kLogDebug, "looking up '%s' in catalog: '%s'",
           path.c_str(), best_fit->mountpoint().c_str());
  bool found = best_fit->LookupPath(path, dirent);

  // Possibly in a nested catalog
  if (!found && MountSubtree(path, best_fit, false /* is_listable */, NULL)) {
    LogCvmfs(kLogCatalog, kLogDebug, "looking up '%s' in a nested catalog",
             path.c_str());
    Unlock();
    WriteLock();
    // Check again to avoid race
    best_fit = FindCatalog(path);
    assert(best_fit != NULL);
    perf::Inc(statistics_.n_lookup_path);
    found = best_fit->LookupPath(path, dirent);

    if (!found) {
      LogCvmfs(kLogCatalog, kLogDebug,
               "entry not found, we may have to load nested catalogs");

      CatalogT *nested_catalog;
      found =
        MountSubtree(path, best_fit, false /* is_listable */, &nested_catalog);

      if (!found) {
        LogCvmfs(kLogCatalog, kLogDebug,
                 "failed to load nested catalog for '%s'", path.c_str());
        goto lookup_path_notfound;
      }

      if (nested_catalog != best_fit) {
        perf::Inc(statistics_.n_lookup_path);
        found = nested_catalog->LookupPath(path, dirent);
        if (!found) {
          LogCvmfs(kLogCatalog, kLogDebug,
                   "nested catalogs loaded but entry '%s' was still not found",
                   path.c_str());
          if (dirent != NULL) *dirent = dirent_negative;
          goto lookup_path_notfound;
        } else {
          best_fit = nested_catalog;
        }
      } else {
        LogCvmfs(kLogCatalog, kLogDebug, "no nested catalog fits");
        if (dirent != NULL) *dirent = dirent_negative;
        goto lookup_path_notfound;
      }
    }
    assert(found);
  }
  // Not in a nested catalog (because no nested cataog fits), ENOENT
  if (!found) {
    LogCvmfs(kLogCatalog, kLogDebug, "ENOENT: '%s'", path.c_str());
    if (dirent != NULL) *dirent = dirent_negative;
    goto lookup_path_notfound;
  }

  LogCvmfs(kLogCatalog, kLogDebug, "found entry '%s' in catalog '%s'",
           path.c_str(), best_fit->mountpoint().c_str());

  if ((options & kLookupRawSymlink) == kLookupRawSymlink) {
    LinkString raw_symlink;
    bool retval = best_fit->LookupRawSymlink(path, &raw_symlink);
    assert(retval);  // Must be true, we have just found the entry
    dirent->set_symlink(raw_symlink);
  }

  Unlock();
  return true;

 lookup_path_notfound:
  Unlock();
  // Includes both: ENOENT and not found due to I/O error
  perf::Inc(statistics_.n_lookup_path_negative);
  return false;
}


/**
 * Perform a lookup for Nested Catalog that serves this path.
 *  If the path specified is a catalog mountpoint the catalog at that point is
 *  mounted and returned.
 * @param path       the path to find in the catalogs
 * @param mountpoint the path to the nested catalog found
 * @param hash       the hash of the nested catalog found
 * @param size       the size of the nested catalog, 0 for root. Root is not a
 *                   nested catalog in the database.
 * @return true if lookup succeeded otherwise false (available catalog failed
 *         to mount)
 */
template <class CatalogT>
bool AbstractCatalogManager<CatalogT>::LookupNested(
  const PathString &path,
  PathString *mountpoint,
  shash::Any *hash,
  uint64_t *size
) {
  EnforceSqliteMemLimit();
  bool result = false;
  ReadLock();

  // Look past current path to mount up to intended location
  PathString catalog_path(path);
  catalog_path.Append("/.cvmfscatalog", 14);

  // Find catalog, possibly load nested
  CatalogT *best_fit = FindCatalog(catalog_path);
  CatalogT *catalog = best_fit;
  if (MountSubtree(catalog_path, best_fit, false /* is_listable */, NULL)) {
    Unlock();
    WriteLock();
    // Check again to avoid race
    best_fit = FindCatalog(catalog_path);
    result =
      MountSubtree(catalog_path, best_fit, false /* is_listable */, &catalog);
    // Result is false if an available catalog failed to load (error happened)
    if (!result) {
      Unlock();
      return false;
    }
  }

  // If the found catalog is the Root there is no parent to lookup
  if (catalog->HasParent()) {
    result = catalog->parent()->FindNested(catalog->root_prefix(), hash, size);
  }

  // Mountpoint now points to the found catalog
  mountpoint->Assign(catalog->root_prefix());

  // If the result is false, it means that no nested catalog was found for
  // this path. As the root catalog does not have a Nested Catalog of
  // itself, we manually set the values and leave the size as 0.
  // TODO(nhazekam) Allow for Root Catalog to be returned
  if (!result) {
    *hash =  GetRootCatalog()->hash();
    *size = 0;
    result = true;
  }

  Unlock();
  return result;
}


/**
 * Create a listing of the parents, catalog, and children of the catalog
 *  that serves the specified path.
 *  If the path specified is a catalog mountpoint the catalog at that point is
 *  mounted and returned.
 * @param path        the path to find in the catalogs
 * @param result_list the list where the results will be added.
 * @return true if the list could be created, false if catalog fails to mount.
 */
template <class CatalogT>
bool AbstractCatalogManager<CatalogT>::ListCatalogSkein(
  const PathString &path,
  std::vector<PathString> *result_list
) {
  EnforceSqliteMemLimit();
  bool result;
  ReadLock();

  // Look past current path to mount up to intended location
  PathString test(path);
  test.Append("/.cvmfscatalog", 14);

  // Find catalog, possibly load nested
  CatalogT *best_fit = FindCatalog(test);
  CatalogT *catalog = best_fit;
  // True if there is an available nested catalog
  if (MountSubtree(test, best_fit, false /* is_listable */, NULL)) {
    Unlock();
    WriteLock();
    // Check again to avoid race
    best_fit = FindCatalog(test);
    result = MountSubtree(test, best_fit, false /* is_listable */, &catalog);
    // result is false if an available catalog failed to load
    if (!result) {
      Unlock();
      return false;
    }
  }

  // Build listing
  CatalogT *cur_parent = catalog->parent();
  if (cur_parent) {
    // Walk up parent tree to find base
    std::vector<catalog::Catalog*> parents;
    while (cur_parent->HasParent()) {
      parents.push_back(cur_parent);
      cur_parent = cur_parent->parent();
    }
    parents.push_back(cur_parent);
    while (!parents.empty()) {
      // Add to list in order starting at root
      result_list->push_back(parents.back()->root_prefix());
      parents.pop_back();
    }
  }
  // Add the current catalog
  result_list->push_back(catalog->root_prefix());

  Catalog::NestedCatalogList children = catalog->ListOwnNestedCatalogs();

  // Add all children nested catalogs
  for (unsigned i = 0; i < children.size(); i++) {
    result_list->push_back(children.at(i).mountpoint);
  }

  Unlock();
  return true;
}


template <class CatalogT>
bool AbstractCatalogManager<CatalogT>::LookupXattrs(
  const PathString &path,
  XattrList *xattrs)
{
  EnforceSqliteMemLimit();
  bool result;
  ReadLock();

  // Find catalog, possibly load nested
  CatalogT *best_fit = FindCatalog(path);
  CatalogT *catalog = best_fit;
  if (MountSubtree(path, best_fit, false /* is_listable */, NULL)) {
    Unlock();
    WriteLock();
    // Check again to avoid race
    best_fit = FindCatalog(path);
    result = MountSubtree(path, best_fit, false /* is_listable */, &catalog);
    if (!result) {
      Unlock();
      return false;
    }
  }

  perf::Inc(statistics_.n_lookup_xattrs);
  result = catalog->LookupXattrsPath(path, xattrs);

  Unlock();
  return result;
}


/**
 * Do a listing of the specified directory.
 * @param path the path of the directory to list
 * @param listing the resulting DirectoryEntryList
 * @return true if listing succeeded otherwise false
 */
template <class CatalogT>
bool AbstractCatalogManager<CatalogT>::Listing(const PathString &path,
                                     DirectoryEntryList *listing,
                                     const bool expand_symlink)
{
  EnforceSqliteMemLimit();
  bool result;
  ReadLock();

  // Find catalog, possibly load nested
  CatalogT *best_fit = FindCatalog(path);
  CatalogT *catalog = best_fit;
  if (MountSubtree(path, best_fit, true /* is_listable */, NULL)) {
    Unlock();
    WriteLock();
    // Check again to avoid race
    best_fit = FindCatalog(path);
    result = MountSubtree(path, best_fit, true /* is_listable */, &catalog);
    if (!result) {
      Unlock();
      return false;
    }
  }

  perf::Inc(statistics_.n_listing);
  result = catalog->ListingPath(path, listing, expand_symlink);

  Unlock();
  return result;
}


/**
 * Do a listing of the specified directory, return only struct stat values.
 * @param path the path of the directory to list
 * @param listing the resulting StatEntryList
 * @return true if listing succeeded otherwise false
 */
template <class CatalogT>
bool AbstractCatalogManager<CatalogT>::ListingStat(const PathString &path,
                                        StatEntryList *listing)
{
  EnforceSqliteMemLimit();
  bool result;
  ReadLock();

  // Find catalog, possibly load nested
  CatalogT *best_fit = FindCatalog(path);
  CatalogT *catalog = best_fit;
  if (MountSubtree(path, best_fit, true /* is_listable */, NULL)) {
    Unlock();
    WriteLock();
    // Check again to avoid race
    best_fit = FindCatalog(path);
    result = MountSubtree(path, best_fit, true /* is_listable */, &catalog);
    if (!result) {
      Unlock();
      return false;
    }
  }

  perf::Inc(statistics_.n_listing);
  result = catalog->ListingPathStat(path, listing);

  Unlock();
  return result;
}


/**
 * Collect file chunks (if exist)
 * @param path the path of the directory to list
 * @param interpret_hashes_as hash of the directory entry (by convention the
 *        same than the chunk hashes)
 * @return true if listing succeeded otherwise false
 */
template <class CatalogT>
bool AbstractCatalogManager<CatalogT>::ListFileChunks(
  const PathString &path,
  const shash::Algorithms interpret_hashes_as,
  FileChunkList *chunks)
{
  EnforceSqliteMemLimit();
  bool result;
  ReadLock();

  // Find catalog, possibly load nested
  CatalogT *best_fit = FindCatalog(path);
  CatalogT *catalog = best_fit;
  if (MountSubtree(path, best_fit, false /* is_listable */, NULL)) {
    Unlock();
    WriteLock();
    // Check again to avoid race
    best_fit = FindCatalog(path);
    result = MountSubtree(path, best_fit, false /* is_listable */, &catalog);
    if (!result) {
      Unlock();
      return false;
    }
  }

  result = catalog->ListPathChunks(path, interpret_hashes_as, chunks);

  Unlock();
  return result;
}

template <class CatalogT>
catalog::Counters AbstractCatalogManager<CatalogT>::LookupCounters(
  const PathString &path,
  std::string *subcatalog_path)
{
  EnforceSqliteMemLimit();
  bool result;
  ReadLock();

  // Look past current path to mount up to intended location
  PathString catalog_path(path);
  catalog_path.Append("/.cvmfscatalog", 14);

  // Find catalog, possibly load nested
  CatalogT *best_fit = FindCatalog(catalog_path);
  CatalogT *catalog = best_fit;
  if (MountSubtree(catalog_path, best_fit, false /* is_listable */, NULL)) {
    Unlock();
    WriteLock();
    // Check again to avoid race
    best_fit = FindCatalog(catalog_path);
    result =
      MountSubtree(catalog_path, best_fit, false /* is_listable */, &catalog);
    // Result is false if an available catalog failed to load (error happened)
    if (!result) {
      Unlock();
      *subcatalog_path = "error: failed to load catalog!";
      return catalog::Counters();
    }
  }

  *subcatalog_path = catalog->mountpoint().ToString();
  catalog::Counters counters = catalog->GetCounters();
  Unlock();
  return counters;
}


template <class CatalogT>
uint64_t AbstractCatalogManager<CatalogT>::GetRevision() const {
  ReadLock();
  const uint64_t revision = revision_cache_;
  Unlock();
  return revision;
}


template <class CatalogT>
bool AbstractCatalogManager<CatalogT>::GetVOMSAuthz(std::string *authz) const {
  ReadLock();
  const bool has_authz = has_authz_cache_;
  if (has_authz && authz)
    *authz = authz_cache_;
  Unlock();
  return has_authz;
}


template <class CatalogT>
bool AbstractCatalogManager<CatalogT>::HasExplicitTTL() const {
  ReadLock();
  const bool result = GetRootCatalog()->HasExplicitTTL();
  Unlock();
  return result;
}


template <class CatalogT>
uint64_t AbstractCatalogManager<CatalogT>::GetTTL() const {
  ReadLock();
  const uint64_t ttl = GetRootCatalog()->GetTTL();
  Unlock();
  return ttl;
}


template <class CatalogT>
int AbstractCatalogManager<CatalogT>::GetNumCatalogs() const {
  ReadLock();
  int result = catalogs_.size();
  Unlock();
  return result;
}


/**
 * Gets a formatted tree of the currently attached catalogs
 */
template <class CatalogT>
string AbstractCatalogManager<CatalogT>::PrintHierarchy() const {
  ReadLock();
  const string output = PrintHierarchyRecursively(GetRootCatalog(), 0);
  Unlock();
  return output;
}


/**
 * Assigns the next free numbers in the 64 bit space
 */
template <class CatalogT>
InodeRange AbstractCatalogManager<CatalogT>::AcquireInodes(uint64_t size) {
  InodeRange result;
  result.offset = inode_gauge_;
  result.size = size;

  inode_gauge_ += size;
  LogCvmfs(kLogCatalog, kLogDebug, "allocating inodes from %d to %d.",
           result.offset + 1, inode_gauge_);

  return result;
}


/**
 * Called if a catalog is detached which renders the associated InodeChunk
 * invalid.
 * @param chunk the InodeChunk to be freed
 */
template <class CatalogT>
void AbstractCatalogManager<CatalogT>::ReleaseInodes(const InodeRange chunk) {
  // TODO(jblomer) currently inodes are only released on remount
}


/**
 * Find the catalog leaf in the tree that fits the path.
 * The path might be served by a not yet loaded nested catalog.
 * @param path the path a catalog is searched for
 * @return the catalog which is best fitting at the given path
 */
template <class CatalogT>
CatalogT* AbstractCatalogManager<CatalogT>::FindCatalog(
    const PathString &path) const {
  assert(catalogs_.size() > 0);

  // Start at the root catalog and successively go down the catalog tree
  CatalogT *best_fit = GetRootCatalog();
  CatalogT *next_fit = NULL;
  while (best_fit->mountpoint() != path) {
    next_fit = best_fit->FindSubtree(path);
    if (next_fit == NULL)
      break;
    best_fit = next_fit;
  }

  return best_fit;
}


/**
 * Checks if a searched catalog is already mounted to this CatalogManager
 * @param root_path the root path of the searched catalog
 * @param attached_catalog is set to the searched catalog, if not NULL
 * @return true if catalog is already present, false otherwise
 */
template <class CatalogT>
bool AbstractCatalogManager<CatalogT>::IsAttached(const PathString &root_path,
                                        CatalogT **attached_catalog) const
{
  if (catalogs_.size() == 0)
    return false;

  CatalogT *best_fit = FindCatalog(root_path);
  if (best_fit->mountpoint() != root_path)
    return false;

  if (attached_catalog != NULL) *attached_catalog = best_fit;
  return true;
}


/**
 * Recursively mounts all nested catalogs required to serve a path.
 * If leaf_catalog is NULL, just indicate if it is necessary to load a
 * nested catalog for the given path.
 * The final leaf nested catalog is returned.
 * The is_listable parameter is relevant if path is a nested catalog.  Only
 * if is_listable is true, the nested catalog will be used; otherwise the parent
 * with the transation point is sufficient.
 */
template <class CatalogT>
bool AbstractCatalogManager<CatalogT>::MountSubtree(
  const PathString &path,
  const CatalogT *entry_point,
  bool is_listable,
  CatalogT **leaf_catalog)
{
  bool result = true;
  CatalogT *parent = (entry_point == NULL) ?
                     GetRootCatalog() : const_cast<CatalogT *>(entry_point);
  assert(path.StartsWith(parent->mountpoint()));

  // Try to find path as a super string of nested catalog mount points
  PathString path_slash(path);
  path_slash.Append("/", 1);
  perf::Inc(statistics_.n_nested_listing);
  typedef typename CatalogT::NestedCatalogList NestedCatalogList;
  const NestedCatalogList& nested_catalogs =
    parent->ListNestedCatalogs();
  for (typename NestedCatalogList::const_iterator i = nested_catalogs.begin(),
       iEnd = nested_catalogs.end(); i != iEnd; ++i)
  {
    // Next nesting level
    PathString nested_path_slash(i->mountpoint);
    nested_path_slash.Append("/", 1);
    if (path_slash.StartsWith(nested_path_slash)) {
      // Found a nested catalog transition point
      if (!is_listable && (path_slash == nested_path_slash))
        break;

      if (leaf_catalog == NULL)
        return true;
      CatalogT *new_nested;
      LogCvmfs(kLogCatalog, kLogDebug, "load nested catalog at %s",
               i->mountpoint.c_str());
      // prevent endless recursion with corrupted catalogs
      // (due to reloading root)
      if (i->hash.IsNull())
        return false;
      new_nested = MountCatalog(i->mountpoint, i->hash, parent);
      if (!new_nested)
        return false;

      result = MountSubtree(path, new_nested, is_listable, &parent);
      break;
    }
  }

  if (leaf_catalog == NULL)
    return false;
  *leaf_catalog = parent;
  return result;
}


/**
 * Load a catalog file and attach it to the tree of Catalog objects.
 * Loading of catalogs is implemented by derived classes.
 */
template <class CatalogT>
CatalogT *AbstractCatalogManager<CatalogT>::MountCatalog(
                                              const PathString &mountpoint,
                                              const shash::Any &hash,
                                              CatalogT *parent_catalog)
{
  CatalogT *attached_catalog = NULL;
  if (IsAttached(mountpoint, &attached_catalog))
    return attached_catalog;

  string     catalog_path;
  shash::Any catalog_hash;
  const LoadError retval =
    LoadCatalog(mountpoint, hash, &catalog_path, &catalog_hash);
  if ((retval == kLoadFail) || (retval == kLoadNoSpace)) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to load catalog '%s' (%d - %s)",
             mountpoint.c_str(), retval, Code2Ascii(retval));
    return NULL;
  }

  attached_catalog = CreateCatalog(mountpoint, catalog_hash, parent_catalog);

  // Attach loaded catalog
  if (!AttachCatalog(catalog_path, attached_catalog)) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to attach catalog '%s'",
             mountpoint.c_str());
    UnloadCatalog(attached_catalog);
    return NULL;
  }

  if ((catalog_watermark_ > 0) && (catalogs_.size() >= catalog_watermark_)) {
    DetachSiblings(mountpoint);
  }

  return attached_catalog;
}


/**
 * Load a catalog file as a freestanding Catalog object.
 * Loading of catalogs is implemented by derived classes.
 */
template <class CatalogT>
CatalogT *AbstractCatalogManager<CatalogT>::LoadFreeCatalog(
                                            const PathString     &mountpoint,
                                            const shash::Any     &hash)
{
  string new_path;
  shash::Any check_hash;
  const LoadError load_error = LoadCatalog(mountpoint, hash, &new_path,
                                           &check_hash);
  if (load_error != kLoadNew)
    return NULL;
  assert(hash == check_hash);
  CatalogT *catalog = CatalogT::AttachFreely(mountpoint.ToString(),
                                             new_path, hash);
  catalog->TakeDatabaseFileOwnership();
  return catalog;
}


/**
 * Attaches a newly created catalog.
 * @param db_path the file on a local file system containing the database
 * @param new_catalog the catalog to attach to this CatalogManager
 * @return true on success, false otherwise
 */
template <class CatalogT>
bool AbstractCatalogManager<CatalogT>::AttachCatalog(const string &db_path,
                                           CatalogT *new_catalog)
{
  LogCvmfs(kLogCatalog, kLogDebug, "attaching catalog file %s",
           db_path.c_str());

  // Initialize the new catalog
  if (!new_catalog->OpenDatabase(db_path)) {
    LogCvmfs(kLogCatalog, kLogDebug, "initialization of catalog %s failed",
             db_path.c_str());
    return false;
  }

  // Determine the inode offset of this catalog
  uint64_t inode_chunk_size = new_catalog->max_row_id();
  InodeRange range = AcquireInodes(inode_chunk_size);
  new_catalog->set_inode_range(range);
  new_catalog->SetInodeAnnotation(inode_annotation_);
  new_catalog->SetOwnerMaps(&uid_map_, &gid_map_);

  // Add catalog to the manager
  if (!new_catalog->IsInitialized()) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "catalog initialization failed (obscure data)");
    inode_gauge_ -= inode_chunk_size;
    return false;
  }
  CheckInodeWatermark();

  // The revision of the catalog tree is given by the root catalog revision
  if (catalogs_.empty()) {
    revision_cache_ = new_catalog->GetRevision();
    has_authz_cache_ = new_catalog->GetVOMSAuthz(&authz_cache_);
    volatile_flag_ = new_catalog->volatile_flag();
  }

  catalogs_.push_back(new_catalog);
  ActivateCatalog(new_catalog);
  return true;
}


/**
 * Removes a catalog from this CatalogManager, the catalog pointer is
 * freed if the call succeeds.
 * This method can create dangling children if a catalog in the middle of
 * a tree is removed.
 * @param catalog the catalog to detach
 * @return true on success, false otherwise
 */
template <class CatalogT>
void AbstractCatalogManager<CatalogT>::DetachCatalog(CatalogT *catalog) {
  if (catalog->HasParent())
    catalog->parent()->RemoveChild(catalog);

  ReleaseInodes(catalog->inode_range());
  UnloadCatalog(catalog);

  // Delete catalog from internal lists
  typename CatalogList::iterator i;
  typename CatalogList::const_iterator iend;
  for (i = catalogs_.begin(), iend = catalogs_.end(); i != iend; ++i) {
    if (*i == catalog) {
      catalogs_.erase(i);
      delete catalog;
      return;
    }
  }

  assert(false);
}


/**
 * Removes a catalog (and all of it's children) from this CatalogManager.
 * The given catalog and all children are freed, if this call succeeds.
 * @param catalog the catalog to detach
 * @return true on success, false otherwise
 */
template <class CatalogT>
void AbstractCatalogManager<CatalogT>::DetachSubtree(CatalogT *catalog) {
  // Detach all child catalogs recursively
  typename CatalogList::const_iterator i;
  typename CatalogList::const_iterator iend;
  CatalogList catalogs_to_detach = catalog->GetChildren();
  for (i = catalogs_to_detach.begin(), iend = catalogs_to_detach.end();
       i != iend; ++i)
  {
    DetachSubtree(*i);
  }

  DetachCatalog(catalog);
}


/**
 * Detaches all nested catalogs that are not on a prefix of the given tree.
 * Used when the catalog_watermark_ is surpassed.
 */
template <class CatalogT>
void AbstractCatalogManager<CatalogT>::DetachSiblings(
  const PathString &current_tree)
{
  bool again;
  do {
    again = false;
    unsigned N = catalogs_.size();
    for (unsigned i = 0; i < N; ++i) {
      if (!HasPrefix(current_tree.ToString(),
                     catalogs_[i]->mountpoint().ToString(),
                     false /* ignore_case */))
      {
        DetachSubtree(catalogs_[i]);
        again = true;
        break;
      }
    }
  } while (again);
  perf::Inc(statistics_.n_detach_siblings);
}


/**
 * Formats the catalog hierarchy
 */
template <class CatalogT>
string AbstractCatalogManager<CatalogT>::PrintHierarchyRecursively(
                                                      const CatalogT *catalog,
                                                      const int level) const
{
  string output;

  // Indent according to level
  for (int i = 0; i < level; ++i)
    output += "    ";

  output += "-> " + string(catalog->mountpoint().GetChars(),
                           catalog->mountpoint().GetLength())
            + "\n";

  CatalogList children = catalog->GetChildren();
  typename CatalogList::const_iterator i = children.begin();
  typename CatalogList::const_iterator iend = children.end();
  for (; i != iend; ++i) {
    output += PrintHierarchyRecursively(*i, level + 1);
  }

  return output;
}


template <class CatalogT>
std::string AbstractCatalogManager<CatalogT>::PrintMemStatsRecursively(
  const CatalogT *catalog) const
{
  string result = catalog->PrintMemStatistics() + "\n";

  CatalogList children = catalog->GetChildren();
  typename CatalogList::const_iterator i = children.begin();
  typename CatalogList::const_iterator iend = children.end();
  for (; i != iend; ++i) {
    result += PrintMemStatsRecursively(*i);
  }
  return result;
}


/**
 * Statistics from all catalogs
 */
template <class CatalogT>
std::string AbstractCatalogManager<CatalogT>::PrintAllMemStatistics() const {
  string result;
  ReadLock();
  result = PrintMemStatsRecursively(GetRootCatalog());
  Unlock();
  return result;
}


template <class CatalogT>
void AbstractCatalogManager<CatalogT>::EnforceSqliteMemLimit() {
  char *mem_enforced =
    static_cast<char *>(pthread_getspecific(pkey_sqlitemem_));
  if (mem_enforced == NULL) {
    sqlite3_soft_heap_limit(kSqliteMemPerThread);
    pthread_setspecific(pkey_sqlitemem_, reinterpret_cast<char *>(1));
  }
}

}  // namespace catalog




#endif  // CVMFS_CATALOG_MGR_IMPL_H_
