/**
 * This file is part of the CernVM File System
 */

#define __STDC_FORMAT_MACROS

#include "catalog_mgr.h"

#include <inttypes.h>
#include <cassert>

#include "logging.h"
#include "smalloc.h"
#include "shortstring.h"

using namespace std;  // NOLINT

namespace catalog {

InodeGenerationAnnotation::InodeGenerationAnnotation(const unsigned inode_width) 
{
  generation_annotation_ = 0;
  inode_width_ = inode_width;
  switch (inode_width_) {
    case 32:
      num_protected_bits_ = 26;
      break;
    case 64:
      num_protected_bits_ = 32;
      break;
    default:
      abort();
  }
}


void InodeGenerationAnnotation::SetGeneration(const uint64_t new_generation) 
{
  LogCvmfs(kLogCatalog, kLogDebug, "new inode generation: %"PRIu64, 
           new_generation);
  
  const unsigned generation_width = inode_width_ - num_protected_bits_;
  const uint64_t generation_max = uint64_t(1) << generation_width;
  
  const uint64_t generation_cut = new_generation % generation_max;
  generation_annotation_ = generation_cut << num_protected_bits_;
}
  
  
void InodeGenerationAnnotation::CheckForOverflow( 
  const uint64_t new_generation, 
  const uint64_t initial_generation,
  uint32_t *overflow_counter)
{
  const unsigned generation_width = inode_width_ - num_protected_bits_;
  const uint64_t generation_max = uint64_t(1) << generation_width;
  const uint64_t generation_span = new_generation - initial_generation;
  if ((generation_span >= generation_max) &&
      ((generation_span / generation_max) > *overflow_counter)) 
  {
    *overflow_counter = generation_span / generation_max;
    LogCvmfs(kLogCatalog, kLogSyslog, "inode generation overflow");
  }  
}


AbstractCatalogManager::AbstractCatalogManager() {
  inode_gauge_ = AbstractCatalogManager::kInodeOffset;
  revision_cache_ = 0;
  inode_annotation_ = NULL;
  incarnation_ = 0;
  rwlock_ =
    reinterpret_cast<pthread_rwlock_t *>(smalloc(sizeof(pthread_rwlock_t)));
  int retval = pthread_rwlock_init(rwlock_, NULL);
  assert(retval == 0);
  retval = pthread_key_create(&pkey_sqlitemem_, NULL);
  assert(retval == 0);
  remount_listener_ = NULL;
}


AbstractCatalogManager::~AbstractCatalogManager() {
  DetachAll();
  pthread_key_delete(pkey_sqlitemem_);
  pthread_rwlock_destroy(rwlock_);
  free(rwlock_);
}


void AbstractCatalogManager::SetInodeAnnotation(InodeAnnotation *new_annotation)
{
  assert(catalogs_.empty() || (new_annotation == inode_annotation_));
  inode_annotation_ = new_annotation;
}


/**
 * Initializes the CatalogManager and loads and attaches the root entry.
 * @return true on successful init, otherwise false
 */
bool AbstractCatalogManager::Init() {
  LogCvmfs(kLogCatalog, kLogDebug, "Initialize catalog");
  WriteLock();
  bool attached = MountCatalog(PathString("", 0), hash::Any(), NULL);
  Unlock();

  if (!attached) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to initialize root catalog");
  }

  if (attached && inode_annotation_) {
    inode_annotation_->SetGeneration(revision_cache_ + incarnation_);
  }

  return attached;
}
  
  
void AbstractCatalogManager::SetIncarnation(const uint64_t new_incarnation) { 
  WriteLock();
  incarnation_ = new_incarnation;
  if (inode_annotation_)
    inode_annotation_->SetGeneration(revision_cache_ + incarnation_);
  for (CatalogList::const_iterator i = catalogs_.begin(),
       iEnd = catalogs_.end(); i != iEnd; ++i)
  {
    (*i)->generation_ = revision_cache_ + incarnation_;
  }
  Unlock();
}


/**
 * Remounts the root catalog if necessary.  If a newer root catalog exists,
 * it is mounted and replaces the currently mounted tree (all existing catalogs
 * are detached)
 */
LoadError AbstractCatalogManager::Remount(const bool dry_run) {
  LogCvmfs(kLogCatalog, kLogDebug,
           "remounting repositories (dry run %d)", dry_run);
  if (dry_run)
    return LoadCatalog(PathString("", 0), hash::Any(), NULL);

  WriteLock();
  if (remount_listener_)
    remount_listener_->BeforeRemount(this);
  
  string catalog_path;
  const LoadError load_error = LoadCatalog(PathString("", 0), hash::Any(),
                                           &catalog_path);
  if (load_error == kLoadNew) {
    DetachAll();
    inode_gauge_ = AbstractCatalogManager::kInodeOffset;

    Catalog *new_root = CreateCatalog(PathString("", 0), NULL);
    assert(new_root);
    bool retval = AttachCatalog(catalog_path, new_root);
    assert(retval);

    if (inode_annotation_) {
      inode_annotation_->SetGeneration(revision_cache_ + incarnation_);
    }
  }
  Unlock();

  return load_error;
}


/**
 * Perform a lookup for a specific DirectoryEntry in the catalogs.
 * @param inode the inode to find in the catalogs
 * @param options whether to perform another lookup to get the parent entry, too
 * @param dirent the resulting DirectoryEntry
 * @return true if lookup succeeded otherwise false
 */
bool AbstractCatalogManager::LookupInode(const inode_t inode,
                                         const LookupOptions options,
                                         DirectoryEntry *dirent)
{
  EnforceSqliteMemLimit();
  ReadLock();
  bool found = false;
  
  // Don't lookup ancient inodes
  if (inode_annotation_ && !inode_annotation_->ValidInode(inode)) {
    Unlock();
    return false;
  }

  // Get corresponding catalog
  Catalog *catalog = NULL;
  const inode_t raw_inode =
    inode_annotation_ ? inode_annotation_->Strip(inode) : inode;
  for (CatalogList::const_iterator i = catalogs_.begin(),
       iEnd = catalogs_.end(); i != iEnd; ++i)
  {
    if ((*i)->inode_range().ContainsInode(raw_inode)) {
      catalog = *i;
      break;
    }
  }
  if (catalog == NULL) {
    LogCvmfs(kLogCatalog, kLogDebug, "cannot find catalog for inode %"PRIu64" "
             "(raw inode: %"PRIu64")", inode, raw_inode);
    goto lookup_inode_fini;
  }

  if ((options == kLookupSole) || (inode == GetRootInode())) {
    atomic_inc64(&statistics_.num_lookup_inode);
    found = catalog->LookupInode(inode, dirent, NULL);
    goto lookup_inode_fini;
  } else {
    atomic_inc64(&statistics_.num_lookup_inode);
    // Lookup including parent entry
    hash::Md5 parent_md5path;
    DirectoryEntry parent;
    bool found_parent = false;

    found = catalog->LookupInode(inode, dirent, &parent_md5path);
    if (!found)
      goto lookup_inode_fini;

    // Parent is possibly in the parent catalog
    atomic_inc64(&statistics_.num_lookup_path);
    if (dirent->IsNestedCatalogRoot() && !catalog->IsRoot()) {
      Catalog *parent_catalog = catalog->parent();
      found_parent = parent_catalog->LookupMd5Path(parent_md5path, &parent);
    } else {
      found_parent = catalog->LookupMd5Path(parent_md5path, &parent);
    }

    // If there is no parent entry, it might be data corruption
    if (!found_parent) {
      LogCvmfs(kLogCatalog, kLogDebug | kLogSyslog,
               "cannot find parent entry for inode %"PRIu64" --> data corrupt?",
               inode);
      found = false;
    } else {
      dirent->set_parent_inode(parent.inode());
      found = true;
    }
  }

 lookup_inode_fini:
  Unlock();
  return found;
}


/**
 * Perform a lookup for a specific DirectoryEntry in the catalogs.
 * @param path the path to find in the catalogs
 * @param options whether to perform another lookup to get the parent entry, too
 * @param dirent the resulting DirectoryEntry
 * @return true if lookup succeeded otherwise false
 */
bool AbstractCatalogManager::LookupPath(const PathString &path,
                                        const LookupOptions options,
                                        DirectoryEntry *dirent)
{
  EnforceSqliteMemLimit();
  ReadLock();

  Catalog *best_fit = FindCatalog(path);
  assert(best_fit != NULL);

  atomic_inc64(&statistics_.num_lookup_path);
  LogCvmfs(kLogCatalog, kLogDebug, "looking up '%s' in catalog: '%s'",
           path.c_str(), best_fit->path().c_str());
  bool found = best_fit->LookupPath(path, dirent);

  // Possibly in a nested catalog
  if (!found && MountSubtree(path, best_fit, NULL)) {
    LogCvmfs(kLogCatalog, kLogDebug, "looking up '%s' in a nested catalog",
             path.c_str());
    Unlock();
    WriteLock();
    // Check again to avoid race
    best_fit = FindCatalog(path);
    assert(best_fit != NULL);
    atomic_inc64(&statistics_.num_lookup_path);
    found = best_fit->LookupPath(path, dirent);

    if (found) {
      // DowngradeLock(); TODO
    } else {
      LogCvmfs(kLogCatalog, kLogDebug,
               "entry not found, we may have to load nested catalogs");

      Catalog *nested_catalog;
      found = MountSubtree(path, best_fit, &nested_catalog);
      // DowngradeLock(); TODO

      if (!found) {
        LogCvmfs(kLogCatalog, kLogDebug,
                 "failed to load nested catalog for '%s'", path.c_str());
        goto lookup_path_notfound;
      }

      if (nested_catalog != best_fit) {
        atomic_inc64(&statistics_.num_lookup_path);
        found = nested_catalog->LookupPath(path, dirent);
        if (!found) {
          LogCvmfs(kLogCatalog, kLogDebug,
                   "nested catalogs loaded but entry '%s' was still not found",
                   path.c_str());
          goto lookup_path_notfound;
        } else {
          best_fit = nested_catalog;
        }
      } else {
        LogCvmfs(kLogCatalog, kLogDebug, "no nested catalog fits");
        goto lookup_path_notfound;
      }
    }
    assert(found);
  }
  // Not in a nested catalog, ENOENT
  if (!found) {
    LogCvmfs(kLogCatalog, kLogDebug, "ENOENT: %s", path.c_str());
    goto lookup_path_notfound;
  }

  LogCvmfs(kLogCatalog, kLogDebug, "found entry %s in catalog %s",
           path.c_str(), best_fit->path().c_str());

  // Look for parent entry
  if (options == kLookupFull) {
    DirectoryEntry parent;
    PathString parent_path = GetParentPath(path);
    if (dirent->IsNestedCatalogRoot()) {
      if (best_fit->parent())
        found = best_fit->parent()->LookupPath(parent_path, &parent);
      else
        found = false;
    } else {
      found = best_fit->LookupPath(parent_path, &parent);
    }
    if (!found) {
      LogCvmfs(kLogCatalog, kLogDebug | kLogSyslog,
               "cannot find parent '%s' for entry '%s' --> data corrupt?",
               parent_path.c_str(), path.c_str());
      goto lookup_path_notfound;
    }
    dirent->set_parent_inode(parent.inode());
  }

  Unlock();
  return true;

 lookup_path_notfound:
  Unlock();
  atomic_inc64(&statistics_.num_lookup_path_negative);
  return false;
}
  
  
/**
 * Don't use.  Only for the CwdBuffer.
 */
bool AbstractCatalogManager::Path2InodeUnprotected(const PathString &path, 
                                                   inode_t *inode)
{
  EnforceSqliteMemLimit();
  
  Catalog *best_fit = FindCatalog(path);
  assert(best_fit != NULL);
  
  atomic_inc64(&statistics_.num_lookup_path);
  LogCvmfs(kLogCatalog, kLogDebug, "inode lookup for '%s' in catalog: '%s'",
           path.c_str(), best_fit->path().c_str());
  DirectoryEntry dirent;
  bool found = best_fit->LookupPath(path, &dirent);
  
  if (!found) {
    LogCvmfs(kLogCatalog, kLogDebug, "ENOENT: %s", path.c_str());
    atomic_inc64(&statistics_.num_lookup_path_negative);
    return false;
  }
  
  LogCvmfs(kLogCatalog, kLogDebug, "found entry %s in catalog %s",
           path.c_str(), best_fit->path().c_str());
  *inode = dirent.inode();
  return true;
}


/**
 * Do a listing of the specified directory.
 * @param path the path of the directory to list
 * @param listing the resulting DirectoryEntryList
 * @return true if listing succeeded otherwise false
 */
bool AbstractCatalogManager::Listing(const PathString &path,
                                     DirectoryEntryList *listing)
{
  EnforceSqliteMemLimit();
  bool result;
  ReadLock();

  // Find catalog, possibly load nested
  Catalog *best_fit = FindCatalog(path);
  Catalog *catalog = best_fit;
  if (MountSubtree(path, best_fit, NULL)) {
    Unlock();
    WriteLock();
    // Check again to avoid race
    best_fit = FindCatalog(path);
    result = MountSubtree(path, best_fit, &catalog);
    // DowngradeLock(); TODO
    if (!result) {
      Unlock();
      return false;
    }
  }

  atomic_inc64(&statistics_.num_listing);
  result = catalog->ListingPath(path, listing);

  Unlock();
  return result;
}


/**
 * Do a listing of the specified directory, return only struct stat values.
 * @param path the path of the directory to list
 * @param listing the resulting StatEntryList
 * @return true if listing succeeded otherwise false
 */
bool AbstractCatalogManager::ListingStat(const PathString &path,
                                        StatEntryList *listing)
{
  EnforceSqliteMemLimit();
  bool result;
  ReadLock();

  // Find catalog, possibly load nested
  Catalog *best_fit = FindCatalog(path);
  Catalog *catalog = best_fit;
  if (MountSubtree(path, best_fit, NULL)) {
    Unlock();
    WriteLock();
    // Check again to avoid race
    best_fit = FindCatalog(path);
    result = MountSubtree(path, best_fit, &catalog);
    // DowngradeLock(); TODO
    if (!result) {
      Unlock();
      return false;
    }
  }

  atomic_inc64(&statistics_.num_listing);
  result = catalog->ListingPathStat(path, listing);

  Unlock();
  return result;
}


uint64_t AbstractCatalogManager::GetRevision() const {
  ReadLock();
  const uint64_t revision = revision_cache_;
  Unlock();
  return revision;
}


uint64_t AbstractCatalogManager::GetTTL() const {
  ReadLock();
  const uint64_t revision = GetRootCatalog()->GetTTL();
  Unlock();
  return revision;
}


int AbstractCatalogManager::GetNumCatalogs() const {
  ReadLock();
  int result = catalogs_.size();
  Unlock();
  return result;
}


/**
 * Gets a formatted tree of the currently attached catalogs
 */
string AbstractCatalogManager::PrintHierarchy() const {
  ReadLock();
  const string output = PrintHierarchyRecursively(GetRootCatalog(), 0);
  Unlock();
  return output;
}


/**
 * Assigns the next free numbers in the 64 bit space
 * TODO: this may run out of free inodes at some point (with 32bit at least)
 */
InodeRange AbstractCatalogManager::AcquireInodes(uint64_t size) {
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
void AbstractCatalogManager::ReleaseInodes(const InodeRange chunk) {
  // TODO currently inodes are only released on remount
}


/**
 * Find the catalog leaf in the tree that fits the path.
 * The path might be served by a not yet loaded nested catalog.
 * @param path the path a catalog is searched for
 * @return the catalog which is best fitting at the given path
 */
Catalog* AbstractCatalogManager::FindCatalog(const PathString &path) const {
  assert (catalogs_.size() > 0);

  // Start at the root catalog and successive go down the catalog tree
  Catalog *best_fit = GetRootCatalog();
  Catalog *next_fit = NULL;
  while (best_fit->path() != path) {
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
bool AbstractCatalogManager::IsAttached(const PathString &root_path,
                                        Catalog **attached_catalog) const
{
  if (catalogs_.size() == 0)
    return false;

  Catalog *best_fit = FindCatalog(root_path);
  if (best_fit->path() != root_path)
    return false;

  if (attached_catalog != NULL) *attached_catalog = best_fit;
  return true;
}


/**
 * Recursively mounts all nested catalogs required to serve a path.
 * If leaf_catalog is NULL, just indicate if it is necessary to load a
 * nested catalog for the given path.
 * The final leaf nested catalog is returned.
 */
bool AbstractCatalogManager::MountSubtree(const PathString &path,
                                          const Catalog *entry_point,
                                          Catalog **leaf_catalog)
{
  bool result = true;
  Catalog *parent = (entry_point == NULL) ?
                    GetRootCatalog() : const_cast<Catalog *>(entry_point);
  assert(path.StartsWith(parent->path()));

  // Try to find path as a super string of nested catalog mount points
  PathString path_slash(path);
  path_slash.Append("/", 1);
  atomic_inc64(&statistics_.num_nested_listing);
  const Catalog::NestedCatalogList *nested_catalogs =
    parent->ListNestedCatalogs();
  for (Catalog::NestedCatalogList::const_iterator i = nested_catalogs->begin(),
       iEnd = nested_catalogs->end(); i != iEnd; ++i)
  {
    // Next nesting level
    PathString nested_path_slash(i->path);
    nested_path_slash.Append("/", 1);
    if (path_slash.StartsWith(nested_path_slash)) {
      if (leaf_catalog == NULL)
        return true;
      Catalog *new_nested;
      LogCvmfs(kLogCatalog, kLogDebug, "load nested catalog at %s",
               i->path.c_str());
      // prevent endless recursion with corrupted catalogs
      // (due to reloading root)
      if (i->hash.IsNull())
        return false;
      new_nested = MountCatalog(i->path, i->hash, parent);
      if (!new_nested)
        return false;

      result = MountSubtree(path, new_nested, &parent);
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
Catalog *AbstractCatalogManager::MountCatalog(const PathString &mountpoint,
                                              const hash::Any &hash,
                                              Catalog *parent_catalog)
{
  Catalog *attached_catalog = NULL;
  if (IsAttached(mountpoint, &attached_catalog))
    return attached_catalog;

  string catalog_path;
  const LoadError retval = LoadCatalog(mountpoint, hash, &catalog_path);
  if ((retval == kLoadFail) || (retval == kLoadNoSpace)) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to load catalog '%s' (%d)",
             mountpoint.c_str(), retval);
    return NULL;
  }

  attached_catalog = CreateCatalog(mountpoint, parent_catalog);

  // Attach loaded catalog
  if (!AttachCatalog(catalog_path, attached_catalog)) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to attach catalog '%s'",
             mountpoint.c_str());
    UnloadCatalog(attached_catalog);
    return NULL;
  }

  return attached_catalog;
}


/**
 * Attaches a newly created catalog.
 * @param db_path the file on a local file system containing the database
 * @param new_catalog the catalog to attach to this CatalogManager
 * @return true on success, false otherwise
 */
bool AbstractCatalogManager::AttachCatalog(const string &db_path,
                                           Catalog *new_catalog)
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

  // Add catalog to the manager
  if (!new_catalog->IsInitialized()) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "catalog initialization failed (obscure data)");
    inode_gauge_ -= inode_chunk_size;
    return false;
  }

  // The revision of the catalog tree is given by the root catalog revision
  if (catalogs_.empty())
    revision_cache_ = new_catalog->GetRevision();
  new_catalog->generation_ = revision_cache_ + incarnation_;
  
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
void AbstractCatalogManager::DetachCatalog(Catalog *catalog) {
  if (!catalog->IsRoot())
    catalog->parent()->RemoveChild(catalog);

  ReleaseInodes(catalog->inode_range());
  UnloadCatalog(catalog);

  // Delete catalog from internal lists
  CatalogList::iterator i;
  CatalogList::const_iterator iend;
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
void AbstractCatalogManager::DetachSubtree(Catalog *catalog) {
  // Detach all child catalogs recursively
  CatalogList::const_iterator i;
  CatalogList::const_iterator iend;
  CatalogList catalogs_to_detach = catalog->GetChildren();
  for (i = catalogs_to_detach.begin(), iend = catalogs_to_detach.end();
       i != iend; ++i)
  {
    DetachSubtree(*i);
  }

  DetachCatalog(catalog);
}


/**
 * Formats the catalog hierarchy
 */
string AbstractCatalogManager::PrintHierarchyRecursively(const Catalog *catalog,
                                                         const int level) const
{
  string output;

  // Indent according to level
  for (int i = 0; i < level; ++i)
    output += "    ";

  output += "-> " +
    string(catalog->path().GetChars(), catalog->path().GetLength()) + "\n";

  CatalogList children = catalog->GetChildren();
  CatalogList::const_iterator i,iend;
  for (CatalogList::const_iterator i = children.begin(), iEnd = children.end();
       i != iEnd; ++i)
  {
    output += PrintHierarchyRecursively(*i, level + 1);
  }

  return output;
}


void AbstractCatalogManager::EnforceSqliteMemLimit() {
  char *mem_enforced =
    static_cast<char *>(pthread_getspecific(pkey_sqlitemem_));
  if (mem_enforced == NULL) {
    sqlite3_soft_heap_limit(kSqliteMemPerThread);
    pthread_setspecific(pkey_sqlitemem_, (char *)(1));
  }
}

}
