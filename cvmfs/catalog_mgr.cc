/**
 * This file is part of the CernVM File System
 */

#include "catalog_mgr.h"
#include <iostream>
#include "logging.h"

using namespace std;  // NOLINT

namespace catalog {

AbstractCatalogManager::AbstractCatalogManager() {
  current_inode_offset_ = AbstractCatalogManager::kInitialInodeOffset;
}


AbstractCatalogManager::~AbstractCatalogManager() {
  DetachAllCatalogs();
}


/**
 * Initializes the CatalogManager and loads and attaches the root entry.
 * @return true on successful init, otherwise false
 */
bool AbstractCatalogManager::Init() {
  LogCvmfs(kLogCatalog, kLogDebug, "Initialize catalog");
  WriteLock();
  bool attached = MountCatalog("", hash::Any(), NULL);
  Unlock();

  if (!attached) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to initialize root catalog");
  }

  return attached;
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
                                         DirectoryEntry *dirent) const
{
  ReadLock();
  bool found = false;

  // Get corresponding catalog
  Catalog *catalog = NULL;
  for (CatalogList::const_iterator i = catalogs_.begin(),
       iEnd = catalogs_.end(); i != iEnd; ++i)
  {
    if ((*i)->inode_range().ContainsInode(inode)) {
      catalog = *i;
      break;
    }
  }
  if (catalog == NULL) {
    LogCvmfs(kLogCatalog, kLogDebug, "cannot find catalog for inode %d", inode);
    goto lookup_inode_fini;
  }

  if ((options == kLookupSole) || (inode == GetRootInode())) {
    found = catalog->LookupInode(inode, dirent, NULL);
    goto lookup_inode_fini;
  } else {
    // Lookup including parent entry
    hash::Md5 parent_md5path;
    DirectoryEntry parent;
    bool found_parent = false;

    found = catalog->LookupInode(inode, dirent, &parent_md5path);
    if (!found)
      goto lookup_inode_fini;

    // Parent is possibly in the parent catalog
    if (dirent->IsNestedCatalogRoot() && !catalog->IsRoot()) {
      Catalog *parent_catalog = catalog->parent();
      found_parent = parent_catalog->LookupMd5Path(parent_md5path, &parent);
    } else {
      found_parent = catalog->LookupMd5Path(parent_md5path, &parent);
    }

    // If there is no parent entry, it might be data corruption
    if (!found_parent) {
      LogCvmfs(kLogCatalog, kLogDebug | kLogSyslog,
               "cannot find parent entry for inode %d --> data corrupt?",
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
bool AbstractCatalogManager::LookupPath(const string &path,
                                        const LookupOptions options,
                                        DirectoryEntry *dirent)
{
  ReadLock();

  Catalog *best_fit = FindCatalog(path);
  assert(best_fit != NULL);

  LogCvmfs(kLogCatalog, kLogDebug, "looking up '%s' in catalog: '%s'",
           path.c_str(), best_fit->path().c_str());
  bool found = best_fit->LookupPath(path, dirent);

  // Possibly in a nested catalog
  if (!found) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "entry not found, we may have to load nested catalogs");

    Catalog *nested_catalog;
    UpgradeLock();
    found = MountSubtree(path, best_fit, &nested_catalog);
    DowngradeLock();

    if (!found) {
      LogCvmfs(kLogCatalog, kLogDebug,
               "failed to load nested catalog for '%s'", path.c_str());
      goto lookup_path_notfound;
    }

    if (nested_catalog != best_fit) {
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
  LogCvmfs(kLogCatalog, kLogDebug, "found entry %s in catalog %s",
           path.c_str(), best_fit->path().c_str());

  // Look for parent entry
  if (options == kLookupFull) {
    string parent_path = GetParentPath(path);
    DirectoryEntry parent;
    found = LookupPath(parent_path, kLookupSole, &parent);
    if (!found) {
      LogCvmfs(kLogCatalog, kLogDebug | kLogSyslog,
               "cannot find parent '%s' for entry '%s' --> data corrupt?",
               parent_path.c_str(), path.c_str());
    } else {
      dirent->set_parent_inode(parent.inode());
    }
  }

  Unlock();
  return true;

 lookup_path_notfound:
  Unlock();
  return false;
}


/**
 * Do a listing of the specified directory.
 * @param path the path of the directory to list
 * @param listing the resulting DirectoryEntryList
 * @return true if listing succeeded otherwise false
 */
bool AbstractCatalogManager::Listing(const string &path,
                                     DirectoryEntryList *listing)
{
  bool result;
  ReadLock();

  // Find catalog, possibly load nested
  Catalog *best_fit = FindCatalog(path);
  Catalog *catalog;
  UpgradeLock();
  result = MountSubtree(path, best_fit, &catalog);
  DowngradeLock();
  if (!result) {
    Unlock();
    return false;
  }

  result = catalog->ListingPath(path, listing);

  Unlock();
  return result;
}


uint64_t AbstractCatalogManager::GetRevision() const {
  ReadLock();
  const uint64_t revision = GetRootCatalog()->GetRevision();
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
 *  currently this is a very simple approach
 *  just assign the next free numbers in this 64 bit space
 *  TODO: think about other allocation methods, this may run out
 *        of free inodes some time (in the late future admittedly)
 */
InodeRange AbstractCatalogManager::GetInodeChunkOfSize(uint64_t size) {
  InodeRange result;
  result.offset = current_inode_offset_;
  result.size = size;

  current_inode_offset_ = current_inode_offset_ + size;
  LogCvmfs(kLogCatalog, kLogDebug, "allocating inodes from %d to %d.",
           result.offset + 1, current_inode_offset_);

  return result;
}

void AbstractCatalogManager::AnnounceInvalidInodeChunk(const InodeRange chunk)
  const
{
  // TODO: actually do something here
}


/**
 * Find the catalog leaf in the tree that fits the path.
 * The path might be served by a not yet loaded nested catalog.
 * @param path the path a catalog is searched for
 * @return the catalog which is best fitting at the given path
 */
Catalog* AbstractCatalogManager::FindCatalog(const string &path) const {
  assert (GetNumCatalogs() > 0);

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

bool AbstractCatalogManager::IsCatalogAttached(const string &root_path,
                                               Catalog **attached_catalog) const
{
  if (GetNumCatalogs() == 0) {
    return false;
  }

  // look through the attached catalogs to find the searched one
  Catalog *best_fit = FindCatalog(root_path);

  // not found... too bad
  if (best_fit->path() != root_path) {
    return false;
  }

  // found... yeepie!
  if (NULL != attached_catalog) *attached_catalog = best_fit;
  return true;
}


/**
 * Recursively mounts all nested catalogs required to serve a path.
 * The final leaf nested catalog is returned.
 */
bool AbstractCatalogManager::MountSubtree(const string &path,
                                          const Catalog *entry_point,
                                          Catalog **leaf_catalog)
{
  bool result = true;
  Catalog *parent = (entry_point == NULL) ?
                    GetRootCatalog() : const_cast<Catalog *>(entry_point);
  assert(path.find(parent->path()) == 0);

  // Try to find path as a super string of nested catalog mount points
  const Catalog::NestedCatalogList nested_catalogs =
    parent->ListNestedCatalogs();
  for (Catalog::NestedCatalogList::const_iterator i = nested_catalogs.begin(),
       iEnd = nested_catalogs.end(); i != iEnd; ++i)
  {
    // Next nesting level
    if ((path + "/").find(i->path + "/") == 0) {
      Catalog *new_nested;
      LogCvmfs(kLogCatalog, kLogDebug, "load nested catalog at %s",
               i->path.c_str());
      new_nested = MountCatalog(i->path, i->hash, parent);
      if (!new_nested)
        return false;

      result = MountSubtree(path, new_nested, &parent);
    }
  }

  *leaf_catalog = parent;
  return result;
}


/**
 * Load a catalog file and attach it to the tree of Catalog objects.
 * Loading of catalogs is implemented by derived classes.
 */
Catalog *AbstractCatalogManager::MountCatalog(const string &mountpoint,
                                              const hash::Any &hash,
                                              Catalog *parent_catalog)
{
  Catalog *attached_catalog = NULL;
  if (IsCatalogAttached(mountpoint, &attached_catalog))
    return attached_catalog;

  string catalog_path;
  const LoadError retval = LoadCatalog(mountpoint, hash, &catalog_path);
  if ((retval == kLoadFail) || (retval == kLoadNoSpace)) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to load catalog '%s' (%d)",
             mountpoint.c_str(), retval);
    return NULL;
  }

  attached_catalog = CreateCatalog(mountpoint, parent_catalog);

  // Attach loaded catalog (end of virtual behavior)
  if (!AttachCatalog(catalog_path, attached_catalog, false)) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to attach catalog '%s'",
             mountpoint.c_str());
    DetachCatalog(attached_catalog);
    return NULL;
  }

  return attached_catalog;
}


bool AbstractCatalogManager::AttachCatalog(const std::string &db_file,
                                           Catalog *new_catalog,
                                           const bool open_transaction) {
  LogCvmfs(kLogCatalog, kLogDebug, "attaching catalog file %s",
           db_file.c_str());

  // initialize the new catalog
  if (not new_catalog->OpenDatabase(db_file)) {
    LogCvmfs(kLogCatalog, kLogDebug, "initialization of catalog %s failed",
             db_file.c_str());
    return false;
  }

  // determine the inode offset of this catalog
  uint64_t inode_chunk_size = new_catalog->max_row_id();
  InodeRange range = GetInodeChunkOfSize(inode_chunk_size);
  new_catalog->set_inode_range(range);

  // check if everything worked out
  if (not new_catalog->IsInitialized()) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "catalog initialization failed (obscure data)");
    return false;
  }

  // save the catalog
  catalogs_.push_back(new_catalog);
  return true;
}

bool AbstractCatalogManager::DetachCatalogTree(Catalog *catalog) {
  bool successful = true;

  // detach all child catalogs recursively
  CatalogList::const_iterator i;
  CatalogList::const_iterator iend;
  CatalogList catalogs_to_detach = catalog->GetChildren();
  for (i = catalogs_to_detach.begin(), iend = catalogs_to_detach.end();
       i != iend;
       ++i) {
    if (not DetachCatalogTree(*i)) {
      successful = false;
    }
  }

  // detach the catalog itself
  if (not DetachCatalog(catalog)) {
    successful = false;
  }

  return successful;
}

bool AbstractCatalogManager::DetachCatalog(Catalog *catalog) {
  // detach *catalog from catalog tree
  if (not catalog->IsRoot()) {
    catalog->parent()->RemoveChild(catalog);
  }

  AnnounceInvalidInodeChunk(catalog->inode_range());

  // delete catalog from internal lists
  CatalogList::iterator i;
  CatalogList::const_iterator iend;
  for (i = catalogs_.begin(), iend = catalogs_.end(); i != iend; ++i) {
    if (*i == catalog) {
      catalogs_.erase(i);
      delete catalog;
      return true;
    }
  }

  return false;
}

bool AbstractCatalogManager::LoadAndAttachCatalogsRecursively(Catalog *catalog) {
  bool successful = true;

  // go through all children of the given parent catalog and attach them
  Catalog::NestedCatalogList children = catalog->ListNestedCatalogs();
  Catalog::NestedCatalogList::const_iterator j,jend;
  for (j = children.begin(), jend = children.end();
       j != jend;
       ++j) {
 // TODO
 //   Catalog *new_catalog;
 //   if (not LoadAndAttachCatalog(j->path, catalog, &new_catalog) ||
 //       not LoadAndAttachCatalogsRecursively(new_catalog)) {
 //     successful = false;
 //   }
  }

  return successful;
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
 * Formats the catalog hierarchy
 */
string AbstractCatalogManager::PrintHierarchyRecursively(const Catalog *catalog,
                                                         const int level) const
{
  string output;

  // Indent according to level
  for (int i = 0; i < level; ++i)
    output += "    ";

  output += "'-> " + catalog->path() + "\n";

  CatalogList children = catalog->GetChildren();
  CatalogList::const_iterator i,iend;
  for (CatalogList::const_iterator i = children.begin(), iEnd = children.end();
       i != iEnd; ++i)
  {
    output += PrintHierarchyRecursively(*i, level + 1);
  }

  return output;
}

}
