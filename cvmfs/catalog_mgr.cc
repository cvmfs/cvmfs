/**
 * This file is part of the CernVM File System
 */

#include "catalog_mgr.h"

#include <iostream>
#include "logging.h"

using namespace std;  // NOLINT

namespace catalog {

CatalogManager::CatalogManager() {
  current_inode_offset_ = CatalogManager::kInitialInodeOffset;
}


CatalogManager::~CatalogManager() {
  DetachAllCatalogs();
}


/**
 * Initializes the CatalogManager and loads and attaches the root entry.
 * @return true on successful init, otherwise false
 */
bool CatalogManager::Init() {
  LogCvmfs(kLogCatalog, kLogDebug, "Initialize catalog");
  WriteLock();
  bool attached = LoadAndAttachCatalog("", NULL);
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
bool CatalogManager::LookupInode(const inode_t inode,
                                 const LookupOptions options,
                                 DirectoryEntry *dirent) const
{
  ReadLock();
  bool found = false;

  // Get corresponding catalog
  Catalog *catalog;
  const bool found_catalog = GetCatalogByInode(inode, &catalog);
  if (!found_catalog) {
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
bool CatalogManager::LookupPath(const string &path, const LookupOptions options,
                                DirectoryEntry *dirent)
{
  ReadLock();

  Catalog *best_fit = WalkTree(path);
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
bool CatalogManager::Listing(const string &path,
                             DirectoryEntryList *listing) {
  bool result;
  ReadLock();

  // Find catalog, possibly load nested
  Catalog *best_fit = WalkTree(path);
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



/**
 *  currently this is a very simple approach
 *  just assign the next free numbers in this 64 bit space
 *  TODO: think about other allocation methods, this may run out
 *        of free inodes some time (in the late future admittedly)
 */
InodeRange CatalogManager::GetInodeChunkOfSize(uint64_t size) {
  InodeRange result;
  result.offset = current_inode_offset_;
  result.size = size;

  current_inode_offset_ = current_inode_offset_ + size;
  LogCvmfs(kLogCatalog, kLogDebug, "allocating inodes from %d to %d.",
           result.offset + 1, current_inode_offset_);

  return result;
}

void CatalogManager::AnnounceInvalidInodeChunk(const InodeRange chunk) const {
  // TODO: actually do something here
}


bool CatalogManager::GetCatalogByPath(const string &path,
                                              const bool load_final_catalog,
                                              Catalog **catalog,
                                              DirectoryEntry *entry) {
  // find the best fitting loaded catalog for this path
  Catalog *best_fitting_catalog = WalkTree(path);
  assert (best_fitting_catalog != NULL);

  // path lookup in this catalog
  // (if the entry we are looking for was found, we're basically finished)
  LogCvmfs(kLogCatalog, kLogDebug, "looking up '%s' in catalog: '%s'",
           path.c_str(), best_fitting_catalog->path().c_str());
  DirectoryEntry d;
  bool entry_found = best_fitting_catalog->LookupPath(path, &d);

  // if we did not find the entry, there are two possible reasons:
  //    1. the entry in question resides in a not yet loaded nested catalog
  //    2. the entry does not exist at all
  if (not entry_found) {
     LogCvmfs(kLogCatalog, kLogDebug,
              "entry not found, we may have to load nested catalogs");

    // try to load the nested catalogs for this path
    Catalog *nested_catalog;
    UpgradeLock();
    entry_found = LoadNestedCatalogForPath(path,
                                           best_fitting_catalog,
                                           load_final_catalog,
                                           &nested_catalog);
    DowngradeLock();
    if (not entry_found) {
      LogCvmfs(kLogCatalog, kLogDebug,
               "nested catalog for '%s' could not be found", path.c_str());
      return false;

    // retry the lookup with the nested catalog
    } else {
      entry_found = nested_catalog->LookupPath(path, &d);
      if (not entry_found) {
        LogCvmfs(kLogCatalog, kLogDebug,
                 "nested catalogs loaded but entry '%s' was still not found",
                 path.c_str());
        return false;
      } else {
        best_fitting_catalog = nested_catalog;
      }
    }
  }

  // if the found entry is a nested catalog mount point we have to load it on request
  else if (load_final_catalog && d.IsNestedCatalogMountpoint()) {
    Catalog *new_catalog;
    UpgradeLock();
    bool attached_successfully = LoadAndAttachCatalog(path,
                                                      best_fitting_catalog,
                                                      &new_catalog);
    DowngradeLock();
    if (not attached_successfully) {
      return false;
    }
    best_fitting_catalog = new_catalog;
  }

  // all done, write back results and give a party
  LogCvmfs(kLogCatalog, kLogDebug, "found entry %s in catalog %s",
           path.c_str(), best_fitting_catalog->path().c_str());
  if (NULL != catalog) *catalog = best_fitting_catalog;
  if (NULL != entry)   *entry = d;
  return true;
}

bool CatalogManager::GetCatalogByInode(const uint64_t inode,
                                               Catalog **catalog) const {
  // TODO: replace this with a more clever algorithm
  //       maybe exploit the ordering in the vector
  CatalogList::const_iterator i,end;
  for (i = catalogs_.begin(), end = catalogs_.end(); i != end; ++i) {
    if ((*i)->inode_range().ContainsInode(inode)) {
      *catalog = *i;
      return true;
    }
  }

  // inode was not found... might be data corruption
  return false;
}


/**
 * Find the catalog leaf in the tree that fits the path.
 * The path might be served by a not yet loaded nested catalog.
 * @param path the path a catalog is searched for
 * @return the catalog which is best fitting at the given path
 */
Catalog* CatalogManager::WalkTree(const string &path) const {
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

bool CatalogManager::IsCatalogAttached(const string &root_path,
                                               Catalog **attached_catalog) const {
  if (GetNumCatalogs() == 0) {
    return false;
  }

  // look through the attached catalogs to find the searched one
  Catalog *best_fit = WalkTree(root_path);

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
bool CatalogManager::MountSubtree(const string &path,
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
      result = LoadAndAttachCatalog(i->path, parent, &new_nested);
      if (!result)
        return false;

      result = MountSubtree(path, new_nested, &parent);
    }
  }

  *leaf_catalog = parent;
  return result;
}


bool CatalogManager::LoadNestedCatalogForPath(const string &path,
                                                      const Catalog *entry_point,
                                                      const bool load_final_catalog,
                                                      Catalog **final_catalog) {
  std::vector<string> path_elements;
  string sub_path, relative_path;
  Catalog *containing_catalog = (entry_point == NULL) ? GetRootCatalog() : (Catalog *)entry_point;

  // do all processing relative to the entry_point catalog
  assert (path.find(containing_catalog->path()) == 0);
  relative_path = path.substr(containing_catalog->path().length() + 1); // +1 --> remove slash '/' from beginning relative path
  sub_path = containing_catalog->path();

  path_elements = SplitString(relative_path, '/');
  bool entry_found;
  DirectoryEntry entry;

  // step through the path and attach nested catalogs on the way
  // TODO: this might get faster by exploiting the 'nested catalog table'
  std::vector<string>::const_iterator i,end;
  for (i = path_elements.begin(), end = path_elements.end(); i != end; ++i) {
    sub_path += "/" + *i;

    entry_found = containing_catalog->LookupPath(sub_path, &entry);
    if (not entry_found) {
      return false;
    }

    if (entry.IsNestedCatalogMountpoint()) {
      // nested catalogs on the way are downloaded
      // if load_final_catalog is false we do not download a nested
      // catalog pointed to by the whole path
      if (sub_path.length() < path.length() || load_final_catalog) {
        Catalog *new_catalog;
        bool attached_successfully = LoadAndAttachCatalog(sub_path,
                                                          containing_catalog,
                                                          &new_catalog);
        if (not attached_successfully) {
          return false;
        }
        containing_catalog = new_catalog;
      }
    }
  }

  *final_catalog = containing_catalog;
  return true;
}

bool CatalogManager::LoadAndAttachCatalog(const string &mountpoint,
                                                  Catalog *parent_catalog,
                                                  Catalog **attached_catalog) {
  // check if catalog is already attached
  if (IsCatalogAttached(mountpoint, attached_catalog)) {
    return true;
  }

  // this process depends on the derived class, because
  // loading a concrete type of catalog depend on the
  // acctual implementation of this abstract class.

  // load catalog file (LoadCatalogFile is virtual)
  string new_catalog_file;
  if (0 != LoadCatalogFile(mountpoint, hash::Md5(hash::AsciiPtr(mountpoint)), &new_catalog_file)) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to load catalog '%s'",
             mountpoint.c_str());
    return false;
  }

  // create a catalog (CreateCatalogStub is virtual)
  Catalog *new_catalog = CreateCatalogStub(mountpoint, parent_catalog);

  // attach loaded catalog (from here on everything is the same)
  if (not AttachCatalog(new_catalog_file, new_catalog, false)) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to attach catalog '%s'",
             mountpoint.c_str());
    DetachCatalog(new_catalog);
    return false;
  }

  // all went well...
  if (NULL != attached_catalog) *attached_catalog = new_catalog;
  return true;
}

bool CatalogManager::AttachCatalog(const std::string &db_file,
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

bool CatalogManager::DetachCatalogTree(Catalog *catalog) {
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

bool CatalogManager::DetachCatalog(Catalog *catalog) {
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

bool CatalogManager::LoadAndAttachCatalogsRecursively(Catalog *catalog) {
  bool successful = true;

  // go through all children of the given parent catalog and attach them
  Catalog::NestedCatalogList children = catalog->ListNestedCatalogs();
  Catalog::NestedCatalogList::const_iterator j,jend;
  for (j = children.begin(), jend = children.end();
       j != jend;
       ++j) {
    Catalog *new_catalog;
    if (not LoadAndAttachCatalog(j->path, catalog, &new_catalog) ||
        not LoadAndAttachCatalogsRecursively(new_catalog)) {
      successful = false;
    }
  }

  return successful;
}

void CatalogManager::PrintCatalogHierarchyRecursively(const Catalog *catalog,
                                                              const int recursion_depth) const {
  CatalogList children = catalog->GetChildren();

  // indent the stuff according to the recursion_depth (ASCII art ftw!!)
  for (int j = 0; j < recursion_depth; ++j) {
    cout << "    ";
  }
  cout << "'-> " << catalog->path() << endl;

  // recursively go through all children
  CatalogList::const_iterator i,iend;
  for (i = children.begin(), iend = children.end();
       i != iend;
       ++i) {
    PrintCatalogHierarchyRecursively(*i, recursion_depth + 1);
  }
}

}
