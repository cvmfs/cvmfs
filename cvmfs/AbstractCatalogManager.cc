#include "AbstractCatalogManager.h"

#include <iostream>
#include "logging.h"

using namespace std;

namespace cvmfs {

AbstractCatalogManager::AbstractCatalogManager() {
  current_inode_offset_ = AbstractCatalogManager::kInitialInodeOffset;
}

AbstractCatalogManager::~AbstractCatalogManager() {
  WriteLock();
  DetachAllCatalogs();
  Unlock();
}

bool AbstractCatalogManager::Init() {
  // attaching root catalog
  LogCvmfs(kLogCatalog, kLogDebug, "Initialize catalog");
  WriteLock();
  bool root_catalog_attached = LoadAndAttachCatalog("", NULL);
  Unlock();

  if (not root_catalog_attached) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to initialize root catalog");
  }

  return root_catalog_attached;
}

/**
 *  currently this is a very simple approach
 *  just assign the next free numbers in this 64 bit space
 *  TODO: think about other allocation methods, this may run out
 *        of free inodes some time (in the late future admittedly)
 */
InodeChunk AbstractCatalogManager::GetInodeChunkOfSize(uint64_t size) {
  InodeChunk result;
  result.offset = current_inode_offset_;
  result.size = size;

  current_inode_offset_ = current_inode_offset_ + size;
  LogCvmfs(kLogCatalog, kLogDebug, "allocating inodes from %d to %d.",
           result.offset + 1, current_inode_offset_);

  return result;
}

void AbstractCatalogManager::AnnounceInvalidInodeChunk(const InodeChunk chunk) const {
  // TODO: actually do something here
}

bool AbstractCatalogManager::Lookup(const inode_t inode,
                                    DirectoryEntry *entry,
                                    const bool with_parent) const {
  ReadLock();
  bool found = false;

  // get appropriate catalog
  Catalog *catalog;
  bool found_catalog = GetCatalogByInode(inode, &catalog);
  if (not found_catalog) {
    LogCvmfs(kLogCatalog, kLogDebug, "cannot find catalog for inode %d", inode);
    found = false;
    goto out;
  }

  // if we are not asked to lookup the parent inode or if we are
  // asked for the root inode (which of course has no parent)
  // we simply look in the best suited catalog and are done
  if (not with_parent || inode == GetRootInode()) {
    found = catalog->Lookup(inode, entry);
    goto out;
  }

  // to lookup the parent of this entry, we obtain the parent
  // md5 hash and make potentially two lookups, first in the
  // previously found catalog and second its parent catalog
  else {
    hash::t_md5 parent_hash;
    DirectoryEntry parent;
    bool found_parent_entry = false;
    bool found_entry = catalog->Lookup(inode, entry, &parent_hash);

    // entry was not found... we're done with that --> return
    if (not found_entry) {
      found = false;
      goto out;
    }

    // if our entry is the root of a nested catalog it's parent resides
    // in the parent catalog and we have to search there. Otherwise
    // it should be found in the current catalog
    if (entry->IsNestedCatalogRoot() && not catalog->IsRoot()) {
      Catalog *parent_catalog = catalog->parent();
      found_parent_entry = parent_catalog->Lookup(parent_hash, &parent);
    } else {
      found_parent_entry = catalog->Lookup(parent_hash, &parent);
    }

    // if we didn't find a parent entry, there may be some data corruption!
    if (not found_parent_entry) {
      LogCvmfs(kLogCatalog, kLogDebug,
               "cannot find parent entry for inode %d --> data corrupt?",
               inode);
      found = false;
      goto out;
    }

    // all set
    entry->set_parent_inode(parent.inode());
    found = true;
    goto out;
  }

out:
  Unlock();
  return found;
}

bool AbstractCatalogManager::Lookup(const string &path,
                                    DirectoryEntry *entry,
                                    const bool with_parent) {
  ReadLock();

  // the actual lookup is performed while finding the correct
  // catalog as a side product
  bool found = false;
  found = GetCatalogByPath(path, false, NULL, entry);

  // lookup the parent entry, if asked for
  if (found && with_parent) {
    string parent_path = get_parent_path(path);
    DirectoryEntry parent;
    found = LookupWithoutParent(parent_path, &parent);
    if (not found) {
      LogCvmfs(kLogCatalog, kLogDebug,
                "cannot find parent '%s' for entry '%s' --> data corrupt?",
               parent_path.c_str(), path.c_str());
    } else {
      entry->set_parent_inode(parent.inode());
    }
  }

  Unlock();
  return found;
}

bool AbstractCatalogManager::Listing(const string &path,
                                     DirectoryEntryList *listing) {
  ReadLock();
  bool result = false;

  // find the catalog where path resides
  Catalog *catalog;
  bool found_catalog = GetCatalogByPath(path, true, &catalog);

  // do the listing
  if (not found_catalog) {
    result = false;
  } else {
    result = catalog->Listing(path, listing);
  }

  Unlock();
  return result;
}

bool AbstractCatalogManager::GetCatalogByPath(const string &path,
                                              const bool load_final_catalog,
                                              Catalog **catalog,
                                              DirectoryEntry *entry) {
  // find the best fitting loaded catalog for this path
  Catalog *best_fitting_catalog = FindBestFittingCatalogForPath(path);
  assert (best_fitting_catalog != NULL);

  // path lookup in this catalog
  // (if the entry we are looking for was found, we're basically finished)
  LogCvmfs(kLogCatalog, kLogDebug, "looking up '%s' in catalog: '%s'",
           path.c_str(), best_fitting_catalog->path().c_str());
  DirectoryEntry d;
  bool entry_found = best_fitting_catalog->Lookup(path, &d);

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
      entry_found = nested_catalog->Lookup(path, &d);
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

bool AbstractCatalogManager::GetCatalogByInode(const uint64_t inode,
                                               Catalog **catalog) const {
  // TODO: replace this with a more clever algorithm
  //       maybe exploit the ordering in the vector
  CatalogList::const_iterator i,end;
  for (i = catalogs_.begin(), end = catalogs_.end(); i != end; ++i) {
    if ((*i)->ContainsInode(inode)) {
      *catalog = *i;
      return true;
    }
  }

  // inode was not found... might be data corruption
  return false;
}

Catalog* AbstractCatalogManager::FindBestFittingCatalogForPath(const string &path) const {
  assert (GetNumberOfAttachedCatalogs() > 0);

  // we start at the root catalog and successive go down the catalog
  // tree to find the best fitting catalog for the given path
  Catalog *best_fit = GetRootCatalog();
  Catalog *next_best_fit = NULL;
  while (best_fit->path() != path) {
    next_best_fit = best_fit->FindBestFittingChild(path);

    // if there was a child which fitted better than
    // continue in this catalog, otherwise break
    if (next_best_fit != NULL) {
      best_fit = next_best_fit;
    } else {
      break;
    }
  }

  return best_fit;
}

bool AbstractCatalogManager::IsCatalogAttached(const string &root_path,
                                               Catalog **attached_catalog) const {
  if (GetNumberOfAttachedCatalogs() == 0) {
    return false;
  }

  // look through the attached catalogs to find the searched one
  Catalog *best_fit = FindBestFittingCatalogForPath(root_path);

  // not found... too bad
  if (best_fit->path() != root_path) {
    return false;
  }

  // found... yeepie!
  if (NULL != attached_catalog) *attached_catalog = best_fit;
  return true;
}

bool AbstractCatalogManager::LoadNestedCatalogForPath(const string &path,
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

  path_elements = split_string(relative_path, "/");
  bool entry_found;
  DirectoryEntry entry;

  // step through the path and attach nested catalogs on the way
  // TODO: this might get faster by exploiting the 'nested catalog table'
  std::vector<string>::const_iterator i,end;
  for (i = path_elements.begin(), end = path_elements.end(); i != end; ++i) {
    sub_path += "/" + *i;

    entry_found = containing_catalog->Lookup(sub_path, &entry);
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

bool AbstractCatalogManager::LoadAndAttachCatalog(const string &mountpoint,
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
  if (0 != LoadCatalogFile(mountpoint, hash::t_md5(mountpoint), &new_catalog_file)) {
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
  InodeChunk chunk = GetInodeChunkOfSize(inode_chunk_size);
  new_catalog->set_inode_chunk(chunk);

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

  AnnounceInvalidInodeChunk(catalog->inode_chunk());

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
  Catalog::NestedCatalogReferenceList children = catalog->ListNestedCatalogReferences();
  Catalog::NestedCatalogReferenceList::const_iterator j,jend;
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

void AbstractCatalogManager::PrintCatalogHierarchyRecursively(const Catalog *catalog,
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
