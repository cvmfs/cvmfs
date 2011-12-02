#include "WritableCatalogManager.h"

#include <iostream>

#include <string>
#include <sstream>

#include "WritableCatalog.h"
#include "util.h"

using namespace std;

namespace cvmfs {
  
const string WritableCatalogManager::kCatalogFilename = ".cvmfscatalog.working";

WritableCatalogManager::WritableCatalogManager(
                         const string catalog_directory,
                         const string data_directory,
                         const bool lazy_attaching)
{
  catalog_directory_ = canonical_path(catalog_directory);
	data_directory_    = canonical_path(data_directory);
	lazy_attach_       = lazy_attaching;
	
  Init();
}

WritableCatalogManager::~WritableCatalogManager() {
  
}

bool WritableCatalogManager::Init() {
  bool succeeded = AbstractCatalogManager::Init();
  
  // if the abstract initialization fails, we have a fresh repository here
  // create a root catalog
  if (not succeeded &&
      not CreateAndAttachRootCatalog()) {
    pmesg(D_CATALOG, "unable to init catalog manager (cannot create root catalog)");
    return false;
  }
  
  // do lazy attach if asked for
  if (not IsLazyAttaching() &&
      not LoadAndAttachCatalogsRecursively()) {
    pmesg(D_CATALOG, "unable to load catalogs recursively");
    return false;
  }

	return true;
}

int WritableCatalogManager::LoadCatalogFile(const std::string &url_path, 
                                            const hash::t_md5 &mount_point, 
                                            string *catalog_file)
{
  // actually we have nothing to load here...
  // just redirect to the appropriate catalog file on disk
  *catalog_file = GetCatalogFilenameForPath(url_path);
  
  // check if the file exists
  // if not, the 'loading' fails
  return (file_exists(*catalog_file)) ? 0 : -1;
}

Catalog* WritableCatalogManager::CreateCatalogStub(const std::string &mountpoint, Catalog *parent_catalog) const {
  return new WritableCatalog(mountpoint, parent_catalog);
}

bool WritableCatalogManager::CreateAndAttachRootCatalog() {
  // create a new root catalog at file_path
  string file_path = GetCatalogFilenameForPath("");
  
  // a newly created catalog always needs a root entry
  // we create and configure this here
  DirectoryEntry root_entry;
  root_entry.inode_             = DirectoryEntry::kInvalidInode;
  root_entry.parent_inode_      = DirectoryEntry::kInvalidInode;
  root_entry.mode_              = 16877;
  root_entry.size_              = 4096;
  root_entry.mtime_             = time(NULL);
  root_entry.checksum_          = hash::t_sha1();
  root_entry.linkcount_         = 1;
  
  string root_entry_parent_path = "";

  // create the database schema and the inital root entry
  if (not WritableCatalog::CreateNewCatalogDatabase(file_path,
                                                    root_entry,
                                                    root_entry_parent_path)) {
    pmesg(D_CATALOG, "creation of catalog '%s' failed", file_path.c_str());
    return false;
  }
  
  // attach the just created catalog
  if (not LoadAndAttachCatalog("", NULL)) {
    pmesg(D_CATALOG, "failed to attach newly created root catalog");
    return false;
  }
  
  return true;
}

bool WritableCatalogManager::LoadAndAttachCatalogsRecursively() {
  
  return true;
}

bool WritableCatalogManager::GetCatalogByPath(const string &path, WritableCatalog **result) {
  const bool load_final_catalog = true;
  Catalog *catalog = NULL;
  bool found = AbstractCatalogManager::GetCatalogByPath(path, load_final_catalog, &catalog);
  
  if (not found || not catalog->IsWritable()) {
    return false;
  }
  
  *result = (WritableCatalog*)catalog;
  return true;
}

bool WritableCatalogManager::RemoveFile(DirEntry *entry) {
	const string parent_path = RelativeToCatalogPath(entry->getParentPath());
  const string file_path = RelativeToCatalogPath(entry->getRelativePath());
	
  WritableCatalog *catalog;
  if (not GetCatalogByPath(parent_path, &catalog)) {
    pmesg(D_CATALOG, "catalog for file '%s' cannot be found", file_path.c_str());
    return false;
  }
  
  if (not catalog->Lookup(file_path)) {
    pmesg(D_CATALOG, "file '%s' does not exist and thus cannot be deleted", file_path.c_str());
    return false;
  }
  
  if (not catalog->RemoveEntry(file_path)) {
    pmesg(D_CATALOG, "something went wrong while deleting '%s'", file_path.c_str());
    return false;
  }
	
	return true;
}

bool WritableCatalogManager::RemoveDirectory(DirEntry *entry) {
	const string parent_path = RelativeToCatalogPath(entry->getParentPath());
  const string directory_path = RelativeToCatalogPath(entry->getRelativePath());
	
  WritableCatalog *catalog;
  if (not GetCatalogByPath(parent_path, &catalog)) {
    pmesg(D_CATALOG, "catalog for directory '%s' cannot be found", directory_path.c_str());
    return false;
  }
  
  DirectoryEntry dir;
  if (not catalog->Lookup(directory_path, &dir)) {
    pmesg(D_CATALOG, "directory '%s' does not exist and thus cannot be deleted", directory_path.c_str());
    return false;
  }
  
  if (dir.IsNestedCatalogMountpoint()) {
    pmesg(D_CATALOG, "directory '%s' is a mount point of a nested catalog, delete is not allowed", directory_path.c_str());
    return false;
  }
  
  if (dir.IsNestedCatalogRoot()) {
    pmesg(D_CATALOG, "directory '%s' is the root of a nested catalog, delete is not allowed", directory_path.c_str());
    return false;
  }
  
  DirectoryEntryList listing;
  if (not catalog->Listing(directory_path, &listing) && listing.size() > 0) {
    pmesg(D_CATALOG, "directory '%s' is not empty and cannot be deleted", directory_path.c_str());
    return false;
  }
  
  if (not catalog->RemoveEntry(directory_path)) {
    pmesg(D_CATALOG, "something went wrong while deleting '%s'", directory_path.c_str());
    return false;
  }
  
  return true;
}


DirectoryEntry WritableCatalogManager::CreateNewDirectoryEntry(DirEntry *entry, 
                                                               Catalog *catalog, 
                                                               const int hardlink_group_id) const {
  DirectoryEntry dEntry;
  dEntry.inode_             = DirectoryEntry::kInvalidInode; // inode is determined at runtime of client
  dEntry.parent_inode_      = DirectoryEntry::kInvalidInode; // ... dito
  dEntry.mode_              = entry->getUnionStat().st_mode;
  dEntry.size_              = entry->getUnionStat().st_size;
  dEntry.mtime_             = entry->getUnionStat().st_mtime;
  dEntry.checksum_          = entry->getContentHash();
  dEntry.name_              = entry->getFilename();
  dEntry.symlink_           = "";
  dEntry.linkcount_         = entry->getUnionLinkcount();
  
  dEntry.catalog_           = catalog;
  dEntry.hardlink_group_id_ = hardlink_group_id;
  return dEntry;
}

bool WritableCatalogManager::AddDirectory(DirEntry *entry) {
  const string directory_path = RelativeToCatalogPath(entry->getRelativePath());
  const string parent_path = RelativeToCatalogPath(entry->getParentPath());
  
  WritableCatalog *catalog;
  if (not GetCatalogByPath(parent_path, &catalog)) {
    pmesg(D_CATALOG, "catalog for directory '%s' cannot be found", directory_path.c_str());
    return false;
  }
  
  DirectoryEntry dEntry = CreateNewDirectoryEntry(entry, catalog);
  catalog->CheckForExistanceAndAddEntry(dEntry, directory_path, parent_path);

  return true;
}

bool WritableCatalogManager::AddFile(DirEntry *entry) {
  const string parent_path = RelativeToCatalogPath(entry->getParentPath());
  const string file_path = RelativeToCatalogPath(entry->getRelativePath());

  WritableCatalog *catalog;
  if (not GetCatalogByPath(parent_path, &catalog)) {
    pmesg(D_CATALOG, "catalog for file '%s' cannot be found", file_path.c_str());
    return false;
  }
  
  DirectoryEntry dEntry = CreateNewDirectoryEntry(entry, catalog);
  
  if (entry->isSymlink()) {
		char slnk[PATH_MAX+1];
		ssize_t l = readlink((entry->getUnionPath()).c_str(), slnk, PATH_MAX);
		if (l >= 0) {
			slnk[l] = '\0';
			dEntry.symlink_ = slnk;
		} else {
      pmesg(D_CATALOG, "unable to read link destination for symlink '%s' - add failed", file_path.c_str());
			return false;
		}

	} else if (entry->isRegularFile() && not entry->hasContentHash()) {
    pmesg(D_CATALOG, "regular file '%s' has no content hash and cannot be added", file_path.c_str());
		return false;
	}

  catalog->CheckForExistanceAndAddEntry(dEntry, file_path, parent_path);
  
  return true;
}

bool WritableCatalogManager::AddHardlinkGroup(DirEntryList group) {
  // sanity check
	if (group.size() == 0) {
    pmesg(D_CATALOG, "tried to add an empty hardlink group");
		return false;
	}
	
	if (group.size() == 1) {
    pmesg(D_CATALOG, "tried to add a hardlink group with just one member... added as normal file instead");
    return AddFile(group.front());
	}
	
	// hardlink groups have to reside in the same directory.
	// therefore it is enough to look for the first in the group
	const string parent_path = RelativeToCatalogPath(group.front()->getParentPath());

  WritableCatalog *catalog;
  if (not GetCatalogByPath(parent_path, &catalog)) {
    pmesg(D_CATALOG, "catalog for hardlink group containing '%s' cannot be found", group.front()->getRelativePath().c_str());
    return false;
  }
	
	// get a valid hardlink group id for the catalog the group will end up in
	int new_group_id = catalog->GetMaximalHardlinkGroupId() + 1;
	if (new_group_id <= 0) {
    pmesg(D_CATALOG, "failed to retrieve a new valid hardlink group id");
		return false;
	}
	
	// add the file entries to the catalog
	DirEntryList::const_iterator i, end;
  bool result = true;
  bool successful = true;
  DirEntry *currentEntry = NULL;
	for (i = group.begin(), end = group.end(); i != end; ++i) {
    currentEntry = *i;
	  string file_path = RelativeToCatalogPath(currentEntry->getRelativePath());
	  DirectoryEntry dEntry = CreateNewDirectoryEntry(currentEntry, catalog, new_group_id);
	  successful = catalog->CheckForExistanceAndAddEntry(dEntry, file_path, parent_path);
	  if (not successful) {
      result = false;
	  }
	}
	
	if (false == result) {
    pmesg(D_CATALOG, "something went wrong while adding a hardlink group");
	}
	
	return result;
}

bool WritableCatalogManager::TouchEntry(DirEntry *entry) {
  const string parent_path = RelativeToCatalogPath(entry->getParentPath());
  const string entry_path = RelativeToCatalogPath(entry->getRelativePath());
  
  WritableCatalog *catalog;
  if (not GetCatalogByPath(parent_path, &catalog)) {
    pmesg(D_CATALOG, "catalog for entry '%s' cannot be found", entry_path.c_str());
    return false;
  }
  
  if (not catalog->Lookup(entry_path)) {
    pmesg(D_CATALOG, "entry '%s' does not exist and thus cannot be touched", entry_path.c_str());
    return false;
  }
  
  if (not catalog->TouchEntry(entry_path, entry->getUnionStat().st_mtime)) {
    pmesg(D_CATALOG, "something went wrong while touching entry '%s'", entry_path.c_str());
    return false;
  }
  
  return true;
}

bool WritableCatalogManager::CreateNestedCatalog(const std::string &mountpoint) {
  
  return true;
}

bool WritableCatalogManager::RemoveNestedCatalog(const std::string &mountpoint) {
  
  return true;
}

bool WritableCatalogManager::PrecalculateListings() {
  
  return true;
}

bool WritableCatalogManager::Commit() {
  
  return true;
}

}
