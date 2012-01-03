#include "WritableCatalogManager.h"

#include <stdio.h>

#include <iostream>

#include <string>
#include <sstream>

extern "C" {
  #include "compression.h"
}

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
  if (not file_exists(*catalog_file)) {
    pmesg(D_CATALOG, "failed to load catalog file: catalog file '%s' not found", catalog_file->c_str());
    return -1;
  }
  
  return 0;
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
  const bool create_root_catalog = true;
  if (not WritableCatalog::CreateNewCatalogDatabase(file_path,
                                                    root_entry,
                                                    root_entry_parent_path,
                                                    create_root_catalog)) {
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
  // TODO: implement me!
  
  return true;
}

bool WritableCatalogManager::GetCatalogByPath(const string &path, WritableCatalog **result) {
  const bool load_final_catalog = true;
  Catalog *catalog = NULL;
  bool found = AbstractCatalogManager::GetCatalogByPath(path, load_final_catalog, &catalog);
  
  if (not found || not catalog->IsWritable()) {
    return false;
  }
  
  *result = static_cast<WritableCatalog*>(catalog);
  return true;
}

bool WritableCatalogManager::RemoveFile(SyncItem *entry) {
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

bool WritableCatalogManager::RemoveDirectory(SyncItem *entry) {
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


DirectoryEntry WritableCatalogManager::CreateNewDirectoryEntry(SyncItem *entry, 
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

bool WritableCatalogManager::AddDirectory(SyncItem *entry) {
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

bool WritableCatalogManager::AddFile(SyncItem *entry) {
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

bool WritableCatalogManager::AddHardlinkGroup(SyncItemList group) {
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
	SyncItemList::const_iterator i, end;
  bool result = true;
  bool successful = true;
  SyncItem *currentEntry = NULL;
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

bool WritableCatalogManager::TouchEntry(SyncItem *entry) {
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
  const string nested_root_path = RelativeToCatalogPath(mountpoint);
  
  // find the catalog currently containing the directory structure, which 
  // will be represented as a new nested catalog from now on
  WritableCatalog *old_catalog = NULL;
  if (not GetCatalogByPath(nested_root_path, &old_catalog)) {
    pmesg(D_CATALOG, "failed to create nested catalog '%s': mountpoint was not found in current catalog structure", nested_root_path.c_str());
    return false;
  }
  
  // get the DirectoryEntry for the given path, this will serve as root
  // entry for the nested catalog we are about to create
  DirectoryEntry new_root_entry;
  old_catalog->Lookup(nested_root_path, &new_root_entry);
  
  // create the database schema and the inital root entry
  // for the new nested catalog
  const string root_entry_parent_path = get_parent_path(nested_root_path);
  const string database_file_path = GetCatalogFilenameForPath(nested_root_path);
  const bool create_root_catalog = false;
  if (not WritableCatalog::CreateNewCatalogDatabase(database_file_path,
                                                    new_root_entry,
                                                    root_entry_parent_path,
                                                    create_root_catalog)) {
    pmesg(D_CATALOG, "failed to create nested catalog '%s': database schema creation failed", nested_root_path.c_str());
    return false;
  }
  
  // attach the just created nested catalog
  Catalog *new_catalog = NULL;
  if (not LoadAndAttachCatalog(nested_root_path, old_catalog, &new_catalog)) {
    pmesg(D_CATALOG, "failed to create nested catalog '%s': unable to attach newly created nested catalog", nested_root_path.c_str());
    return false;
  }
  
  // sanity check, just to be sure, followed by a cast to make new catalog writable
  assert (new_catalog->IsWritable());
  WritableCatalog *wr_new_catalog = static_cast<WritableCatalog *>(new_catalog);
  
  // from now on, there are two catalogs, spanning the same directory structure
  // we have to split the overlapping directory entries from the old catalog
  // to the new catalog to re-gain a valid catalog structure
  if (not old_catalog->SplitContentIntoNewNestedCatalog(wr_new_catalog)) {
    DetachCatalogTree(new_catalog);
    
    // TODO: if this happens, we may have destroyed our catalog structure...
    //       it might be a good idea to take some counter measures here
    pmesg(D_CATALOG, "[FATAL] failed to create nested catalog '%s': splitting of catalog content failed", nested_root_path.c_str());
    return false;
  }
  
  // add the newly created nested catalog to the references of the containing
  // catalog
  if (not old_catalog->InsertNestedCatalogReference(new_catalog->path())) {
    pmesg(D_CATALOG, "failed to insert new nested catalog reference '%s' in catalog '%s'", new_catalog->path().c_str(), old_catalog->path().c_str());
    return false;
  }
  
  return true;
}

bool WritableCatalogManager::RemoveNestedCatalog(const std::string &mountpoint) {
  const string nested_root_path = RelativeToCatalogPath(mountpoint);
  
  // find the catalog which should be removed
  WritableCatalog *nested_catalog = NULL;
  if (not GetCatalogByPath(nested_root_path, &nested_catalog)) {
    pmesg(D_CATALOG, "failed to remove nested catalog '%s': mountpoint was not found in current catalog structure", nested_root_path.c_str());
    return false;
  }
  
  // check if the found catalog is really the nested catalog to be deleted
  if (nested_catalog->IsRoot() || nested_catalog->path() != nested_root_path) {
    pmesg(D_CATALOG, "failed to remove nested catalog '%s': mountpoint '%s' does not name a nested catalog", nested_catalog->path().c_str(), nested_root_path.c_str());
    return false;
  }
  
  // merge all data from the nested catalog into it's parent
  if (not nested_catalog->MergeIntoParentCatalog()) {
    pmesg(D_CATALOG, "failed to remove nested catalog '%s': merging of content unsuccessful.", nested_catalog->path().c_str());
    return false;
  }
  
  // remove the catalog from our internal data structures
  const string database_file = GetCatalogFilenameForPath(nested_catalog->path());
  if (not DetachCatalog(nested_catalog)) {
    pmesg(D_CATALOG, "something went wrong while detaching the removed catalog '%s'", nested_catalog->path().c_str());
    return false;
  }
  
  // delete the catalog database file from the working copy
  if (remove(database_file.c_str()) != 0) {
    pmesg(D_CATALOG, "unable to delete the removed nested catalog database file '%s'", database_file.c_str());
    return false;
  }
  
  return true;
}

bool WritableCatalogManager::PrecalculateListings() {
  
  return true;
}

bool WritableCatalogManager::Commit() {
  WritableCatalogList catalogs_to_snapshot;
  GetCatalogsToSnapshot(catalogs_to_snapshot);
  
  WritableCatalogList::iterator i;
  WritableCatalogList::const_iterator iend;
  for (i = catalogs_to_snapshot.begin(), iend = catalogs_to_snapshot.end(); i != iend; ++i) {
    SnapshotCatalog(*i);
  }
  
  return true;
}

int WritableCatalogManager::GetCatalogsToSnapshotRecursively(const Catalog *catalog, WritableCatalogList &result) const {
  // a catalog must be snapshot, if itself or one of it's descendants is dirty
  // meaning: go through the catalog tree recursively and look
  //          for dirty catalogs on the way.
  
  // this variable will contain the number of dirty catalogs in the sub tree
  // with *catalog as it's root.
  const WritableCatalog *wr_catalog = static_cast<const WritableCatalog*>(catalog);
  int dirty_catalogs = (wr_catalog->IsDirty()) ? 1 : 0;
  
  // look for dirty catalogs in the descendants of *catalog
  CatalogList::const_iterator i,iend;
  for (i = wr_catalog->children().begin(), iend = wr_catalog->children().end(); i != iend; ++i) {
    dirty_catalogs += GetCatalogsToSnapshotRecursively(*i, result);
  }

  // if we found a dirty catalog in the checked sub tree, the root (*catalog)
  // must be snapshot and ends up in the result list
  if (dirty_catalogs > 0) {
    result.push_back(const_cast<WritableCatalog*>(wr_catalog));
  }
  
  // tell the upper layer about our findings
  return dirty_catalogs;
}

bool WritableCatalogManager::SnapshotCatalog(WritableCatalog *catalog) const {
  
  // TODO: this method needs a revision!!
  //       I (René) don't understand all bits and pieces of this stuff
  //       and just adapted it to work in this environment.
  //       It might be useful if a WritableCatalog is capable of doing
  //       most of the stuff going on here. Especially the parent-
  //       catalog bookkeeping.
  
  // TODO: We are creating a variety of files here, which are probably
  //       read somewhere else in the client. It seems important to me,
  //       to aggregate the knowledge of this file intrinsics in one place!
  
  // TODO: The mechanics around the data store might also be a candidate for
  //       refactoring... the knowledge about on disk handling of catalogs
  //       does definitely not belong in this class structure!
  
  cout << "creating snapshot of catalog '" << catalog->path() << "'" << endl;

	const string clg_path = catalog->path();
	const string cat_path = (clg_path.empty()) ? 
	                            catalog_directory_ :
                              catalog_directory_ + clg_path;

	/* Data symlink, whitelist symlink */  
	string backlink = "../";
	string parent = get_parent_path(cat_path);
	while (parent != get_parent_path(data_directory_)) {
		if (parent == "") {
			printWarning("cannot find data dir");
			break;
		}
		parent = get_parent_path(parent);
		backlink += "../";
	}
   
	const string lnk_path_data = cat_path + "/data";
	const string lnk_path_whitelist = cat_path + "/.cvmfswhitelist";
	const string backlink_data = backlink + get_file_name(data_directory_);
	const string backlink_whitelist = backlink + get_file_name(catalog_directory_) + "/.cvmfswhitelist";

	PortableStat64 info;
	if (portableLinkStat64(lnk_path_data.c_str(), &info) != 0) 
	{
		if (symlink(backlink_data.c_str(), lnk_path_data.c_str()) != 0) {
			printWarning("cannot create catalog store -> data store symlink");
		}
	}
	
	/* Don't make the symlink for the root catalog */
	if ((portableLinkStat64(lnk_path_whitelist.c_str(), &info) != 0) && (get_parent_path(cat_path) != get_parent_path(data_directory_)))
	{
		if (symlink(backlink_whitelist.c_str(), lnk_path_whitelist.c_str()) != 0) {
			printWarning("cannot create whitelist symlink");
		}
	}

	/* Last-modified time stamp */
	// TODO: revision hint!
	//       do this inside the catalog (make UpdateLastModified private)
	if (not catalog->UpdateLastModified()) {
		printWarning("failed to update last modified time stamp");
	}
	
	/* Current revision */
	// TODO: revision hint!
	//       do this inside the catalog (make IncrementRevision private)
	if (not catalog->IncrementRevision()) {
		printWarning("failed to increase revision");
	}
	
	/* Previous revision */
	map<char, string> ext_chksum;
	if (parse_keyval(cat_path + "/.cvmfspublished", ext_chksum)) {
		map<char, string>::const_iterator i = ext_chksum.find('C');
		if (i != ext_chksum.end()) {
			hash::t_sha1 sha1_previous;
			sha1_previous.from_hash_str(i->second);
			
    	// TODO: revision hint!
    	//       do this inside the catalog (make SetPreviousRevision private)
			if (not catalog->SetPreviousRevision(sha1_previous)) {
				stringstream ss;
				ss << "failed store previous catalog revision " << sha1_previous.to_string();
				printWarning(ss.str());
			}
		} else {
			printWarning("failed to find catalog SHA1 key in .cvmfspublished");
		}
	}

	/* Compress catalog */
	const string src_path = cat_path + "/.cvmfscatalog.working";
	const string dst_path = data_directory_ + "/txn/compressing.catalog";
	//const string dst_path = cat_path + "/.cvmfscatalog";
	hash::t_sha1 sha1;
	FILE *fsrc = NULL, *fdst = NULL;
	int fd_dst;
	
	if ( !(fsrc = fopen(src_path.c_str(), "r")) ||
	     (fd_dst = open(dst_path.c_str(), O_CREAT | O_TRUNC | O_RDWR, plain_file_mode)) < 0 ||
	     !(fdst = fdopen(fd_dst, "w")) ||
	     compress_file_fp_sha1(fsrc, fdst, sha1.digest) != 0)
	{
		stringstream ss;
		ss << "could not compress catalog '" << src_path << "'";
		printWarning(ss.str());

	} else {
		const string sha1str = sha1.to_string();
		const string hash_name = sha1str.substr(0, 2) + "/" + sha1str.substr(2) + "C";
		const string cache_path = data_directory_ + "/" + hash_name;
		if (rename(dst_path.c_str(), cache_path.c_str()) != 0) {
			stringstream ss;
			ss << "could not store catalog in data store as " << cache_path;
			printWarning(ss.str());
		}
		const string entry_path = cat_path + "/.cvmfscatalog"; 
		unlink(entry_path.c_str());
		if (symlink(("data/" + hash_name).c_str(), entry_path.c_str()) != 0) {
			stringstream ss;
			ss << "could not create symlink to catalog " << cache_path;
			printWarning(ss.str());
		}
	}
	if (fsrc) fclose(fsrc);
	if (fdst) fclose(fdst);

	/* Remove pending certificate */
	unlink((cat_path + "/.cvmfspublisher.x509").c_str());   

	/* Create extended checksum */
	FILE *fpublished = fopen((cat_path + "/.cvmfspublished").c_str(), "w");
	if (fpublished) {
		string fields = "C" + sha1.to_string() + "\n";
		fields += "R" + hash::t_md5(clg_path).to_string() + "\n";

		/* Mucro catalogs */
		DirectoryEntry d;
		if (not catalog->Lookup(catalog->path(), &d)) {
			printWarning("failed to find root entry");
		}
		fields += "L" + d.checksum().to_string() + "\n";
		const uint64_t ttl = catalog->GetTTL();
		ostringstream strm_ttl;
		strm_ttl << ttl;
		fields += "D" + strm_ttl.str() + "\n";

		/* Revision */
		ostringstream strm_revision;
		strm_revision << catalog->GetRevision();
		fields += "S" + strm_revision.str() + "\n";

		if (fwrite(&(fields[0]), 1, fields.length(), fpublished) != fields.length()) {
			printWarning("failed to write extended checksum");
		}
		fclose(fpublished);
		
	} else {
		printWarning("failed to write extended checksum");
	}
   
	/* Update registered catalog SHA1 in nested catalog */
	// TODO: revision hint
	//       this might be done implicitly when snapshoting a nested catalog
	//       Catalogs know about their parent catalog!
	if (not catalog->IsRoot()) {
		cout << "updating nested catalog link" << endl;
		
		// TODO: this is fishy! but I leave it this way for the moment
		//       (dynamic_cast<> at least dies, if something goes wrong)
		if (not dynamic_cast<WritableCatalog*>(catalog->parent())->UpdateNestedCatalogLink(clg_path, sha1)) {
			stringstream ss;
			ss << "failed to register modified catalog at " << clg_path << " in parent catalog";
			printWarning(ss.str());
		}
	}

	/* Compress and write SHA1 checksum */
	char chksum[40];
	int lchksum = 40;
	memcpy(chksum, &((sha1.to_string())[0]), 40);
	void *compr_buf = NULL;
	size_t compr_size;
	if (compress_mem(chksum, lchksum, &compr_buf, &compr_size) != 0) {
		printWarning("could not compress catalog checksum");
	}

	FILE *fsha1 = NULL;
	int fd_sha1;
	if (((fd_sha1 = open((cat_path + "/.cvmfschecksum").c_str(), O_CREAT | O_TRUNC | O_RDWR, plain_file_mode)) < 0) ||
		!(fsha1 = fdopen(fd_sha1, "w")) ||
		(fwrite(compr_buf, 1, compr_size, fsha1) != compr_size))
	{			
		stringstream ss;
		ss << "could not store checksum at " << cat_path;
		printWarning(ss.str());
	}

	if (fsha1) fclose(fsha1);
	if (compr_buf) free(compr_buf);
  
  return true;
}

}
