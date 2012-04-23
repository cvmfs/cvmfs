/**
 * This file is part of the CernVM file system.
 */

#include "catalog_mgr_rw.h"

#include <unistd.h>

#include <cstdio>
#include <cstdlib>

#include <string>

#include "compression.h"
#include "catalog_rw.h"
#include "util.h"
#include "logging.h"

using namespace std;  // NOLINT

namespace catalog {

const string WritableCatalogManager::kCatalogFilename = ".cvmfscatalog.working";


WritableCatalogManager::WritableCatalogManager(
                         const string catalog_directory,
                         const string data_directory) :
  catalog_directory_(catalog_directory),
  data_directory_(data_directory)
{
  Init();
}


/**
 * Initializes the WritableCatalogManager, creates a new repository
 * if necessary.
 * @return true on success, false otherwise
 */
bool WritableCatalogManager::Init() {
  bool succeeded = AbstractCatalogManager::Init();

  // If the abstract initialization fails, we have a fresh repository here
  // create a root catalog
  if (!succeeded && !CreateRepository()) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "unable to init catalog manager (cannot create root catalog)");
    return false;
  }

	return true;
}


/**
 * 'Loads' a catalog. Actually the WritableCatalogManager assumes
 * that it runs in an environment where all catalog files are already
 * accessible.  Therefore it does not actually load the catalog.
 * TODO: Load from HTTP
 * @param url_path the url of the catalog to load
 * @param mount_point the file system path where the catalog should be mounted
 * @param catalog_file a pointer to the string containing the full qualified
 *                     name of the catalog afterwards
 * @return 0 on success, different otherwise
 */
LoadError WritableCatalogManager::LoadCatalog(const PathString &mountpoint,
                                              const hash::Any &hash,
                                              std::string *catalog_path)
{
  *catalog_path = GetCatalogPath(mountpoint.ToString());

  // Check if the file exists
  // if not, the 'loading' fails
  if (!FileExists(*catalog_path)) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "failed to load catalog file: catalog file '%s' not found",
             catalog_path->c_str());
    return kLoadFail;
  }

  return kLoadNew;
}


/**
 * This method is virtual in AbstractCatalogManager.  It returns a new catalog
 * structure in the form the different CatalogManagers need it.
 * In this case it returns a stub for a WritableCatalog.
 * @param mountpoint the mount point of the catalog stub to create
 * @param parent_catalog the parent of the catalog stub to create
 * @return a pointer to the catalog stub structure created
 */
Catalog* WritableCatalogManager::CreateCatalog(const PathString &mountpoint,
                                               Catalog *parent_catalog)
{
  return new WritableCatalog(mountpoint.ToString(), parent_catalog);
}


/**
 * This method is invoked if we create a completely new repository.
 * It initializes a new root catalog and attaches it afterwards.
 * The new root catalog will already contain a root entry.
 * @return true on success, false otherwise
 */
bool WritableCatalogManager::CreateRepository() {
  // Create a new root catalog at file_path
  string file_path = GetCatalogPath("");

  // A newly created catalog always needs a root entry
  // we create and configure this here
  DirectoryEntry root_entry;
  root_entry.inode_             = DirectoryEntry::kInvalidInode;
  root_entry.parent_inode_      = DirectoryEntry::kInvalidInode;
  root_entry.mode_              = 16877;
  root_entry.size_              = 4096;
  root_entry.mtime_             = time(NULL);
  root_entry.checksum_          = hash::Any(hash::kSha1);
  root_entry.linkcount_         = 1;

  string root_parent_path = "";

  // Create the database schema and the inital root entry
  const bool new_repository = true;
  if (!WritableCatalog::CreateDatabase(file_path, root_entry, root_parent_path,
                                       new_repository))
  {
    LogCvmfs(kLogCatalog, kLogDebug, "creation of catalog '%s' failed",
             file_path.c_str());
    return false;
  }

  // Attach the just created catalog
  if (!MountCatalog(PathString("", 0), hash::Any(), NULL)) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "failed to attach newly created root catalog");
    return false;
  }

  return true;
}


/**
 * Tries to retrieve the catalog containing the given path
 * This method is just a wrapper around the FindCatalog method of
 * AbstractCatalogManager to provide a direct interface returning
 * WritableCatalog classes.
 * @param path the path to look for
 * @param result the retrieved catalog (as a pointer)
 * @return true if catalog was found, false otherwise
 */
bool WritableCatalogManager::FindCatalog(const string &path,
                                         WritableCatalog **result)
{
  Catalog *best_fit =
    AbstractCatalogManager::FindCatalog(PathString(path.data(), path.length()));
  assert (best_fit != NULL);
  Catalog *catalog = NULL;
  bool retval = MountSubtree(PathString(path.data(), path.length()),
                             best_fit, &catalog);
  if (!retval)
    return false;

  bool found = LookupPath(path, kLookupSole, NULL);
  if (!found || !catalog->IsWritable())
    return false;

  *result = static_cast<WritableCatalog *>(catalog);
  return true;
}


/**
 * Remove the given file from the catalogs.
 * @param file_path the full path to the file to be removed
 * @return true on success, false otherwise
 */
bool WritableCatalogManager::RemoveFile(const std::string &path) {
  const string file_path = MakeRelativePath(path);
	const string parent_path = GetParentPath(file_path);

  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogDebug, "catalog for file '%s' cannot be found",
             file_path.c_str());
    return false;
  }

  if (!catalog->RemoveEntry(file_path)) {
    LogCvmfs(kLogCatalog, kLogDebug, "something went wrong while deleting '%s'",
             file_path.c_str());
    return false;
  }

	return true;
}


/**
 * Remove the given directory from the catalogs.
 * @param directory_path the full path to the directory to be removed
 * @return true on success, false otherwise
 */
bool WritableCatalogManager::RemoveDirectory(const std::string &path) {
  const string directory_path = MakeRelativePath(path);
	const string parent_path = GetParentPath(directory_path);

  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "catalog for directory '%s' cannot be found",
             directory_path.c_str());
    return false;
  }

  DirectoryEntry dir;
  if (!catalog->LookupPath(PathString(directory_path.data(),
                                      directory_path.length()), &dir))
  {
    LogCvmfs(kLogCatalog, kLogDebug,
             "directory '%s' does not exist and thus cannot be deleted",
             directory_path.c_str());
    return false;
  }

  assert(!dir.IsNestedCatalogMountpoint() && !dir.IsNestedCatalogRoot());
  DirectoryEntryList listing;
  bool retval = catalog->ListingPath(PathString(directory_path.data(),
                                     directory_path.length()), &listing);
  assert(retval && (listing.size() == 0));

  if (!catalog->RemoveEntry(directory_path)) {
    LogCvmfs(kLogCatalog, kLogDebug, "something went wrong while deleting '%s'",
             directory_path.c_str());
    return false;
  }

  return true;
}


/**
 * Add a new directory to the catalogs.
 * @param entry a DirectoryEntry structure describing the new directory
 * @param parent_directory the absolute path of the directory containing the
 *                         directory to be created
 * @return true on success, false otherwise
 */
bool WritableCatalogManager::AddDirectory(const DirectoryEntry &entry,
                                          const std::string &parent_directory)
{
  const string parent_path = MakeRelativePath(parent_directory);
  string directory_path = parent_path + "/";
  directory_path.append(entry.name().GetChars(), entry.name().GetLength());

  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "catalog for directory '%s' cannot be found",
             directory_path.c_str());
    return false;
  }

  catalog->AddEntry(entry, directory_path, parent_path);
  return true;
}


/**
 * Add a new file to the catalogs.
 * @param entry a DirectoryEntry structure describing the new file
 * @param parent_directory the absolute path of the directory containing the
 *                         file to be created
 * @return true on success, false otherwise
 */
bool WritableCatalogManager::AddFile(const DirectoryEntry &entry,
                                     const std::string &parent_directory) {
  const string parent_path = MakeRelativePath(parent_directory);
  string file_path = parent_path + "/";
  file_path.append(entry.name().GetChars(), entry.name().GetLength());

  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogDebug, "catalog for file '%s' cannot be found",
             file_path.c_str());
    return false;
  }

  assert(!entry.IsRegular() || !entry.checksum().IsNull());

  catalog->AddEntry(entry, file_path, parent_path);
  return true;
}


/**
 * Add a hardlink group to the catalogs.
 * @param entries a list of DirectoryEntries describing the new files
 * @param parent_directory the absolute path of the directory containing the
 *                         files to be created
 * @return true on success, false otherwise
 */
bool WritableCatalogManager::AddHardlinkGroup(DirectoryEntryList &entries,
                                          const std::string &parent_directory)
{
  // Sanity checks
	if (entries.size() == 0) {
    LogCvmfs(kLogCatalog, kLogDebug, "tried to add an empty hardlink group");
		return false;
	}
	if (entries.size() == 1) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "tried to add a hardlink group with just one member... "
             "added as normal file instead");
    return AddFile(entries.front(), parent_directory);
	}

	// Hardlink groups have to reside in the same directory.
	// Therefore we only have one parent directory here
	const string parent_path = MakeRelativePath(parent_directory);

  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "catalog for hardlink group containing '%s' cannot be found",
             parent_path.c_str());
    return false;
  }

	// Get a valid hardlink group id for the catalog the group will end up in
	int new_group_id = catalog->GetMaxLinkId() + 1;
	if (new_group_id <= 0) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "failed to retrieve a new valid hardlink group id");
		return false;
	}

	// Add the file entries to the catalog
  bool result = true;
  bool successful = true;
	for (DirectoryEntryList::iterator i = entries.begin(), iEnd = entries.end();
       i != iEnd; ++i)
  {
	  string file_path = parent_path + "/";
    file_path.append(i->name().GetChars(), i->name().GetLength());
    i->hardlink_group_id_ = new_group_id;
	  successful = catalog->AddEntry(*i, file_path, parent_path);
	  if (!successful)
      result = false;
	}

	if (result = false) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "something went wrong while adding a hardlink group");
	}

	return result;
}


/**
 * Updated time stamp (utime / 'touch' utility).
 */
bool WritableCatalogManager::TouchEntry(const DirectoryEntry entry,
                                        const std::string &path) {
  const string entry_path = MakeRelativePath(path);
  const string parent_path = GetParentPath(entry_path);

  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogDebug, "catalog for entry '%s' cannot be found",
             entry_path.c_str());
    return false;
  }

  if (!catalog->TouchEntry(entry, entry_path)) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "something went wrong while touching entry '%s'",
             entry_path.c_str());
    return false;
  }

  return true;
}


/**
 * Create a new nested catalog.  Involves to creating a new catalog and moveing
 * all entries belonging there from it's parent catalog.
 * @param mountpoint the path of the directory to become a nested root
 * @return true on success, false otherwise
 */
bool WritableCatalogManager::CreateNestedCatalog(const std::string &mountpoint) {
  const string nested_root_path = MakeRelativePath(mountpoint);

  // Find the catalog currently containing the directory structure, which
  // will be represented as a new nested catalog from now on
  WritableCatalog *old_catalog = NULL;
  if (!FindCatalog(nested_root_path, &old_catalog)) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to create nested catalog '%s': "
             "mountpoint was not found in current catalog structure",
             nested_root_path.c_str());
    return false;
  }

  // Get the DirectoryEntry for the given path, this will serve as root
  // entry for the nested catalog we are about to create
  DirectoryEntry new_root_entry;
  old_catalog->LookupPath(PathString(nested_root_path.data(),
                          nested_root_path.length()), &new_root_entry);

  // Create the database schema and the inital root entry
  // for the new nested catalog
  const string root_parent_path = GetParentPath(nested_root_path);
  const string database_file_path = GetCatalogPath(nested_root_path);
  const bool new_repository = false;
  if (!WritableCatalog::CreateDatabase(database_file_path, new_root_entry,
                                       root_parent_path, new_repository))
  {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to create nested catalog '%s': "
             "database schema creation failed", nested_root_path.c_str());
    return false;
  }

  // Attach the just created nested catalog
  Catalog *new_catalog =
    MountCatalog(PathString(nested_root_path.data(), nested_root_path.length()),
                 hash::Any(), old_catalog);
  if (!new_catalog) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to create nested catalog '%s': "
             "unable to attach newly created nested catalog",
             nested_root_path.c_str());
    return false;
  }

  assert(new_catalog->IsWritable());
  WritableCatalog *wr_new_catalog = static_cast<WritableCatalog *>(new_catalog);

  // From now on, there are two catalogs, spanning the same directory structure
  // we have to split the overlapping directory entries from the old catalog
  // to the new catalog to re-gain a valid catalog structure
  if (!old_catalog->Partition(wr_new_catalog)) {
    DetachSubtree(new_catalog);

    // TODO: if this happens, we may have destroyed our catalog structure...
    //       it might be a good idea to take some counter measures here
    LogCvmfs(kLogCatalog, kLogDebug, "failed to create nested catalog '%s': "
             "splitting of catalog content failed", nested_root_path.c_str());
    return false;
  }

  // Add the newly created nested catalog to the references of the containing
  // catalog
  if (!old_catalog->InsertNestedCatalog(new_catalog->path().ToString(), NULL,
                                        hash::Any(hash::kSha1)))
  {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to insert new nested catalog "
             "reference '%s' in catalog '%s'",
             new_catalog->path().c_str(), old_catalog->path().c_str());
    return false;
  }

  return true;
}


/**
 * Remove a nested catalog.
 * When you remove a nested catalog all entries currently held by it
 * will be merged into its parent catalog.
 * @param mountpoint the path of the nested catalog to be removed
 * @return true on success, false otherwise
 */
bool WritableCatalogManager::RemoveNestedCatalog(const string &mountpoint) {
  const string nested_root_path = MakeRelativePath(mountpoint);

  // Find the catalog which should be removed
  WritableCatalog *nested_catalog = NULL;
  if (!FindCatalog(nested_root_path, &nested_catalog)) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to remove nested catalog '%s': "
             "mountpoint was not found in current catalog structure",
             nested_root_path.c_str());
    return false;
  }

  // Check if the found catalog is really the nested catalog to be deleted
  assert(!nested_catalog->IsRoot() &&
         (nested_catalog->path().ToString() != nested_root_path));

  // Merge all data from the nested catalog into it's parent
  if (!nested_catalog->MergeIntoParent()) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to remove nested catalog '%s': "
             "merging of content unsuccessful.",
             nested_catalog->path().c_str());
    return false;
  }

  // Remove the catalog from internal data structures
  const string database_file =
    GetCatalogPath(nested_catalog->path().ToString());
  DetachCatalog(nested_catalog);

  // Delete the catalog database file from the working copy
  if (unlink(database_file.c_str()) != 0) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "unable to delete the removed nested catalog database file '%s'",
             database_file.c_str());
    return false;
  }

  return true;
}


bool WritableCatalogManager::PrecalculateListings() {
  // TODO
  return true;
}


bool WritableCatalogManager::Commit() {
  reinterpret_cast<WritableCatalog *>(GetRootCatalog())->SetDirty();
  WritableCatalogList catalogs_to_snapshot;
  GetModifiedCatalogs(&catalogs_to_snapshot);

  for (WritableCatalogList::iterator i = catalogs_to_snapshot.begin(),
       iEnd = catalogs_to_snapshot.end(); i != iEnd; ++i)
  {
    (*i)->Commit();
    SnapshotCatalog(*i);
  }

  return true;
}


int WritableCatalogManager::GetModifiedCatalogsRecursively(
  const Catalog *catalog,
  WritableCatalogList *result) const
{
  // A catalog must be snapshot, if itself or one of it's descendants is dirty.
  // So we traverse the catalog tree recursively and look for dirty catalogs
  // on the way.

  const WritableCatalog *wr_catalog =
    static_cast<const WritableCatalog *>(catalog);
  // This variable will contain the number of dirty catalogs in the sub tree
  // with *catalog as it's root.
  int dirty_catalogs = (wr_catalog->IsDirty()) ? 1 : 0;

  // Look for dirty catalogs in the descendants of *catalog
  CatalogList children = wr_catalog->GetChildren();
  for (CatalogList::const_iterator i = children.begin(), iEnd = children.end();
       i != iEnd; ++i)
  {
    dirty_catalogs += GetModifiedCatalogsRecursively(*i, result);
  }

  // If we found a dirty catalog in the checked sub tree, the root (*catalog)
  // must be snapshot and ends up in the result list
  if (dirty_catalogs > 0) {
    result->push_back(const_cast<WritableCatalog *>(wr_catalog));
  }

  // tell the upper layer about number of catalogs
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

  LogCvmfs(kLogCvmfs, kLogStdout, "creating snapshot of catalog '%s'",
           catalog->path().c_str());

	const string clg_path = catalog->path().ToString();
	const string cat_path = (clg_path.empty()) ?
	                            catalog_directory_ :
                              catalog_directory_ + clg_path;

	/* Data symlink, whitelist symlink */
	string backlink = "../";
	string parent = GetParentPath(cat_path);
	while (parent != GetParentPath(data_directory_)) {
		if (parent == "") {
			PrintWarning("cannot find data dir");
			break;
		}
		parent = GetParentPath(parent);
		backlink += "../";
	}

	const string lnk_path_data = cat_path + "/data";
	const string lnk_path_whitelist = cat_path + "/.cvmfswhitelist";
	const string backlink_data = backlink + GetFileName(data_directory_);
	const string backlink_whitelist = backlink + GetFileName(catalog_directory_) + "/.cvmfswhitelist";

	platform_stat64 info;
	if (platform_lstat(lnk_path_data.c_str(), &info) != 0)
	{
		if (symlink(backlink_data.c_str(), lnk_path_data.c_str()) != 0) {
			PrintWarning("cannot create catalog store -> data store symlink");
		}
	}

	/* Don't make the symlink for the root catalog */
	if ((platform_lstat(lnk_path_whitelist.c_str(), &info) != 0) && (GetParentPath(cat_path) != GetParentPath(data_directory_)))
	{
		if (symlink(backlink_whitelist.c_str(), lnk_path_whitelist.c_str()) != 0) {
			PrintWarning("cannot create whitelist symlink");
		}
	}

	if (!catalog->UpdateLastModified()) {
		PrintWarning("failed to update last modified time stamp");
	}
	if (!catalog->IncrementRevision()) {
		PrintWarning("failed to increase revision");
	}

	/* Previous revision */
	map<char, string> ext_chksum;
	if (ParseKeyvalPath(cat_path + "/.cvmfspublished", &ext_chksum)) {
		map<char, string>::const_iterator i = ext_chksum.find('C');
		if (i != ext_chksum.end()) {
			hash::Any sha1_previous(hash::kSha1, hash::HexPtr(i->second));

			if (!catalog->SetPreviousRevision(sha1_previous)) {
				PrintWarning("failed store previous catalog revision " +
                     sha1_previous.ToString());
			}
		} else {
			PrintWarning("failed to find catalog SHA1 key in .cvmfspublished");
		}
	}

	/* Compress catalog */
	const string src_path = cat_path + "/.cvmfscatalog.working";
	const string dst_path = data_directory_ + "/txn/compressing.catalog";
	//const string dst_path = cat_path + "/.cvmfscatalog";
	hash::Any sha1(hash::kSha1);
	FILE *fsrc = NULL, *fdst = NULL;
	int fd_dst;

	if ( !(fsrc = fopen(src_path.c_str(), "r")) ||
	     (fd_dst = open(dst_path.c_str(), O_CREAT | O_TRUNC | O_RDWR, kDefaultFileMode)) < 0 ||
	     !(fdst = fdopen(fd_dst, "w")) ||
       !zlib::CompressFile2File(fsrc, fdst, &sha1) )
	{
		PrintWarning("could not compress catalog '" + src_path + "'");

	} else {
		const string sha1str = sha1.ToString();
		const string hash_name = sha1str.substr(0, 2) + "/" + sha1str.substr(2) + "C";
		const string cache_path = data_directory_ + "/" + hash_name;
		if (rename(dst_path.c_str(), cache_path.c_str()) != 0) {
			PrintWarning("could not store catalog in data store as " + cache_path);
		}
	}
	if (fsrc) fclose(fsrc);
	if (fdst) fclose(fdst);

	/* Create extended checksum */
	FILE *fpublished = fopen((cat_path + "/.cvmfspublished").c_str(), "w");
	if (fpublished) {
		string fields = "C" + sha1.ToString() + "\n";
		fields += "R" + hash::Md5(hash::AsciiPtr(clg_path)).ToString() + "\n";

		/* Extra fields */
		DirectoryEntry d;
		if (not catalog->LookupPath(catalog->path(), &d)) {
			PrintWarning("failed to find root entry");
		}
		fields += "L" + d.checksum().ToString() + "\n";
		fields += "D" + StringifyInt(catalog->GetTTL()) + "\n";
		fields += "S" + StringifyInt(catalog->GetRevision()) + "\n";

		if (fwrite(&(fields[0]), 1, fields.length(), fpublished) != fields.length()) {
			PrintWarning("failed to write extended checksum");
		}
		fclose(fpublished);

	} else {
		PrintWarning("failed to write extended checksum");
	}

	/* Update registered catalog SHA1 in nested catalog */
	// TODO: revision hint
	//       this might be done implicitly when snapshoting a nested catalog
	//       Catalogs know about their parent catalog!
	if (not catalog->IsRoot()) {
		LogCvmfs(kLogCvmfs, kLogStdout, "updating nested catalog link");

		// TODO: this is fishy! but I leave it this way for the moment
		//       (dynamic_cast<> at least dies, if something goes wrong)
		if (not dynamic_cast<WritableCatalog*>(catalog->parent())->UpdateNestedCatalog(clg_path, sha1)) {
			PrintWarning("failed to register modified catalog at " + clg_path +
                   " in parent catalog");
		}
	}

	/* Compress and write SHA1 checksum */
	char chksum[40];
	int lchksum = 40;
	memcpy(chksum, &((sha1.ToString())[0]), 40);
	void *compr_buf = NULL;
	int64_t compr_size;
	if (!zlib::CompressMem2Mem(chksum, lchksum, &compr_buf, &compr_size)) {
		PrintWarning("could not compress catalog checksum");
	}
	if (compr_buf) free(compr_buf);

  return true;
}

}
