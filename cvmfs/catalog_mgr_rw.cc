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
#include "download.h"
#include "manifest.h"
#include "upload.h"

using namespace std;  // NOLINT

namespace catalog {

WritableCatalogManager::WritableCatalogManager(const hash::Any &base_hash,
                                               const std::string &stratum0,
                                               const string &dir_temp,
                                               upload::Spooler *spooler)
{
  base_hash_ = base_hash;
  stratum0_ = stratum0;
  dir_temp_ = dir_temp;
  spooler_ = spooler;
  Init();
}


/**
 * Initializes the WritableCatalogManager, creates a new repository
 * if necessary.
 * @return true on success, false otherwise
 */
bool WritableCatalogManager::Init() {
  bool succeeded = AbstractCatalogManager::Init();

  if (!succeeded) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "unable to init catalog manager (cannot create root catalog)");
    return false;
  }

	return true;
}


/**
 * Loads a catalog via HTTP from Statum 0 into a temporary file.
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
  hash::Any effective_hash = hash.IsNull() ? base_hash_ : hash;
  const string url = stratum0_ + "/data" + effective_hash.MakePath(1, 2) + "C";
  FILE *fcatalog = CreateTempFile(dir_temp_ + "/catalog", 0666, "w",
                                  catalog_path);
  download::JobInfo download_catalog(&url, true, false, fcatalog,
                                     &effective_hash);

  download::Failures retval = download::Fetch(&download_catalog);
  fclose(fcatalog);

  if (retval != download::kFailOk) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "failed to load %s from Stratum 0 (%d)", url.c_str(), retval);
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
 * The new root catalog will already contain a root entry.
 * It is uploaded by a Forklift to the upstream storage.
 * @return true on success, false otherwise
 */
Manifest *WritableCatalogManager::CreateRepository(
  const string &dir_temp,
  upload::Spooler *spooler)
{
  // Create a new root catalog at file_path
  string file_path = dir_temp + "/new_root_catalog";

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
    LogCvmfs(kLogCatalog, kLogStderr, "creation of catalog '%s' failed",
             file_path.c_str());
    return NULL;
  }

  // Compress root catalog;
  string file_path_compressed = file_path + ".compressed";
  hash::Any hash_catalog(hash::kSha1);
  bool retval = zlib::CompressPath2Path(file_path, file_path_compressed,
                                        &hash_catalog);
  if (!retval) {
    LogCvmfs(kLogCatalog, kLogStderr, "compression of catalog '%s' failed",
             file_path.c_str());
    unlink(file_path.c_str());
    return NULL;
  }
  unlink(file_path.c_str());

  // Create manifest
  const string manifest_path = dir_temp + "/manifest";
  Manifest *manifest = new Manifest(hash_catalog, "");

  // Upload catalog
  spooler->SpoolCopy(file_path_compressed,
                     "data" + hash_catalog.MakePath(1, 2) + "C");
  spooler->EndOfTransaction();
  while (!spooler->IsIdle())
    sleep(1);
  unlink(file_path_compressed.c_str());
  if (spooler->num_errors() > 0) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to commit catalog %s",
             file_path_compressed.c_str());
    return false;
  }

  return manifest;
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

	if (result == false) {
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
 * Create a new nested catalog.  Includes moving all entries belonging there
 * from it's parent catalog.
 * @param mountpoint the path of the directory to become a nested root
 * @return true on success, false otherwise
 */
bool WritableCatalogManager::CreateNestedCatalog(const std::string &mountpoint)
{
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
  const string database_file_path = CreateTempPath(dir_temp_ + "/catalog",
                                                   0666);
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
    CreateCatalog(PathString(nested_root_path.data(), nested_root_path.length()),
                  old_catalog);
  if (!AttachCatalog(database_file_path, new_catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to create nested catalog '%s': "
             "unable to attach newly created nested catalog",
             mountpoint.c_str());
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
         (nested_catalog->path().ToString() == nested_root_path));

  // Merge all data from the nested catalog into it's parent
  if (!nested_catalog->MergeIntoParent()) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to remove nested catalog '%s': "
             "merging of content unsuccessful.",
             nested_catalog->path().c_str());
    return false;
  }

  // Delete the catalog database file from the working copy
  if (unlink(nested_catalog->database_path().c_str()) != 0) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "unable to delete the removed nested catalog database file '%s'",
             nested_catalog->database_path().c_str());
    return false;
  }

  // Remove the catalog from internal data structures
  DetachCatalog(nested_catalog);

  return true;
}


bool WritableCatalogManager::PrecalculateListings() {
  // TODO
  return true;
}


Manifest *WritableCatalogManager::Commit() {
  reinterpret_cast<WritableCatalog *>(GetRootCatalog())->SetDirty();
  WritableCatalogList catalogs_to_snapshot;
  GetModifiedCatalogs(&catalogs_to_snapshot);

  Manifest *result = NULL;
  for (WritableCatalogList::iterator i = catalogs_to_snapshot.begin(),
       iEnd = catalogs_to_snapshot.end(); i != iEnd; ++i)
  {
    (*i)->Commit();
    hash::Any hash = SnapshotCatalog(*i);
    if ((*i)->IsRoot()) {
      base_hash_ = hash;
      LogCvmfs(kLogCatalog, kLogStdout, "Wait for upload of catalogs");
      while (!spooler_->IsIdle())
        sleep(1);
      if (spooler_->num_errors() > 0) {
        LogCvmfs(kLogCatalog, kLogStderr, "failed to commit catalogs");
        return false;
      }

      // .cvmfspublished
      LogCvmfs(kLogCatalog, kLogStdout, "Committing repository manifest");
      result = new Manifest(hash, "");
      result->set_ttl((*i)->GetTTL());
      result->set_revision((*i)->GetRevision());
    }
  }

  return result;
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
  if (dirty_catalogs > 0)
    result->push_back(const_cast<WritableCatalog *>(wr_catalog));

  // tell the upper layer about number of catalogs
  return dirty_catalogs;
}


/**
 * Makes a new catalog revision.  Compresses and uploads catalog.  Returns
 * content hash.
 */
hash::Any WritableCatalogManager::SnapshotCatalog(WritableCatalog *catalog)
  const
{
  LogCvmfs(kLogCvmfs, kLogStdout, "creating snapshot of catalog '%s'",
           catalog->path().c_str());

  if (!catalog->UpdateLastModified()) {
		PrintError("failed to update last modified time stamp");
    return hash::Any();
	}
	if (!catalog->IncrementRevision()) {
		PrintError("failed to increase revision");
    return hash::Any();
	}

	// Previous revision
  if (catalog->IsRoot()) {
    catalog->SetPreviousRevision(base_hash_);
  } else {
    hash::Any hash_previous;
    catalog->parent()->FindNested(catalog->path(), &hash_previous);
    catalog->SetPreviousRevision(hash_previous);
  }

	// Compress catalog
  hash::Any hash_catalog(hash::kSha1);
  if (!zlib::CompressPath2Path(catalog->database_path(),
                               catalog->database_path() + ".compressed",
                               &hash_catalog))
  {
		PrintError("could not compress catalog " + catalog->path().ToString());
    return hash::Any();
	}

  // Upload catalog
  spooler_->SpoolCopy(catalog->database_path() + ".compressed",
                      "data" + hash_catalog.MakePath(1, 2) + "C");

	/* Update registered catalog SHA1 in nested catalog */
	if (!catalog->IsRoot()) {
		LogCvmfs(kLogCvmfs, kLogStdout, "updating nested catalog link");
    WritableCatalog *parent = static_cast<WritableCatalog *>(catalog->parent());
		if (!parent->UpdateNestedCatalog(catalog->path().ToString(), hash_catalog))
    {
			PrintError("failed to register modified catalog at " +
                 catalog->path().ToString() + " in parent catalog");
      return hash::Any();
		}
	}

  return hash_catalog;
}

}
