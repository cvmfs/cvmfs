/**
 * This file is part of the CernVM file system.
 */

#define __STDC_FORMAT_MACROS

#include "catalog_mgr_rw.h"

#include <unistd.h>
#include <inttypes.h>

#include <cassert>
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
#include "smalloc.h"

using namespace std;  // NOLINT

namespace catalog {

WritableCatalogManager::WritableCatalogManager(const hash::Any   &base_hash,
                                               const std::string &stratum0,
                                               const string      &dir_temp,
                                               upload::Spooler   *spooler)
{
  sync_lock_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(sync_lock_, NULL);
  assert(retval == 0);
  base_hash_ = base_hash;
  stratum0_ = stratum0;
  dir_temp_ = dir_temp;
  spooler_ = spooler;
  Init();
}


WritableCatalogManager::~WritableCatalogManager() {
  pthread_mutex_destroy(sync_lock_);
  free(sync_lock_);
}


bool WritableCatalogManager::Init() {
  return AbstractCatalogManager::Init();
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
  if (!fcatalog) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "failed to create temp file when loading %s", url.c_str());
    assert(false);
  }

  download::JobInfo download_catalog(&url, true, false, fcatalog,
                                     &effective_hash);

  download::Failures retval = download::Fetch(&download_catalog);
  fclose(fcatalog);

  if (retval != download::kFailOk) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "failed to load %s from Stratum 0 (%d)", url.c_str(), retval);
    assert(false);
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
manifest::Manifest *WritableCatalogManager::CreateRepository(
  const string     &dir_temp,
  upload::Spooler  *spooler)
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
  root_entry.uid_               = getuid();
  root_entry.gid_               = getgid();
  root_entry.checksum_          = hash::Any(hash::kSha1);
  root_entry.linkcount_         = 2;
  string root_path = "";

  // Create the database schema and the inital root entry
  if (!Database::Create(file_path, root_entry, root_path)) {
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
  manifest::Manifest *manifest = new manifest::Manifest(hash_catalog, "");

  // Upload catalog
  spooler->Upload(file_path_compressed,
                  "data" + hash_catalog.MakePath(1, 2) + "C");
  spooler->WaitForUpload();
  spooler->WaitForTermination();
  unlink(file_path_compressed.c_str());
  if (spooler->GetNumberOfErrors() > 0) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to commit catalog %s",
             file_path_compressed.c_str());
    delete manifest;
    return NULL;
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
void WritableCatalogManager::RemoveFile(const std::string &path) {
  const string file_path = MakeRelativePath(path);
	const string parent_path = GetParentPath(file_path);

  SyncLock();
  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr, "catalog for file '%s' cannot be found",
             file_path.c_str());
    assert(false);
  }

  catalog->RemoveEntry(file_path);
  SyncUnlock();
}


/**
 * Remove the given directory from the catalogs.
 * @param directory_path the full path to the directory to be removed
 * @return true on success, false otherwise
 */
void WritableCatalogManager::RemoveDirectory(const std::string &path) {
  const string directory_path = MakeRelativePath(path);
	const string parent_path = GetParentPath(directory_path);

  SyncLock();
  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "catalog for directory '%s' cannot be found",
             directory_path.c_str());
    assert(false);
  }

  DirectoryEntry parent_entry;
  if (!catalog->LookupPath(PathString(parent_path.data(), parent_path.length()),
                           &parent_entry))
  {
    LogCvmfs(kLogCatalog, kLogStderr,
             "parent directory of directory '%s' not found",
             directory_path.c_str());
    assert(false);
  }

  parent_entry.set_linkcount(parent_entry.linkcount() - 1);

  catalog->RemoveEntry(directory_path);
  catalog->UpdateEntry(parent_entry, parent_path);
  if (parent_entry.IsNestedCatalogRoot()) {
    LogCvmfs(kLogCatalog, kLogVerboseMsg, "updating transition point %s",
             parent_path.c_str());
    WritableCatalog *parent_catalog =
      reinterpret_cast<WritableCatalog *>(catalog->parent());
    parent_entry.set_is_nested_catalog_mountpoint(true);
    parent_entry.set_is_nested_catalog_root(false);
    parent_catalog->UpdateEntry(parent_entry, parent_path);
  }
  SyncUnlock();
}


/**
 * Add a new directory to the catalogs.
 * @param entry a DirectoryEntry structure describing the new directory
 * @param parent_directory the absolute path of the directory containing the
 *                         directory to be created
 * @return true on success, false otherwise
 */
void WritableCatalogManager::AddDirectory(const DirectoryEntryBase &entry,
                                          const std::string &parent_directory)
{
  const string parent_path = MakeRelativePath(parent_directory);
  string directory_path = parent_path + "/";
  directory_path.append(entry.name().GetChars(), entry.name().GetLength());

  SyncLock();
  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "catalog for directory '%s' cannot be found",
             directory_path.c_str());
    assert(false);
  }
  DirectoryEntry parent_entry;
  if (!catalog->LookupPath(PathString(parent_path.data(), parent_path.length()),
                           &parent_entry))
  {
    LogCvmfs(kLogCatalog, kLogStderr,
             "parent directory for directory '%s' cannot be found",
             directory_path.c_str());
    assert(false);
  }

  DirectoryEntry fixed_hardlink_count(entry);
  fixed_hardlink_count.set_linkcount(2);
  catalog->AddEntry(fixed_hardlink_count, directory_path, parent_path);

  parent_entry.set_linkcount(parent_entry.linkcount() + 1);
  catalog->UpdateEntry(parent_entry, parent_path);
  if (parent_entry.IsNestedCatalogRoot()) {
    LogCvmfs(kLogCatalog, kLogVerboseMsg, "updating transition point %s",
             parent_path.c_str());
    WritableCatalog *parent_catalog =
      reinterpret_cast<WritableCatalog *>(catalog->parent());
    parent_entry.set_is_nested_catalog_mountpoint(true);
    parent_entry.set_is_nested_catalog_root(false);
    parent_catalog->UpdateEntry(parent_entry, parent_path);
  }
  SyncUnlock();
}

/**
 * Add a new file to the catalogs.
 * @param entry a DirectoryEntry structure describing the new file
 * @param parent_directory the absolute path of the directory containing the
 *                         file to be created
 * @return true on success, false otherwise
 */
void WritableCatalogManager::AddFile(const DirectoryEntry  &entry,
                                     const std::string     &parent_directory) {
  const string parent_path = MakeRelativePath(parent_directory);
  const string file_path   = entry.GetFullPath(parent_path);

  SyncLock();
  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr, "catalog for file '%s' cannot be found",
             file_path.c_str());
    assert(false);
  }

  assert(!entry.IsRegular() || !entry.checksum().IsNull());
  catalog->AddEntry(entry, file_path, parent_path);
  SyncUnlock();
}


void WritableCatalogManager::AddChunkedFile(const DirectoryEntryBase  &entry,
                                            const std::string         &parent_directory,
                                            const FileChunks          &file_chunks) {
  assert (file_chunks.size() > 0);

  DirectoryEntry full_entry(entry);
  full_entry.set_is_chunked_file(true);

  AddFile(full_entry, parent_directory);

  const string parent_path = MakeRelativePath(parent_directory);
  const string file_path   = entry.GetFullPath(parent_path);

  SyncLock();
  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr, "catalog for file '%s' cannot be found",
             file_path.c_str());
    assert(false);
  }

  FileChunks::const_iterator i    = file_chunks.begin();
  FileChunks::const_iterator iend = file_chunks.end();
  for (; i != iend; ++i) {
    catalog->AddFileChunk(file_path, *i);
  }
  SyncUnlock();
}


/**
 * Add a hardlink group to the catalogs.
 * @param entries a list of DirectoryEntries describing the new files
 * @param parent_directory the absolute path of the directory containing the
 *                         files to be created
 * @return true on success, false otherwise
 */
void WritableCatalogManager::AddHardlinkGroup(DirectoryEntryBaseList &entries,
                                              const std::string &parent_directory)
{
  assert(entries.size() >= 1);
  if (entries.size() == 1)
    return AddFile(entries[0], parent_directory);

  LogCvmfs(kLogCatalog, kLogVerboseMsg, "adding hardlink group %s/%s",
           parent_directory.c_str(), entries[0].name().c_str());

  // Hardlink groups have to reside in the same directory.
  // Therefore we only have one parent directory here
  const string parent_path = MakeRelativePath(parent_directory);

  SyncLock();
  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "catalog for hardlink group containing '%s' cannot be found",
             parent_path.c_str());
    assert(false);
  }

  // Get a valid hardlink group id for the catalog the group will end up in
  // TODO: Compaction
  uint32_t new_group_id = catalog->GetMaxLinkId() + 1;
  LogCvmfs(kLogCatalog, kLogVerboseMsg, "hardlink group id %u issued",
           new_group_id);
  assert(new_group_id > 0);

  // Add the file entries to the catalog
  for (DirectoryEntryBaseList::iterator i = entries.begin(), iEnd = entries.end();
       i != iEnd; ++i)
  {
    string file_path = parent_path + "/";
    file_path.append(i->name().GetChars(), i->name().GetLength());

    // create a full fledged DirectoryEntry to add the hardlink group to it
    // which is CVMFS specific meta data.
    DirectoryEntry hardlink(*i);
    hardlink.set_hardlink_group(new_group_id);
    hardlink.set_linkcount(entries.size());

    catalog->AddEntry(hardlink, file_path, parent_path);
  }
  SyncUnlock();
}


void WritableCatalogManager::ShrinkHardlinkGroup(const string &remove_path) {
  const string relative_path = MakeRelativePath(remove_path);

  SyncLock();
  WritableCatalog *catalog;
  if (!FindCatalog(relative_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "catalog for hardlink group containing '%s' cannot be found",
             remove_path.c_str());
    assert(false);
  }

  catalog->IncLinkcount(relative_path, -1);
  SyncUnlock();
}


/**
 * Update entry meta data (mode, owner, ...).
 * CVMFS specific meta data are NOT changed by this method.
 * @param entry      the directory entry to be touched
 * @param path       the path of the directory entry to be touched
 */
void WritableCatalogManager::TouchFile(const DirectoryEntryBase &entry,
                                       const std::string &file_path) {
  assert (!entry.IsDirectory());

  const string entry_path = MakeRelativePath(file_path);
  const string parent_path = GetParentPath(entry_path);

  SyncLock();
  // find the catalog to be updated
  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr, "catalog for entry '%s' cannot be found",
             entry_path.c_str());
    assert(false);
  }

  catalog->TouchEntry(entry, entry_path);
  SyncUnlock();
}


/**
 * Update entry meta data (mode, owner, ...).
 * CVMFS specific meta data (i.e. nested catalog transition points) are NOT
 * changed by this method, although transition points intrinsics are taken into
 * account, to keep nested catalogs consistent.
 * @param entry      the directory entry to be touched
 * @param path       the path of the directory entry to be touched
 */
void WritableCatalogManager::TouchDirectory(const DirectoryEntryBase &entry,
                                            const std::string &directory_path)
{
  assert(entry.IsDirectory());

  const string entry_path = MakeRelativePath(directory_path);
  const string parent_path = GetParentPath(entry_path);

  SyncLock();
  // find the catalog to be updated
  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr, "catalog for entry '%s' cannot be found",
             entry_path.c_str());
    assert(false);
  }

  catalog->TouchEntry(entry, entry_path);

  // since we deal with a directory here, we might just touch a
  // nested catalog transition point. If this is the case we would need to
  // update two catalog entries:
  //   * the nested catalog MOUNTPOINT in the parent catalog
  //   * the nested catalog ROOT in the nested catalog

  // first check if we really have a nested catalog transition point
  catalog::DirectoryEntry potential_transition_point;
  PathString transition_path(entry_path.data(), entry_path.length());
  bool retval = catalog->LookupPath(transition_path,
                                    &potential_transition_point);
  assert(retval);
  if (potential_transition_point.IsNestedCatalogMountpoint()) {
    LogCvmfs(kLogCatalog, kLogVerboseMsg,
             "updating transition point at %s", entry_path.c_str());

    // find and mount nested catalog assciated to this transition point
    hash::Any nested_hash;
    retval = catalog->FindNested(transition_path, &nested_hash);
    assert(retval);
    Catalog *nested_catalog;
    nested_catalog = MountCatalog(transition_path, nested_hash, catalog);
    assert(nested_catalog != NULL);

    // update nested catalog root in the child catalog
    reinterpret_cast<WritableCatalog *>(nested_catalog)->
      TouchEntry(entry, entry_path);
  }

  SyncUnlock();
}


/**
 * Create a new nested catalog.  Includes moving all entries belonging there
 * from it's parent catalog.
 * @param mountpoint the path of the directory to become a nested root
 * @return true on success, false otherwise
 */
void WritableCatalogManager::CreateNestedCatalog(const std::string &mountpoint)
{
  const string nested_root_path = MakeRelativePath(mountpoint);

  SyncLock();
  // Find the catalog currently containing the directory structure, which
  // will be represented as a new nested catalog from now on
  WritableCatalog *old_catalog = NULL;
  if (!FindCatalog(nested_root_path, &old_catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to create nested catalog '%s': "
             "mountpoint was not found in current catalog structure",
             nested_root_path.c_str());
    assert(false);
  }

  // Get the DirectoryEntry for the given path, this will serve as root
  // entry for the nested catalog we are about to create
  DirectoryEntry new_root_entry;
  bool retval = old_catalog->LookupPath(PathString(nested_root_path.data(),
                                        nested_root_path.length()),
                                        &new_root_entry);
  assert(retval);

  // Create the database schema and the inital root entry
  // for the new nested catalog
  const string database_file_path = CreateTempPath(dir_temp_ + "/catalog",
                                                   0666);
  retval =
    Database::Create(database_file_path, new_root_entry, nested_root_path);
  assert(retval);

  // Attach the just created nested catalog
  Catalog *new_catalog =
    CreateCatalog(PathString(nested_root_path.data(), nested_root_path.length()),
                  old_catalog);
  retval = AttachCatalog(database_file_path, new_catalog);
  assert(retval);

  assert(new_catalog->IsWritable());
  WritableCatalog *wr_new_catalog = static_cast<WritableCatalog *>(new_catalog);

  // From now on, there are two catalogs, spanning the same directory structure
  // we have to split the overlapping directory entries from the old catalog
  // to the new catalog to re-gain a valid catalog structure
  old_catalog->Partition(wr_new_catalog);

  // Add the newly created nested catalog to the references of the containing
  // catalog
  old_catalog->InsertNestedCatalog(new_catalog->path().ToString(), NULL,
                                   hash::Any(hash::kSha1));

  // Fix subtree counters in new nested catalogs: subtree is the sum of all
  // entries of all "grand-nested" catalogs
  Catalog::NestedCatalogList *grand_nested =
    wr_new_catalog->ListNestedCatalogs();
  DeltaCounters fix_subtree_counters;
  for (Catalog::NestedCatalogList::const_iterator i = grand_nested->begin(),
       iEnd = grand_nested->end(); i != iEnd; ++i)
  {
    WritableCatalog *grand_catalog;
    retval = FindCatalog(i->path.ToString(), &grand_catalog);
    assert(retval);
    Counters grand_counters;
    retval = grand_catalog->GetCounters(&grand_counters);
    assert(retval);
    grand_counters.AddAsSubtree(&fix_subtree_counters);
  }
  DeltaCounters save_counters = wr_new_catalog->delta_counters_;
  wr_new_catalog->delta_counters_ = fix_subtree_counters;
  wr_new_catalog->UpdateCounters();
  wr_new_catalog->delta_counters_ = save_counters;

  SyncUnlock();
}


/**
 * Remove a nested catalog.
 * When you remove a nested catalog all entries currently held by it
 * will be merged into its parent catalog.
 * @param mountpoint the path of the nested catalog to be removed
 * @return true on success, false otherwise
 */
void WritableCatalogManager::RemoveNestedCatalog(const string &mountpoint) {
  const string nested_root_path = MakeRelativePath(mountpoint);

  SyncLock();
  // Find the catalog which should be removed
  WritableCatalog *nested_catalog = NULL;
  if (!FindCatalog(nested_root_path, &nested_catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to remove nested catalog '%s': "
             "mountpoint was not found in current catalog structure",
             nested_root_path.c_str());
    assert(false);
  }

  // Check if the found catalog is really the nested catalog to be deleted
  assert(!nested_catalog->IsRoot() &&
         (nested_catalog->path().ToString() == nested_root_path));

  // Merge all data from the nested catalog into it's parent
  nested_catalog->MergeIntoParent();

  // Delete the catalog database file from the working copy
  if (unlink(nested_catalog->database_path().c_str()) != 0) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "unable to delete the removed nested catalog database file '%s'",
             nested_catalog->database_path().c_str());
    assert(false);
  }

  // Remove the catalog from internal data structures
  DetachCatalog(nested_catalog);
  SyncUnlock();
}


void WritableCatalogManager::PrecalculateListings() {
  // TODO
}


manifest::Manifest *WritableCatalogManager::Commit() {
  reinterpret_cast<WritableCatalog *>(GetRootCatalog())->SetDirty();
  WritableCatalogList catalogs_to_snapshot;
  GetModifiedCatalogs(&catalogs_to_snapshot);

  manifest::Manifest *result = NULL;
  for (WritableCatalogList::iterator i = catalogs_to_snapshot.begin(),
       iEnd = catalogs_to_snapshot.end(); i != iEnd; ++i)
  {
    (*i)->Commit();
    hash::Any hash = SnapshotCatalog(*i);
    if ((*i)->IsRoot()) {
      base_hash_ = hash;
      LogCvmfs(kLogCatalog, kLogVerboseMsg, "waiting for upload of catalogs");
      spooler_->WaitForUpload();
      if (spooler_->GetNumberOfErrors() > 0) {
        LogCvmfs(kLogCatalog, kLogStderr, "failed to commit catalogs");
        return NULL;
      }

      // .cvmfspublished
      LogCvmfs(kLogCatalog, kLogVerboseMsg, "Committing repository manifest");
      result = new manifest::Manifest(hash, "");
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
  LogCvmfs(kLogCatalog, kLogVerboseMsg, "creating snapshot of catalog '%s'",
           catalog->path().c_str());

  catalog->UpdateCounters();
  if (catalog->parent()) {
    catalog->delta_counters_.PopulateToParent(
      &catalog->GetWritableParent()->delta_counters_);
  }
  catalog->delta_counters_.SetZero();

  catalog->UpdateLastModified();
  catalog->IncrementRevision();

	// Previous revision
  if (catalog->IsRoot()) {
    catalog->SetPreviousRevision(base_hash_);
  } else {
    hash::Any hash_previous;
    const bool retval =
      catalog->parent()->FindNested(catalog->path(), &hash_previous);
    assert (retval);
    catalog->SetPreviousRevision(hash_previous);
  }

	// Compress catalog
  hash::Any hash_catalog(hash::kSha1);
  if (!zlib::CompressPath2Path(catalog->database_path(),
                               catalog->database_path() + ".compressed",
                               &hash_catalog))
  {
		PrintError("could not compress catalog " + catalog->path().ToString());
    assert(false);
	}

  // Upload catalog
  spooler_->Upload(catalog->database_path() + ".compressed",
                   "data" + hash_catalog.MakePath(1, 2) + "C");

	// Update registered catalog SHA1 in nested catalog
	if (!catalog->IsRoot()) {
		LogCvmfs(kLogCatalog, kLogVerboseMsg, "updating nested catalog link");
    WritableCatalog *parent = static_cast<WritableCatalog *>(catalog->parent());
		parent->UpdateNestedCatalog(catalog->path().ToString(), hash_catalog);
	}

  return hash_catalog;
}

}
