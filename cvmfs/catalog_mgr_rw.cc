/**
 * This file is part of the CernVM file system.
 */

#define __STDC_FORMAT_MACROS

#include "catalog_mgr_rw.h"

#include <inttypes.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "catalog_balancer.h"
#include "catalog_rw.h"
#include "logging.h"
#include "manifest.h"
#include "smalloc.h"
#include "statistics.h"
#include "upload.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace catalog {

WritableCatalogManager::WritableCatalogManager(
  const shash::Any          &base_hash,
  const std::string         &stratum0,
  const string              &dir_temp,
  upload::Spooler           *spooler,
  download::DownloadManager *download_manager,
  bool                       enforce_limits,
  const unsigned             nested_kcatalog_limit,
  const unsigned             root_kcatalog_limit,
  const unsigned             file_mbyte_limit,
  perf::Statistics          *statistics,
  bool                       is_balanceable,
  unsigned                   max_weight,
  unsigned                   min_weight)
  : SimpleCatalogManager(base_hash, stratum0, dir_temp, download_manager,
      statistics)
  , spooler_(spooler)
  , enforce_limits_(enforce_limits)
  , nested_kcatalog_limit_(nested_kcatalog_limit)
  , root_kcatalog_limit_(root_kcatalog_limit)
  , file_mbyte_limit_(file_mbyte_limit)
  , is_balanceable_(is_balanceable)
  , max_weight_(max_weight)
  , min_weight_(min_weight)
  , balance_weight_(max_weight / 2)
{
  sync_lock_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(sync_lock_, NULL);
  assert(retval == 0);
  catalog_processing_lock_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  retval = pthread_mutex_init(catalog_processing_lock_, NULL);
  assert(retval == 0);
}


WritableCatalogManager::~WritableCatalogManager() {
  pthread_mutex_destroy(sync_lock_);
  free(sync_lock_);
  pthread_mutex_destroy(catalog_processing_lock_);
  free(catalog_processing_lock_);
}


/**
 * This method is virtual in AbstractCatalogManager.  It returns a new catalog
 * structure in the form the different CatalogManagers need it.
 * In this case it returns a stub for a WritableCatalog.
 * @param mountpoint     the mount point of the catalog stub to create
 * @param catalog_hash   the content hash of the catalog to create
 * @param parent_catalog the parent of the catalog stub to create
 * @return a pointer to the catalog stub structure created
 */
Catalog* WritableCatalogManager::CreateCatalog(
  const PathString &mountpoint,
  const shash::Any &catalog_hash,
  Catalog          *parent_catalog)
{
  return new WritableCatalog(mountpoint.ToString(),
                             catalog_hash,
                             parent_catalog);
}


void WritableCatalogManager::ActivateCatalog(Catalog *catalog) {
  catalog->TakeDatabaseFileOwnership();
}


/**
 * This method is invoked if we create a completely new repository.
 * The new root catalog will already contain a root entry.
 * It is uploaded by a Forklift to the upstream storage.
 * @return true on success, false otherwise
 */
manifest::Manifest *WritableCatalogManager::CreateRepository(
  const string      &dir_temp,
  const bool         volatile_content,
  const std::string &voms_authz,
  upload::Spooler   *spooler)
{
  // Create a new root catalog at file_path
  string file_path = dir_temp + "/new_root_catalog";

  shash::Algorithms hash_algorithm = spooler->GetHashAlgorithm();

  // A newly created catalog always needs a root entry
  // we create and configure this here
  DirectoryEntry root_entry;
  root_entry.inode_             = DirectoryEntry::kInvalidInode;
  root_entry.mode_              = 16877;
  root_entry.size_              = 4096;
  root_entry.mtime_             = time(NULL);
  root_entry.uid_               = getuid();
  root_entry.gid_               = getgid();
  root_entry.checksum_          = shash::Any(hash_algorithm);
  root_entry.linkcount_         = 2;
  string root_path = "";

  // Create the database schema and the inital root entry
  {
    UniquePtr<CatalogDatabase> new_clg_db(CatalogDatabase::Create(file_path));
    if (!new_clg_db.IsValid() ||
        !new_clg_db->InsertInitialValues(root_path,
                                         volatile_content,
                                         voms_authz,
                                         root_entry))
    {
      LogCvmfs(kLogCatalog, kLogStderr, "creation of catalog '%s' failed",
               file_path.c_str());
      return NULL;
    }
  }

  // Compress root catalog;
  int64_t catalog_size = GetFileSize(file_path);
  if (catalog_size < 0) {
    unlink(file_path.c_str());
    return NULL;
  }
  string file_path_compressed = file_path + ".compressed";
  shash::Any hash_catalog(hash_algorithm, shash::kSuffixCatalog);
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
  manifest::Manifest *manifest =
    new manifest::Manifest(hash_catalog, catalog_size, "");
  if (!voms_authz.empty()) {
    manifest->set_has_alt_catalog_path(true);
  }

  // Upload catalog
  spooler->Upload(file_path_compressed, "data/" + hash_catalog.MakePath());
  spooler->WaitForUpload();
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
 * Retrieve the catalog containing the given path.
 * Other than AbstractCatalogManager::FindCatalog() this mounts nested
 * catalogs if necessary and returns  WritableCatalog objects.
 * Furthermore it optionally returns the looked-up DirectoryEntry.
 *
 * @param path    the path to look for
 * @param result  the retrieved catalog (as a pointer)
 * @param dirent  is set to looked up DirectoryEntry for 'path' if non-NULL
 * @return        true if catalog was found
 */
bool WritableCatalogManager::FindCatalog(const string     &path,
                                         WritableCatalog **result,
                                         DirectoryEntry   *dirent) {
  const PathString ps_path(path);

  Catalog *best_fit =
    AbstractCatalogManager<Catalog>::FindCatalog(ps_path);
  assert(best_fit != NULL);
  Catalog *catalog = NULL;
  bool retval = MountSubtree(ps_path, best_fit, &catalog);
  if (!retval)
    return false;

  catalog::DirectoryEntry dummy;
  if (NULL == dirent) {
    dirent = &dummy;
  }
  bool found = catalog->LookupPath(ps_path, dirent);
  if (!found || !catalog->IsWritable())
    return false;

  *result = static_cast<WritableCatalog *>(catalog);
  return true;
}


WritableCatalog *WritableCatalogManager::GetHostingCatalog(
  const std::string &path)
{
  WritableCatalog *result = NULL;
  bool retval = FindCatalog(MakeRelativePath(path), &result, NULL);
  if (!retval) return NULL;
  return result;
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
  DirectoryEntry parent_entry;
  if (!FindCatalog(parent_path, &catalog, &parent_entry)) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "catalog for directory '%s' cannot be found",
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
  DirectoryEntry parent_entry;
  if (!FindCatalog(parent_path, &catalog, &parent_entry)) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "catalog for directory '%s' cannot be found",
             directory_path.c_str());
    assert(false);
  }

  DirectoryEntry fixed_hardlink_count(entry);
  fixed_hardlink_count.set_linkcount(2);
  // No support for extended attributes on directories yet
  catalog->AddEntry(fixed_hardlink_count, empty_xattrs,
                    directory_path, parent_path);

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
void WritableCatalogManager::AddFile(
  const DirectoryEntry  &entry,
  const XattrList       &xattrs,
  const std::string     &parent_directory)
{
  const string parent_path = MakeRelativePath(parent_directory);
  const string file_path   = entry.GetFullPath(parent_path);

  SyncLock();
  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr, "catalog for file '%s' cannot be found",
             file_path.c_str());
    assert(false);
  }

  assert(!entry.IsRegular() || entry.IsChunkedFile() ||
         !entry.checksum().IsNull());
  assert(entry.IsRegular() || !entry.IsExternalFile());

  // check if file is too big
  unsigned mbytes = entry.size() / (1024 * 1024);
  if ((file_mbyte_limit_ > 0) && (mbytes > file_mbyte_limit_)) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "%s: file at %s is larger than %u megabytes (%u). "
             "CernVM-FS works best with small files. "
             "Please remove the file or increase the limit.",
             enforce_limits_ ? "FATAL" : "WARNING",
             file_path.c_str(),
             file_mbyte_limit_,
             mbytes);
    assert(!enforce_limits_);
  }

  catalog->AddEntry(entry, xattrs, file_path, parent_path);
  SyncUnlock();
}


void WritableCatalogManager::AddChunkedFile(
  const DirectoryEntryBase  &entry,
  const XattrList           &xattrs,
  const std::string         &parent_directory,
  const FileChunkList       &file_chunks)
{
  assert(file_chunks.size() > 0);

  DirectoryEntry full_entry(entry);
  full_entry.set_is_chunked_file(true);

  AddFile(full_entry, xattrs, parent_directory);

  const string parent_path = MakeRelativePath(parent_directory);
  const string file_path   = entry.GetFullPath(parent_path);

  SyncLock();
  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr, "catalog for file '%s' cannot be found",
             file_path.c_str());
    assert(false);
  }

  for (unsigned i = 0; i < file_chunks.size(); ++i) {
    catalog->AddFileChunk(file_path, *file_chunks.AtPtr(i));
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
void WritableCatalogManager::AddHardlinkGroup(
  const DirectoryEntryBaseList &entries,
  const XattrList &xattrs,
  const std::string &parent_directory,
  const FileChunkList &file_chunks)
{
  assert(entries.size() >= 1);
  assert(file_chunks.IsEmpty() || entries[0].IsRegular());
  if (entries.size() == 1) {
    DirectoryEntry fix_linkcount(entries[0]);
    fix_linkcount.set_linkcount(1);
    if (file_chunks.IsEmpty())
      return AddFile(fix_linkcount, xattrs, parent_directory);
    return AddChunkedFile(fix_linkcount, xattrs, parent_directory, file_chunks);
  }

  LogCvmfs(kLogCatalog, kLogVerboseMsg, "adding hardlink group %s/%s",
           parent_directory.c_str(), entries[0].name().c_str());

  // Hardlink groups have to reside in the same directory.
  // Therefore we only have one parent directory here
  const string parent_path = MakeRelativePath(parent_directory);

  // check if hard link is too big
  unsigned mbytes = entries[0].size() / (1024 * 1024);
  if ((file_mbyte_limit_ > 0) && (mbytes > file_mbyte_limit_)) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "%s: hard link at %s is larger than %u megabytes (%u). "
             "CernVM-FS works best with small files. "
             "Please remove the file or increase the limit.",
             enforce_limits_ ? "FATAL" : "WARNING",
             (parent_path + entries[0].name().ToString()).c_str(),
             file_mbyte_limit_,
             mbytes);
    assert(!enforce_limits_);
  }

  SyncLock();
  WritableCatalog *catalog;
  if (!FindCatalog(parent_path, &catalog)) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "catalog for hardlink group containing '%s' cannot be found",
             parent_path.c_str());
    assert(false);
  }

  // Get a valid hardlink group id for the catalog the group will end up in
  // TODO(unkown): Compaction
  uint32_t new_group_id = catalog->GetMaxLinkId() + 1;
  LogCvmfs(kLogCatalog, kLogVerboseMsg, "hardlink group id %u issued",
           new_group_id);
  assert(new_group_id > 0);

  // Add the file entries to the catalog
  for (DirectoryEntryBaseList::const_iterator i = entries.begin(),
       iEnd = entries.end(); i != iEnd; ++i)
  {
    string file_path = parent_path + "/";
    file_path.append(i->name().GetChars(), i->name().GetLength());

    // create a fully fledged DirectoryEntry to add the hardlink group to it
    // which is CVMFS specific meta data.
    DirectoryEntry hardlink(*i);
    hardlink.set_hardlink_group(new_group_id);
    hardlink.set_linkcount(entries.size());
    hardlink.set_is_chunked_file(!file_chunks.IsEmpty());

    catalog->AddEntry(hardlink, xattrs, file_path, parent_path);
    if (hardlink.IsChunkedFile()) {
      for (unsigned i = 0; i < file_chunks.size(); ++i) {
        catalog->AddFileChunk(file_path, *file_chunks.AtPtr(i));
      }
    }
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
    shash::Any nested_hash;
    uint64_t nested_size;
    retval = catalog->FindNested(transition_path, &nested_hash, &nested_size);
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
  const PathString ps_nested_root_path(nested_root_path);

  SyncLock();
  // Find the catalog currently containing the directory structure, which
  // will be represented as a new nested catalog and its root-entry/mountpoint
  // along the way
  WritableCatalog *old_catalog = NULL;
  DirectoryEntry new_root_entry;
  if (!FindCatalog(nested_root_path, &old_catalog, &new_root_entry)) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to create nested catalog '%s': "
             "mountpoint was not found in current catalog structure",
             nested_root_path.c_str());
    assert(false);
  }

  // Create the database schema and the inital root entry
  // for the new nested catalog
  const string database_file_path = CreateTempPath(dir_temp() + "/catalog",
                                                   0666);
  const bool volatile_content = false;
  CatalogDatabase *new_catalog_db = CatalogDatabase::Create(database_file_path);
  assert(NULL != new_catalog_db);
  // Note we do not set the external_data bit for nested catalogs
  bool retval =
           new_catalog_db->InsertInitialValues(nested_root_path,
                                               volatile_content,
                                               "",  // At this point, only root
                                                    // catalog gets VOMS authz
                                               new_root_entry);
  assert(retval);
  // TODO(rmeusel): we need a way to attach a catalog directy from an open
  // database to remove this indirection
  delete new_catalog_db;
  new_catalog_db = NULL;

  // Attach the just created nested catalog
  Catalog *new_catalog =
    CreateCatalog(ps_nested_root_path, shash::Any(), old_catalog);
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
  old_catalog->InsertNestedCatalog(new_catalog->mountpoint().ToString(), NULL,
                                   shash::Any(spooler_->GetHashAlgorithm()), 0);

  // Fix subtree counters in new nested catalogs: subtree is the sum of all
  // entries of all "grand-nested" catalogs
  // Note: taking a copy of the nested catalog list here
  const Catalog::NestedCatalogList &grand_nested =
    wr_new_catalog->ListOwnNestedCatalogs();
  DeltaCounters fix_subtree_counters;
  for (Catalog::NestedCatalogList::const_iterator i = grand_nested.begin(),
       iEnd = grand_nested.end(); i != iEnd; ++i)
  {
    WritableCatalog *grand_catalog;
    retval = FindCatalog(i->mountpoint.ToString(), &grand_catalog);
    assert(retval);
    const Counters &grand_counters = grand_catalog->GetCounters();
    grand_counters.AddAsSubtree(&fix_subtree_counters);
  }
  DeltaCounters save_counters = wr_new_catalog->delta_counters_;
  wr_new_catalog->delta_counters_ = fix_subtree_counters;
  wr_new_catalog->UpdateCounters();
  wr_new_catalog->delta_counters_ = save_counters;

  SyncUnlock();
}


/**
 * Remove a nested catalog
 *
 * If the merged parameter is true, when you remove a nested catalog
 * all entries currently held by it will be merged into its parent
 * catalog.
 * @param mountpoint - the path of the nested catalog to be removed
 * @param merge - merge the subtree associated with the nested catalog
 *                into its parent catalog
 * @return - true on success, false otherwise
 */
void WritableCatalogManager::RemoveNestedCatalog(const string &mountpoint,
                                                 const bool merge) {
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
         (nested_catalog->mountpoint().ToString() == nested_root_path));

  if (merge) {
    // Merge all data from the nested catalog into it's parent
    nested_catalog->MergeIntoParent();
  } else {
    nested_catalog->RemoveFromParent();
  }

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


/**
 * Checks if a nested catalog starts at this path.  The path must be valid.
 */
bool WritableCatalogManager::IsTransitionPoint(const string &mountpoint) {
  const string path = MakeRelativePath(mountpoint);

  SyncLock();
  WritableCatalog *catalog;
  DirectoryEntry entry;
  if (!FindCatalog(path, &catalog, &entry)) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "catalog for directory '%s' cannot be found", path.c_str());
    assert(false);
  }
  const bool result = entry.IsNestedCatalogRoot();
  SyncUnlock();
  return result;
}


void WritableCatalogManager::PrecalculateListings() {
  // TODO(jblomer): meant for micro catalogs
}


void WritableCatalogManager::SetTTL(const uint64_t new_ttl) {
  SyncLock();
  reinterpret_cast<WritableCatalog *>(GetRootCatalog())->SetTTL(new_ttl);
  SyncUnlock();
}


bool WritableCatalogManager::SetVOMSAuthz(const std::string &voms_authz) {
  bool result;
  SyncLock();
  result = reinterpret_cast<WritableCatalog *>(
    GetRootCatalog())->SetVOMSAuthz(voms_authz);
  SyncUnlock();
  return result;
}


bool WritableCatalogManager::Commit(const bool           stop_for_tweaks,
                                    const uint64_t       manual_revision,
                                    manifest::Manifest  *manifest) {
  WritableCatalog *root_catalog =
    reinterpret_cast<WritableCatalog *>(GetRootCatalog());
  root_catalog->SetDirty();

  // set root catalog revision to manually provided number if available
  if (manual_revision > 0) {
    const uint64_t revision = root_catalog->GetRevision();
    if (revision >= manual_revision) {
      LogCvmfs(kLogCatalog, kLogStderr, "Manual revision (%d) must not be "
                                        "smaller than the current root "
                                        "catalog's (%d). Skipped!",
                                        manual_revision, revision);
    } else {
      // Gets incremented by FinalizeCatalog() afterwards!
      root_catalog->SetRevision(manual_revision - 1);
    }
  }

  // do the actual catalog snapshotting and upload
  CatalogInfo root_catalog_info;
  if (getenv("_CVMFS_SERIALIZED_CATALOG_PROCESSING_") == NULL)
    root_catalog_info = SnapshotCatalogs(stop_for_tweaks);
  else
    root_catalog_info = SnapshotCatalogsSerialized(stop_for_tweaks);
  if (spooler_->GetNumberOfErrors() > 0) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to commit catalogs");
    return false;
  }

  // .cvmfspublished export
  LogCvmfs(kLogCatalog, kLogVerboseMsg, "Committing repository manifest");
  set_base_hash(root_catalog_info.content_hash);

  manifest->set_catalog_hash(root_catalog_info.content_hash);
  manifest->set_catalog_size(root_catalog_info.size);
  manifest->set_root_path("");
  manifest->set_ttl(root_catalog_info.ttl);
  manifest->set_revision(root_catalog_info.revision);

  return true;
}


/**
 * Handles the snapshotting of dirty (i.e. modified) catalogs while trying to
 * parallize the compression and upload as much as possible. We use a parallel
 * depth first post order tree traversal based on 'continuations'.
 *
 * The idea is as follows:
 *  1. find all leaf-catalogs (i.e. dirty catalogs with no dirty children)
 *     --> these can be processed and uploaded immedately and independently
 *         see WritableCatalogManager::GetModifiedCatalogLeafs()
 *  2. annotate non-leaf catalogs with their number of dirty children
 *     --> a finished child will notify it's parent and decrement this number
 *         see WritableCatalogManager::CatalogUploadCallback()
 *  3. if a non-leaf catalog's dirty children number reaches 0, it is scheduled
 *     for processing as well (continuation)
 *     --> the parallel processing walks bottom-up through the catalog tree
 *         see WritableCatalogManager::CatalogUploadCallback()
 *  4. when the root catalog is reached, we notify the main thread and return
 *     --> done through a Future<> in WritableCatalogManager::SnapshotCatalogs
 *
 * Note: The catalog finalisation (see WritableCatalogManager::FinalizeCatalog)
 *       happens in a worker thread (i.e. the callback method) for non-leaf
 *       catalogs.
 *
 * TODO(rmeusel): since all leaf catalogs are finalized in the main thread, we
 *                sacrafice some potential concurrency for simplicity.
 */
WritableCatalogManager::CatalogInfo WritableCatalogManager::SnapshotCatalogs(
                                                   const bool stop_for_tweaks) {
  // prepare environment for parallel processing
  Future<CatalogInfo>  root_catalog_info_future;
  CatalogUploadContext upload_context;
  upload_context.root_catalog_info = &root_catalog_info_future;
  upload_context.stop_for_tweaks   = stop_for_tweaks;

  spooler_->RegisterListener(
    &WritableCatalogManager::CatalogUploadCallback, this, upload_context);

  // find dirty leaf catalogs and annotate non-leaf catalogs (dirty child count)
  // post-condition: the entire catalog tree is ready for concurrent processing
  WritableCatalogList leafs_to_snapshot;
  GetModifiedCatalogLeafs(&leafs_to_snapshot);

  // finalize and schedule the catalog processing
        WritableCatalogList::const_iterator i    = leafs_to_snapshot.begin();
  const WritableCatalogList::const_iterator iend = leafs_to_snapshot.end();
  for (; i != iend; ++i) {
    FinalizeCatalog(*i, stop_for_tweaks);
    ScheduleCatalogProcessing(*i);
  }

  LogCvmfs(kLogCatalog, kLogVerboseMsg, "waiting for upload of catalogs");
  CatalogInfo& root_catalog_info = root_catalog_info_future.Get();
  spooler_->WaitForUpload();

  spooler_->UnregisterListeners();
  return root_catalog_info;
}


void WritableCatalogManager::FinalizeCatalog(WritableCatalog *catalog,
                                             const bool stop_for_tweaks) {
  // update meta information of this catalog
  LogCvmfs(kLogCatalog, kLogVerboseMsg, "creating snapshot of catalog '%s'",
           catalog->mountpoint().c_str());

  catalog->UpdateCounters();
  catalog->UpdateLastModified();
  catalog->IncrementRevision();

  // update the previous catalog revision pointer
  if (catalog->IsRoot()) {
    LogCvmfs(kLogCatalog, kLogVerboseMsg, "setting '%s' as previous revision "
                                          "for root catalog",
             base_hash().ToStringWithSuffix().c_str());
    catalog->SetPreviousRevision(base_hash());
  } else {
    // Multiple catalogs might query the parent concurrently
    SyncLock();
    shash::Any hash_previous;
    uint64_t size_previous;
    const bool retval =
      catalog->parent()->FindNested(catalog->mountpoint(),
                                    &hash_previous, &size_previous);
    assert(retval);
    SyncUnlock();

    LogCvmfs(kLogCatalog, kLogVerboseMsg, "found '%s' as previous revision "
                                          "for nested catalog '%s'",
             hash_previous.ToStringWithSuffix().c_str(),
             catalog->mountpoint().c_str());
    catalog->SetPreviousRevision(hash_previous);
  }
  catalog->Commit();

  // check if catalog has too many entries
  uint64_t catalog_limit = uint64_t(1000) *
    uint64_t((catalog->IsRoot()
              ? root_kcatalog_limit_
              : nested_kcatalog_limit_));
  if ((catalog_limit > 0) &&
      (catalog->GetCounters().GetSelfEntries() > catalog_limit)) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "%s: catalog at %s has more than %u entries (%u). "
             "Large catalogs stress the CernVM-FS transport infrastructure. "
             "Please split it into nested catalogs or increase the limit.",
             enforce_limits_ ? "FATAL" : "WARNING",
             (catalog->IsRoot() ? "/" : catalog->mountpoint().c_str()),
             catalog_limit,
             catalog->GetCounters().GetSelfEntries());
    assert(!enforce_limits_);
  }

  // allow for manual adjustments in the catalog
  if (stop_for_tweaks) {
    LogCvmfs(kLogCatalog, kLogStdout, "Allowing for tweaks in %s at %s "
                                      "(hit return to continue)",
             catalog->database_path().c_str(), catalog->mountpoint().c_str());
    int read_char = getchar();
    assert(read_char != EOF);
  }

  // compaction of bloated catalogs (usually after high database churn)
  catalog->VacuumDatabaseIfNecessary();
}


void WritableCatalogManager::ScheduleCatalogProcessing(
                                                     WritableCatalog *catalog) {
  {
    MutexLockGuard guard(catalog_processing_lock_);
    // register catalog object for WritableCatalogManager::CatalogUploadCallback
    catalog_processing_map_[catalog->database_path()] = catalog;
  }
  IngestionSource *catalog_source =
      new FileIngestionSource(catalog->database_path());
  spooler_->ProcessCatalog(catalog_source);
}


void WritableCatalogManager::CatalogUploadCallback(
                          const upload::SpoolerResult &result,
                          const CatalogUploadContext   catalog_upload_context) {
  if (result.return_code != 0) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to upload '%s' (retval: %d)",
             result.local_path.c_str(), result.return_code);
    assert(false);
  }

  // retrieve the catalog object based on the callback information
  // see WritableCatalogManager::ScheduleCatalogProcessing()
  WritableCatalog *catalog = NULL;
  {
    MutexLockGuard guard(catalog_processing_lock_);
    std::map<std::string, WritableCatalog*>::iterator c =
      catalog_processing_map_.find(result.local_path);
    assert(c != catalog_processing_map_.end());
    catalog = c->second;
  }

  uint64_t catalog_size = GetFileSize(result.local_path);
  assert(catalog_size > 0);

  SyncLock();
  if (catalog->HasParent()) {
    // finalized nested catalogs will update their parent's pointer and schedule
    // them for processing (continuation) if the 'dirty children count' == 0
    LogCvmfs(kLogCatalog, kLogVerboseMsg, "updating nested catalog link");
    WritableCatalog *parent = catalog->GetWritableParent();

    parent->UpdateNestedCatalog(catalog->mountpoint().ToString(),
                                result.content_hash,
                                catalog_size,
                                catalog->delta_counters_);
    catalog->delta_counters_.SetZero();

    const int remaining_dirty_children =
      catalog->GetWritableParent()->DecrementDirtyChildren();

    SyncUnlock();

    // continuation of the dirty catalog tree traversal
    // see WritableCatalogManager::SnapshotCatalogs()
    if (remaining_dirty_children == 0) {
      FinalizeCatalog(parent, catalog_upload_context.stop_for_tweaks);
      ScheduleCatalogProcessing(parent);
    }

  } else if (catalog->IsRoot()) {
    // once the root catalog is reached, we are done with processing and report
    // back to the main via a Future<> and provide the necessary information
    CatalogInfo root_catalog_info;
    root_catalog_info.size         = catalog_size;
    root_catalog_info.ttl          = catalog->GetTTL();
    root_catalog_info.content_hash = result.content_hash;
    root_catalog_info.revision     = catalog->GetRevision();
    catalog_upload_context.root_catalog_info->Set(root_catalog_info);
    SyncUnlock();
  } else {
    assert(false && "inconsistent state detected");
  }
}


/**
 * Finds dirty catalogs that can be snapshot right away and annotates all the
 * other catalogs with their number of dirty decendants.
 * Note that there is a convenience wrapper to start the recursion:
 *   WritableCatalogManager::GetModifiedCatalogLeafs()
 *
 * @param catalog  the catalog for this recursion step
 * @param result   the result list to be appended to
 * @return         true if 'catalog' is dirty
 */
bool WritableCatalogManager::GetModifiedCatalogLeafsRecursively(
                                            Catalog             *catalog,
                                            WritableCatalogList *result) const {
  WritableCatalog *wr_catalog = static_cast<WritableCatalog *>(catalog);

  // Look for dirty catalogs in the descendants of *catalog
  int dirty_children = 0;
  CatalogList children = wr_catalog->GetChildren();
        CatalogList::const_iterator i    = children.begin();
  const CatalogList::const_iterator iend = children.end();
  for (; i != iend; ++i) {
    if (GetModifiedCatalogLeafsRecursively(*i, result)) {
      ++dirty_children;
    }
  }

  // a catalog is dirty if itself or one of its children has changed
  // a leaf catalog doesn't have any dirty children
  wr_catalog->set_dirty_children(dirty_children);
  const bool is_dirty = wr_catalog->IsDirty() || dirty_children > 0;
  const bool is_leaf  = dirty_children == 0;
  if (is_dirty && is_leaf) {
    result->push_back(const_cast<WritableCatalog *>(wr_catalog));
  }

  return is_dirty;
}


void WritableCatalogManager::DoBalance() {
  CatalogList catalog_list = GetCatalogs();
  reverse(catalog_list.begin(), catalog_list.end());
  for (unsigned i = 0; i < catalog_list.size(); ++i) {
    FixWeight(static_cast<WritableCatalog*>(catalog_list[i]));
  }
}

void WritableCatalogManager::FixWeight(WritableCatalog* catalog) {
  // firstly check underflow because they can provoke overflows
  if (catalog->GetNumEntries() < min_weight_ &&
      !catalog->IsRoot() &&
      catalog->IsAutogenerated()) {
    LogCvmfs(kLogCatalog, kLogStdout,
             "Deleting an autogenerated catalog in '%s'",
             catalog->mountpoint().c_str());
    // Remove the .cvmfscatalog and .cvmfsautocatalog files first
    string path = catalog->mountpoint().ToString();
    catalog->RemoveEntry(path + "/.cvmfscatalog");
    catalog->RemoveEntry(path + "/.cvmfsautocatalog");
    // Remove the actual catalog
    string catalog_path = catalog->mountpoint().ToString().substr(1);
    RemoveNestedCatalog(catalog_path);
  } else if (catalog->GetNumEntries() > max_weight_) {
    CatalogBalancer<WritableCatalogManager> catalog_balancer(this);
    catalog_balancer.Balance(catalog);
  }
}


//****************************************************************************
// Workaround -- Serialized Catalog Committing

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


void WritableCatalogManager::CatalogUploadSerializedCallback(
  const upload::SpoolerResult &result,
  const CatalogUploadContext unused)
{
  if (result.return_code != 0) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to upload '%s' (retval: %d)",
             result.local_path.c_str(), result.return_code);
    assert(false);
  }
  unlink(result.local_path.c_str());
}


WritableCatalogManager::CatalogInfo
WritableCatalogManager::SnapshotCatalogsSerialized(
  const bool stop_for_tweaks)
{
  LogCvmfs(kLogCvmfs, kLogStdout, "Serialized committing of file catalogs...");
  reinterpret_cast<WritableCatalog *>(GetRootCatalog())->SetDirty();
  WritableCatalogList catalogs_to_snapshot;
  GetModifiedCatalogs(&catalogs_to_snapshot);
  CatalogUploadContext unused;
  unused.root_catalog_info = NULL;
  unused.stop_for_tweaks = false;
  spooler_->RegisterListener(
    &WritableCatalogManager::CatalogUploadSerializedCallback, this, unused);

  CatalogInfo root_catalog_info;
  WritableCatalogList::const_iterator i = catalogs_to_snapshot.begin();
  const WritableCatalogList::const_iterator iend = catalogs_to_snapshot.end();
  for (; i != iend; ++i) {
    FinalizeCatalog(*i, stop_for_tweaks);

    // Compress and upload catalog
    shash::Any hash_catalog(spooler_->GetHashAlgorithm(),
                            shash::kSuffixCatalog);
    if (!zlib::CompressPath2Null((*i)->database_path(),
                                 &hash_catalog))
    {
      PrintError("could not compress catalog " + (*i)->mountpoint().ToString());
      assert(false);
    }

    int64_t catalog_size = GetFileSize((*i)->database_path());
    assert(catalog_size > 0);

    if ((*i)->HasParent()) {
      LogCvmfs(kLogCatalog, kLogVerboseMsg, "updating nested catalog link");
      WritableCatalog *parent = (*i)->GetWritableParent();
      parent->UpdateNestedCatalog((*i)->mountpoint().ToString(), hash_catalog,
                                  catalog_size, (*i)->delta_counters_);
      (*i)->delta_counters_.SetZero();
    } else if ((*i)->IsRoot()) {
      root_catalog_info.size = catalog_size;
      root_catalog_info.ttl = (*i)->GetTTL();
      root_catalog_info.content_hash = hash_catalog;
      root_catalog_info.revision = (*i)->GetRevision();
    } else {
      assert(false && "inconsistent state detected");
    }

    IngestionSource *catalog_source =
        new FileIngestionSource((*i)->database_path());
    spooler_->ProcessCatalog(catalog_source);
  }
  spooler_->WaitForUpload();

  spooler_->UnregisterListeners();
  return root_catalog_info;
}

}  // namespace catalog
