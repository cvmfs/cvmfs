/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "catalog_rw.h"

#include <inttypes.h>

#include <cstdio>
#include <cstdlib>

#include "util/concurrency.h"
#include "util/exception.h"
#include "util/logging.h"
#include "xattr.h"

using namespace std;  // NOLINT

namespace catalog {

const double WritableCatalog::kMaximalFreePageRatio = 0.20;
const double WritableCatalog::kMaximalRowIdWasteRatio = 0.25;


WritableCatalog::WritableCatalog(const string &path,
                                 const shash::Any &catalog_hash,
                                 Catalog *parent,
                                 const bool is_not_root)
    : Catalog(PathString(path.data(), path.length()),
              catalog_hash,  // This is 0 for a newly created catalog!
              parent,
              is_not_root)
    , sql_insert_(NULL)
    , sql_unlink_(NULL)
    , sql_touch_(NULL)
    , sql_update_(NULL)
    , sql_chunk_insert_(NULL)
    , sql_chunks_remove_(NULL)
    , sql_chunks_count_(NULL)
    , sql_max_link_id_(NULL)
    , sql_inc_linkcount_(NULL)
    , dirty_(false) {
  atomic_init32(&dirty_children_);
}


WritableCatalog *WritableCatalog::AttachFreely(const string &root_path,
                                               const string &file,
                                               const shash::Any &catalog_hash,
                                               Catalog *parent,
                                               const bool is_not_root) {
  WritableCatalog *catalog = new WritableCatalog(root_path, catalog_hash,
                                                 parent, is_not_root);
  const bool successful_init = catalog->InitStandalone(file);
  if (!successful_init) {
    delete catalog;
    return NULL;
  }
  return catalog;
}


WritableCatalog::~WritableCatalog() {
  // CAUTION HOT!
  // (see Catalog.h - near the definition of FinalizePreparedStatements)
  FinalizePreparedStatements();
}


void WritableCatalog::Transaction() {
  LogCvmfs(kLogCatalog, kLogVerboseMsg, "opening SQLite transaction for '%s'",
           mountpoint().c_str());
  const bool retval = database().BeginTransaction();
  assert(retval == true);
}


void WritableCatalog::Commit() {
  LogCvmfs(kLogCatalog, kLogVerboseMsg, "closing SQLite transaction for '%s'",
           mountpoint().c_str());
  const bool retval = database().CommitTransaction();
  assert(retval == true);
  dirty_ = false;
}


void WritableCatalog::InitPreparedStatements() {
  Catalog::InitPreparedStatements();  // polymorphism: up call

  const bool retval = SqlCatalog(database(), "PRAGMA foreign_keys = ON;")
                          .Execute();
  assert(retval);
  sql_insert_ = new SqlDirentInsert(database());
  sql_unlink_ = new SqlDirentUnlink(database());
  sql_touch_ = new SqlDirentTouch(database());
  sql_update_ = new SqlDirentUpdate(database());
  sql_chunk_insert_ = new SqlChunkInsert(database());
  sql_chunks_remove_ = new SqlChunksRemove(database());
  sql_chunks_count_ = new SqlChunksCount(database());
  sql_max_link_id_ = new SqlMaxHardlinkGroup(database());
  sql_inc_linkcount_ = new SqlIncLinkcount(database());
}


void WritableCatalog::FinalizePreparedStatements() {
  // no polymorphism: no up call (see Catalog.h -
  // near the definition of this method)
  delete sql_insert_;
  delete sql_unlink_;
  delete sql_touch_;
  delete sql_update_;
  delete sql_chunk_insert_;
  delete sql_chunks_remove_;
  delete sql_chunks_count_;
  delete sql_max_link_id_;
  delete sql_inc_linkcount_;
}


/**
 * Find out the maximal hardlink group id in this catalog.
 */
uint32_t WritableCatalog::GetMaxLinkId() const {
  int result = -1;

  if (sql_max_link_id_->FetchRow()) {
    result = sql_max_link_id_->GetMaxGroupId();
  }
  sql_max_link_id_->Reset();

  return result;
}


/**
 * Adds a directory entry.
 * @param entry the DirectoryEntry to add to the catalog
 * @param entry_path the full path of the DirectoryEntry to add
 * @param parent_path the full path of the containing directory
 */
void WritableCatalog::AddEntry(const DirectoryEntry &entry,
                               const XattrList &xattrs,
                               const string &entry_path,
                               const string &parent_path) {
  SetDirty();

  LogCvmfs(kLogCatalog, kLogVerboseMsg, "add entry '%s' to '%s'",
           entry_path.c_str(), mountpoint().c_str());

  const shash::Md5 path_hash((shash::AsciiPtr(entry_path)));
  const shash::Md5 parent_hash((shash::AsciiPtr(parent_path)));
  DirectoryEntry effective_entry(entry);
  effective_entry.set_has_xattrs(!xattrs.IsEmpty());

  bool retval = sql_insert_->BindPathHash(path_hash)
                && sql_insert_->BindParentPathHash(parent_hash)
                && sql_insert_->BindDirent(effective_entry);
  assert(retval);
  if (xattrs.IsEmpty()) {
    retval = sql_insert_->BindXattrEmpty();
  } else {
    retval = sql_insert_->BindXattr(xattrs);
  }
  assert(retval);
  retval = sql_insert_->Execute();
  assert(retval);
  sql_insert_->Reset();

  delta_counters_.Increment(effective_entry);
}


/**
 * Removes the specified entry from the catalog.
 * Note: removing a directory which is non-empty results in dangling entries.
 *       (this should be treated in upper layers)
 * @param entry_path the full path of the DirectoryEntry to delete
 */
void WritableCatalog::RemoveEntry(const string &file_path) {
  DirectoryEntry entry;
  bool retval = LookupPath(PathString(file_path), &entry);
  assert(retval);

  SetDirty();

  // If the entry used to be a chunked file... remove the chunks
  if (entry.IsChunkedFile()) {
    RemoveFileChunks(file_path);
  }

  // remove the entry itself
  const shash::Md5 path_hash = shash::Md5(shash::AsciiPtr(file_path));
  retval = sql_unlink_->BindPathHash(path_hash) && sql_unlink_->Execute();
  assert(retval);
  sql_unlink_->Reset();

  delta_counters_.Decrement(entry);
}


void WritableCatalog::IncLinkcount(const string &path_within_group,
                                   const int delta) {
  SetDirty();

  const shash::Md5 path_hash = shash::Md5(shash::AsciiPtr(path_within_group));

  const bool retval = sql_inc_linkcount_->BindPathHash(path_hash)
                      && sql_inc_linkcount_->BindDelta(delta)
                      && sql_inc_linkcount_->Execute();
  assert(retval);
  sql_inc_linkcount_->Reset();
}


void WritableCatalog::TouchEntry(const DirectoryEntryBase &entry,
                                 const XattrList &xattrs,
                                 const shash::Md5 &path_hash) {
  SetDirty();

  catalog::DirectoryEntry prev_entry;
  bool retval = LookupMd5Path(path_hash, &prev_entry);
  assert(retval);

  retval = sql_touch_->BindPathHash(path_hash)
           && sql_touch_->BindDirentBase(entry);
  assert(retval);
  if (xattrs.IsEmpty()) {
    retval = sql_touch_->BindXattrEmpty();
    if (prev_entry.HasXattrs())
      delta_counters_.self.xattrs--;
  } else {
    retval = sql_touch_->BindXattr(xattrs);
    if (!prev_entry.HasXattrs())
      delta_counters_.self.xattrs++;
  }
  assert(retval);
  retval = sql_touch_->Execute();
  assert(retval);
  sql_touch_->Reset();
}


void WritableCatalog::UpdateEntry(const DirectoryEntry &entry,
                                  const shash::Md5 &path_hash) {
  SetDirty();

  const bool retval = sql_update_->BindPathHash(path_hash)
                      && sql_update_->BindDirent(entry)
                      && sql_update_->Execute();
  assert(retval);
  sql_update_->Reset();
}

void WritableCatalog::AddFileChunk(const std::string &entry_path,
                                   const FileChunk &chunk) {
  SetDirty();

  const shash::Md5 path_hash((shash::AsciiPtr(entry_path)));

  LogCvmfs(kLogCatalog, kLogVerboseMsg,
           "adding chunk for %s from offset %ld "
           "and chunk size: %ld bytes",
           entry_path.c_str(), chunk.offset(), chunk.offset() + chunk.size());

  delta_counters_.self.file_chunks++;

  const bool retval = sql_chunk_insert_->BindPathHash(path_hash)
                      && sql_chunk_insert_->BindFileChunk(chunk)
                      && sql_chunk_insert_->Execute();
  assert(retval);
  sql_chunk_insert_->Reset();
}


/**
 * Removes the file chunks for a given file path
 * @param entry_path   the file path to clear from it's file chunks
 */
void WritableCatalog::RemoveFileChunks(const std::string &entry_path) {
  const shash::Md5 path_hash((shash::AsciiPtr(entry_path)));
  bool retval;

  // subtract the number of chunks from the statistics counters
  retval = sql_chunks_count_->BindPathHash(path_hash)
           && sql_chunks_count_->Execute();
  assert(retval);
  const int chunks_count = sql_chunks_count_->GetChunkCount();
  delta_counters_.self.file_chunks -= chunks_count;
  sql_chunks_count_->Reset();

  // remove the chunks associated to `entry_path`
  retval = sql_chunks_remove_->BindPathHash(path_hash)
           && sql_chunks_remove_->Execute();
  assert(retval);
  sql_chunks_remove_->Reset();
}


/**
 * Sets the last modified time stamp of this catalog to current time.
 */
void WritableCatalog::UpdateLastModified() {
  database().SetProperty("last_modified", static_cast<uint64_t>(time(NULL)));
}


/**
 * Increments the revision of the catalog in the database.
 */
void WritableCatalog::IncrementRevision() { SetRevision(GetRevision() + 1); }


void WritableCatalog::SetRevision(const uint64_t new_revision) {
  database().SetProperty("revision", new_revision);
}


void WritableCatalog::SetBranch(const std::string &branch_name) {
  database().SetProperty("branch", branch_name);
}


void WritableCatalog::SetTTL(const uint64_t new_ttl) {
  database().SetProperty("TTL", new_ttl);
}


bool WritableCatalog::SetVOMSAuthz(const std::string &voms_authz) {
  return database().SetVOMSAuthz(voms_authz);
}


/**
 * Sets the content hash of the previous catalog revision.
 */
void WritableCatalog::SetPreviousRevision(const shash::Any &hash) {
  database().SetProperty("previous_revision", hash.ToString());
}


/**
 * Moves a subtree from this catalog into a just created nested catalog.
 */
void WritableCatalog::Partition(WritableCatalog *new_nested_catalog) {
  // Create connection between parent and child catalogs
  MakeTransitionPoint(new_nested_catalog->mountpoint().ToString());
  new_nested_catalog->MakeNestedRoot();
  delta_counters_.subtree.directories++;  // Root directory in nested catalog

  // Move the present directory tree into the newly created nested catalog
  // if we hit nested catalog mountpoints on the way, we return them through
  // the passed list
  vector<string> GrandChildMountpoints;
  MoveToNested(new_nested_catalog->mountpoint().ToString(), new_nested_catalog,
               &GrandChildMountpoints);

  // Nested catalog mountpoints found in the moved directory structure are now
  // links to nested catalogs of the newly created nested catalog.
  // Move these references into the new nested catalog
  MoveCatalogsToNested(GrandChildMountpoints, new_nested_catalog);
}


void WritableCatalog::MakeTransitionPoint(const string &mountpoint) {
  // Find the directory entry to edit
  DirectoryEntry transition_entry;
  const bool retval = LookupPath(
      PathString(mountpoint.data(), mountpoint.length()), &transition_entry);
  assert(retval);

  assert(transition_entry.IsDirectory()
         && !transition_entry.IsNestedCatalogRoot());

  transition_entry.set_is_nested_catalog_mountpoint(true);
  UpdateEntry(transition_entry, mountpoint);
}


void WritableCatalog::MakeNestedRoot() {
  DirectoryEntry root_entry;
  const bool retval = LookupPath(mountpoint(), &root_entry);
  assert(retval);

  assert(root_entry.IsDirectory() && !root_entry.IsNestedCatalogMountpoint());

  root_entry.set_is_nested_catalog_root(true);
  UpdateEntry(root_entry, mountpoint().ToString());
}


void WritableCatalog::MoveToNestedRecursively(
    const string directory,
    WritableCatalog *new_nested_catalog,
    vector<string> *grand_child_mountpoints) {
  // After creating a new nested catalog we have to move all elements
  // now contained by the new one.  List and move them recursively.
  DirectoryEntryList listing;
  const bool resolve_magic_symlinks = false;
  bool retval = ListingPath(PathString(directory), &listing,
                            resolve_magic_symlinks);
  assert(retval);

  // Go through the listing
  const XattrList empty_xattrs;
  for (DirectoryEntryList::const_iterator i = listing.begin(),
                                          iEnd = listing.end();
       i != iEnd;
       ++i) {
    const string full_path = i->GetFullPath(directory);

    // The entries are first inserted into the new catalog
    if (i->HasXattrs()) {
      XattrList xattrs;
      retval = LookupXattrsPath(PathString(full_path), &xattrs);
      assert(retval);
      assert(!xattrs.IsEmpty());
      new_nested_catalog->AddEntry(*i, xattrs, full_path);
    } else {
      new_nested_catalog->AddEntry(*i, empty_xattrs, full_path);
    }

    // Then we check if we have some special cases:
    if (i->IsNestedCatalogMountpoint()) {
      grand_child_mountpoints->push_back(full_path);
    } else if (i->IsDirectory()) {
      // Recurse deeper into the directory tree
      MoveToNestedRecursively(full_path, new_nested_catalog,
                              grand_child_mountpoints);
    } else if (i->IsChunkedFile()) {
      MoveFileChunksToNested(full_path, i->hash_algorithm(),
                             new_nested_catalog);
    }

    // Remove the entry from the current catalog
    RemoveEntry(full_path);
  }
}


void WritableCatalog::MoveCatalogsToNested(
    const vector<string> &nested_catalogs,
    WritableCatalog *new_nested_catalog) {
  for (vector<string>::const_iterator i = nested_catalogs.begin(),
                                      iEnd = nested_catalogs.end();
       i != iEnd;
       ++i) {
    shash::Any hash_nested;
    uint64_t size_nested;
    const bool retval = FindNested(PathString(*i), &hash_nested, &size_nested);
    assert(retval);

    Catalog *attached_reference = NULL;
    RemoveNestedCatalog(*i, &attached_reference);

    new_nested_catalog->InsertNestedCatalog(*i, attached_reference, hash_nested,
                                            size_nested);
  }
}


void WritableCatalog::MoveFileChunksToNested(
    const std::string &full_path,
    const shash::Algorithms algorithm,
    WritableCatalog *new_nested_catalog) {
  FileChunkList chunks;
  ListPathChunks(PathString(full_path), algorithm, &chunks);
  assert(chunks.size() > 0);

  for (unsigned i = 0; i < chunks.size(); ++i) {
    new_nested_catalog->AddFileChunk(full_path, *chunks.AtPtr(i));
  }
}


/**
 * Insert a nested catalog reference into this catalog.
 * The attached catalog object of this mountpoint can be specified (optional)
 * This way, the in-memory representation of the catalog tree is updated, too
 * @param mountpoint the path to the catalog to add a reference to
 * @param attached_reference can contain a reference to the attached catalog
 *        object of mountpoint
 * @param content_hash can be set to safe a content hash together with the
 *        reference
 */
void WritableCatalog::InsertNestedCatalog(const string &mountpoint,
                                          Catalog *attached_reference,
                                          const shash::Any content_hash,
                                          const uint64_t size) {
  const string hash_string = (!content_hash.IsNull()) ? content_hash.ToString()
                                                      : "";

  SqlCatalog stmt(database(), "INSERT INTO nested_catalogs (path, sha1, size) "
                              "VALUES (:p, :sha1, :size);");
  const bool retval = stmt.BindText(1, mountpoint)
                      && stmt.BindText(2, hash_string)
                      && stmt.BindInt64(3, size) && stmt.Execute();
  assert(retval);

  // If a reference of the in-memory object of the newly referenced
  // catalog was passed, we add this to our own children
  if (attached_reference != NULL)
    AddChild(attached_reference);

  ResetNestedCatalogCacheUnprotected();

  delta_counters_.self.nested_catalogs++;
}


/**
 * Registers a snapshot in /.cvmfs/snapshots. Note that bind mountpoints are
 * not universally handled: in Partition and MergeIntoParent, bind mountpoint
 * handling is missing!
 */
void WritableCatalog::InsertBindMountpoint(const string &mountpoint,
                                           const shash::Any content_hash,
                                           const uint64_t size) {
  SqlCatalog stmt(database(),
                  "INSERT INTO bind_mountpoints (path, sha1, size) "
                  "VALUES (:p, :sha1, :size);");
  const bool retval = stmt.BindText(1, mountpoint)
                      && stmt.BindText(2, content_hash.ToString())
                      && stmt.BindInt64(3, size) && stmt.Execute();
  assert(retval);
}


/**
 * Remove a nested catalog reference from the database.
 * If the catalog 'mountpoint' is currently attached as a child, it will be
 * removed, too (but not detached).
 * @param[in] mountpoint the mountpoint of the nested catalog to dereference in
              the database
 * @param[out] attached_reference is set to the object of the attached child or
 *             to NULL
 */
void WritableCatalog::RemoveNestedCatalog(const string &mountpoint,
                                          Catalog **attached_reference) {
  shash::Any dummy;
  uint64_t dummy_size;
  bool retval = FindNested(PathString(mountpoint.data(), mountpoint.length()),
                           &dummy, &dummy_size);
  assert(retval);

  SqlCatalog stmt(database(), "DELETE FROM nested_catalogs WHERE path = :p;");
  retval = stmt.BindText(1, mountpoint) && stmt.Execute();
  assert(retval);

  // If the reference was successfully deleted, we also have to check whether
  // there is also an attached reference in our in-memory data.
  // In this case we remove the child and return it through **attached_reference
  Catalog *child = FindChild(PathString(mountpoint));
  if (child != NULL)
    RemoveChild(child);
  if (attached_reference != NULL)
    *attached_reference = child;

  ResetNestedCatalogCacheUnprotected();

  delta_counters_.self.nested_catalogs--;
}


/**
 * Unregisters a snapshot from /.cvmfs/snapshots. Note that bind mountpoints
 * are not universally handled: in Partition and MergeIntoParent, bind
 * mountpoint handling is missing!
 */
void WritableCatalog::RemoveBindMountpoint(const std::string &mountpoint) {
  shash::Any dummy;
  uint64_t dummy_size;
  bool retval = FindNested(PathString(mountpoint.data(), mountpoint.length()),
                           &dummy, &dummy_size);
  assert(retval);

  SqlCatalog stmt(database(), "DELETE FROM bind_mountpoints WHERE path = :p;");
  retval = stmt.BindText(1, mountpoint) && stmt.Execute();
  assert(retval);
}


/**
 * Updates the link to a nested catalog in the database.
 * @param path             the path of the nested catalog to update
 * @param hash             the hash to set the given nested catalog link to
 * @param size             the uncompressed catalog database file size
 * @param child_counters   the statistics counters of the nested catalog
 */
void WritableCatalog::UpdateNestedCatalog(const std::string &path,
                                          const shash::Any &hash,
                                          const uint64_t size,
                                          const DeltaCounters &child_counters) {
  const MutexLockGuard guard(lock_);
  SetDirty();

  child_counters.PopulateToParent(&delta_counters_);

  const string hash_str = hash.ToString();
  const string sql = "UPDATE nested_catalogs SET sha1 = :sha1, size = :size  "
                     "WHERE path = :path;";
  SqlCatalog stmt(database(), sql);

  const bool retval = stmt.BindText(1, hash_str) && stmt.BindInt64(2, size)
                      && stmt.BindText(3, path) && stmt.Execute();

  ResetNestedCatalogCacheUnprotected();

  assert(retval);
}


void WritableCatalog::MergeIntoParent() {
  assert(!IsRoot() && HasParent());
  WritableCatalog *parent = GetWritableParent();

  CopyToParent();

  // Copy the nested catalog references
  CopyCatalogsToParent();

  // Fix counters in parent
  delta_counters_.PopulateToParent(&parent->delta_counters_);
  Counters &counters = GetWritableCounters();
  counters.ApplyDelta(delta_counters_);
  counters.MergeIntoParent(&parent->delta_counters_);

  // Remove the nested catalog reference for this nested catalog.
  // From now on this catalog will be dangling!
  parent->RemoveNestedCatalog(this->mountpoint().ToString(), NULL);
}


void WritableCatalog::RemoveFromParent() {
  assert(!IsRoot() && HasParent());
  WritableCatalog *parent = GetWritableParent();

  // Remove the nested catalog reference for this nested catalog.
  // From now on this catalog will be dangling!
  parent->RemoveNestedCatalog(this->mountpoint().ToString(), NULL);
  parent->delta_counters_.RemoveFromSubtree(
      Counters::Diff(Counters(), GetCounters()));
}


void WritableCatalog::CopyCatalogsToParent() {
  WritableCatalog *parent = GetWritableParent();

  // Obtain a list of all nested catalog references
  const NestedCatalogList nested_catalog_references = ListOwnNestedCatalogs();

  // Go through the list and update the databases
  // simultaneously we are checking if the referenced catalogs are currently
  // attached and update the in-memory data structures as well
  for (NestedCatalogList::const_iterator i = nested_catalog_references.begin(),
                                         iEnd = nested_catalog_references.end();
       i != iEnd;
       ++i) {
    Catalog *child = FindChild(i->mountpoint);
    parent->InsertNestedCatalog(i->mountpoint.ToString(), child, i->hash,
                                i->size);
    parent->delta_counters_.self.nested_catalogs--;  // Will be fixed later
  }
}

void WritableCatalog::CopyToParent() {
  // We could simply copy all entries from this database to the 'other' database
  // BUT: 1. this would create collisions in hardlink group IDs.
  //         therefore we first update all hardlink group IDs to fit behind the
  //         ones in the 'other' database
  //      2. the root entry of the nested catalog is present twice:
  //         1. in the parent directory (as mount point) and
  //         2. in the nested catalog (as root entry)
  //         therefore we delete the mount point from the parent before merging

  WritableCatalog *parent = GetWritableParent();

  // Update hardlink group IDs in this nested catalog.
  // To avoid collisions we add the maximal present hardlink group ID in parent
  // to all hardlink group IDs in the nested catalog.
  const uint64_t offset = static_cast<uint64_t>(parent->GetMaxLinkId()) << 32;
  const string update_link_ids = "UPDATE catalog SET hardlinks = hardlinks + "
                                 + StringifyInt(offset)
                                 + " WHERE hardlinks > (1 << 32);";

  SqlCatalog sql_update_link_ids(database(), update_link_ids);
  bool retval = sql_update_link_ids.Execute();
  assert(retval);

  // Remove the nested catalog root.
  // It is already present in the parent.
  RemoveEntry(this->mountpoint().ToString());

  // Now copy all DirectoryEntries to the 'other' catalog.
  // There will be no data collisions, as we resolved them beforehand
  if (dirty_)
    Commit();
  if (parent->dirty_)
    parent->Commit();
  SqlCatalog sql_attach(database(), "ATTACH '" + parent->database_path()
                                        + "' "
                                          "AS other;");
  retval = sql_attach.Execute();
  assert(retval);
  retval = SqlCatalog(database(), "INSERT INTO other.catalog "
                                  "SELECT * FROM main.catalog;")
               .Execute();
  assert(retval);
  retval = SqlCatalog(database(), "INSERT INTO other.chunks "
                                  "SELECT * FROM main.chunks;")
               .Execute();
  assert(retval);
  retval = SqlCatalog(database(), "DETACH other;").Execute();
  assert(retval);
  parent->SetDirty();

  // Change the just copied nested catalog root to an ordinary directory
  // (the nested catalog is merged into it's parent)
  DirectoryEntry old_root_entry;
  retval = parent->LookupPath(this->mountpoint(), &old_root_entry);
  assert(retval);

  assert(old_root_entry.IsDirectory()
         && old_root_entry.IsNestedCatalogMountpoint()
         && !old_root_entry.IsNestedCatalogRoot());

  // Remove the nested catalog root mark
  old_root_entry.set_is_nested_catalog_mountpoint(false);
  parent->UpdateEntry(old_root_entry, this->mountpoint().ToString());
}


/**
 * Writes delta_counters_ to the database.
 */
void WritableCatalog::UpdateCounters() {
  const bool retval = delta_counters_.WriteToDatabase(database())
                      && ReadCatalogCounters();
  assert(retval);
}


/**
 * Checks if the database of this catalogs needs cleanup and defragments it
 * if necessary
 */
void WritableCatalog::VacuumDatabaseIfNecessary() {
  const CatalogDatabase &db = database();
  bool needs_defragmentation = false;
  double ratio = 0.0;
  std::string reason;

  if ((ratio = db.GetFreePageRatio()) > kMaximalFreePageRatio) {
    needs_defragmentation = true;
    reason = "free pages";
  } else if ((ratio = db.GetRowIdWasteRatio()) > kMaximalRowIdWasteRatio) {
    needs_defragmentation = true;
    reason = "wasted row IDs";
  }

  if (needs_defragmentation) {
    LogCvmfs(kLogCatalog, kLogStdout | kLogNoLinebreak,
             "Note: Catalog at %s gets defragmented (%.2f%% %s)... ",
             (IsRoot()) ? "/" : mountpoint().c_str(), ratio * 100.0,
             reason.c_str());
    if (!db.Vacuum()) {
      PANIC(kLogStderr, "failed (SQLite: %s)", db.GetLastErrorMsg().c_str());
    }
    LogCvmfs(kLogCatalog, kLogStdout, "done");
  }
}

}  // namespace catalog
