/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "catalog_rw.h"

#include <inttypes.h>
#include <cstdio>
#include <cstdlib>

#include "logging.h"
#include "util.h"

using namespace std;  // NOLINT

namespace catalog {

WritableCatalog::WritableCatalog(const string &path, Catalog *parent) :
  Catalog(PathString(path.data(), path.length()), parent),
  sql_insert_(NULL),
  sql_unlink_(NULL),
  sql_touch_(NULL),
  sql_update_(NULL),
  sql_max_link_id_(NULL),
  sql_inc_linkcount_(NULL)
{
  read_only_ = false;
  dirty_ = false;
}


WritableCatalog::~WritableCatalog() {
  // CAUTION HOT!
  // (see Catalog.h - near the definition of FinalizePreparedStatements)
  FinalizePreparedStatements();
}


void WritableCatalog::Transaction() {
  Sql transaction(database(), "BEGIN;");
  bool retval = transaction.Execute();
  assert(retval == true);
}


void WritableCatalog::Commit() {
  Sql commit(database(), "COMMIT;");
  bool retval = commit.Execute();
  assert(retval == true);
  dirty_ = false;
}


void WritableCatalog::InitPreparedStatements() {
  Catalog::InitPreparedStatements(); // polymorphism: up call

  bool retval = Sql(database(), "PRAGMA foreign_keys = ON;").Execute();
  assert(retval);
  sql_insert_        = new SqlDirentInsert     (database());
  sql_unlink_        = new SqlDirentUnlink     (database());
  sql_touch_         = new SqlDirentTouch      (database());
  sql_update_        = new SqlDirentUpdate     (database());
  sql_max_link_id_   = new SqlMaxHardlinkGroup (database());
  sql_inc_linkcount_ = new SqlIncLinkcount     (database());
}


void WritableCatalog::FinalizePreparedStatements() {
  // no polymorphism: no up call (see Catalog.h -
  // near the definition of this method)
  delete sql_insert_;
  delete sql_unlink_;
  delete sql_touch_;
  delete sql_update_;
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
 * Adds a direcotry entry.
 * @param entry the DirectoryEntry to add to the catalog
 * @param entry_path the full path of the DirectoryEntry to add
 * @param parent_path the full path of the containing directory
 * @return true if DirectoryEntry was added, false otherwise
 */
void WritableCatalog::AddEntry(const DirectoryEntry &entry,
                               const string &entry_path,
                               const string &parent_path)
{
  SetDirty();

  hash::Md5 path_hash((hash::AsciiPtr(entry_path)));
  hash::Md5 parent_hash((hash::AsciiPtr(parent_path)));

  LogCvmfs(kLogCatalog, kLogVerboseMsg, "add entry %s", entry_path.c_str());

  bool retval =
    sql_insert_->BindPathHash(path_hash) &&
    sql_insert_->BindParentPathHash(parent_hash) &&
    sql_insert_->BindDirent(entry) &&
    sql_insert_->Execute();
  assert(retval);
  sql_insert_->Reset();

  delta_counters_.DeltaDirent(entry, 1);
}


/**
 * Removes the specified entry from the catalog.
 * Note: removing a directory which is non-empty results in dangling entries.
 *       (this should be treated in upper layers)
 * @param entry_path the full path of the DirectoryEntry to delete
 * @return true if entry was removed, false otherwise
 */
void WritableCatalog::RemoveEntry(const string &file_path) {
  hash::Md5 path_hash = hash::Md5(hash::AsciiPtr(file_path));

  DirectoryEntry entry;
  bool retval = LookupMd5Path(path_hash, &entry);
  assert(retval);

  SetDirty();

  retval =
    sql_unlink_->BindPathHash(path_hash) &&
    sql_unlink_->Execute();
  assert(retval);
  sql_unlink_->Reset();

  delta_counters_.DeltaDirent(entry, -1);
}


void WritableCatalog::IncLinkcount(const string &path_within_group,
                                   const int delta)
{
  SetDirty();

  hash::Md5 path_hash = hash::Md5(hash::AsciiPtr(path_within_group));

  bool retval =
    sql_inc_linkcount_->BindPathHash(path_hash) &&
    sql_inc_linkcount_->BindDelta(delta)        &&
    sql_inc_linkcount_->Execute();
  assert(retval);
  sql_inc_linkcount_->Reset();
}


void WritableCatalog::TouchEntry(const DirectoryEntryBase &entry,
                                 const hash::Md5 &path_hash) {
  SetDirty();

  bool retval =
    sql_touch_->BindPathHash(path_hash) &&
    sql_touch_->BindDirentBase(entry)   &&
    sql_touch_->Execute();
  assert(retval);
  sql_touch_->Reset();
}


void WritableCatalog::UpdateEntry(const DirectoryEntry &entry,
                                  const hash::Md5 &path_hash) {
  SetDirty();

  bool retval =
    sql_update_->BindPathHash(path_hash) &&
    sql_update_->BindDirent(entry)       &&
    sql_update_->Execute();
  assert(retval);
  sql_update_->Reset();
}


/**
 * Sets the last modified time stamp of this catalog to current time.
 * @return true on success, false otherwise
 */
void WritableCatalog::UpdateLastModified() {
  const time_t now = time(NULL);
  const string sql = "INSERT OR REPLACE INTO properties "
     "(key, value) VALUES ('last_modified', '" + StringifyInt(now) + "');";
  bool retval = Sql(database(), sql).Execute();
  assert(retval);
}


/**
 * Increments the revision of the catalog in the database.
 * @return true on success, false otherwise
 */
void WritableCatalog::IncrementRevision() {
  const string sql =
    "UPDATE properties SET value=value+1 WHERE key='revision';";
  bool retval = Sql(database(), sql).Execute();
  assert(retval);
}


/**
 * Sets the content hash of the previous catalog revision.
 * @return true on success, false otherwise
 */
void WritableCatalog::SetPreviousRevision(const hash::Any &hash) {
  const string sql = "INSERT OR REPLACE INTO properties "
    "(key, value) VALUES ('previous_revision', '" + hash.ToString() + "');";
  bool retval = Sql(database(), sql).Execute();
  assert(retval);
}


/**
 * Moves a subtree from this catalog into a just created nested catalog.
 */
void WritableCatalog::Partition(WritableCatalog *new_nested_catalog) {
  // Create connection between parent and child catalogs
  MakeTransitionPoint(new_nested_catalog->path().ToString());
  new_nested_catalog->MakeNestedRoot();
  delta_counters_.d_subtree_dir++;  // Root directory in nested catalog

  // Move the present directory tree into the newly created nested catalog
  // if we hit nested catalog mountpoints on the way, we return them through
  // the passed list
  vector<string> GrandChildMountpoints;
  MoveToNested(new_nested_catalog->path().ToString(), new_nested_catalog,
               &GrandChildMountpoints);

  // Nested catalog mountpoints found in the moved directory structure are now
  // links to nested catalogs of the newly created nested catalog.
  // Move these references into the new nested catalog
  MoveCatalogsToNested(GrandChildMountpoints, new_nested_catalog);
}


void WritableCatalog::MakeTransitionPoint(const string &mountpoint) {
  // Find the directory entry to edit
  DirectoryEntry transition_entry;
  bool retval = LookupPath(PathString(mountpoint.data(), mountpoint.length()),
                           &transition_entry);
  assert(retval);

  assert(transition_entry.IsDirectory() &&
         !transition_entry.IsNestedCatalogRoot());

  transition_entry.set_is_nested_catalog_mountpoint(true);
  UpdateEntry(transition_entry, mountpoint);
}


void WritableCatalog::MakeNestedRoot() {
  DirectoryEntry root_entry;
  bool retval = LookupPath(path(), &root_entry);
  assert(retval);

  assert(root_entry.IsDirectory() && !root_entry.IsNestedCatalogMountpoint());

  root_entry.set_is_nested_catalog_root(true);
  UpdateEntry(root_entry, path().ToString());
}


void WritableCatalog::MoveToNestedRecursively(
       const string directory,
       WritableCatalog *new_nested_catalog,
       vector<string> *grand_child_mountpoints)
{
  // After creating a new nested catalog we have move all elements
  // now contained by the new one.  List and move them recursively.
  DirectoryEntryList listing;
  bool retval = ListingPath(PathString(directory.data(), directory.length()),
                            &listing);
  assert(retval);

  // Go through the listing
  string full_path;
  for (DirectoryEntryList::const_iterator i = listing.begin(),
       iEnd = listing.end(); i != iEnd; ++i)
  {
    full_path = directory + "/";
    full_path.append(i->name().GetChars(), i->name().GetLength());

    // The entries are first inserted into the new catalog
    new_nested_catalog->AddEntry(*i, full_path);

    // Then we check if we have some special cases:
    if (i->IsNestedCatalogMountpoint()) {
      grand_child_mountpoints->push_back(full_path);
    } else if (i->IsDirectory()) {
      // Recurse deeper into the directory tree
      MoveToNestedRecursively(full_path, new_nested_catalog,
                              grand_child_mountpoints);
    }

    // Remove the entry from the current catalog
    RemoveEntry(full_path);
  }
}


void WritableCatalog::MoveCatalogsToNested(
       const vector<string> &nested_catalogs,
       WritableCatalog *new_nested_catalog)
{
  for (vector<string>::const_iterator i = nested_catalogs.begin(),
       iEnd = nested_catalogs.end(); i != iEnd; ++i)
  {
    hash::Any hash_nested;
    bool retval = FindNested(PathString(i->data(), i->length()),
                             &hash_nested);
    assert(retval);

    Catalog *attached_reference = NULL;
    RemoveNestedCatalog(*i, &attached_reference);

    new_nested_catalog->InsertNestedCatalog(*i, attached_reference,
                                            hash_nested);
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
 * @return true on success, false otherwise
 */
void WritableCatalog::InsertNestedCatalog(const string &mountpoint,
                                          Catalog *attached_reference,
                                          const hash::Any content_hash)
{
  const string sha1_string = (!content_hash.IsNull()) ?
                             content_hash.ToString() : "";

  Sql stmt(database(),
    "INSERT INTO nested_catalogs (path, sha1) VALUES (:p, :sha1);");
  bool retval =
    stmt.BindText(1, mountpoint) &&
    stmt.BindText(2, sha1_string) &&
    stmt.Execute();
  assert(retval);

  // If a reference of the in-memory object of the newly referenced
  // catalog was passed, we add this to our own children
  if (attached_reference != NULL)
    AddChild(attached_reference);

  delta_counters_.d_self_nested++;
}


/**
 * Remove a nested catalog reference from the database.
 * If the catalog 'mountpoint' is currently attached as a child, it will be
 * removed, too (but not detached).
 * @param[in] mountpoint the mountpoint of the nested catalog to dereference in
              the database
 * @param[out] attached_reference is set to the object of the attached child or
 *             to NULL
 * @return true on success, false otherwise
 */
void WritableCatalog::RemoveNestedCatalog(const string &mountpoint,
                                          Catalog **attached_reference)
{
  hash::Any dummy;
  bool retval = FindNested(PathString(mountpoint.data(), mountpoint.length()),
                           &dummy);
  assert(retval);

  Sql stmt(database(),
           "DELETE FROM nested_catalogs WHERE path = :p;");
  retval =
    stmt.BindText(1, mountpoint) &&
    stmt.Execute();
  assert(retval);

  // If the reference was successfully deleted, we also have to check whether
  // there is also an attached reference in our in-memory data.
  // In this case we remove the child and return it through **attached_reference
  Catalog *child = FindChild(PathString(mountpoint.data(),
                                        mountpoint.length()));
  if (child != NULL)
    RemoveChild(child);
  if (attached_reference != NULL)
    *attached_reference = child;

  delta_counters_.d_self_nested--;
}


/**
 * Updates the link to a nested catalog in the database.
 * @param path the path of the nested catalog to update
 * @param hash the hash to set the given nested catalog link to
 * @return true on success, false otherwise
 */
void WritableCatalog::UpdateNestedCatalog(const string &path,
                                          const hash::Any &hash)
{
  const string sha1_str = hash.ToString();
  const string sql = "UPDATE nested_catalogs SET sha1 = :sha1 "
    "WHERE path = :path;";
  Sql stmt(database(), sql);

  bool retval =
    stmt.BindText(1, sha1_str) &&
    stmt.BindText(2, path) &&
    stmt.Execute();

  assert(retval);
}


void WritableCatalog::MergeIntoParent() {
  assert(!IsRoot());
  WritableCatalog *parent = GetWritableParent();

  CopyToParent();

  // Copy the nested catalog references
  CopyCatalogsToParent();

  // Fix counters in parent
  delta_counters_.PopulateToParent(&parent->delta_counters_);
  Counters counters;
  bool retval = GetCounters(&counters);
  assert(retval);
  counters.ApplyDelta(delta_counters_);
  counters.MergeIntoParent(&parent->delta_counters_);

  // Remove the nested catalog reference for this nested catalog.
  // From now on this catalog will be dangling!
  parent->RemoveNestedCatalog(this->path().ToString(), NULL);
}


void WritableCatalog::CopyCatalogsToParent() {
  WritableCatalog *parent = GetWritableParent();

  // Obtain a list of all nested catalog references
  NestedCatalogList *nested_catalog_references = ListNestedCatalogs();

  // Go through the list and update the databases
  // simultaneously we are checking if the referenced catalogs are currently
  // attached and update the in-memory data structures as well
  for (NestedCatalogList::const_iterator i = nested_catalog_references->begin(),
       iEnd = nested_catalog_references->end(); i != iEnd; ++i)
  {
    Catalog *child = FindChild(i->path);
    parent->InsertNestedCatalog(i->path.ToString(), child, i->hash);
    parent->delta_counters_.d_self_nested--;  // Will be fixed later
  }
}

void WritableCatalog::CopyToParent() {
  // We could simply copy all entries from this database to the 'other' database
  // BUT: 1. this would create collisions in hardlink group IDs.
  //         therefor we first update all hardlink group IDs to fit behind the
  //         ones in the 'other' database
  //      2. the root entry of the nested catalog is present twice:
  //         1. in the parent directory (as mount point) and
  //         2. in the nested catalog (as root entry)
  //         therefore we delete the mount point from the parent before merging

  WritableCatalog *parent = GetWritableParent();

  // Update hardlink group IDs in this nested catalog.
  // To avoid collisions we add the maximal present hardlink group ID in parent
  // to all hardlink group IDs in the nested catalog.
  // (CAUTION: hardlink group ID is saved in the inode field --> legacy :-) )
  const uint64_t offset = static_cast<uint64_t>(parent->GetMaxLinkId()) << 32;
  const string update_link_ids =
    "UPDATE catalog SET hardlinks = hardlinks + " + StringifyInt(offset) +
    " WHERE hardlinks > (1 << 32);";

  Sql sql_update_link_ids(database(), update_link_ids);
  bool retval = sql_update_link_ids.Execute();
  assert(retval);

  // Remove the nested catalog root.
  // It is already present in the parent.
  RemoveEntry(this->path().ToString());

  // Now copy all DirectoryEntries to the 'other' catalog.
  // There will be no data collisions, as we resolved them beforehand
  if (dirty_)
    Commit();
  if (parent->dirty_)
    parent->Commit();
  Sql sql_attach(database(), "ATTACH '" + parent->database_path() +
                             "' AS other;");
  retval = sql_attach.Execute();
  assert(retval);
  retval = Sql(database(), "INSERT INTO other.catalog "
                           "SELECT * FROM main.catalog;").Execute();
  assert(retval);
  retval = Sql(database(), "DETACH other;").Execute();
  assert(retval);
  parent->SetDirty();

  // Change the just copied nested catalog root to an ordinary directory
  // (the nested catalog is merged into it's parent)
  DirectoryEntry old_root_entry;
  retval = parent->LookupPath(this->path(), &old_root_entry);
  assert(retval);

  assert(old_root_entry.IsDirectory() &&
         old_root_entry.IsNestedCatalogMountpoint() &&
         !old_root_entry.IsNestedCatalogRoot());

  // Remove the nested catalog root mark
  old_root_entry.set_is_nested_catalog_mountpoint(false);
  parent->UpdateEntry(old_root_entry, this->path().ToString());
}


/**
 * Writes delta_counters_ to the database.
 */
void WritableCatalog::UpdateCounters() {
  SqlUpdateCounter sql_counter(database());
  bool retval;

  if (delta_counters_.d_self_regular != 0) {
    retval =
      sql_counter.BindCounter("self_regular") &&
      sql_counter.BindDelta(delta_counters_.d_self_regular) &&
      sql_counter.Execute();
    assert(retval);
    sql_counter.Reset();
  }

  if (delta_counters_.d_self_symlink != 0) {
    retval =
      sql_counter.BindCounter("self_symlink") &&
      sql_counter.BindDelta(delta_counters_.d_self_symlink) &&
      sql_counter.Execute();
    assert(retval);
    sql_counter.Reset();
  }

  if (delta_counters_.d_self_dir != 0) {
    retval =
      sql_counter.BindCounter("self_dir") &&
      sql_counter.BindDelta(delta_counters_.d_self_dir) &&
      sql_counter.Execute();
    assert(retval);
    sql_counter.Reset();
  }

  if (delta_counters_.d_self_nested != 0) {
    retval =
      sql_counter.BindCounter("self_nested") &&
      sql_counter.BindDelta(delta_counters_.d_self_nested) &&
      sql_counter.Execute();
    assert(retval);
    sql_counter.Reset();
  }

  if (delta_counters_.d_subtree_regular != 0) {
    retval =
      sql_counter.BindCounter("subtree_regular") &&
      sql_counter.BindDelta(delta_counters_.d_subtree_regular) &&
      sql_counter.Execute();
    assert(retval);
    sql_counter.Reset();
  }

  if (delta_counters_.d_subtree_symlink != 0) {
    retval =
      sql_counter.BindCounter("subtree_symlink") &&
      sql_counter.BindDelta(delta_counters_.d_subtree_symlink) &&
      sql_counter.Execute();
    assert(retval);
    sql_counter.Reset();
  }

  if (delta_counters_.d_subtree_dir != 0) {
    retval =
      sql_counter.BindCounter("subtree_dir") &&
      sql_counter.BindDelta(delta_counters_.d_subtree_dir) &&
      sql_counter.Execute();
    assert(retval);
    sql_counter.Reset();
  }

  if (delta_counters_.d_subtree_nested != 0) {
    retval =
      sql_counter.BindCounter("subtree_nested") &&
      sql_counter.BindDelta(delta_counters_.d_subtree_nested) &&
      sql_counter.Execute();
    assert(retval);
    sql_counter.Reset();
  }
}

}  // namespace catalog
