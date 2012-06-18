/**
 * This file is part of the CernVM File System.
 */

#include "catalog.h"

#include <errno.h>

#include "platform.h"
#include "catalog_mgr.h"
#include "util.h"
#include "logging.h"
#include "smalloc.h"

using namespace std;  // NOLINT

namespace catalog {

const int kSqliteThreadMem = 4;  /**< TODO SQLite3 heap limit per thread */


Catalog::Catalog(const PathString &path, Catalog *parent) {
  path_ = path;
  parent_ = parent;
  max_row_id_ = 0;
  lock_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


Catalog::~Catalog() {
  pthread_mutex_destroy(lock_);
  free(lock_);
  FinalizePreparedStatements();
  sqlite3_close(database_);
}

/**
 * InitPreparedStatement uses polymorphism in case of a r/w catalog.
 * FinalizePreparedStatements is called in the destructor where
 * polymorphism does not work any more and has to be called both in
 * the WritableCatalog and the Catalog destructor
 */
void Catalog::InitPreparedStatements() {
  sql_listing_ = new ListingLookupSqlStatement(database_);
  sql_lookup_md5path_ = new PathHashLookupSqlStatement(database_);
  sql_lookup_inode_ = new InodeLookupSqlStatement(database_);
  sql_lookup_nested_ = new FindNestedCatalogSqlStatement(database_);
  sql_list_nested_ = new ListNestedCatalogsSqlStatement(database_);
}


void Catalog::FinalizePreparedStatements() {
  delete sql_listing_;
  delete sql_lookup_md5path_;
  delete sql_lookup_inode_;
  delete sql_lookup_nested_;
  delete sql_list_nested_;
}


/**
 * Establishes the database structures and opens the sqlite database file.
 * @param db_path the absolute path to the database file on local file system
 * @return true on successful initialization otherwise false
 */
bool Catalog::OpenDatabase(const string &db_path) {
  const int flags = DatabaseOpenFlags();
  database_path_ = db_path;

  // Open database file (depending on the flags read-only or read-write)
  LogCvmfs(kLogCatalog, kLogDebug, "opening database file %s", db_path.c_str());
  if (SQLITE_OK != sqlite3_open_v2(db_path.c_str(), &database_, flags, NULL)) {
    LogCvmfs(kLogCatalog, kLogDebug, "cannot open catalog database file %s",
             db_path.c_str());
    return false;
  }
  sqlite3_extended_result_codes(database_, 1);
  
  // Read-ahead into file system buffers
  int fd_readahead = open(db_path.c_str(), O_RDONLY);
  if (fd_readahead < 0) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to open %s for read-ahead (%d)",
             db_path.c_str(), errno);
    return false;
  }
  int retval = platform_readahead(fd_readahead);
  if (retval != 0) {
    LogCvmfs(kLogCatalog, kLogDebug, "failed to read-ahead %s (%d)",
             db_path.c_str(), errno);
    close(fd_readahead);
    return false;
  }
  close(fd_readahead);

  // Turbo mode
  //SqlStatement transaction(database_, "PRAGMA locking_mode=EXCLUSIVE;");
  //bool retval = transaction.Execute();
  //assert(retval == true);

  InitPreparedStatements();

  // Find out the maximum row id of this database file
  SqlStatement sql_max_row_id(database_, "SELECT MAX(rowid) FROM catalog;");
  if (!sql_max_row_id.FetchRow()) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "Cannot retrieve maximal row id for database file %s "
             "(SqliteErrorcode: %d)",
             db_path.c_str(), sql_max_row_id.GetLastError());
    return false;
  }
  max_row_id_ = sql_max_row_id.RetrieveInt64(0);

  // Get root prefix
  if (IsRoot()) {
    SqlStatement sql_root_prefix(database_, "SELECT value FROM properties "
                                 "WHERE key='root_prefix';");
    if (sql_root_prefix.FetchRow()) {
      root_prefix_.Assign(
        reinterpret_cast<const char *>(sql_root_prefix.RetrieveText(0)),
        strlen(reinterpret_cast<const char *>(sql_root_prefix.RetrieveText(0))));
      LogCvmfs(kLogCatalog, kLogDebug,
               "found root prefix %s in root catalog file %s",
               root_prefix_.c_str(), db_path.c_str());
    } else {
      LogCvmfs(kLogCatalog, kLogDebug,
               "no root prefix for root catalog file %s", db_path.c_str());
    }
  }

  // Get schema version
  SqlStatement sql_schema(database_, "SELECT value FROM properties "
                                     "WHERE key='schema';");
  if (sql_schema.FetchRow()) {
    schema_ = sql_schema.RetrieveDouble(0);
  } else {
    schema_ = 1.0;
  }

  if (!IsRoot()) {
    parent_->AddChild(this);
  }

  return true;
}


/**
 * Performs a lookup on this Catalog for a given inode
 * @param inode the inode to perform the lookup for
 * @param dirent this will be set to the found entry
 * @param parent_md5path this will be set to the hash of the parent path
 * @return true if lookup was successful, false otherwise
 */
bool Catalog::LookupInode(const inode_t inode, DirectoryEntry *dirent,
                          hash::Md5 *parent_md5path) const
{
  assert(IsInitialized());

  pthread_mutex_lock(lock_);
  sql_lookup_inode_->BindRowId(GetRowIdFromInode(inode));
  const bool found = sql_lookup_inode_->FetchRow();

  // Retrieve the DirectoryEntry if needed
  if (found && (dirent != NULL))
      *dirent = sql_lookup_inode_->GetDirectoryEntry(this);

  // Retrieve the path_hash of the parent path if needed
  if (parent_md5path != NULL)
      *parent_md5path = sql_lookup_inode_->GetParentPathHash();

  sql_lookup_inode_->Reset();
  pthread_mutex_unlock(lock_);

  return found;
}


/**
 * Performs a lookup on this Catalog for a given MD5 path hash.
 * @param md5path the MD5 hash of the searched path
 * @param dirent will be set to the found DirectoryEntry
 * @return true if DirectoryEntry was successfully found, false otherwise
 */
bool Catalog::LookupMd5Path(const hash::Md5 &md5path,
                            DirectoryEntry *dirent) const
{
  assert(IsInitialized());

  pthread_mutex_lock(lock_);
  sql_lookup_md5path_->BindPathHash(md5path);
  bool found = sql_lookup_md5path_->FetchRow();
  if (found && (dirent != NULL)) {
    *dirent = sql_lookup_md5path_->GetDirectoryEntry(this);
    FixTransitionPoint(md5path, dirent);
  }
  sql_lookup_md5path_->Reset();
  pthread_mutex_unlock(lock_);

  return found;
}


/**
 * Perform a listing of the directory with the given MD5 path hash.
 * @param path_hash the MD5 hash of the path of the directory to list
 * @param listing will be set to the resulting DirectoryEntryList
 * @return true on successful listing, false otherwise
 */
bool Catalog::ListingMd5PathStat(const hash::Md5 &md5path,
                                 StatEntryList *listing) const
{
  assert(IsInitialized());

  DirectoryEntry dirent;
  StatEntry entry;

  pthread_mutex_lock(lock_);
  sql_listing_->BindPathHash(md5path);
  while (sql_listing_->FetchRow()) {
    dirent = sql_listing_->GetDirectoryEntry(this);
    FixTransitionPoint(md5path, &dirent);
    entry.name = dirent.name();
    entry.info = dirent.GetStatStructure();
    listing->push_back(entry);
  }
  sql_listing_->Reset();
  pthread_mutex_unlock(lock_);

  return true;
}


/**
 * Perform a listing of the directory with the given MD5 path hash.
 * Returns only struct stat values
 * @param path_hash the MD5 hash of the path of the directory to list
 * @param listing will be set to the resulting DirectoryEntryList
 * @return true on successful listing, false otherwise
 */
bool Catalog::ListingMd5Path(const hash::Md5 &md5path,
                             DirectoryEntryList *listing) const
{
  assert(IsInitialized());

  pthread_mutex_lock(lock_);
  sql_listing_->BindPathHash(md5path);
  while (sql_listing_->FetchRow()) {
    DirectoryEntry dirent = sql_listing_->GetDirectoryEntry(this);
    FixTransitionPoint(md5path, &dirent);
    listing->push_back(dirent);
  }
  sql_listing_->Reset();
  pthread_mutex_unlock(lock_);

  return true;
}


uint64_t Catalog::GetTTL() const {
  const string sql = "SELECT value FROM properties WHERE key='TTL';";

  pthread_mutex_lock(lock_);
  SqlStatement stmt(database(), sql);
  const uint64_t result =
    (stmt.FetchRow()) ?  stmt.RetrieveInt64(0) : kDefaultTTL;
  pthread_mutex_unlock(lock_);

  return result;
}


uint64_t Catalog::GetRevision() const {
  const string sql = "SELECT value FROM properties WHERE key='revision';";

  pthread_mutex_lock(lock_);
  SqlStatement stmt(database(), sql);
  const uint64_t result = (stmt.FetchRow()) ? stmt.RetrieveInt64(0) : 0;
  pthread_mutex_unlock(lock_);

  return result;
}


/**
 * Determine the actual inode of a DirectoryEntry.
 * The first used entry from a hardlink group deterimines the inode of the
 * others.
 * @param row_id the row id of a read row in the sqlite database
 * @param hardlink_group the id of a possibly present hardlink group
 * @return the assigned inode number
 */
inode_t Catalog::GetMangledInode(const uint64_t row_id,
                                 const uint64_t hardlink_group)
{
  assert(IsInitialized());

  inode_t inode = row_id + inode_range_.offset;

  // Hardlinks are encoded in catalog-wide unique hard link group ids.
  // These ids must be resolved to actual inode relationships at runtime.
  if (hardlink_group > 0) {
    HardlinkGroupMap::const_iterator inode_iter =
      hardlink_groups_.find(hardlink_group);

    // Use cached entry if possible
    if (inode_iter == hardlink_groups_.end()) {
      hardlink_groups_[hardlink_group] = inode;
    } else {
      inode = inode_iter->second;
    }
  }

  return inode;
}

/**
 * Get a list of all registered nested catalogs in this catalog.
 * @return a list of all nested catalog references of this catalog
 */
Catalog::NestedCatalogList Catalog::ListNestedCatalogs() const {
  NestedCatalogList result;

  pthread_mutex_lock(lock_);
  while (sql_list_nested_->FetchRow()) {
    NestedCatalog nested;
    nested.path = sql_list_nested_->GetMountpoint();
    nested.hash = sql_list_nested_->GetContentHash();
    result.push_back(nested);
  }
  sql_list_nested_->Reset();
  pthread_mutex_unlock(lock_);

  return result;
}


/**
 * Looks for a specific registered nested catalog based on a path.
 */
bool Catalog::FindNested(const PathString &mountpoint, hash::Any *hash) const {
  pthread_mutex_lock(lock_);
  sql_lookup_nested_->BindSearchPath(mountpoint);
  bool found = sql_lookup_nested_->FetchRow();
  if (found && (hash != NULL)) {
    *hash = sql_lookup_nested_->GetContentHash();
  }
  sql_lookup_nested_->Reset();
  pthread_mutex_unlock(lock_);

  return found;
}


/**
 * Add a Catalog as child to this Catalog.
 * @param child the Catalog to define as child
 */
void Catalog::AddChild(Catalog *child) {
  assert(NULL == FindChild(child->path()));

  pthread_mutex_lock(lock_);
  children_[child->path()] = child;
  child->set_parent(this);
  pthread_mutex_unlock(lock_);
}


/**
 * Removes a Catalog from the children list of this Catalog
 * @param child the Catalog to delete as child
 */
void Catalog::RemoveChild(Catalog *child) {
  assert(NULL != FindChild(child->path()));

  pthread_mutex_lock(lock_);
  child->set_parent(NULL);
  children_.erase(child->path());
  pthread_mutex_unlock(lock_);
}


CatalogList Catalog::GetChildren() const {
  CatalogList result;

  pthread_mutex_lock(lock_);
  for (NestedCatalogMap::const_iterator i = children_.begin(),
       iEnd = children_.end(); i != iEnd; ++i)
  {
    result.push_back(i->second);
  }
  pthread_mutex_unlock(lock_);

  return result;
}


/**
 * Find the nested catalog that serves the given path.
 * It might be possible that the path is in fact served by a child of the found
 * nested catalog.
 * @param path the path to find a best fitting catalog for
 * @return a pointer to the best fitting child or NULL if it does not fit at all
 */
Catalog* Catalog::FindSubtree(const PathString &path) const {
  // Check if this catalog fits the beginning of the path.
  if (!path.StartsWith(path_))
    return NULL;

  PathString remaining(path.Suffix(path_.GetLength()));
  remaining.Append("/", 1);

  // now we recombine the path elements successively
  // in order to find a child which serves a part of the path
  PathString path_prefix(path_);
  Catalog *result = NULL;
  // Skip the first '/'
  path_prefix.Append("/", 1);
  const char *c = remaining.GetChars()+1;
  for (unsigned i = 1; i < remaining.GetLength(); ++i, ++c) {
    if (*c == '/') {
      result = FindChild(path_prefix);

      // If we found a child serving a part of the path we can stop searching.
      // Remaining sub path elements are possbily served by a grand child.
      if (result != NULL)
        break;
    }
    path_prefix.Append(c, 1);
  }

  return result;
}


/**
 * Looks for a child catalog, which is a subset of all registered nested
 * catalogs.
 */
Catalog* Catalog::FindChild(const PathString &mountpoint) const {
  NestedCatalogMap::const_iterator nested_iter;

  pthread_mutex_lock(lock_);
  nested_iter = children_.find(mountpoint);
  Catalog* result =
    (nested_iter == children_.end()) ? NULL : nested_iter->second;
  pthread_mutex_unlock(lock_);

  return result;
}


/**
 * For the transtion points for nested catalogs, the inode is ambiquitous.
 * It has to be set to the parent inode because nested catalogs are lazily
 * loaded.
 * @param md5path the MD5 hash of the entry to check
 * @param dirent the DirectoryEntry to perform coherence fixes on
 */
void Catalog::FixTransitionPoint(const hash::Md5 &md5path,
                                 DirectoryEntry *dirent) const
{
  if (dirent->IsNestedCatalogRoot() && !IsRoot()) {
    DirectoryEntry parent_dirent;
    const bool retval = parent_->LookupMd5Path(md5path, &parent_dirent);
    assert(retval);

    dirent->set_inode(parent_dirent.inode());
  }
}

}  // namespace catalog
