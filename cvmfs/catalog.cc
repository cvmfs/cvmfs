/**
 * This file is part of the CernVM File System.
 */

#include "catalog.h"

#include <errno.h>

#include <algorithm>
#include <cassert>

#include "catalog_mgr.h"
#include "logging.h"
#include "platform.h"
#include "smalloc.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace catalog {


/**
 * Open a catalog outside the framework of a catalog manager.
 */
Catalog* Catalog::AttachFreely(const string     &root_path,
                               const string     &file,
                               const shash::Any &catalog_hash,
                                     Catalog    *parent,
                               const bool        is_nested) {
  Catalog *catalog =
    new Catalog(PathString(root_path.data(), root_path.length()),
                catalog_hash,
                parent,
                is_nested);
  const bool successful_init = catalog->InitStandalone(file);
  if (!successful_init) {
    delete catalog;
    return NULL;
  }
  return catalog;
}


Catalog::Catalog(const PathString &path,
                 const shash::Any &catalog_hash,
                       Catalog    *parent,
                 const bool        is_nested) :
  catalog_hash_(catalog_hash),
  path_(path),
  volatile_flag_(false),
  is_root_(parent == NULL && !is_nested),
  managed_database_(false),
  parent_(parent),
  nested_catalog_cache_dirty_(true),
  voms_authz_status_(kVomsUnknown),
  initialized_(false)
{
  max_row_id_ = 0;
  inode_annotation_ = NULL;
  lock_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);

  database_ = NULL;
  uid_map_ = NULL;
  gid_map_ = NULL;
  sql_listing_ = NULL;
  sql_lookup_md5path_ = NULL;
  sql_lookup_nested_ = NULL;
  sql_list_nested_ = NULL;
  sql_all_chunks_ = NULL;
  sql_chunks_listing_ = NULL;
  sql_lookup_xattrs_ = NULL;
}


Catalog::~Catalog() {
  pthread_mutex_destroy(lock_);
  free(lock_);
  FinalizePreparedStatements();
  delete database_;
}


/**
 * InitPreparedStatement uses polymorphism in case of a r/w catalog.
 * FinalizePreparedStatements is called in the destructor where
 * polymorphism does not work any more and has to be called both in
 * the WritableCatalog and the Catalog destructor
 */
void Catalog::InitPreparedStatements() {
  sql_listing_         = new SqlListing(database());
  sql_lookup_md5path_  = new SqlLookupPathHash(database());
  sql_lookup_nested_   = new SqlNestedCatalogLookup(database());
  sql_list_nested_     = new SqlNestedCatalogListing(database());
  sql_all_chunks_      = new SqlAllChunks(database());
  sql_chunks_listing_  = new SqlChunksListing(database());
  sql_lookup_xattrs_   = new SqlLookupXattrs(database());
}


void Catalog::FinalizePreparedStatements() {
  delete sql_lookup_xattrs_;
  delete sql_chunks_listing_;
  delete sql_all_chunks_;
  delete sql_listing_;
  delete sql_lookup_md5path_;
  delete sql_lookup_nested_;
  delete sql_list_nested_;
}


bool Catalog::InitStandalone(const std::string &database_file) {
  bool retval = OpenDatabase(database_file);
  if (!retval) {
    return false;
  }

  InodeRange inode_range;
  inode_range.MakeDummy();
  set_inode_range(inode_range);
  return true;
}


bool Catalog::ReadCatalogCounters() {
  assert(database_ != NULL);
  bool statistics_loaded;
  if (database().schema_version() <
      CatalogDatabase::kLatestSupportedSchema - CatalogDatabase::kSchemaEpsilon)
  {
    statistics_loaded =
      counters_.ReadFromDatabase(database(), LegacyMode::kLegacy);
  } else if (database().schema_revision() < 2) {
    statistics_loaded =
      counters_.ReadFromDatabase(database(), LegacyMode::kNoXattrs);
  } else if (database().schema_revision() < 3) {
    statistics_loaded =
      counters_.ReadFromDatabase(database(), LegacyMode::kNoExternals);
  } else {
    statistics_loaded = counters_.ReadFromDatabase(database());
  }
  return statistics_loaded;
}


/**
 * Establishes the database structures and opens the sqlite database file.
 * @param db_path the absolute path to the database file on local file system
 * @return true on successful initialization otherwise false
 */
bool Catalog::OpenDatabase(const string &db_path) {
  database_ = CatalogDatabase::Open(db_path, DatabaseOpenMode());
  if (NULL == database_) {
    return false;
  }

  InitPreparedStatements();

  // Set the database file ownership if requested
  if (managed_database_) {
    database_->TakeFileOwnership();
  }

  // Find out the maximum row id of this database file
  SqlCatalog sql_max_row_id(database(), "SELECT MAX(rowid) FROM catalog;");
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
    if (database_->HasProperty("root_prefix")) {
      const std::string root_prefix =
                             database_->GetProperty<std::string>("root_prefix");
      root_prefix_.Assign(root_prefix.data(), root_prefix.size());
      LogCvmfs(kLogCatalog, kLogDebug,
               "found root prefix %s in root catalog file %s",
               root_prefix_.c_str(), db_path.c_str());
    } else {
      LogCvmfs(kLogCatalog, kLogDebug,
               "no root prefix for root catalog file %s", db_path.c_str());
    }
  }

  // Get volatile content flag
  volatile_flag_ = database_->GetPropertyDefault<bool>("volatile",
                                                       volatile_flag_);

  // Read Catalog Counter Statistics
  if (!ReadCatalogCounters()) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "failed to load statistics counters for catalog %s (file %s)",
             path_.c_str(), db_path.c_str());
    return false;
  }

  if (HasParent()) {
    parent_->AddChild(this);
  }

  initialized_ = true;
  return true;
}


/**
 * Performs a lookup on this Catalog for a given MD5 path hash.
 * @param md5path the MD5 hash of the searched path
 * @param expand_symlink indicates if variables in symlink should be resolved
 * @param dirent will be set to the found DirectoryEntry
 * @return true if DirectoryEntry was successfully found, false otherwise
 */
bool Catalog::LookupEntry(const shash::Md5 &md5path, const bool expand_symlink,
                          DirectoryEntry *dirent) const
{
  assert(IsInitialized());

  pthread_mutex_lock(lock_);
  sql_lookup_md5path_->BindPathHash(md5path);
  bool found = sql_lookup_md5path_->FetchRow();
  if (found && (dirent != NULL)) {
    *dirent = sql_lookup_md5path_->GetDirent(this, expand_symlink);
    FixTransitionPoint(md5path, dirent);
  }
  sql_lookup_md5path_->Reset();
  pthread_mutex_unlock(lock_);

  return found;
}


/**
 * Performs a lookup on this Catalog for a given MD5 path hash.
 * @param md5path the MD5 hash of the searched path
 * @param dirent will be set to the found DirectoryEntry
 * @return true if DirectoryEntry was successfully found, false otherwise
 */
bool Catalog::LookupMd5Path(const shash::Md5 &md5path,
                            DirectoryEntry *dirent) const
{
  return LookupEntry(md5path, true, dirent);
}


bool Catalog::LookupRawSymlink(const PathString &path,
                               LinkString *raw_symlink) const
{
  DirectoryEntry dirent;
  bool result = (LookupEntry(shash::Md5(path.GetChars(), path.GetLength()),
                             false, &dirent));
  if (result)
    raw_symlink->Assign(dirent.symlink());
  return result;
}


bool Catalog::LookupXattrsMd5Path(
  const shash::Md5 &md5path,
  XattrList *xattrs) const
{
  assert(IsInitialized());

  pthread_mutex_lock(lock_);
  sql_lookup_xattrs_->BindPathHash(md5path);
  bool found = sql_lookup_xattrs_->FetchRow();
  if (found && (xattrs != NULL)) {
    *xattrs = sql_lookup_xattrs_->GetXattrs();
  }
  sql_lookup_xattrs_->Reset();
  pthread_mutex_unlock(lock_);

  return found;
}


/**
 * Perform a listing of the directory with the given MD5 path hash.
 * @param path_hash the MD5 hash of the path of the directory to list
 * @param listing will be set to the resulting DirectoryEntryList
 * @return true on successful listing, false otherwise
 */
bool Catalog::ListingMd5PathStat(const shash::Md5 &md5path,
                                 StatEntryList *listing) const
{
  assert(IsInitialized());

  DirectoryEntry dirent;
  StatEntry entry;

  pthread_mutex_lock(lock_);
  sql_listing_->BindPathHash(md5path);
  while (sql_listing_->FetchRow()) {
    dirent = sql_listing_->GetDirent(this);
    FixTransitionPoint(md5path, &dirent);
    entry.name = dirent.name();
    entry.info = dirent.GetStatStructure();
    listing->PushBack(entry);
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
 * @param expand_symlink defines if magic symlinks should be resolved
 * @return true on successful listing, false otherwise
 */
bool Catalog::ListingMd5Path(const shash::Md5 &md5path,
                             DirectoryEntryList *listing,
                             const bool expand_symlink) const
{
  assert(IsInitialized());

  pthread_mutex_lock(lock_);
  sql_listing_->BindPathHash(md5path);
  while (sql_listing_->FetchRow()) {
    DirectoryEntry dirent = sql_listing_->GetDirent(this, expand_symlink);
    FixTransitionPoint(md5path, &dirent);
    listing->push_back(dirent);
  }
  sql_listing_->Reset();
  pthread_mutex_unlock(lock_);

  return true;
}


bool Catalog::AllChunksBegin() {
  return sql_all_chunks_->Open();
}


bool Catalog::AllChunksNext(shash::Any *hash, zlib::Algorithms *compression_alg)
{
  return sql_all_chunks_->Next(hash, compression_alg);
}


bool Catalog::AllChunksEnd() {
  return sql_all_chunks_->Close();
}


/**
 * Hash algorithm is given by the unchunked file.
 * Could be figured out by a join but it is faster if the user of this
 * method tells us.
 */
bool Catalog::ListMd5PathChunks(const shash::Md5  &md5path,
                                const shash::Algorithms interpret_hashes_as,
                                FileChunkList    *chunks) const
{
  assert(IsInitialized() && chunks->IsEmpty());

  pthread_mutex_lock(lock_);
  sql_chunks_listing_->BindPathHash(md5path);
  while (sql_chunks_listing_->FetchRow()) {
    chunks->PushBack(sql_chunks_listing_->GetFileChunk(interpret_hashes_as));
  }
  sql_chunks_listing_->Reset();
  pthread_mutex_unlock(lock_);

  return true;
}


const Catalog::HashVector& Catalog::GetReferencedObjects() const {
  if (!referenced_hashes_.empty()) {
    return referenced_hashes_;
  }

  // retrieve all referenced content hashes of both files and file chunks
  SqlListContentHashes list_content_hashes(database());
  while (list_content_hashes.FetchRow()) {
    referenced_hashes_.push_back(list_content_hashes.GetHash());
  }

  return referenced_hashes_;
}


void Catalog::TakeDatabaseFileOwnership() {
  managed_database_ = true;
  if (NULL != database_) {
    database_->TakeFileOwnership();
  }
}


void Catalog::DropDatabaseFileOwnership() {
  managed_database_ = false;
  if (NULL != database_) {
    database_->DropFileOwnership();
  }
}


uint64_t Catalog::GetTTL() const {
  pthread_mutex_lock(lock_);
  const uint64_t result =
    database().GetPropertyDefault<uint64_t>("TTL", kDefaultTTL);
  pthread_mutex_unlock(lock_);
  return result;
}


bool Catalog::GetVOMSAuthz(string *authz) const {
  bool result;
  pthread_mutex_lock(lock_);
  if (voms_authz_status_ == kVomsPresent) {
    if (authz) {*authz = voms_authz_;}
    result = true;
  } else if (voms_authz_status_ == kVomsNone) {
    result = false;
  } else {
    if (database().HasProperty("voms_authz")) {
      voms_authz_ = database().GetProperty<string>("voms_authz");
      if (authz) {*authz = voms_authz_;}
      voms_authz_status_ = kVomsPresent;
    } else {
      voms_authz_status_ = kVomsNone;
    }
    result = (voms_authz_status_ == kVomsPresent);
  }
  pthread_mutex_unlock(lock_);
  return result;
}

uint64_t Catalog::GetRevision() const {
  pthread_mutex_lock(lock_);
  const uint64_t result =
    database().GetPropertyDefault<uint64_t>("revision", 0);
  pthread_mutex_unlock(lock_);
  return result;
}

uint64_t Catalog::GetLastModified() const {
  const std::string prop_name = "last_modified";
  return (database().HasProperty(prop_name))
    ? database().GetProperty<int>(prop_name)
    : 0u;
}


uint64_t Catalog::GetNumChunks() const {
  return counters_.Get("self_regular") + counters_.Get("self_chunks");
}


uint64_t Catalog::GetNumEntries() const {
  const string sql = "SELECT count(*) FROM catalog;";

  pthread_mutex_lock(lock_);
  SqlCatalog stmt(database(), sql);
  const uint64_t result = (stmt.FetchRow()) ? stmt.RetrieveInt64(0) : 0;
  pthread_mutex_unlock(lock_);

  return result;
}


shash::Any Catalog::GetPreviousRevision() const {
  pthread_mutex_lock(lock_);
  const std::string hash_string =
    database().GetPropertyDefault<std::string>("previous_revision", "");
  pthread_mutex_unlock(lock_);
  return (!hash_string.empty())
    ? shash::MkFromHexPtr(shash::HexPtr(hash_string), shash::kSuffixCatalog)
    : shash::Any();
}


string Catalog::PrintMemStatistics() const {
  sqlite::MemStatistics stats;
  pthread_mutex_lock(lock_);
  database().GetMemStatistics(&stats);
  pthread_mutex_unlock(lock_);
  return string(path().GetChars(), path().GetLength()) + ": " +
    StringifyInt(stats.lookaside_slots_used) + " / " +
      StringifyInt(stats.lookaside_slots_max) + " slots -- " +
      StringifyInt(stats.lookaside_hit) + " hits, " +
      StringifyInt(stats.lookaside_miss_size) + " misses-size, " +
      StringifyInt(stats.lookaside_miss_full) + " misses-full -- " +
    StringifyInt(stats.page_cache_used / 1024) + " kB pages -- " +
      StringifyInt(stats.page_cache_hit) + " hits, " +
      StringifyInt(stats.page_cache_miss) + " misses -- " +
    StringifyInt(stats.schema_used / 1024) + " kB schema -- " +
    StringifyInt(stats.stmt_used / 1024) + " kB statements";
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
                                 const uint64_t hardlink_group) const {
  assert(IsInitialized());

  if (inode_range_.IsDummy()) {
    return DirectoryEntry::kInvalidInode;
  }

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

  if (inode_annotation_) {
    inode = inode_annotation_->Annotate(inode);
  }

  return inode;
}


/**
 * Revert the inode mangling.  Required to lookup using inodes.
 */
uint64_t Catalog::GetRowIdFromInode(const inode_t inode) const {
  inode_t row_id = inode;
  if (inode_annotation_)
    row_id = inode_annotation_->Strip(row_id);

  return row_id - inode_range_.offset;
}


/**
 * Get a list of all registered nested catalogs in this catalog.
 * @return  a list of all nested catalog references of this catalog.
 */
const Catalog::NestedCatalogList& Catalog::ListNestedCatalogs() const {
  pthread_mutex_lock(lock_);

  if (nested_catalog_cache_dirty_) {
    LogCvmfs(kLogCatalog, kLogDebug, "refreshing nested catalog cache of '%s'",
             path().c_str());
    while (sql_list_nested_->FetchRow()) {
      NestedCatalog nested;
      nested.path = sql_list_nested_->GetMountpoint();
      nested.hash = sql_list_nested_->GetContentHash();
      nested.size = sql_list_nested_->GetSize();
      nested_catalog_cache_.push_back(nested);
    }
    sql_list_nested_->Reset();
    nested_catalog_cache_dirty_ = false;
  }

  pthread_mutex_unlock(lock_);
  return nested_catalog_cache_;
}


/**
 * Drops the nested catalog cache. Usually this is only useful in subclasses
 * that implement writable catalogs.
 *
 * Note: this action is _not_ secured by the catalog's mutex. If serialisation
 *       is required the subclass needs to ensure that.
 */
void Catalog::ResetNestedCatalogCacheUnprotected() {
  nested_catalog_cache_.clear();
  nested_catalog_cache_dirty_ = true;
}


/**
 * Looks for a specific registered nested catalog based on a path.
 */
bool Catalog::FindNested(const PathString &mountpoint,
                         shash::Any *hash, uint64_t *size) const
{
  pthread_mutex_lock(lock_);
  sql_lookup_nested_->BindSearchPath(mountpoint);
  bool found = sql_lookup_nested_->FetchRow();
  if (found && (hash != NULL)) {
    *hash = sql_lookup_nested_->GetContentHash();
    *size = sql_lookup_nested_->GetSize();
  }
  sql_lookup_nested_->Reset();
  pthread_mutex_unlock(lock_);

  return found;
}


/**
 * Sets a new object to do inode annotations (or set to NULL)
 * The annotation object is not owned by the catalog.
 */
void Catalog::SetInodeAnnotation(InodeAnnotation *new_annotation) {
  pthread_mutex_lock(lock_);
  // Since annotated inodes could come back to the catalog in order to
  // get stripped, exchanging the annotation is not allowed
  assert((inode_annotation_ == NULL) || (inode_annotation_ == new_annotation));
  inode_annotation_ = new_annotation;
  pthread_mutex_unlock(lock_);
}


void Catalog::SetOwnerMaps(const OwnerMap *uid_map, const OwnerMap *gid_map) {
  uid_map_ = (uid_map && uid_map->HasEffect()) ? uid_map : NULL;
  gid_map_ = (gid_map && gid_map->HasEffect()) ? gid_map : NULL;
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
void Catalog::FixTransitionPoint(const shash::Md5 &md5path,
                                 DirectoryEntry *dirent) const
{
  if (dirent->IsNestedCatalogRoot() && HasParent()) {
    DirectoryEntry parent_dirent;
    const bool retval = parent_->LookupMd5Path(md5path, &parent_dirent);
    assert(retval);

    dirent->set_inode(parent_dirent.inode());
  }
}

}  // namespace catalog
