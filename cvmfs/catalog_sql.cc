/**
 * This file is part of the CernVM file system.
 */

#include "catalog_sql.h"

#include <cstdlib>
#include <cstring>
#include <cstdlib>

#include "catalog.h"
#include "logging.h"
#include "util.h"

using namespace std;  // NOLINT

namespace catalog {

// Changelog
const float CatalogDatabase::kLatestSchema = 2.5;
const float CatalogDatabase::kLatestSupportedSchema = 2.5;  // + 1.X catalogs (r/o)
// ChangeLog
//   0 --> 1: add size column to nested catalog table,
//            add schema_revision property
const unsigned CatalogDatabase::kLatestSchemaRevision = 1;


bool CatalogDatabase::CheckSchemaCompatibility() {
  return !( (schema_version() >= 2.0-kSchemaEpsilon)                   &&
            (!IsEqualSchema(schema_version(), kLatestSupportedSchema)) &&
            (!IsEqualSchema(schema_version(), 2.4)           ||
             !IsEqualSchema(kLatestSupportedSchema, 2.5)) );
}


bool CatalogDatabase::LiveSchemaUpgradeIfNecessary() {
  assert (read_write());

  if (IsEqualSchema(schema_version(), 2.5) && (schema_revision() == 0)) {
    LogCvmfs(kLogCatalog, kLogDebug, "upgrading schema revision");

    Sql sql_upgrade(*this, "ALTER TABLE nested_catalogs ADD size INTEGER;");
    if (! sql_upgrade.Execute()) {
      LogCvmfs(kLogCatalog, kLogDebug, "failed tp upgrade nested_catalogs");
      return false;
    }

    set_schema_revision(1);
    if (! StoreSchemaRevision()) {
      LogCvmfs(kLogCatalog, kLogDebug, "failed tp upgrade schema revision");
      return false;
    }
  } else {
    LogCvmfs(kLogCatalog, kLogDebug, "schema revision is up to date.");
  }

  return true;
}


bool CatalogDatabase::CreateEmptyDatabase() {
  assert (read_write());

  // generate the catalog table and index structure
  const bool retval =
  Sql(*this,
    "CREATE TABLE catalog "
    "(md5path_1 INTEGER, md5path_2 INTEGER, parent_1 INTEGER, parent_2 INTEGER,"
    " hardlinks INTEGER, hash BLOB, size INTEGER, mode INTEGER, mtime INTEGER,"
    " flags INTEGER, name TEXT, symlink TEXT, uid INTEGER, gid INTEGER, "
    " CONSTRAINT pk_catalog PRIMARY KEY (md5path_1, md5path_2));").Execute()  &&
  Sql(*this,
    "CREATE INDEX idx_catalog_parent "
    "ON catalog (parent_1, parent_2);")                           .Execute()  &&
  Sql(*this,
    "CREATE TABLE chunks "
    "(md5path_1 INTEGER, md5path_2 INTEGER, offset INTEGER, size INTEGER, "
    " hash BLOB, "
    " CONSTRAINT pk_chunks PRIMARY KEY (md5path_1, md5path_2, offset, size), "
    " FOREIGN KEY (md5path_1, md5path_2) REFERENCES "
    "   catalog(md5path_1, md5path_2));")                         .Execute()  &&
  Sql(*this,
    "CREATE TABLE nested_catalogs (path TEXT, sha1 TEXT, size INTEGER, "
    "CONSTRAINT pk_nested_catalogs PRIMARY KEY (path));")         .Execute()  &&
  Sql(*this,
    "CREATE TABLE statistics (counter TEXT, value INTEGER, "
    "CONSTRAINT pk_statistics PRIMARY KEY (counter));")           .Execute();

  if (! retval) {
    PrintSqlError("failed to create catalog database tables.");
  }

  return retval;
}


bool CatalogDatabase::InsertInitialValues(const std::string    &root_path,
                                          const bool            volatile_content,
                                          const DirectoryEntry &root_entry) {
  assert (read_write());
  bool retval = false;

  // Path hashes
  shash::Md5 root_path_hash = shash::Md5(shash::AsciiPtr(root_path));
  shash::Md5 root_parent_hash =
                    (root_path == "")
                      ? shash::Md5()
                      : shash::Md5(shash::AsciiPtr(GetParentPath(root_path)));

  // start initial filling transaction
  retval = Sql(*this, "BEGIN;").Execute();
  if (!retval) {
    PrintSqlError("failed to enter initial filling transaction");
    return false;
  }

  // insert initial values to properties
  if (! this->SetProperty("revision", 0)) {
    PrintSqlError("failed to insert default initial values into the newly created "
                  "catalog tables.");
    return false;
  }

  if (volatile_content) {
    if (! this->SetProperty("volatile", 1)) {
      PrintSqlError("failed to insert volatile flag into the newly created "
                    "catalog tables.");
      return false;
    }
  }

  // create initial statistics counters
  catalog::Counters counters;

  // insert root entry (when given)
  if (!root_entry.IsNegative()) {
    SqlDirentInsert sql_insert(*this);
    retval = sql_insert.BindPathHash(root_path_hash)         &&
             sql_insert.BindParentPathHash(root_parent_hash) &&
             sql_insert.BindDirent(root_entry)               &&
             sql_insert.Execute();
    if (! retval) {
      PrintSqlError("failed to insert root entry into newly created catalog.");
      return false;
    }

    // account for the created root entry
    counters.self.directories = 1;
  }

  // save initial statistics counters
  if (!counters.InsertIntoDatabase(*this)) {
    PrintSqlError("failed to insert initial catalog statistics counters.");
    return false;
  }

  // insert root path (when given)
  if (! root_path.empty()) {
    if (! this->SetProperty("root_prefix", root_path)) {
      PrintSqlError("failed to store root prefix in the newly created catalog.");
      return false;
    }
  }

  // commit initial filling transaction
  retval = Sql(*this, "COMMIT;").Execute();
  if (!retval) {
    PrintSqlError("failed to commit initial filling transaction");
    return false;
  }

  return true;
}


double CatalogDatabase::GetRowIdWasteRatio() const {
  Sql rowid_waste_ratio_query(*this,
   "SELECT 1.0 - CAST(COUNT(*) AS DOUBLE) / MAX(rowid) AS ratio FROM catalog;");
  const bool retval = rowid_waste_ratio_query.FetchRow();
  assert (retval);

  return rowid_waste_ratio_query.RetrieveDouble(0);
}

/**
 * Cleanup unused database space
 *
 * This creates a temporary table 'mapping' filled with the current rowIDs from
 * the catalog table. The new table will implicitly have an auto-increment rowID
 * that is compact. Thus, we create a 'mapping' from each catalog's rowID to a
 * new rowID-space that does not have any gaps. Notice, that the order of old
 * and new rowIDs will stay the same to fulfill the PRIMARY KEY constraints
 * during update.
 * Thereafter the catalog's rowIDs are mapped to their new (unique and compact)
 * rowIDs and the temporary table is deleted.
 *
 * Note: VACUUM used to have a similar behaviour but it was dropped from SQLite
 *       at some point. Since we compute client-inodes from the rowIDs, we are
 *       probably one of the few use cases where a defragmented rowID is indeed
 *       beneficial.
 *
 * See: http://www.sqlite.org/lang_vacuum.html
 */
bool CatalogDatabase::CompactDatabase() const {
  assert (read_write());

  return Sql(*this, "BEGIN;").Execute()                                 &&
         Sql(*this, "CREATE TEMPORARY TABLE mapping AS "
                    "  SELECT rowid AS cid FROM catalog "
                    "  ORDER BY cid;").Execute()                        &&
         Sql(*this, "UPDATE OR ROLLBACK catalog SET "
                    "  rowid = (SELECT mapping.rowid FROM mapping "
                    "           WHERE cid = catalog.rowid);").Execute() &&
         Sql(*this, "DROP TABLE mapping;").Execute()                    &&
         Sql(*this, "COMMIT;").Execute();
}


//------------------------------------------------------------------------------


unsigned SqlDirent::CreateDatabaseFlags(const DirectoryEntry &entry) const {
  unsigned int database_flags = 0;

  if (entry.IsNestedCatalogRoot())
    database_flags |= kFlagDirNestedRoot;
  else if (entry.IsNestedCatalogMountpoint())
    database_flags |= kFlagDirNestedMountpoint;

  if (entry.IsDirectory())
    database_flags |= kFlagDir;
  else if (entry.IsLink())
    database_flags |= kFlagFile | kFlagLink;
  else
    database_flags |= kFlagFile;

  if (entry.IsChunkedFile())
    database_flags |= kFlagFileChunk;

  if (!entry.checksum_ptr()->IsNull())
    StoreHashAlgorithm(entry.checksum_ptr()->algorithm, &database_flags);

  return database_flags;
}

void SqlDirent::StoreHashAlgorithm(const shash::Algorithms algo,
                                   unsigned *flags) const
{
  // Md5 unusable for content hashes
  *flags |= (algo - 1) << kFlagPosHash;
}

shash::Algorithms SqlDirent::RetrieveHashAlgorithm(const unsigned flags) const {
  unsigned in_flags = ((7 << kFlagPosHash) & flags) >> kFlagPosHash;
  // Skip Md5
  in_flags++;
  assert(in_flags < shash::kAny);
  return static_cast<shash::Algorithms>(in_flags);
}

uint32_t SqlDirent::Hardlinks2Linkcount(const uint64_t hardlinks) const {
  return (hardlinks << 32) >> 32;
}

uint32_t SqlDirent::Hardlinks2HardlinkGroup(const uint64_t hardlinks) const {
  return hardlinks >> 32;
}

uint64_t SqlDirent::MakeHardlinks(const uint32_t hardlink_group,
                                  const uint32_t linkcount) const {
  return (static_cast<uint64_t>(hardlink_group) << 32) | linkcount;
}


/**
 * Expands variant symlinks containing $(VARIABLE) string.  Uses the environment
 * variables of the current process (cvmfs2)
 */
void SqlDirent::ExpandSymlink(LinkString *raw_symlink) const {
  const char *c = raw_symlink->GetChars();
  const char *cEnd = c+raw_symlink->GetLength();
  for (; c < cEnd; ++c) {
    if (*c == '$')
      goto expand_symlink;
  }
  return;

 expand_symlink:
  LinkString result;
  for (c = raw_symlink->GetChars(); c < cEnd; ++c) {
    if ((*c == '$') && (c < cEnd-2) && (*(c+1) == '(')) {
      c += 2;
      const char *rpar = c;
      while (rpar < cEnd) {
        if (*rpar == ')')
          goto expand_symlink_getenv;
        rpar++;
      }
      // right parenthesis missing
      result.Append("$(", 2);
      result.Append(c, 1);
      continue;

     expand_symlink_getenv:
      // Check for default value
      const char *default_separator = c;
      const char *default_value = rpar;
      while (default_separator != rpar) {
        if ((*default_separator == ':') && (*(default_separator + 1) == '-')) {
          default_value = default_separator+2;
          break;
        }
        default_separator++;
      }

      const unsigned environ_var_length = default_separator-c;
      char environ_var[environ_var_length+1];
      environ_var[environ_var_length] = '\0';
      memcpy(environ_var, c, environ_var_length);
      const char *environ_value = getenv(environ_var);  // Don't free!
      if (environ_value) {
        result.Append(environ_value, strlen(environ_value));
      } else {
        const unsigned default_length = rpar-default_value;
        result.Append(default_value, default_length);
      }
      c = rpar;
      continue;
    }
    result.Append(c, 1);
  }
  raw_symlink->Assign(result);
  return;
}


//------------------------------------------------------------------------------


bool SqlDirentWrite::BindDirentFields(const int hash_idx,
                                      const int hardlinks_idx,
                                      const int size_idx,
                                      const int mode_idx,
                                      const int mtime_idx,
                                      const int flags_idx,
                                      const int name_idx,
                                      const int symlink_idx,
                                      const int uid_idx,
                                      const int gid_idx,
                                      const DirectoryEntry &entry)
{
  const uint64_t hardlinks =
    MakeHardlinks(entry.hardlink_group_,
                  entry.linkcount_);

  return (
    BindHashBlob(hash_idx, entry.checksum_) &&
    BindInt64(hardlinks_idx, hardlinks) &&
    BindInt64(size_idx, entry.size_) &&
    BindInt(mode_idx, entry.mode_) &&
    BindInt64(uid_idx, entry.uid_) &&
    BindInt64(gid_idx, entry.gid_) &&
    BindInt64(mtime_idx, entry.mtime_) &&
    BindInt(flags_idx, CreateDatabaseFlags(entry)) &&
    BindText(name_idx, entry.name_.GetChars(), entry.name_.GetLength()) &&
    BindText(symlink_idx, entry.symlink_.GetChars(), entry.symlink_.GetLength())
  );
}


//------------------------------------------------------------------------------


string SqlLookup::GetFieldsToSelect(const float schema_version) const {
  if (schema_version < 2.1 - CatalogDatabase::kSchemaEpsilon) {
    return "catalog.hash, catalog.inode, catalog.size, catalog.mode, "
        //           0              1             2             3
           "catalog.mtime, catalog.flags, catalog.name, catalog.symlink, "
        //            4              5             6               7
           "catalog.md5path_1, catalog.md5path_2, catalog.parent_1, "
        //              8                  9                 10
           "catalog.parent_2, catalog.rowid";
        //             11              12
  } else {
    return "catalog.hash, catalog.hardlinks, catalog.size, catalog.mode, "
        //           0                1               2             3
           "catalog.mtime, catalog.flags, catalog.name, catalog.symlink, "
        //            4              5             6               7
           "catalog.md5path_1, catalog.md5path_2, catalog.parent_1, "
        //              8                  9                 10
           "catalog.parent_2, catalog.rowid, catalog.uid, catalog.gid";
        //             11               12            13           14
  }
}


shash::Md5 SqlLookup::GetPathHash() const {
  return RetrieveMd5(8, 9);
}


shash::Md5 SqlLookup::GetParentPathHash() const {
  return RetrieveMd5(10, 11);
}


/**
 * This method is a friend of DirectoryEntry.
 */
DirectoryEntry SqlLookup::GetDirent(const Catalog *catalog,
                                    const bool expand_symlink) const
{
  DirectoryEntry result;

  const unsigned database_flags = RetrieveInt(5);
  //result.generation_ = catalog->GetGeneration();
  result.is_nested_catalog_root_ = (database_flags & kFlagDirNestedRoot);
  result.is_nested_catalog_mountpoint_ =
    (database_flags & kFlagDirNestedMountpoint);
  const char *name = reinterpret_cast<const char *>(RetrieveText(6));
  const char *symlink = reinterpret_cast<const char *>(RetrieveText(7));

  // must be set later by a second catalog lookup
  result.parent_inode_ = DirectoryEntry::kInvalidInode;

  // retrieve the hardlink information from the hardlinks database field
  if (catalog->schema() < 2.1 - CatalogDatabase::kSchemaEpsilon) {
    result.linkcount_       = 1;
    result.hardlink_group_  = 0;
    result.inode_           = catalog->GetMangledInode(RetrieveInt64(12), 0);
    result.uid_             = g_uid;
    result.gid_             = g_gid;
    result.is_chunked_file_ = false;
    result.checksum_        = RetrieveHashBlob(0, shash::kSha1);
  } else {
    const uint64_t hardlinks = RetrieveInt64(1);
    result.linkcount_        = Hardlinks2Linkcount(hardlinks);
    result.hardlink_group_   = Hardlinks2HardlinkGroup(hardlinks);
    result.inode_            = catalog->GetMangledInode(RetrieveInt64(12),
                                                        result.hardlink_group_);
    result.uid_              = RetrieveInt64(13);
    result.gid_              = RetrieveInt64(14);
    result.is_chunked_file_  = (database_flags & kFlagFileChunk);
    result.checksum_         =
      RetrieveHashBlob(0, RetrieveHashAlgorithm(database_flags));
    if (catalog->uid_map_) {
      OwnerMap::const_iterator i = catalog->uid_map_->find(result.uid_);
      if (i != catalog->uid_map_->end())
        result.uid_ = i->second;
    }
    if (catalog->gid_map_) {
      OwnerMap::const_iterator i = catalog->gid_map_->find(result.gid_);
      if (i != catalog->gid_map_->end())
        result.gid_ = i->second;
    }
  }

  result.mode_     = RetrieveInt(3);
  result.size_     = RetrieveInt64(2);
  result.mtime_    = RetrieveInt64(4);
  result.name_.Assign(name, strlen(name));
  result.symlink_.Assign(symlink, strlen(symlink));
  if (expand_symlink)
    ExpandSymlink(&result.symlink_);

  return result;
}


//------------------------------------------------------------------------------


SqlListing::SqlListing(const CatalogDatabase &database) {
  const string statement =
    "SELECT " + GetFieldsToSelect(database.schema_version()) + " FROM catalog "
    "WHERE (parent_1 = :p_1) AND (parent_2 = :p_2);";
  Init(database.sqlite_db(), statement);
}


bool SqlListing::BindPathHash(const struct shash::Md5 &hash) {
  return BindMd5(1, 2, hash);
}


//------------------------------------------------------------------------------


SqlLookupPathHash::SqlLookupPathHash(const CatalogDatabase &database) {
  const string statement =
    "SELECT " + GetFieldsToSelect(database.schema_version()) + " FROM catalog "
    "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);";
  Init(database.sqlite_db(), statement);
}

bool SqlLookupPathHash::BindPathHash(const struct shash::Md5 &hash) {
  return BindMd5(1, 2, hash);
}


//------------------------------------------------------------------------------


SqlLookupInode::SqlLookupInode(const CatalogDatabase &database) {
  const string statement =
    "SELECT " + GetFieldsToSelect(database.schema_version()) + " FROM catalog "
    "WHERE rowid = :rowid;";
  Init(database.sqlite_db(), statement);
}


bool SqlLookupInode::BindRowId(const uint64_t inode) {
  return BindInt64(1, inode);
}


//------------------------------------------------------------------------------


SqlDirentTouch::SqlDirentTouch(const CatalogDatabase &database) {
  const string statement =
    "UPDATE catalog "
    "SET hash = :hash, size = :size, mode = :mode, mtime = :mtime, "
//            1             2             3               4
    "name = :name, symlink = :symlink, uid = :uid, gid = :gid "
//        5                6               7           8
    "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);";
//                    9                       10
  Init(database.sqlite_db(), statement);
}


bool SqlDirentTouch::BindDirentBase(const DirectoryEntryBase &entry) {
  return (
    BindHashBlob(1, entry.checksum_)                                       &&
    BindInt64   (2, entry.size_)                                           &&
    BindInt     (3, entry.mode_)                                           &&
    BindInt64   (4, entry.mtime_)                                          &&
    BindText    (5, entry.name_.GetChars(),    entry.name_.GetLength())    &&
    BindText    (6, entry.symlink_.GetChars(), entry.symlink_.GetLength()) &&
    BindInt64   (7, entry.uid_)                                            &&
    BindInt64   (8, entry.gid_)
  );
}


bool SqlDirentTouch::BindPathHash(const shash::Md5 &hash) {
  return BindMd5(9, 10, hash);
}


//------------------------------------------------------------------------------


SqlNestedCatalogLookup::SqlNestedCatalogLookup(const CatalogDatabase &database)
{
  if (database.IsEqualSchema(database.schema_version(), 2.5) &&
      (database.schema_revision() >= 1))
  {
    // Internally converts NULL to 0 for size
    Init(database.sqlite_db(),
         "SELECT sha1, size FROM nested_catalogs WHERE path=:path;");
  } else {
    Init(database.sqlite_db(),
         "SELECT sha1, 0 FROM nested_catalogs WHERE path=:path;");
  }
}


bool SqlNestedCatalogLookup::BindSearchPath(const PathString &path) {
  return BindText(1, path.GetChars(), path.GetLength());
}


shash::Any SqlNestedCatalogLookup::GetContentHash() const {
  const string hash = string(reinterpret_cast<const char *>(RetrieveText(0)));
  return (hash.empty()) ? shash::Any(shash::kAny) :
                          shash::MkFromHexPtr(shash::HexPtr(hash),
                                              shash::kSuffixCatalog);
}


uint64_t SqlNestedCatalogLookup::GetSize() const {
  return RetrieveInt64(1);
}


//------------------------------------------------------------------------------


SqlNestedCatalogListing::SqlNestedCatalogListing(
                                              const CatalogDatabase &database) {
  if (database.IsEqualSchema(database.schema_version(), 2.5) &&
      (database.schema_revision() >= 1))
  {
    // Internally converts NULL to 0 for size
    Init(database.sqlite_db(), "SELECT path, sha1, size FROM nested_catalogs;");
  } else {
    Init(database.sqlite_db(), "SELECT path, sha1, 0 FROM nested_catalogs;");
  }
}


PathString SqlNestedCatalogListing::GetMountpoint() const {
  const char *mountpoint = reinterpret_cast<const char *>(RetrieveText(0));
  return PathString(mountpoint, strlen(mountpoint));
}


shash::Any SqlNestedCatalogListing::GetContentHash() const {
  const string hash = string(reinterpret_cast<const char *>(RetrieveText(1)));
  return (hash.empty()) ? shash::Any(shash::kAny) :
                          shash::MkFromHexPtr(shash::HexPtr(hash),
                                              shash::kSuffixCatalog);
}


uint64_t SqlNestedCatalogListing::GetSize() const {
  return RetrieveInt64(2);
}


//------------------------------------------------------------------------------


SqlDirentInsert::SqlDirentInsert(const CatalogDatabase &database) {
  const string statement = "INSERT INTO catalog "
    "(md5path_1, md5path_2, parent_1, parent_2, hash, hardlinks, size, mode,"
    //    1           2         3         4       5       6        7     8
    "mtime, flags, name, symlink, uid, gid) "
    // 9,     10    11     12     13   14
    "VALUES (:md5_1, :md5_2, :p_1, :p_2, :hash, :links, :size, :mode, :mtime,"
    " :flags, :name, :symlink, :uid, :gid);";
  Init(database.sqlite_db(), statement);
}


bool SqlDirentInsert::BindPathHash(const shash::Md5 &hash) {
  return BindMd5(1, 2, hash);
}


bool SqlDirentInsert::BindParentPathHash(const shash::Md5 &hash) {
  return BindMd5(3, 4, hash);
}


bool SqlDirentInsert::BindDirent(const DirectoryEntry &entry) {
  return BindDirentFields(5, 6, 7, 8, 9, 10, 11, 12, 13, 14, entry);
}


//------------------------------------------------------------------------------


SqlDirentUpdate::SqlDirentUpdate(const CatalogDatabase &database) {
  const string statement =
    "UPDATE catalog "
    "SET hash = :hash, size = :size, mode = :mode, mtime = :mtime, "
//            1             2             3               4
    "flags = :flags, name = :name, symlink = :symlink, hardlinks = :hardlinks, "
//          5             6                  7                8
    "uid = :uid, gid = :gid "
//          9           10
    "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);";
//                     11                       12
  Init(database.sqlite_db(), statement);
}


bool SqlDirentUpdate::BindPathHash(const shash::Md5 &hash) {
  return BindMd5(11, 12, hash);
}


bool SqlDirentUpdate::BindDirent(const DirectoryEntry &entry) {
  return BindDirentFields(1, 8, 2, 3, 4, 5, 6, 7, 9, 10, entry);
}


//------------------------------------------------------------------------------


SqlDirentUnlink::SqlDirentUnlink(const CatalogDatabase &database) {
  Init(database.sqlite_db(),
       "DELETE FROM catalog "
       "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);");
}

bool SqlDirentUnlink::BindPathHash(const shash::Md5 &hash) {
  return BindMd5(1, 2, hash);
}


//------------------------------------------------------------------------------


SqlIncLinkcount::SqlIncLinkcount(const CatalogDatabase &database) {
  // This command changes the linkcount of a whole hardlink group at once!
  // We can do this, since the 'hardlinks'-field contains the hardlink group ID
  // in the higher 32bit as well as the 'linkcount' in the lower 32bit.
  // This field will be equal for all entries belonging to the same hardlink
  // group while adding/subtracting small values from it will only effect the
  // linkcount in the lower 32bit.
  // Take a deep breath!
  const string statememt =
    "UPDATE catalog SET hardlinks = hardlinks + :delta "
    "WHERE hardlinks = (SELECT hardlinks from catalog "
    "WHERE md5path_1 = :md5_1 AND md5path_2 = :md5_2);";
  Init(database.sqlite_db(), statememt);
}


bool SqlIncLinkcount::BindPathHash(const shash::Md5 &hash) {
  return BindMd5(2, 3, hash);
}


bool SqlIncLinkcount::BindDelta(const int delta) {
  return BindInt(1, delta);
}


//------------------------------------------------------------------------------


SqlChunkInsert::SqlChunkInsert(const CatalogDatabase &database) {
  const string statememt =
    "INSERT INTO chunks (md5path_1, md5path_2, offset, size, hash) "
    //                       1          2        3      4     5
    "VALUES (:md5_1, :md5_2, :offset, :size, :hash);";
  Init(database.sqlite_db(), statememt);
}


bool SqlChunkInsert::BindPathHash(const shash::Md5 &hash) {
  return BindMd5(1, 2, hash);
}


bool SqlChunkInsert::BindFileChunk(const FileChunk &chunk) {
  return
    BindInt64(3,    chunk.offset())       &&
    BindInt64(4,    chunk.size())         &&
    BindHashBlob(5, chunk.content_hash());
}


//------------------------------------------------------------------------------


SqlChunksRemove::SqlChunksRemove(const CatalogDatabase &database) {
  const string statement =
    "DELETE FROM chunks "
    "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);";
  Init(database.sqlite_db(), statement);
}


bool SqlChunksRemove::BindPathHash(const shash::Md5 &hash) {
  return BindMd5(1, 2, hash);
}


//------------------------------------------------------------------------------


SqlChunksListing::SqlChunksListing(const CatalogDatabase &database) {
  const string statement =
    "SELECT offset, size, hash FROM chunks "
    //         0      1     2
    "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2) "
    //                    1                          2
    "ORDER BY offset ASC;";
  Init(database.sqlite_db(), statement);
}


bool SqlChunksListing::BindPathHash(const shash::Md5 &hash) {
  return BindMd5(1, 2, hash);
}


FileChunk SqlChunksListing::GetFileChunk(
  const shash::Algorithms interpret_hash_as) const
{
  return FileChunk(RetrieveHashBlob(2, interpret_hash_as, shash::kSuffixPartial),
                   RetrieveInt64(0),
                   RetrieveInt64(1));
}


//------------------------------------------------------------------------------


SqlChunksCount::SqlChunksCount(const CatalogDatabase &database) {
  const string statement =
    "SELECT count(*) FROM chunks "
    //         0
    "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2)";
    //                    1                          2
  Init(database.sqlite_db(), statement);
}


bool SqlChunksCount::BindPathHash(const shash::Md5 &hash) {
  return BindMd5(1, 2, hash);
}


int SqlChunksCount::GetChunkCount() const {
  return RetrieveInt64(0);
}


//------------------------------------------------------------------------------


SqlMaxHardlinkGroup::SqlMaxHardlinkGroup(const CatalogDatabase &database) {
  Init(database.sqlite_db(), "SELECT max(hardlinks) FROM catalog;");
}


uint32_t SqlMaxHardlinkGroup::GetMaxGroupId() const {
  return RetrieveInt64(0) >> 32;
}


//------------------------------------------------------------------------------


SqlGetCounter::SqlGetCounter(const CatalogDatabase &database) {
  if (database.schema_version() >= 2.4 - CatalogDatabase::kSchemaEpsilon) {
    compat_ = false;
    Init(database.sqlite_db(),
         "SELECT value from statistics WHERE counter = :counter;");
  } else {
    Init(database.sqlite_db(), "SELECT 0;");
    compat_ = true;
  }
}


bool SqlGetCounter::BindCounter(const std::string &counter) {
  if (compat_) return true;
  return BindText(1, counter);
}


uint64_t SqlGetCounter::GetCounter() const {
  if (compat_) return 0;
  return RetrieveInt64(0);
}


//------------------------------------------------------------------------------


SqlUpdateCounter::SqlUpdateCounter(const CatalogDatabase &database) {
  Init(database.sqlite_db(),
       "UPDATE statistics SET value=value+:val WHERE counter=:counter;");
}


bool SqlUpdateCounter::BindCounter(const std::string &counter) {
  return BindText(2, counter);
}


bool SqlUpdateCounter::BindDelta(const int64_t delta) {
  return BindInt64(1, delta);
}


//------------------------------------------------------------------------------


SqlCreateCounter::SqlCreateCounter(const CatalogDatabase &database) {
  Init(database.sqlite_db(),
       "INSERT OR REPLACE INTO statistics (counter, value) "
       "VALUES (:counter, :value);");
}


bool SqlCreateCounter::BindCounter(const std::string &counter) {
  return BindText(1, counter);
}


bool SqlCreateCounter::BindInitialValue(const int64_t value) {
  return BindInt64(2, value);
}


//------------------------------------------------------------------------------


SqlAllChunks::SqlAllChunks(const CatalogDatabase &database) {
  int hash_mask = 7 << SqlDirent::kFlagPosHash;
  string flags2hash =
    " ((flags&" + StringifyInt(hash_mask) + ") >> " +
    StringifyInt(SqlDirent::kFlagPosHash) + ")+1 AS hash_algorithm ";

  string sql = "SELECT DISTINCT hash, "
  "CASE WHEN flags & " + StringifyInt(SqlDirent::kFlagFile) + " THEN " +
    StringifyInt(kChunkFile) + " " +
  "WHEN flags & " + StringifyInt(SqlDirent::kFlagDir) + " THEN " +
    StringifyInt(kChunkMicroCatalog) + " END " +
  "AS chunk_type, " + flags2hash +
  "FROM catalog WHERE hash IS NOT NULL";
  if (database.schema_version() >= 2.4 - CatalogDatabase::kSchemaEpsilon) {
    sql += " UNION SELECT DISTINCT chunks.hash, " + StringifyInt(kChunkPiece) +
      ", " + flags2hash + "FROM chunks, catalog WHERE "
      "chunks.md5path_1=catalog.md5path_1 AND "
      "chunks.md5path_2=catalog.md5path_2";
  }
  sql += ";";
  Init(database.sqlite_db(), sql);
}


bool SqlAllChunks::Open() {
  return true;
}


bool SqlAllChunks::Next(shash::Any *hash, ChunkTypes *type) {
  if (FetchRow()) {
    *hash = RetrieveHashBlob(0, static_cast<shash::Algorithms>(RetrieveInt(2)),
                             shash::kSuffixPartial);
    *type = static_cast<ChunkTypes>(RetrieveInt(1));
    return true;
  }
  return false;
}


bool SqlAllChunks::Close() {
  return Reset();
}

}  // namespace catalog
