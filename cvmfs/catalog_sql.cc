/**
 * This file is part of the CernVM file system.
 */

#include "cvmfs_config.h"
#include "catalog_sql.h"

#include <cstdlib>
#include <cstring>

#include "catalog.h"
#include "globals.h"
#include "logging.h"
#include "util.h"
#include "xattr.h"

using namespace std;  // NOLINT

namespace catalog {

/**
 * NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE
 * Always remember to update the legacy catalog migration classes to produce a
 * compatible catalog structure when updating the schema revisions here!
 *
 * Repository rollbacks to an outdated catalog schema is not supported. Have a
 * look into CVM-252 if that becomes necessary at some point.
 * NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE
 */

// ChangeLog
// 2.5 (Jun 26 2013 - Git: e79baec22c6abd6ddcdf8f8d7d33921027a052ab)
//     * add (backward compatible) schema revision - see below
//     * add statistics counters for chunked files
//       Note: this was retrofitted and needed a catalog migration step
//
// 2.4 (Aug 15 2012 - Git: 17de8fc782b5b8dc4404dda925627b5ec2b552e1)
// 2.3 (Aug 15 2012 - Git: ab77688cdb2f851af3fe983bf3694dc2465e65be)
// 2.2 (never existed)
// 2.1 (Aug  7 2012 - Git: beba36c12d2b1123ffbb169f865a861e570adc68)
//     * add 'chunks' table for file chunks
//     * add 'statistics' table for accumulative counters
//     * rename 'inode' field to 'hardlinks'
//       * containing both hardlink group ID and linkcount
//     * .cvmfscatalog files become first-class entries in the catalogs
//
// 2.0 (Aug  6 2012 - Git: c8a81ede603e57fbe4324b6ab6bc8c41e3a2fa5f)
//     * beginning of CernVM-FS 2.1.x branch ('modern' era)
//
// 1.x (earlier - based on SVN :-) )
//     * pre-historic times
const float CatalogDatabase::kLatestSchema = 2.5;
const float CatalogDatabase::kLatestSupportedSchema = 2.5;  // + 1.X (r/o)

// ChangeLog
//   0 --> 1: (Jan  6 2014 - Git: 3667fe7a669d0d65e07275b753a7c6f23fc267df)
//            * add size column to nested catalog table,
//            * add schema_revision property
//   1 --> 2: (Jan 22 2014 - Git: 85e6680e52cfe56dc1213a5ad74a5cc62fd50ead):
//            * add xattr column to catalog table
//            * add self_xattr and subtree_xattr statistics counters
//   2 --> 3: (Sep 28 2015 - Git: f4171234b13ea448589820c1524ee52eae141bb4):
//            * add kFlagFileExternal to entries in catalog table
//            * add self_external and subtree_external statistics counters
//            * store compression algorithm in flags
const unsigned CatalogDatabase::kLatestSchemaRevision = 3;

bool CatalogDatabase::CheckSchemaCompatibility() {
  return !( (schema_version() >= 2.0-kSchemaEpsilon)                   &&
            (!IsEqualSchema(schema_version(), kLatestSupportedSchema)) &&
            (!IsEqualSchema(schema_version(), 2.4)           ||
             !IsEqualSchema(kLatestSupportedSchema, 2.5)) );
}


bool CatalogDatabase::LiveSchemaUpgradeIfNecessary() {
  assert(read_write());

  if (IsEqualSchema(schema_version(), 2.5) && (schema_revision() == 0)) {
    LogCvmfs(kLogCatalog, kLogDebug, "upgrading schema revision (0 --> 1)");

    Sql sql_upgrade(*this, "ALTER TABLE nested_catalogs ADD size INTEGER;");
    if (!sql_upgrade.Execute()) {
      LogCvmfs(kLogCatalog, kLogDebug, "failed to upgrade nested_catalogs");
      return false;
    }

    set_schema_revision(1);
    if (!StoreSchemaRevision()) {
      LogCvmfs(kLogCatalog, kLogDebug, "failed to upgrade schema revision");
      return false;
    }
  }

  if (IsEqualSchema(schema_version(), 2.5) && (schema_revision() == 1)) {
    LogCvmfs(kLogCatalog, kLogDebug, "upgrading schema revision (1 --> 2)");

    Sql sql_upgrade1(*this, "ALTER TABLE catalog ADD xattr BLOB;");
    Sql sql_upgrade2(*this,
      "INSERT INTO statistics (counter, value) VALUES ('self_xattr', 0);");
    Sql sql_upgrade3(*this,
      "INSERT INTO statistics (counter, value) VALUES ('subtree_xattr', 0);");
    if (!sql_upgrade1.Execute() || !sql_upgrade2.Execute() ||
        !sql_upgrade3.Execute())
    {
      LogCvmfs(kLogCatalog, kLogDebug, "failed to upgrade catalogs (1 --> 2)");
      return false;
    }

    set_schema_revision(2);
    if (!StoreSchemaRevision()) {
      LogCvmfs(kLogCatalog, kLogDebug, "failed to upgrade schema revision");
      return false;
    }
  }

  if (IsEqualSchema(schema_version(), 2.5) && (schema_revision() == 2)) {
    LogCvmfs(kLogCatalog, kLogDebug, "upgrading schema revision (2 --> 3)");

    Sql sql_upgrade4(*this,
      "INSERT INTO statistics (counter, value) VALUES ('self_external', 0);");
    Sql sql_upgrade5(*this, "INSERT INTO statistics (counter, value) VALUES "
                            "('self_external_file_size', 0);");
    Sql sql_upgrade6(*this, "INSERT INTO statistics (counter, value) VALUES "
                            "('subtree_external', 0);");
    Sql sql_upgrade7(*this, "INSERT INTO statistics (counter, value) VALUES "
                            "('subtree_external_file_size', 0);");
    if (!sql_upgrade4.Execute() || !sql_upgrade5.Execute() ||
        !sql_upgrade6.Execute() || !sql_upgrade7.Execute())
    {
      LogCvmfs(kLogCatalog, kLogDebug, "failed to upgrade catalogs (2 --> 3)");
      return false;
    }

    set_schema_revision(3);
    if (!StoreSchemaRevision()) {
      LogCvmfs(kLogCatalog, kLogDebug, "failed to upgrade schema revision");
      return false;
    }
  }

  return true;
}


bool CatalogDatabase::CreateEmptyDatabase() {
  assert(read_write());

  // generate the catalog table and index structure
  const bool retval =
  Sql(*this,
    "CREATE TABLE catalog "
    "(md5path_1 INTEGER, md5path_2 INTEGER, parent_1 INTEGER, parent_2 INTEGER,"
    " hardlinks INTEGER, hash BLOB, size INTEGER, mode INTEGER, mtime INTEGER,"
    " flags INTEGER, name TEXT, symlink TEXT, uid INTEGER, gid INTEGER, "
    " xattr BLOB, "
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

  if (!retval) {
    PrintSqlError("failed to create catalog database tables.");
  }

  return retval;
}


bool CatalogDatabase::InsertInitialValues(
  const std::string    &root_path,
  const bool            volatile_content,
  const std::string    &voms_authz,
  const DirectoryEntry &root_entry)
{
  assert(read_write());
  bool retval = false;

  // Path hashes
  shash::Md5 root_path_hash = shash::Md5(shash::AsciiPtr(root_path));
  shash::Md5 root_parent_hash = (root_path == "")
    ? shash::Md5()
    : shash::Md5(shash::AsciiPtr(GetParentPath(root_path)));

  // Start initial filling transaction
  retval = Sql(*this, "BEGIN;").Execute();
  if (!retval) {
    PrintSqlError("failed to enter initial filling transaction");
    return false;
  }

  // Insert initial values to properties
  if (!this->SetProperty("revision", 0)) {
    PrintSqlError(
      "failed to insert default initial values into the newly created "
      "catalog tables.");
    return false;
  }

  if (volatile_content) {
    if (!this->SetProperty("volatile", 1)) {
      PrintSqlError("failed to insert volatile flag into the newly created "
                    "catalog tables.");
      return false;
    }
  }

  if (!voms_authz.empty()) {
    if (!SetVOMSAuthz(voms_authz)) {
      PrintSqlError("failed to insert VOMS authz flag into the newly created "
                    "catalog tables.");
      return false;
    }
  }

  // Create initial statistics counters
  catalog::Counters counters;

  // Insert root entry (when given)
  if (!root_entry.IsNegative()) {
    SqlDirentInsert sql_insert(*this);
    retval = sql_insert.BindPathHash(root_path_hash)         &&
             sql_insert.BindParentPathHash(root_parent_hash) &&
             sql_insert.BindDirent(root_entry)               &&
             sql_insert.Execute();
    if (!retval) {
      PrintSqlError("failed to insert root entry into newly created catalog.");
      return false;
    }

    // account for the created root entry
    counters.self.directories = 1;
  }

  // Save initial statistics counters
  if (!counters.InsertIntoDatabase(*this)) {
    PrintSqlError("failed to insert initial catalog statistics counters.");
    return false;
  }

  // Insert root path (when given)
  if (!root_path.empty()) {
    if (!this->SetProperty("root_prefix", root_path)) {
      PrintSqlError(
        "failed to store root prefix in the newly created catalog.");
      return false;
    }
  }

  // Set creation timestamp
  if (!this->SetProperty("last_modified", static_cast<uint64_t>(time(NULL)))) {
    PrintSqlError("failed to store creation timestamp in the new catalog.");
    return false;
  }

  // Commit initial filling transaction
  retval = Sql(*this, "COMMIT;").Execute();
  if (!retval) {
    PrintSqlError("failed to commit initial filling transaction");
    return false;
  }

  return true;
}


bool
CatalogDatabase::SetVOMSAuthz(const std::string &voms_authz) {
  return this->SetProperty("voms_authz", voms_authz);
}


double CatalogDatabase::GetRowIdWasteRatio() const {
  Sql rowid_waste_ratio_query(*this,
    "SELECT 1.0 - CAST(COUNT(*) AS DOUBLE) / MAX(rowid) "
    "AS ratio FROM catalog;");
  const bool retval = rowid_waste_ratio_query.FetchRow();
  assert(retval);

  return rowid_waste_ratio_query.RetrieveDouble(0);
}

/**
 * Cleanup unused database space
 *
 * This copies the entire catalog content into a temporary SQLite table, sweeps
 * the original data from the 'catalog' table and reinserts everything from the
 * temporary table afterwards. That way the implicit rowid field of 'catalog' is
 * defragmented.
 *
 * Since the 'chunks' table has a foreign key relationship to the 'catalog' we
 * need to temporarily switch off the foreign key checks. Otherwise the clearing
 * of the 'catalog' table would fail due to foreign key violations. Note that it
 * is a NOOP to change the foreign key setting during a transaction!
 *
 * Note: VACUUM used to have a similar behaviour but it was dropped from SQLite
 *       at some point. Since we compute client-inodes from the rowIDs, we are
 *       probably one of the few use cases where a defragmented rowID is indeed
 *       beneficial.
 *
 * See: http://www.sqlite.org/lang_vacuum.html
 */
bool CatalogDatabase::CompactDatabase() const {
  assert(read_write());

  return Sql(*this, "PRAGMA foreign_keys = OFF;").Execute() &&
         BeginTransaction()                                 &&
         Sql(*this, "CREATE TEMPORARY TABLE duplicate AS "
                    "  SELECT * FROM catalog "
                    "  ORDER BY rowid ASC;").Execute()      &&
         Sql(*this, "DELETE FROM catalog;").Execute()       &&
         Sql(*this, "INSERT INTO catalog "
                    "  SELECT * FROM duplicate "
                    "  ORDER BY rowid").Execute()           &&
         Sql(*this, "DROP TABLE duplicate;").Execute()      &&
         CommitTransaction()                                &&
         Sql(*this, "PRAGMA foreign_keys = ON;").Execute();
}


//------------------------------------------------------------------------------


unsigned SqlDirent::CreateDatabaseFlags(const DirectoryEntry &entry) const {
  unsigned int database_flags = 0;

  if (entry.IsNestedCatalogRoot())
    database_flags |= kFlagDirNestedRoot;
  else if (entry.IsNestedCatalogMountpoint())
    database_flags |= kFlagDirNestedMountpoint;

  if (entry.IsDirectory()) {
    database_flags |= kFlagDir;
  } else if (entry.IsLink()) {
    database_flags |= kFlagFile | kFlagLink;
  } else {
    database_flags |= kFlagFile;
    database_flags |= entry.compression_algorithm() << kFlagPosCompression;
    if (entry.IsChunkedFile())
      database_flags |= kFlagFileChunk;
    if (entry.IsExternalFile())
      database_flags |= kFlagFileExternal;
  }

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


zlib::Algorithms SqlDirent::RetrieveCompressionAlgorithm(const unsigned flags)
  const
{
  // 3 bits, so use 7 (111) to only pull out the flags we want
  unsigned in_flags =
    ((7 << kFlagPosCompression) & flags) >> kFlagPosCompression;
  return static_cast<zlib::Algorithms>(in_flags);
}


uint32_t SqlDirent::Hardlinks2Linkcount(const uint64_t hardlinks) const {
  return (hardlinks << 32) >> 32;
}


uint32_t SqlDirent::Hardlinks2HardlinkGroup(const uint64_t hardlinks) const {
  return hardlinks >> 32;
}


uint64_t SqlDirent::MakeHardlinks(const uint32_t hardlink_group,
                                  const uint32_t linkcount) const
{
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
  );  // NOLINT
}


//------------------------------------------------------------------------------


SqlListContentHashes::SqlListContentHashes(const CatalogDatabase &database) {
  const string statement =
    (database.schema_version() < 2.4-CatalogDatabase::kSchemaEpsilon)
    ? "SELECT hash, flags, 0 "
      "  FROM catalog "
      "  WHERE length(hash) > 0;"

    : "SELECT hash, flags, 0 "
      "  FROM catalog "
      "  WHERE length(catalog.hash) > 0 "
      "UNION "
      "SELECT chunks.hash, catalog.flags, 1 "
      "  FROM catalog "
      "  LEFT JOIN chunks "
      "  ON catalog.md5path_1 = chunks.md5path_1 AND "
      "     catalog.md5path_2 = chunks.md5path_2 "
      "  WHERE length(catalog.hash) > 0;";

  const bool successful_init = Init(database.sqlite_db(), statement);
  assert(successful_init);
}


shash::Any SqlListContentHashes::GetHash() const {
  const unsigned int      db_flags       = RetrieveInt(1);
  const shash::Algorithms hash_algorithm = RetrieveHashAlgorithm(db_flags);
  shash::Any              hash           = RetrieveHashBlob(0, hash_algorithm);
  if (RetrieveInt(2) == 1) {
    hash.suffix = shash::kSuffixPartial;
  }

  return hash;
}


//------------------------------------------------------------------------------


string SqlLookup::GetFieldsToSelect(
  const float schema_version,
  const unsigned schema_revision) const
{
  if (schema_version < 2.1 - CatalogDatabase::kSchemaEpsilon) {
    return "catalog.hash, catalog.inode, catalog.size, catalog.mode, "
        //           0              1             2             3
           "catalog.mtime, catalog.flags, catalog.name, catalog.symlink, "
        //            4              5             6               7
           "catalog.md5path_1, catalog.md5path_2, catalog.parent_1, "
        //              8                  9                 10
           "catalog.parent_2, catalog.rowid";
        //             11              12
  }

  string fields =
    "catalog.hash, catalog.hardlinks, catalog.size, catalog.mode, "
    //          0                1               2             3
    "catalog.mtime, catalog.flags, catalog.name, catalog.symlink, "
    //           4              5             6               7
    "catalog.md5path_1, catalog.md5path_2, catalog.parent_1, "
    //              8                  9                 10
    "catalog.parent_2, catalog.rowid, catalog.uid, catalog.gid, ";
    //            11              12           13           14

  // Field 15: has extended attributes?
  if (schema_revision < 2) {
    fields += "0";  // Generally no xattrs
  } else {
    fields += "catalog.xattr IS NOT NULL";
  }
  return fields;
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
  result.is_nested_catalog_root_ = (database_flags & kFlagDirNestedRoot);
  result.is_nested_catalog_mountpoint_ =
    (database_flags & kFlagDirNestedMountpoint);
  const char *name = reinterpret_cast<const char *>(RetrieveText(6));
  const char *symlink = reinterpret_cast<const char *>(RetrieveText(7));

  // Must be set later by a second catalog lookup
  result.parent_inode_ = DirectoryEntry::kInvalidInode;

  // Retrieve the hardlink information from the hardlinks database field
  if (catalog->schema() < 2.1 - CatalogDatabase::kSchemaEpsilon) {
    result.linkcount_       = 1;
    result.hardlink_group_  = 0;
    result.inode_           = catalog->GetMangledInode(RetrieveInt64(12), 0);
    result.is_chunked_file_ = false;
    result.has_xattrs_      = false;
    result.checksum_        = RetrieveHashBlob(0, shash::kSha1);
    result.uid_             = g_uid;
    result.gid_             = g_gid;
  } else {
    const uint64_t hardlinks = RetrieveInt64(1);
    result.linkcount_        = Hardlinks2Linkcount(hardlinks);
    result.hardlink_group_   = Hardlinks2HardlinkGroup(hardlinks);
    result.inode_            = catalog->GetMangledInode(RetrieveInt64(12),
                                                        result.hardlink_group_);
    result.is_chunked_file_  = (database_flags & kFlagFileChunk);
    result.is_external_file_ = (database_flags & kFlagFileExternal);
    result.has_xattrs_       = RetrieveInt(15) != 0;
    result.checksum_         =
      RetrieveHashBlob(0, RetrieveHashAlgorithm(database_flags));
    result.compression_algorithm_ =
      RetrieveCompressionAlgorithm(database_flags);

    if (g_claim_ownership) {
      result.uid_             = g_uid;
      result.gid_             = g_gid;
    } else {
      result.uid_              = RetrieveInt64(13);
      result.gid_              = RetrieveInt64(14);
      if (catalog->uid_map_)
        result.uid_ = catalog->uid_map_->Map(result.uid_);
      if (catalog->gid_map_)
        result.gid_ = catalog->gid_map_->Map(result.gid_);
    }
  }

  result.mode_     = RetrieveInt(3);
  result.size_     = RetrieveInt64(2);
  result.mtime_    = RetrieveInt64(4);
  result.name_.Assign(name, strlen(name));
  result.symlink_.Assign(symlink, strlen(symlink));
  if (expand_symlink && !g_raw_symlinks)
    ExpandSymlink(&result.symlink_);

  return result;
}


//------------------------------------------------------------------------------


SqlListing::SqlListing(const CatalogDatabase &database) {
  const string statement =
    "SELECT " +
    GetFieldsToSelect(database.schema_version(), database.schema_revision()) +
    " FROM catalog WHERE (parent_1 = :p_1) AND (parent_2 = :p_2);";
  Init(database.sqlite_db(), statement);
}


bool SqlListing::BindPathHash(const struct shash::Md5 &hash) {
  return BindMd5(1, 2, hash);
}


//------------------------------------------------------------------------------


SqlLookupPathHash::SqlLookupPathHash(const CatalogDatabase &database) {
  const string statement =
    "SELECT " +
    GetFieldsToSelect(database.schema_version(), database.schema_revision()) +
    " FROM catalog WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);";
  Init(database.sqlite_db(), statement);
}

bool SqlLookupPathHash::BindPathHash(const struct shash::Md5 &hash) {
  return BindMd5(1, 2, hash);
}


//------------------------------------------------------------------------------


SqlLookupInode::SqlLookupInode(const CatalogDatabase &database) {
  const string statement =
    "SELECT " +
    GetFieldsToSelect(database.schema_version(), database.schema_revision()) +
    " FROM catalog WHERE rowid = :rowid;";
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
    BindHashBlob(1, entry.checksum_) &&
    BindInt64(2, entry.size_) &&
    BindInt(3, entry.mode_) &&
    BindInt64(4, entry.mtime_) &&
    BindText(5, entry.name_.GetChars(),    entry.name_.GetLength()) &&
    BindText(6, entry.symlink_.GetChars(), entry.symlink_.GetLength()) &&
    BindInt64(7, entry.uid_) &&
    BindInt64(8, entry.gid_));
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
  const CatalogDatabase &database)
{
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
    "mtime, flags, name, symlink, uid, gid, xattr) "
    // 9,     10    11     12     13   14   15
    "VALUES (:md5_1, :md5_2, :p_1, :p_2, :hash, :links, :size, :mode, :mtime,"
    " :flags, :name, :symlink, :uid, :gid, :xattr);";
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


bool SqlDirentInsert::BindXattr(const XattrList &xattrs) {
  unsigned char *packed_xattrs;
  unsigned size;
  xattrs.Serialize(&packed_xattrs, &size);
  if (packed_xattrs == NULL)
    return BindNull(15);
  return BindBlobTransient(15, packed_xattrs, size);
}


bool SqlDirentInsert::BindXattrEmpty() {
  return BindNull(15);
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
  return FileChunk(
    RetrieveHashBlob(2, interpret_hash_as, shash::kSuffixPartial),
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

  int compression_mask = 7 << SqlDirent::kFlagPosCompression;
  string flags2compression =
    " ((flags&" + StringifyInt(compression_mask) + ") >> " +
    StringifyInt(SqlDirent::kFlagPosCompression) + ") " +
    "AS compression_algorithm ";

  // TODO(reneme): this depends on shash::kSuffix* being a char!
  //               it should be more generic or replaced entirely
  // TODO(reneme): this is practically the same as SqlListContentHashes and
  //               should be consolidated
  string sql = "SELECT DISTINCT hash, "
  "CASE WHEN flags & " + StringifyInt(SqlDirent::kFlagFile) + " THEN " +
    StringifyInt(shash::kSuffixNone) + " " +
  "WHEN flags & " + StringifyInt(SqlDirent::kFlagDir) + " THEN " +
    StringifyInt(shash::kSuffixMicroCatalog) + " END " +
  "AS chunk_type, " + flags2hash + "," + flags2compression +
  "FROM catalog WHERE (hash IS NOT NULL) AND "
    "(flags & " + StringifyInt(SqlDirent::kFlagFileExternal) + " = 0)";
  if (database.schema_version() >= 2.4 - CatalogDatabase::kSchemaEpsilon) {
    sql +=
      " UNION "
      "SELECT DISTINCT chunks.hash, " + StringifyInt(shash::kSuffixPartial) +
      ", " + flags2hash + "," + flags2compression +
      "FROM chunks, catalog WHERE "
      "chunks.md5path_1=catalog.md5path_1 AND "
      "chunks.md5path_2=catalog.md5path_2 AND "
      "(catalog.flags & " + StringifyInt(SqlDirent::kFlagFileExternal) +
      " = 0)";
  }
  sql += ";";
  Init(database.sqlite_db(), sql);
}


bool SqlAllChunks::Open() {
  return true;
}


bool SqlAllChunks::Next(shash::Any *hash, zlib::Algorithms *compression_alg) {
  if (!FetchRow()) {
    return false;
  }

  *hash = RetrieveHashBlob(0, static_cast<shash::Algorithms>(RetrieveInt(2)),
                              static_cast<shash::Suffix>(RetrieveInt(1)));
  *compression_alg = static_cast<zlib::Algorithms>(RetrieveInt(3));
  return true;
}


bool SqlAllChunks::Close() {
  return Reset();
}


//------------------------------------------------------------------------------


SqlLookupXattrs::SqlLookupXattrs(const CatalogDatabase &database) {
  const string statement =
    "SELECT xattr FROM catalog "
    "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);";
  Init(database.sqlite_db(), statement);
}


bool SqlLookupXattrs::BindPathHash(const shash::Md5 &hash) {
  return BindMd5(1, 2, hash);
}


XattrList SqlLookupXattrs::GetXattrs() {
  const unsigned char *packed_xattrs =
    reinterpret_cast<const unsigned char *>(RetrieveBlob(0));
  if (packed_xattrs == NULL)
    return XattrList();

  int size = RetrieveBytes(0);
  assert(size >= 0);
  UniquePtr<XattrList> xattrs(XattrList::Deserialize(packed_xattrs, size));
  if (!xattrs.IsValid()) {
    LogCvmfs(kLogCatalog, kLogDebug, "corrupted xattr data");
    return XattrList();
  }
  return *xattrs;
}

}  // namespace catalog
