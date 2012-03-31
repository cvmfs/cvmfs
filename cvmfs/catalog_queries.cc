#include "catalog_queries.h"

#include <cstdlib>

#include "catalog.h"
#include "logging.h"

using namespace std;

namespace catalog {

SqlStatement::SqlStatement(const sqlite3 *database, const std::string &statement) {
  Init(database, statement);
}

SqlStatement::~SqlStatement() {
  last_error_code_ = sqlite3_finalize(statement_);

  if (not Successful()) {
    LogCvmfs(kLogSql, kLogDebug,
             "FAILED to finalize statement - error code: %d", last_error_code_);
  }

  LogCvmfs(kLogSql, kLogDebug, "successfully finalized statement");
}

bool SqlStatement::Init(const sqlite3 *database, const std::string &statement) {
  last_error_code_ = sqlite3_prepare_v2((sqlite3*)database,
                                        statement.c_str(),
                                        -1, // parse until null termination
                                        &statement_,
                                        NULL);

  if (not Successful()) {
    LogCvmfs(kLogSql, kLogDebug,
             "FAILED to prepare statement '%s' - error code: %d",
             statement.c_str(), GetLastError());
    LogCvmfs(kLogSql, kLogDebug, "Error message: '%s'",
             sqlite3_errmsg((sqlite3*)database));
    return false;
  }

  LogCvmfs(kLogSql, kLogDebug, "successfully prepared statement '%s'",
           statement.c_str());
  return true;
}

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

unsigned int DirectoryEntrySqlStatement::CreateDatabaseFlags(const DirectoryEntry &entry) const {
  unsigned int database_flags = 0;

  if (entry.IsNestedCatalogRoot()) {
    database_flags |= kFlagDirNestedRoot;
  }

  if (entry.IsNestedCatalogMountpoint()) {
    database_flags |= kFlagDirNestedMountpoint;
  }

  if (entry.IsDirectory()) {
    database_flags |= kFlagDir;
  } else if (entry.IsLink()) {
    database_flags |= kFlagFile | kFlagLink;
  } else {
    database_flags |= kFlagFile;
  }

  database_flags = SetLinkcountInFlags(database_flags, entry.linkcount());
  return database_flags;
}

std::string DirectoryEntrySqlStatement::ExpandSymlink(const std::string raw_symlink) const {
  string result = "";

  for (string::size_type i = 0; i < raw_symlink.length(); i++) {
    string::size_type lpar;
    string::size_type rpar;
    if ((raw_symlink[i] == '$') &&
        ((lpar = raw_symlink.find('(', i+1)) != string::npos) &&
        ((rpar = raw_symlink.find(')', i+2)) != string::npos) &&
        (rpar > lpar))
    {
      string var = raw_symlink.substr(lpar + 1, rpar-lpar-1);
      char *var_exp = getenv(var.c_str()); /* Don't free! Nothing is allocated here */
      if (var_exp) {
        result += var_exp;
      }
      i = rpar;
    } else {
      result += raw_symlink[i];
    }
  }

  return result;
}

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

bool ManipulateDirectoryEntrySqlStatement::BindDirectoryEntryFields(const int hash_field,
                                                                    const int inode_field,
                                                                    const int size_field,
                                                                    const int mode_field,
                                                                    const int mtime_field,
                                                                    const int flags_field,
                                                                    const int name_field,
                                                                    const int symlink_field,
                                                                    const DirectoryEntry &entry) {
  return (
    BindSha1Hash( hash_field,    entry.checksum_) &&
    BindInt64(    inode_field,   entry.hardlink_group_id_) && // quirky database layout here ( legacy ;-) )
    BindInt64(    size_field,    entry.size_) &&
    BindInt(      mode_field,    entry.mode_) &&
    BindInt64(    mtime_field,   entry.mtime_) &&
    BindInt(      flags_field,   CreateDatabaseFlags(entry)) &&
    BindText(     name_field,    entry.name_) &&
    BindText(     symlink_field, entry.symlink_)
  );
}

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

std::string LookupSqlStatement::GetFieldsToSelect() const {
  return "hash, inode, size, mode, mtime, flags, name, symlink, md5path_1, md5path_2, parent_1, parent_2, rowid";
      //    0     1      2     3     4      5      6      7        8          9           10        11       12
}

hash::Md5 LookupSqlStatement::GetPathHash() const {
  return RetrieveMd5Hash(8, 9);
}

hash::Md5 LookupSqlStatement::GetParentPathHash() const {
  return RetrieveMd5Hash(10, 11);
}

DirectoryEntry LookupSqlStatement::GetDirectoryEntry(const Catalog *catalog) const {
  // fill the directory entry
  // (this method is a friend of DirectoryEntry ;-) )
  DirectoryEntry result;

  // read administrative stuff from the result
  int database_flags                   = RetrieveInt(5);
  result.catalog_                      = (Catalog*)catalog;
  result.is_nested_catalog_root_       = (database_flags & kFlagDirNestedRoot);
  result.is_nested_catalog_mountpoint_ = (database_flags & kFlagDirNestedMountpoint);
  result.hardlink_group_id_            = (catalog->schema() < 2.0) ?
                                         0 : RetrieveInt64(1); // quirky database layout here ( legacy ;-) )

  // read the usual file information
  result.inode_        = ((Catalog*)catalog)->GetMangledInode(RetrieveInt64(12),
                          (catalog->schema() < 2.0) ? 0 : RetrieveInt64(1));
  result.parent_inode_ = DirectoryEntry::kInvalidInode; // must be set later by a second catalog lookup
  result.linkcount_    = GetLinkcountFromFlags(database_flags);
  result.mode_         = RetrieveInt(3);
  result.size_         = RetrieveInt64(2);
  result.mtime_        = RetrieveInt64(4);
  result.checksum_     = RetrieveSha1HashFromBlob(0);
  result.name_         = string((char *)RetrieveText(6));
  result.symlink_      = ExpandSymlink((char *)RetrieveText(7));

  return result;
}

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

ListingLookupSqlStatement::ListingLookupSqlStatement(const sqlite3 *database) {
  std::ostringstream statement;
  statement << "SELECT " << GetFieldsToSelect() << " FROM catalog "
               "WHERE (parent_1 = :p_1) AND (parent_2 = :p_2);";
  Init(database, statement.str());
}

bool ListingLookupSqlStatement::BindPathHash(const struct hash::Md5 &hash) {
  return BindMd5Hash(1, 2, hash);
}

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

PathHashLookupSqlStatement::PathHashLookupSqlStatement(const sqlite3 *database) {
  std::ostringstream statement;
  statement << "SELECT " << GetFieldsToSelect() << " FROM catalog "
               "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);";
  Init(database, statement.str());
}

bool PathHashLookupSqlStatement::BindPathHash(const struct hash::Md5 &hash) {
  return BindMd5Hash(1, 2, hash);
}

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

InodeLookupSqlStatement::InodeLookupSqlStatement(const sqlite3 *database) {
  std::ostringstream statement;
  statement << "SELECT " << GetFieldsToSelect() << " FROM catalog "
               "WHERE rowid = :rowid;";
  Init(database, statement.str());
}

bool InodeLookupSqlStatement::BindRowId(const uint64_t inode) {
  return BindInt64(1, inode);
}

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

FindNestedCatalogSqlStatement::FindNestedCatalogSqlStatement(const sqlite3 *database) {
  Init(database, "SELECT sha1 FROM nested_catalogs WHERE path=:path;");
}

bool FindNestedCatalogSqlStatement::BindSearchPath(const std::string &path) {
  return BindText(1, &path[0], path.length(), SQLITE_STATIC);
}

hash::Any FindNestedCatalogSqlStatement::GetContentHash() const {
  const std::string sha1_str = std::string((char *)RetrieveText(0));
  return (sha1_str.empty()) ? hash::Any(hash::kSha1) : hash::Any(hash::kSha1, hash::HexPtr(sha1_str));
}

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

ListNestedCatalogsSqlStatement::ListNestedCatalogsSqlStatement(const sqlite3 *database) {
  Init(database, "SELECT path, sha1 FROM nested_catalogs;");
}

string ListNestedCatalogsSqlStatement::GetMountpoint() const {
  return string((char*)RetrieveText(0));
}

hash::Any ListNestedCatalogsSqlStatement::GetContentHash() const {
  const std::string sha1_str = std::string((char *)RetrieveText(1));
  return (sha1_str.empty()) ? hash::Any(hash::kSha1) : hash::Any(hash::kSha1, hash::HexPtr(sha1_str));
}

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

InsertDirectoryEntrySqlStatement::InsertDirectoryEntrySqlStatement(const sqlite3 *database) {
  Init(database, "INSERT INTO catalog "
  //                  1           2         3         4       5     6      7     8      9      10    11     12
                 "(md5path_1, md5path_2, parent_1, parent_2, hash, inode, size, mode, mtime, flags, name, symlink) "
                 "VALUES (:md5_1, :md5_2, :p_1, :p_2, :hash, :ino, :size, :mode, :mtime, :flags, :name, :symlink);");
}

bool InsertDirectoryEntrySqlStatement::BindPathHash(const hash::Md5 &hash) {
  return BindMd5Hash(1, 2, hash);
}

bool InsertDirectoryEntrySqlStatement::BindParentPathHash(const hash::Md5 &hash) {
  return BindMd5Hash(3, 4, hash);
}

bool InsertDirectoryEntrySqlStatement::BindDirectoryEntry(const DirectoryEntry &entry) {
  return BindDirectoryEntryFields(5,6,7,8,9,10,11,12, entry);
}

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

UpdateDirectoryEntrySqlStatement::UpdateDirectoryEntrySqlStatement(const sqlite3 *database) {
  Init(database, "UPDATE catalog "
                 //            1             2             3               4
                 "SET hash = :hash, size = :size, mode = :mode, mtime = :mtime, "
                 //          5             6                  7                8
                 "flags = :flags, name = :name, symlink = :symlink, inode = :inode "
                 //                     9                        10
                 "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);");
}

bool UpdateDirectoryEntrySqlStatement::BindPathHash(const hash::Md5 &hash) {
  return BindMd5Hash(9, 10, hash);
}

bool UpdateDirectoryEntrySqlStatement::BindDirectoryEntry(const DirectoryEntry &entry) {
  return BindDirectoryEntryFields(1,8,2,3,4,5,6,7, entry);
}

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

TouchSqlStatement::TouchSqlStatement(const sqlite3 *database) {
  Init(database, "UPDATE catalog SET mtime = :mtime "
                 "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);");
}

bool TouchSqlStatement::BindPathHash(const hash::Md5 &hash) {
  return BindMd5Hash(2, 3, hash);
}

bool TouchSqlStatement::BindTimestamp(const time_t timestamp) {
  return BindInt64(1, timestamp);
}

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

UnlinkSqlStatement::UnlinkSqlStatement(const sqlite3 *database) {
  Init(database, "DELETE FROM catalog "
                 "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);");
}

bool UnlinkSqlStatement::BindPathHash(const hash::Md5 &hash) {
  return BindMd5Hash(1, 2, hash);
}

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

GetMaximalHardlinkGroupIdStatement::GetMaximalHardlinkGroupIdStatement(const sqlite3 *database) {
  Init(database, "SELECT max(inode) FROM catalog;");
}

int GetMaximalHardlinkGroupIdStatement::GetMaximalGroupId() const {
  return RetrieveInt64(0);
}


} // namespace cvmfs
