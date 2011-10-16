#include "catalog_queries.h"

#include "catalog_class.h"

using namespace std;

namespace cvmfs {

SqlStatement::SqlStatement(const sqlite3 *database, const std::string &statement) {
  Init(database, statement);
}

SqlStatement::~SqlStatement() {
  last_error_code_ = sqlite3_finalize(statement_);

  if (not Successful()) {
    pmesg(D_SQL, "FAILED to finalize statement - error code: %d", last_error_code_);
  }

  pmesg(D_SQL, "successfully finalized statement");
}

bool SqlStatement::Init(const sqlite3 *database, const std::string &statement) {
  last_error_code_ = sqlite3_prepare_v2((sqlite3*)database,
                                        statement.c_str(),
                                        -1, // parse until null termination
                                        &statement_,
                                        NULL);

  if (not Successful()) {
    pmesg(D_SQL, "FAILED to prepare statement '%s' - error code: %d", statement.c_str(), last_error_code_);
    return false;
  }

  pmesg(D_SQL, "successfully prepared statement '%s'", statement.c_str());
  return true;
}

std::string LookupSqlStatement::GetFieldsToSelect() const {
  return "hash, inode, size, mode, mtime, flags, name, symlink, md5path_1, md5path_2, parent_1, parent_2, rowid";
      //    0     1      2     3     4      5      6      7        8          9           10        11       12
}

DirectoryEntry LookupSqlStatement::GetDirectoryEntry(const Catalog *catalog) const {
  // compute file checksum, if there is one
  hash::t_sha1 checksum = (RetrieveBytes(0) > 0) ? 
                  hash::t_sha1(RetrieveBlob(0), RetrieveBytes(0)) :
                  hash::t_sha1();

  // fill the directory entry
  // (this method is a friend of DirectoryEntry ;-) )
  DirectoryEntry result;
  result.flags_        = RetrieveInt(5);
  result.catalog_      = catalog;
  result.inode_        = catalog->GetInodeFromRowIdAndHardlinkGroupId(RetrieveInt64(12), RetrieveInt64(1));
  result.parent_inode_ = DirectoryEntry::kInvalidInode; // must be set later by a second catalog lookup
  result.mode_         = RetrieveInt(3);
  result.size_         = RetrieveInt64(2);
  result.mtime_        = RetrieveInt64(4);
  result.checksum_     = checksum;
  result.name_         = string((char *)RetrieveText(6));
  result.symlink_      = string((char *)RetrieveText(7));

  return result;
}

ListingLookupSqlStatement::ListingLookupSqlStatement(const sqlite3 *database) {
  std::ostringstream statement;
  statement << "SELECT " << GetFieldsToSelect() << " FROM catalog "
               "WHERE (parent_1 = :p_1) AND (parent_2 = :p_2);";
  Init(database, statement.str());
}
  
bool ListingLookupSqlStatement::BindPathHash(const struct hash::t_md5 &hash) {
  return (
    BindInt64(1, *((sqlite_int64 *)(&hash.digest[0]))) &&
    BindInt64(2, *((sqlite_int64 *)(&hash.digest[8])))
  );
}

PathHashLookupSqlStatement::PathHashLookupSqlStatement(const sqlite3 *database) {
  std::ostringstream statement;
  statement << "SELECT " << GetFieldsToSelect() << " FROM catalog "
               "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);";
  Init(database, statement.str());
}

bool PathHashLookupSqlStatement::BindPathHash(const struct hash::t_md5 &hash) {
  return (
    BindInt64(1, *((sqlite_int64 *)(&hash.digest[0]))) &&
    BindInt64(2, *((sqlite_int64 *)(&hash.digest[8])))
  );
}

InodeLookupSqlStatement::InodeLookupSqlStatement(const sqlite3 *database) {
  std::ostringstream statement;
  statement << "SELECT " << GetFieldsToSelect() << " FROM catalog "
               "WHERE rowid = :rowid;";
  Init(database, statement.str());
}

bool InodeLookupSqlStatement::BindRowId(const uint64_t inode) {
  return BindInt64(1, inode);
}

FindNestedCatalogSqlStatement::FindNestedCatalogSqlStatement(const sqlite3 *database) {
  Init(database, "SELECT sha1 FROM nested_catalogs WHERE path=:path;");
}

bool FindNestedCatalogSqlStatement::BindSearchPath(const std::string &path) {
  return BindText(1, &path[0], path.length(), SQLITE_STATIC);
}

hash::t_sha1 FindNestedCatalogSqlStatement::GetContentHash() const {
  hash::t_sha1 sha1;
  const std::string sha1_str = std::string((char *)RetrieveText(0));
  sha1.from_hash_str(sha1_str);
  return sha1;
}

} // namespace cvmfs
