#ifndef CATALOG_QUERIES_H
#define CATALOG_QUERIES_H 1

#include <string>
#include <sstream>

#include "hash.h"
#include "directory_entry.h"

extern "C" {
   #include "sqlite3-duplex.h"
   #include "debug.h"
}

namespace cvmfs {

class SqlStatement {
 protected:
  SqlStatement() {};
  
 public:
  SqlStatement(const sqlite3 *database, const std::string &statement) {
    Init(database, statement);
  }
   
  inline bool Init(const sqlite3 *database, const std::string &statement) {
    const bool result = sqlite3_prepare_v2((sqlite3*)database,
                                           statement.c_str(),
                                           -1, // parse until null termination
                                           &statement_,
                                           NULL);

    if (SQLITE_OK != result) {
      pmesg(D_SQL, "FAILED to prepare statement '%s' - error code: %d", statement.c_str(), result);
      return false;
    }

    pmesg(D_SQL, "SUCCESSFULLY prepared statement '%s'", statement.c_str());
    return true;
  }

  virtual ~SqlStatement() {
    const bool result = sqlite3_finalize(statement_);

    if (SQLITE_OK != result) {
      pmesg(D_SQL, "FAILED to finalize statement - error code: %d", result);
    }

    pmesg(D_SQL, "SUCCESSFULLY finalized statement");
  }

  inline bool Reset() {
    return (SQLITE_OK == sqlite3_reset(statement_));
  }

  inline bool FetchRow() {
    return SQLITE_OK == sqlite3_step(statement_);
  }
  
  inline bool BindBlob(const int index, const void* value, const int size, void (*destructor)(void*)) {
    return SQLITE_OK == sqlite3_bind_blob(statement_, index, value, size, destructor);
  }
  inline bool BindDouble(const int index, const double value) {
    return SQLITE_OK == sqlite3_bind_double(statement_, index, value);
  }
  inline bool BindInt(const int index, const int value) {
    return SQLITE_OK == sqlite3_bind_int(statement_, index, value);
  }
  inline bool BindInt64(const int index, const sqlite3_int64 value) {
    return SQLITE_OK == sqlite3_bind_int64(statement_, index, value);
  }
  inline bool BindNull(const int index) {
    return SQLITE_OK == sqlite3_bind_null(statement_, index);
  }
  inline bool BindText(const int index, const char* value, const int size, void (*destructor)(void*)) {
    return SQLITE_OK == sqlite3_bind_text(statement_, index, value, size, destructor);
  }
  inline bool BindText16(const int index, const void* value, const int size, void (*destructor)(void*)) {
    return SQLITE_OK == sqlite3_bind_text16(statement_, index, value, size, destructor);
  }
  inline bool BindValue(const int index, const sqlite3_value* value) {
    return SQLITE_OK == sqlite3_bind_value(statement_, index, value);
  }
  inline bool BindZeroblob(const int index, const int size) {
    return SQLITE_OK == sqlite3_bind_zeroblob(statement_, index, size);
  }
  
  inline const void *RetrieveBlob(const int iCol) const {
    return sqlite3_column_blob(statement_, iCol);
  }
  inline int RetrieveBytes(const int iCol) const {
    return sqlite3_column_bytes(statement_, iCol);
  }
  inline int RetrieveBytes16(const int iCol) const {
    return sqlite3_column_bytes16(statement_, iCol);
  }
  inline double RetrieveDouble(const int iCol) const {
    return sqlite3_column_double(statement_, iCol);
  }
  inline int RetrieveInt(const int iCol) const {
    return sqlite3_column_int(statement_, iCol);
  }
  inline sqlite3_int64 RetrieveInt64(const int iCol) const {
    return sqlite3_column_int64(statement_, iCol);
  }
  inline const unsigned char *RetrieveText(const int iCol) const {
    return sqlite3_column_text(statement_, iCol);
  }
  inline const void *RetrieveText16(const int iCol) const {
    return sqlite3_column_text16(statement_, iCol);
  }
  inline int RetrieveType(const int iCol) const {
    return sqlite3_column_type(statement_, iCol);
  }
  inline sqlite3_value *RetriveValue(const int iCol) const {
    return sqlite3_column_value(statement_, iCol);
  }

 protected:
  sqlite3_stmt *statement_;
};

class LookupSqlStatement : public SqlStatement {
 protected:
  inline std::string GetFieldsToSelect() const {
    return "hash, inode, size, mode, mtime, flags, name, symlink, md5path_1, md5path_2, parent_1, parent_2, rowid";
  }

 public:
  inline DirectoryEntry GetDirectoryEntry() const {
    // TODO: implement me!
    return DirectoryEntry();
  }
  
  inline hash::t_md5 GetPathHash() const {
    // TODO: implement me!
    return hash::t_md5();
  }
  
  inline hash::t_md5 GetParentPathHash() const {
    // TODO: implement me!
    return hash::t_md5();
  }
};

class ListingLookupSqlStatement : public LookupSqlStatement {
 public:
  ListingLookupSqlStatement(const sqlite3 *database) {
    std::ostringstream statement;
    statement << "SELECT " << GetFieldsToSelect() << " FROM catalog "
                 "WHERE (parent_1 = :p_1) AND (parent_2 = :p_2);";
    Init(database, statement.str());
  }
  
  inline bool BindPathHash(const struct hash::t_md5 &hash) {
    return (
      SQLITE_OK == BindInt64(1, *((sqlite_int64 *)(&hash.digest[0]))) &&
      SQLITE_OK == BindInt64(2, *((sqlite_int64 *)(&hash.digest[8])))
    );
  }
};

class PathHashLookupSqlStatement : public LookupSqlStatement {
 public:
  PathHashLookupSqlStatement(const sqlite3 *database) {
    std::ostringstream statement;
    statement << "SELECT " << GetFieldsToSelect() << " FROM catalog "
                 "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);";
    Init(database, statement.str());
  }

  inline bool BindPathHash(const struct hash::t_md5 &hash) {
    return (
      SQLITE_OK == BindInt64(1, *((sqlite_int64 *)(&hash.digest[0]))) &&
      SQLITE_OK == BindInt64(2, *((sqlite_int64 *)(&hash.digest[8])))
    );
  }
};

class InodeLookupSqlStatement : public LookupSqlStatement {
 public:
  InodeLookupSqlStatement(const sqlite3 *database) {
    std::ostringstream statement;
    statement << "SELECT " << GetFieldsToSelect() << " FROM catalog "
                 "WHERE rowid = :rowid;";
    Init(database, statement.str());
  }

  inline bool BindInode(const uint64_t inode) {
    return BindInt64(1, inode);
  }
};

class FindNestedCatalogSqlStatement : public SqlStatement {
 public:
  FindNestedCatalogSqlStatement(const sqlite3 *database) {
    Init(database, "SELECT sha1 FROM nested_catalogs WHERE path=:path;");
  }
  
  inline bool BindSearchPath(const std::string &path) {
    return BindText(1, &path[0], path.length(), SQLITE_STATIC);
  }
  
  inline hash::t_sha1 GetContentHash() const {
    hash::t_sha1 sha1;
    const std::string sha1_str = std::string((char *)RetrieveText(0));
    sha1.from_hash_str(sha1_str);
    return sha1;
  }
};

} // namespace cvmfs

#endif /* CATALOG_QUERIES_H */
