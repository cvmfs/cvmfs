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
  
class Catalog;

class SqlStatement {
 protected:
  SqlStatement() {};
  
 public:
  SqlStatement(const sqlite3 *database, const std::string &statement);
  bool Init(const sqlite3 *database, const std::string &statement);
  virtual ~SqlStatement();

  inline bool Reset() {
    last_error_code_ = sqlite3_reset(statement_);
    return Successful();
  }

  inline bool FetchRow() {
    last_error_code_ = sqlite3_step(statement_);
    return SQLITE_ROW == last_error_code_;
  }
  
  inline bool Successful() const {
    return SQLITE_OK   == last_error_code_ || 
           SQLITE_ROW  == last_error_code_ ||
           SQLITE_DONE == last_error_code_;
  }
  inline int GetLastError() const { return last_error_code_; }
  
  inline bool BindBlob(const int index, const void* value, const int size, void (*destructor)(void*)) {
    last_error_code_ = sqlite3_bind_blob(statement_, index, value, size, destructor);
    return Successful();
  }
  inline bool BindDouble(const int index, const double value) {
    last_error_code_ = sqlite3_bind_double(statement_, index, value);
    return Successful();
  }
  inline bool BindInt(const int index, const int value) {
    last_error_code_ = sqlite3_bind_int(statement_, index, value);
    return Successful();
  }
  inline bool BindInt64(const int index, const sqlite3_int64 value) {
    last_error_code_ = sqlite3_bind_int64(statement_, index, value);
    return Successful();
  }
  inline bool BindNull(const int index) {
    last_error_code_ = sqlite3_bind_null(statement_, index);
    return Successful();
  }
  inline bool BindText(const int index, const char* value, const int size, void (*destructor)(void*)) {
    last_error_code_ = sqlite3_bind_text(statement_, index, value, size, destructor);
    return Successful();
  }
  inline bool BindText16(const int index, const void* value, const int size, void (*destructor)(void*)) {
    last_error_code_ = sqlite3_bind_text16(statement_, index, value, size, destructor);
    return Successful();
  }
  inline bool BindValue(const int index, const sqlite3_value* value) {
    last_error_code_ = sqlite3_bind_value(statement_, index, value);
    return Successful();
  }
  inline bool BindZeroblob(const int index, const int size) {
    last_error_code_ = sqlite3_bind_zeroblob(statement_, index, size);
    return Successful();
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

 private:
  sqlite3_stmt *statement_;
  int last_error_code_;
};

class LookupSqlStatement : public SqlStatement {
 protected:
  std::string GetFieldsToSelect() const;

 public:
  DirectoryEntry GetDirectoryEntry(const Catalog *catalog) const;
  
  hash::t_md5 GetPathHash() const;
  hash::t_md5 GetParentPathHash() const;
};

class ListingLookupSqlStatement : public LookupSqlStatement {
 public:
  ListingLookupSqlStatement(const sqlite3 *database);
  bool BindPathHash(const struct hash::t_md5 &hash);
};

class PathHashLookupSqlStatement : public LookupSqlStatement {
 public:
  PathHashLookupSqlStatement(const sqlite3 *database);
  bool BindPathHash(const struct hash::t_md5 &hash);
};

class InodeLookupSqlStatement : public LookupSqlStatement {
 public:
  InodeLookupSqlStatement(const sqlite3 *database);
  bool BindRowId(const uint64_t inode);
};

class FindNestedCatalogSqlStatement : public SqlStatement {
 public:
  FindNestedCatalogSqlStatement(const sqlite3 *database);
  bool BindSearchPath(const std::string &path);
  hash::t_sha1 GetContentHash() const;
};

} // namespace cvmfs

#endif /* CATALOG_QUERIES_H */
