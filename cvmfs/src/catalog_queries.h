/**
 *  This file provides classes to wrap often used catalog SQL statements.
 *  The C API for sqlite3 is quite odd. It allows named place holders
 *  in prepared SQL statements but requires to fill them by their offset.
 *  To overcome this potential nasty error source, these classes encapsulate
 *  commonly used database queries.
 *
 *  Usage example:
 *
 *   LookupSqlStatement statement(<database>);
 *   statement.BindPathHash(<hash>);
 *   if (statement.FetchRow()) {
 *     statement.GetDirectoryEntry(<catalog>);
 *   }
 *   statement.Reset();
 *
 *  Developed by René Meusel 2011
 */

#ifndef CATALOG_QUERIES_H
#define CATALOG_QUERIES_H 1

#include <string>
#include <sstream>

#include "hash.h"
#include "DirectoryEntry.h"

extern "C" {
   #include "sqlite3-duplex.h"
   #include "debug.h"
}

namespace cvmfs {
  
class Catalog;

/**
 *  Base class for all SQL statement classes
 *  wraps a single SQL statement and all neccessary calls
 *  of the sqlite3 API to deal with this statement
 */
class SqlStatement {
 protected:
  SqlStatement() {};
  
 public:
  /**
   *  basic constructor to use this class for different (not wrapped)
   *  statements, which is highly encouraged.
   *  @param database the database to use the query on
   *  @param statement the statement to prepare
   */
  SqlStatement(const sqlite3 *database, const std::string &statement);
  virtual ~SqlStatement();
  
 protected:
  bool Init(const sqlite3 *database, const std::string &statement);

  /**
   *  checks the last action for success
   *  @return true if last action succeeded otherwise false
   */
  inline bool Successful() const {
    return SQLITE_OK   == last_error_code_ || 
           SQLITE_ROW  == last_error_code_ ||
           SQLITE_DONE == last_error_code_;
  }
  
 public:
  /**
   *  resets a prepared statement to make it reusable
   *  @return true on success otherwise false
   */
  inline bool Reset() {
    last_error_code_ = sqlite3_reset(statement_);
    return Successful();
  }

  /**
   *  execute the prepared statement or fetch it's next row.
   *  (this method is intended to step through the result)
   *  if it returns false this does NOT neccessarily mean, that the actual
   *  statement execution failed, but that NO row was fetched.
   *  @return true if a new row was fetched otherwise false
   */
  inline bool FetchRow() {
    last_error_code_ = sqlite3_step(statement_);
    return SQLITE_ROW == last_error_code_;
  }
  
  /**
   *  executes the prepared statement
   *  (this method should be used for modifying statements like DELETE or INSERT)
   *  @return true on success otherwise false
   */
  inline bool Execute() {
    last_error_code_ = sqlite3_step(statement_);
    return SQLITE_DONE == last_error_code_ || SQLITE_OK == last_error_code_;
  }
  
  /**
   *  return the error code of the last performed action
   *  @return the error code of the last action
   */
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
  
 protected:
  /**
   *  convenience wrapper for binding a MD5 hash
   *  @param iCol1 offset of most significant bits in database query
   *  @param iCol2 offset of least significant bits in database query
   *  @param hash the hash to bind in the query
   *  @result true on success, false otherwise
   */
  inline bool BindMd5Hash(const int iCol1, const int iCol2, const struct hash::t_md5 &hash) {
    return (
      BindInt64(iCol1, *((sqlite_int64 *)(&hash.digest[0]))) &&
      BindInt64(iCol2, *((sqlite_int64 *)(&hash.digest[8])))
    );
  }
  
  /**
   *  convenience wrapper for binding a SHA1 hash
   *  @param iCol offset of the blob field in database query
   *  @param hash the hash to bind in the query
   *  @result true on success, false otherwise
   */
  inline bool BindSha1Hash(const int iCol, const struct hash::t_sha1 &hash) {
    if (hash.is_null()) {
      return BindNull(iCol);
    } else {
      return BindBlob(iCol, hash.digest, 20, SQLITE_STATIC);
    }
  }
  
 public:
  
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
  /**
   *  convenience wrapper for retrieving MD5 hashes
   *  @param iCol1 offset of most significant bits in database query
   *  @param iCol2 offset of least significant bits in database query
   *  @result the retrieved MD5 hash
   */
  inline hash::t_md5 RetrieveMd5Hash(const int iCol1, const int iCol2) const {
    return hash::t_md5(RetrieveInt64(iCol1), RetrieveInt64(iCol2));
  }
  
  /**
   *  convenience wrapper for retrieving a SHA1 hash
   *  @param iCol offset of the blob field in database query
   *  @result the retrieved SHA1 hash
   */
  inline hash::t_sha1 RetrieveSha1Hash(const int iCol) const {
    return (RetrieveBytes(iCol) > 0) ? 
                    hash::t_sha1(RetrieveBlob(iCol), RetrieveBytes(iCol)) :
                    hash::t_sha1();
  }

 private:
  sqlite3_stmt *statement_;
  int last_error_code_;
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class DirectoryEntrySqlStatement : public SqlStatement {
 protected:
  
  // definition of bit positions for the flags field of a DirectoryEntry
  // all not specified bit positions are currently not in use
  const static int kFlagDir                 = 1;
  const static int kFlagDirNestedMountpoint = 2;  /* Link in the parent catalog */
  const static int kFlagDirNestedRoot       = 32; /* Link in the child catalog */
  const static int kFlagFile                = 4;
  const static int kFlagLink                = 8;
  const static int kFlagFileStat            = 16;
  const static int kFlagFileChunk           = 64;
  const static int kFlagLinkCount_0         = 256; // 8 bit for link count of file, starting from here
  const static int kFlagLinkCount_1         = kFlagLinkCount_0 << 1;
  const static int kFlagLinkCount_2         = kFlagLinkCount_0 << 2;
  const static int kFlagLinkCount_3         = kFlagLinkCount_0 << 3;
  const static int kFlagLinkCount_4         = kFlagLinkCount_0 << 4;
  const static int kFlagLinkCount_5         = kFlagLinkCount_0 << 5;
  const static int kFlagLinkCount_6         = kFlagLinkCount_0 << 6;
  const static int kFlagLinkCount_7         = kFlagLinkCount_0 << 7;
  const static int kFlagLinkCount           = kFlagLinkCount_0 | kFlagLinkCount_1 | kFlagLinkCount_2 | kFlagLinkCount_3 | kFlagLinkCount_4 | kFlagLinkCount_5 | kFlagLinkCount_6 | kFlagLinkCount_7;
 
 protected:
  /**
   *  take the meta data from the DirectoryEntry and transform it
   *  into a valid flags field ready to be safed in the database
   *  @param entry the DirectoryEntry to encode
   *  @return an integer containing the bitmap of the flags field
   */
  unsigned int CreateDatabaseFlags(const DirectoryEntry &entry) const;
  
  /**
   *  replaces place holder variables in a symbolic link by actual
   *  path elements
   *  @param raw_symlink the raw symlink path (may) containing place holders
   *  @return the expanded symlink
   */
  std::string ExpandSymlink(const std::string raw_symlink) const;
   
  /**
   *  retrieve the linkcount of the read DirectoryEntry from it's flags field
   *  @param flags the integer containing the flags bitmap
   *  @return the linkcount of the DirectoryEntry to create
   */
  inline int GetLinkcountFromFlags(const unsigned int flags) const {
    return (flags & kFlagLinkCount) / kFlagLinkCount_0;
  }
  
  /**
   *  encodes the linkcount into the flags bitmap.
   *  Note: there are only 8 bit reserved for this number!
   *  @param flags the flags field to change
   *  @param linkcount the linkcount to save in the bitmap
   *  @return a NEW bitmap containing the linkcount on the reserved bits
   */
  inline unsigned int SetLinkcountInFlags(const unsigned int flags, const int linkcount) const {
     // zero the designated area
     unsigned int cleanFlags = (flags & kFlagLinkCount) ^ flags;
     // write the linkcount to the flags field
     return cleanFlags | ((linkcount * kFlagLinkCount_0) & kFlagLinkCount);
  }

};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class LookupSqlStatement : public DirectoryEntrySqlStatement {
 protected:
  /**
   *  there are several lookup statements
   *  all share a list of elements to load
   *  @return a list of sql fields to query for DirectoryEntry
   */
  std::string GetFieldsToSelect() const;

 public:
  /**
   *  retrieves a DirectoryEntry from a freshly performed LookupSqlStatement
   *  @param catalog the catalog in which the DirectoryEntry resides
   *  @return the retrieved DirectoryEntry
   */
  DirectoryEntry GetDirectoryEntry(const Catalog *catalog) const;
  
  /**
   *  DirectoryEntrys do not contain their full path.
   *  This method retrieves the saved path hash from the database
   *  @return the MD5 path hash of a freshly performed lookup
   */
  hash::t_md5 GetPathHash() const;
  
  /**
   *  DirectoryEntrys do not contain their full parent path.
   *  This method retrieves the saved parent path hash from the database
   *  @return the MD5 parent path hash of a freshly performed lookup
   */
  hash::t_md5 GetParentPathHash() const;
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class ListingLookupSqlStatement : public LookupSqlStatement {
 public:
  ListingLookupSqlStatement(const sqlite3 *database);
  bool BindPathHash(const struct hash::t_md5 &hash);
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class PathHashLookupSqlStatement : public LookupSqlStatement {
 public:
  PathHashLookupSqlStatement(const sqlite3 *database);
  bool BindPathHash(const struct hash::t_md5 &hash);
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class InodeLookupSqlStatement : public LookupSqlStatement {
 public:
  InodeLookupSqlStatement(const sqlite3 *database);
  bool BindRowId(const uint64_t inode);
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class FindNestedCatalogSqlStatement : public SqlStatement {
 public:
  FindNestedCatalogSqlStatement(const sqlite3 *database);
  bool BindSearchPath(const std::string &path);
  hash::t_sha1 GetContentHash() const;
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class InsertDirectoryEntrySqlStatement : public DirectoryEntrySqlStatement {
 public:
  InsertDirectoryEntrySqlStatement(const sqlite3 *database);
  bool BindPathHash(const hash::t_md5 &hash);
  bool BindParentPathHash(const hash::t_md5 &hash);
  bool BindDirectoryEntry(const DirectoryEntry &entry);
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class TouchSqlStatement : public SqlStatement {
 public:
  TouchSqlStatement(const sqlite3 *database);
  bool BindPathHash(const hash::t_md5 &hash);
  bool BindTimestamp(time_t timestamp);
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class UnlinkSqlStatement : public SqlStatement {
 public:
  UnlinkSqlStatement(const sqlite3 *database);
  bool BindPathHash(const hash::t_md5 &hash);
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class GetMaximalHardlinkGroupIdStatement : public SqlStatement {
 public:
  GetMaximalHardlinkGroupIdStatement(const sqlite3 *database);
  int GetMaximalGroupId() const;
};

} // namespace cvmfs

#endif /* CATALOG_QUERIES_H */
