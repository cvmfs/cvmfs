/**
 * This file is part of the CernVM file system.
 *
 * This file provides classes to wrap often used catalog SQL statements.
 * In particular, it wraps around sqlite3 prepared statement syntax.
 *
 * Usage example:
 *   LookupSqlStatement statement(<database>);
 *   statement.BindPathHash(<hash>);
 *   if (statement.FetchRow()) {
 *     statement.GetDirectoryEntry(<catalog>);
 *   }
 *   statement.Reset();
 */

#ifndef CVMFS_CATALOG_QUERIES_H_
#define CVMFS_CATALOG_QUERIES_H_

#include <string>
#include <sstream>

#include "hash.h"
#include "dirent.h"
#include "shortstring.h"
#include "duplex_sqlite3.h"

namespace catalog {

class Catalog;

/**
 * Base class for all SQL statement classes.  It wraps a single SQL statement
 * and all neccessary calls of the sqlite3 API to deal with this statement.
 */
class SqlStatement {
 public:
  /**
   * Basic constructor to use this class for a specific statement.
   * @param database the database to use the query on
   * @param statement the statement to prepare
   */
  SqlStatement(const sqlite3 *database, const std::string &statement);
  virtual ~SqlStatement();

  bool Execute();
  bool FetchRow();
  bool Reset();
  inline int GetLastError() const { return last_error_code_; }

  inline bool BindBlob(const int index, const void* value, const int size)
  {
    last_error_code_ = sqlite3_bind_blob(statement_, index, value, size,
                                         SQLITE_STATIC);
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
  inline bool BindText(const int index, const std::string &value) {
    last_error_code_ = sqlite3_bind_text(statement_, index,
                                         value.data(), value.length(), NULL);
    return Successful();
  }
  inline bool BindText(const int index, const char* value, const int size,
                       void (*destructor)(void*))
  {
    last_error_code_ = sqlite3_bind_text(statement_, index, value, size,
                                         destructor);
    return Successful();
  }


  inline int RetrieveType(const int idx_column) const {
    return sqlite3_column_type(statement_, idx_column);
  }
  inline int RetrieveBytes(const int idx_column) const {
    return sqlite3_column_bytes(statement_, idx_column);
  }
  inline const void *RetrieveBlob(const int idx_column) const {
    return sqlite3_column_blob(statement_, idx_column);
  }
  inline double RetrieveDouble(const int idx_column) const {
    return sqlite3_column_double(statement_, idx_column);
  }
  inline int RetrieveInt(const int idx_column) const {
    return sqlite3_column_int(statement_, idx_column);
  }
  inline sqlite3_int64 RetrieveInt64(const int idx_column) const {
    return sqlite3_column_int64(statement_, idx_column);
  }
  inline const unsigned char *RetrieveText(const int idx_column) const {
    return sqlite3_column_text(statement_, idx_column);
  }

  /**
   * Wrapper for retrieving MD5 hashes.
   * @param idx_high offset of most significant bits in database query
   * @param idx_low offset of least significant bits in database query
   * @result the retrieved MD5 hash
   */
  inline hash::Md5 RetrieveMd5Hash(const int idx_high,
                                   const int idx_low) const
  {
    return hash::Md5(RetrieveInt64(idx_high), RetrieveInt64(idx_low));
  }

  /**
   * Wrapper for retrieving a SHA1 hash from a blob field.
   */
  inline hash::Any RetrieveSha1HashFromBlob(const int idx_column) const {
    if (RetrieveBytes(idx_column) > 0) {
      return hash::Any(hash::kSha1,
        static_cast<const unsigned char *>(RetrieveBlob(idx_column)),
        RetrieveBytes(idx_column));
    }
    return hash::Any(hash::kSha1);
  }

  /**
   * Wrapper for retrieving a SHA1 hash from a text field.
   */
  inline hash::Any RetrieveSha1HashFromText(const int idx_column) const {
    const std::string hash_string = std::string(
      reinterpret_cast<const char *>(RetrieveText(idx_column)));
    return hash::Any(hash::kSha1, hash::HexPtr(hash_string));
  }


 protected:
  SqlStatement() { }
  bool Init(const sqlite3 *database, const std::string &statement);

  /**
   * Checks the last action for success
   * @return true if last action succeeded otherwise false
   */
  inline bool Successful() const {
    return SQLITE_OK   == last_error_code_ ||
           SQLITE_ROW  == last_error_code_ ||
           SQLITE_DONE == last_error_code_;
  }

  /**
   * Wrapper for binding a MD5 hash.
   * @param idx_high offset of most significant bits in database query
   * @param idx_low offset of least significant bits in database query
   * @param hash the hash to bind in the query
   * @result true on success, false otherwise
   */
  inline bool BindMd5Hash(const int idx_high, const int idx_low,
                          const hash::Md5 &hash)
  {
    uint64_t high, low;
    hash.ToIntPair(&high, &low);
    bool retval = BindInt64(idx_high, high) && BindInt64(idx_low, low);
    return retval;
  }

  /**
   * Wrapper for binding a SHA1 hash.
   * @param idx_column offset of the blob field in database query
   * @param hash the hash to bind in the query
   * @result true on success, false otherwise
   */
  inline bool BindSha1Hash(const int idx_column, const struct hash::Any &hash) {
    if (hash.IsNull()) {
      return BindNull(idx_column);
    } else {
      return BindBlob(idx_column, hash.digest, hash.GetDigestSize());
    }
  }

 private:
  sqlite3 *database_;
  sqlite3_stmt *statement_;
  int last_error_code_;
};


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
  void ExpandSymlink(LinkString *raw_symlink) const;

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

class ManipulateDirectoryEntrySqlStatement : public DirectoryEntrySqlStatement {
 public:
   /**
    *  to bind a whole DirectoryEntry
    *  @param entry the DirectoryEntry to bind in the SQL statement
    *  @return true on success, false otherwise
    */
  virtual bool BindDirectoryEntry(const DirectoryEntry &entry) = 0;

 protected:
  bool BindDirectoryEntryFields(const int hash_field,
                                const int inode_field,
                                const int size_field,
                                const int mode_field,
                                const int mtime_field,
                                const int flags_field,
                                const int name_field,
                                const int symlink_field,
                                const DirectoryEntry &entry);
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
  hash::Md5 GetPathHash() const;

  /**
   *  DirectoryEntrys do not contain their full parent path.
   *  This method retrieves the saved parent path hash from the database
   *  @return the MD5 parent path hash of a freshly performed lookup
   */
  hash::Md5 GetParentPathHash() const;
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class ListingLookupSqlStatement : public LookupSqlStatement {
 public:
  ListingLookupSqlStatement(const sqlite3 *database);
  bool BindPathHash(const struct hash::Md5 &hash);
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class PathHashLookupSqlStatement : public LookupSqlStatement {
 public:
  PathHashLookupSqlStatement(const sqlite3 *database);
  bool BindPathHash(const struct hash::Md5 &hash);
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
  bool BindSearchPath(const PathString &path);
  hash::Any GetContentHash() const;
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class ListNestedCatalogsSqlStatement : public SqlStatement {
 public:
  ListNestedCatalogsSqlStatement(const sqlite3 *database);
  PathString GetMountpoint() const;
  hash::Any GetContentHash() const;
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class InsertDirectoryEntrySqlStatement : public ManipulateDirectoryEntrySqlStatement {
 public:
  InsertDirectoryEntrySqlStatement(const sqlite3 *database);
  bool BindPathHash(const hash::Md5 &hash);
  bool BindParentPathHash(const hash::Md5 &hash);
  bool BindDirectoryEntry(const DirectoryEntry &entry);
};

//
// ###########################################################################
// ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###   ###
// ###########################################################################
//

class UpdateDirectoryEntrySqlStatement : public ManipulateDirectoryEntrySqlStatement {
 public:
  UpdateDirectoryEntrySqlStatement(const sqlite3 *database);
  bool BindPathHash(const hash::Md5 &hash);
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
  bool BindPathHash(const hash::Md5 &hash);
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
  bool BindPathHash(const hash::Md5 &hash);
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

}  // namespace cvmfs

#endif  // CVMFS_CATALOG_QUERIES_H_
