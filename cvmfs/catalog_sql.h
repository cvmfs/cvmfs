/**
 * This file is part of the CernVM file system.
 *
 * This file provides classes to wrap often used catalog SQL statements.
 * In particular, it wraps around sqlite3 prepared statement syntax.
 *
 * Usage example:
 *   SqlLookup statement(<database>);
 *   statement.BindPathHash(<hash>);
 *   if (statement.FetchRow()) {
 *     statement.GetDirectoryEntry(<catalog>);
 *   }
 *   statement.Reset();
 */

#ifndef CVMFS_CATALOG_SQL_H_
#define CVMFS_CATALOG_SQL_H_

#include <inttypes.h>

#include <string>
#include <sstream>

#include "hash.h"
#include "dirent.h"
#include "shortstring.h"
#include "duplex_sqlite3.h"

namespace catalog {

class Catalog;


/**
 * Encapsulates an SQlite connection.  Abstracts the schema.
 */
class Database {
 public:
  enum OpenMode {
    kOpenReadOnly,
    kOpenReadWrite
  };

  Database(const std::string filename, const OpenMode open_mode);
  ~Database();
  static bool Create(const std::string &filename,
                     const DirectoryEntry &root_entry,
                     const std::string &root_path);

  sqlite3 *sqlite_db() const { return sqlite_db_; }
  std::string filename() const { return filename_; }
  float schema_version() const { return schema_version_; }
  bool ready() const { return ready_; }
 private:
  sqlite3 *sqlite_db_;
  std::string filename_;
  float schema_version_;
  bool read_write_;
  bool ready_;
};


//------------------------------------------------------------------------------


/**
 * Base class for all SQL statement classes.  It wraps a single SQL statement
 * and all neccessary calls of the sqlite3 API to deal with this statement.
 */
class Sql {
 public:
  /**
   * Basic constructor to use this class for a specific statement.
   * @param database the database to use the query on
   * @param statement the statement to prepare
   */
  Sql(const sqlite3 *database, const std::string &statement);
  virtual ~Sql();

  bool Execute();
  bool FetchRow();
  bool Reset();
  inline int GetLastError() const { return last_error_code_; }

  bool BindBlob(const int index, const void* value, const int size) {
    last_error_code_ = sqlite3_bind_blob(statement_, index, value, size,
                                         SQLITE_STATIC);
    return Successful();
  }
  bool BindDouble(const int index, const double value) {
    last_error_code_ = sqlite3_bind_double(statement_, index, value);
    return Successful();
  }
  bool BindInt(const int index, const int value) {
    last_error_code_ = sqlite3_bind_int(statement_, index, value);
    return Successful();
  }
  bool BindInt64(const int index, const sqlite3_int64 value) {
    last_error_code_ = sqlite3_bind_int64(statement_, index, value);
    return Successful();
  }
  bool BindNull(const int index) {
    last_error_code_ = sqlite3_bind_null(statement_, index);
    return Successful();
  }
  bool BindText(const int index, const std::string &value) {
    last_error_code_ = sqlite3_bind_text(statement_, index,
                                         value.data(), value.length(),
                                         SQLITE_STATIC);
    return Successful();
  }
  bool BindText(const int index, const char* value, const int size) {
    last_error_code_ = sqlite3_bind_text(statement_, index, value, size,
                                         SQLITE_STATIC);
    return Successful();
  }


  int RetrieveType(const int idx_column) const {
    return sqlite3_column_type(statement_, idx_column);
  }
  int RetrieveBytes(const int idx_column) const {
    return sqlite3_column_bytes(statement_, idx_column);
  }
  const void *RetrieveBlob(const int idx_column) const {
    return sqlite3_column_blob(statement_, idx_column);
  }
  double RetrieveDouble(const int idx_column) const {
    return sqlite3_column_double(statement_, idx_column);
  }
  int RetrieveInt(const int idx_column) const {
    return sqlite3_column_int(statement_, idx_column);
  }
  sqlite3_int64 RetrieveInt64(const int idx_column) const {
    return sqlite3_column_int64(statement_, idx_column);
  }
  const unsigned char *RetrieveText(const int idx_column) const {
    return sqlite3_column_text(statement_, idx_column);
  }

  /**
   * Wrapper for retrieving MD5 hashes.
   * @param idx_high offset of most significant bits in database query
   * @param idx_low offset of least significant bits in database query
   * @result the retrieved MD5 hash
   */
  inline hash::Md5 RetrieveMd5(const int idx_high, const int idx_low) const
  {
    return hash::Md5(RetrieveInt64(idx_high), RetrieveInt64(idx_low));
  }

  /**
   * Wrapper for retrieving a SHA1 hash from a blob field.
   */
  inline hash::Any RetrieveSha1Blob(const int idx_column) const {
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
  inline hash::Any RetrieveSha1Hex(const int idx_column) const {
    const std::string hash_string = std::string(
      reinterpret_cast<const char *>(RetrieveText(idx_column)));
    return hash::Any(hash::kSha1, hash::HexPtr(hash_string));
  }


 protected:
  Sql() { }
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
  bool BindMd5(const int idx_high, const int idx_low, const hash::Md5 &hash) {
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
  inline bool BindSha1Blob(const int idx_column, const struct hash::Any &hash) {
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


//------------------------------------------------------------------------------


/**
 * Common ancestor of SQL statemnts that deal with directory entries.
 */
class SqlDirent : public Sql {
 public:
  // Definition of bit positions for the flags field of a DirectoryEntry
  // All other bit positions are unused
  const static int kFlagDir                 = 1;
  // Link in the parent catalog
  const static int kFlagDirNestedMountpoint = 2;
  // Link in the child catalog
  const static int kFlagDirNestedRoot       = 32;
  const static int kFlagFile                = 4;
  const static int kFlagLink                = 8;
  const static int kFlagFileStat            = 16;  // currently unused
  const static int kFlagFileChunk           = 64;  // currently unused

 protected:
  /**
   *  take the meta data from the DirectoryEntry and transform it
   *  into a valid flags field ready to be safed in the database
   *  @param entry the DirectoryEntry to encode
   *  @return an integer containing the bitmap of the flags field
   */
  unsigned CreateDatabaseFlags(const DirectoryEntry &entry) const;

  /**
   *  replaces place holder variables in a symbolic link by actual
   *  path elements
   *  @param raw_symlink the raw symlink path (may) containing place holders
   *  @return the expanded symlink
   */
  void ExpandSymlink(LinkString *raw_symlink) const;


  // The hardlinks field encodes the number of links in the first 32 bit
  // and the hardlink group id in the second 32 bit.
  // A value of 0 means: 1 link, normal file

  inline uint32_t Hardlinks2Linkcount(const uint64_t hardlinks) const {
    if (hardlinks == 0)
      return 1;
    return (hardlinks << 32) >> 32;
  }

  inline uint32_t Hardlinks2HardlinkGroup(const uint64_t hardlinks) const {
    return hardlinks >> 32;
  }

  inline uint64_t CreateHardlinks(const uint32_t linkcount,
                                  const uint32_t hardlink_group)
  {
    return (static_cast<uint64_t>(hardlink_group) << 32) | linkcount;
  }
};


//------------------------------------------------------------------------------


class SqlDirentWrite : public SqlDirent {
 public:
   /**
    *  To bind an entire DirectoryEntry
    *  @param entry the DirectoryEntry to bind in the SQL statement
    *  @return true on success, false otherwise
    */
  virtual bool BindDirent(const DirectoryEntry &entry) = 0;

 protected:
  bool BindDirentFields(const int hash_idx,
                        const int inode_idx,
                        const int size_idx,
                        const int mode_idx,
                        const int mtime_idx,
                        const int flags_idx,
                        const int name_idx,
                        const int symlink_idx,
                        const DirectoryEntry &entry);
};


//------------------------------------------------------------------------------


class SqlLookup : public SqlDirent {
 protected:
  /**
   * There are several lookup statements which all share a list of
   * elements to load
   * @return a list of sql fields to query for DirectoryEntry
   */
  std::string GetFieldsToSelect() const;

 public:
  /**
   * Retrieves a DirectoryEntry from a freshly performed SqlLookup statement
   * @param catalog the catalog in which the DirectoryEntry resides
   * @return the retrieved DirectoryEntry
   */
  DirectoryEntry GetDirent(const Catalog *catalog) const;

  /**
   * DirectoryEntrys do not contain their path hash.
   * This method retrieves the saved path hash from the database
   * @return the MD5 path hash of a freshly performed lookup
   */
  hash::Md5 GetPathHash() const;

  /**
   * DirectoryEntries do not contain their parent path hash.
   * This method retrieves the saved parent path hash from the database
   * @return the MD5 parent path hash of a freshly performed lookup
   */
  hash::Md5 GetParentPathHash() const;
};


//------------------------------------------------------------------------------


class SqlListing : public SqlLookup {
 public:
  SqlListing(const sqlite3 *database);
  bool BindPathHash(const struct hash::Md5 &hash);
};


//------------------------------------------------------------------------------


class SqlLookupPathHash : public SqlLookup {
 public:
  SqlLookupPathHash(const sqlite3 *database);
  bool BindPathHash(const struct hash::Md5 &hash);
};


//------------------------------------------------------------------------------


class SqlLookupInode : public SqlLookup {
 public:
  SqlLookupInode(const sqlite3 *database);
  bool BindRowId(const uint64_t inode);
};


//------------------------------------------------------------------------------


class SqlNestedCatalogLookup : public Sql {
 public:
  SqlNestedCatalogLookup(const sqlite3 *database);
  bool BindSearchPath(const PathString &path);
  hash::Any GetContentHash() const;
};


//------------------------------------------------------------------------------


class SqlNestedCatalogListing : public Sql {
 public:
  SqlNestedCatalogListing(const sqlite3 *database);
  PathString GetMountpoint() const;
  hash::Any GetContentHash() const;
};


//------------------------------------------------------------------------------


class SqlDirentInsert : public SqlDirentWrite {
 public:
  SqlDirentInsert(const sqlite3 *database);
  bool BindPathHash(const hash::Md5 &hash);
  bool BindParentPathHash(const hash::Md5 &hash);
  bool BindDirent(const DirectoryEntry &entry);
};


//------------------------------------------------------------------------------


class SqlDirentUpdate : public SqlDirentWrite {
 public:
  SqlDirentUpdate(const sqlite3 *database);
  bool BindPathHash(const hash::Md5 &hash);
  bool BindDirent(const DirectoryEntry &entry);
};


//------------------------------------------------------------------------------


class SqlDirentTouch : public Sql {
 public:
  SqlDirentTouch(const sqlite3 *database);
  bool BindPathHash(const hash::Md5 &hash);
  bool BindTimestamp(time_t timestamp);
};


//------------------------------------------------------------------------------


class SqlDirentUnlink : public Sql {
 public:
  SqlDirentUnlink(const sqlite3 *database);
  bool BindPathHash(const hash::Md5 &hash);
};


//------------------------------------------------------------------------------


/**
 * Changes the linkcount for all files in a hardlink group.
 */
class SqlIncLinkcount : public Sql {
 public:
  SqlIncLinkcount(const sqlite3 *database);
  bool BindPathHash(const hash::Md5 &hash);
  bool BindDelta(const int delta);
};


//------------------------------------------------------------------------------


class SqlMaxHardlinkGroup : public Sql {
 public:
  SqlMaxHardlinkGroup(const sqlite3 *database);
  uint32_t GetMaxGroupId() const;
};

}  // namespace catalog

#endif  // CVMFS_CATALOG_SQL_H_
