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

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <string>
#include <sstream>

#include "hash.h"
#include "directory_entry.h"
#include "file_chunk.h"
#include "shortstring.h"
#include "sql.h"

namespace catalog {

class Catalog;

/**
 * Content-addressable chunks can be entire files, micro catalogs (ending L) or
 * pieces of large files (ending P)
 */
enum ChunkTypes {
  kChunkFile = 0,
  kChunkMicroCatalog,
  kChunkPiece,
};


/**
 * Encapsulates an SQlite connection.  Abstracts the schema.
 */
class Database {
 public:
  static const float kLatestSchema;
  static const float kLatestSupportedSchema;  // + 1.X catalogs (r/o)
  static const float kSchemaEpsilon;  // floats get imprecise in SQlite
  // backwards-compatible schema changes
  static const unsigned kLatestSchemaRevision;

  static bool IsEqualSchema(const float value, const float compare) {
    return (value > compare - kSchemaEpsilon &&
            value < compare + kSchemaEpsilon);
  }

  Database(const std::string filename, const sqlite::DbOpenMode open_mode);
  ~Database();
  static bool Create(const std::string &filename,
                     const std::string &root_path,
                     const bool volatile_content,
                     const DirectoryEntry &root_entry
                                             = DirectoryEntry(kDirentNegative));

  sqlite3 *sqlite_db() const { return sqlite_db_; }
  std::string filename() const { return filename_; }
  float schema_version() const { return schema_version_; }
  unsigned schema_revision() const { return schema_revision_; }
  bool ready() const { return ready_; }

  /**
   * Returns the english language error description of the last error
   * happened in the context of the encapsulated sqlite3 database object.
   * Note: In a multithreaded context it might be unpredictable which
   *       the actual last error is.
   * @return   english language error description of last error
   */
  std::string GetLastErrorMsg() const;
 private:
  Database(const std::string &filename,
           const float schema, const unsigned revision);

  sqlite3 *sqlite_db_;
  std::string filename_;
  float schema_version_;
  unsigned schema_revision_;
  bool read_write_;
  bool ready_;
};


//------------------------------------------------------------------------------


/**
 * Base class for all SQL statement classes.  It wraps a single SQL statement
 * and all neccessary calls of the sqlite3 API to deal with this statement.
 */
class Sql : public sqlite::Sql {
 public:
  /**
   * Basic constructor to use this class for a specific statement.
   * @param database the database to use the query on
   * @param statement the statement to prepare
   */
  Sql(const Database &database, const std::string &statement) {
    Init(database.sqlite_db(), statement);
  }
  virtual ~Sql() { /* Done by super class */ }

  /**
   * Wrapper for retrieving MD5 hashes.
   * @param idx_high offset of most significant bits in database query
   * @param idx_low offset of least significant bits in database query
   * @result the retrieved MD5 hash
   */
  inline shash::Md5 RetrieveMd5(const int idx_high, const int idx_low) const
  {
    return shash::Md5(RetrieveInt64(idx_high), RetrieveInt64(idx_low));
  }

  /**
   * Wrapper for retrieving a cryptographic hash from a blob field.
   */
  inline shash::Any RetrieveHashBlob(const int idx_column,
                                     const shash::Algorithms hash_algo) const
  {
    if (RetrieveBytes(idx_column) > 0) {
      return shash::Any(hash_algo,
        static_cast<const unsigned char *>(RetrieveBlob(idx_column)),
        RetrieveBytes(idx_column));
    }
    return shash::Any(hash_algo);
  }

  /**
   * Wrapper for retrieving a cryptographic hash from a text field.
   */
  inline shash::Any RetrieveHashHex(const int idx_column) const {
    const std::string hash_string = std::string(
      reinterpret_cast<const char *>(RetrieveText(idx_column)));
    return shash::MkFromHexPtr(shash::HexPtr(hash_string));
  }

  /**
   * Wrapper for binding a MD5 hash.
   * @param idx_high offset of most significant bits in database query
   * @param idx_low offset of least significant bits in database query
   * @param hash the hash to bind in the query
   * @result true on success, false otherwise
   */
  inline bool BindMd5(const int idx_high, const int idx_low,
                      const shash::Md5 &hash)
  {
    uint64_t high, low;
    hash.ToIntPair(&high, &low);
    const bool retval = BindInt64(idx_high, high) && BindInt64(idx_low, low);
    return retval;
  }

  /**
   * Wrapper for binding a cryptographic hash.  Algorithm of hash has to be
   * elsewhere (in flags).
   * @param idx_column offset of the blob field in database query
   * @param hash the hash to bind in the query
   * @result true on success, false otherwise
   */
  inline bool BindHashBlob(const int idx_column, const shash::Any &hash) {
    if (hash.IsNull()) {
      return BindNull(idx_column);
    } else {
      return BindBlob(idx_column, hash.digest, hash.GetDigestSize());
    }
  }

 protected:
  Sql() : sqlite::Sql() { }
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
  const static int kFlagFileChunk           = 64;

  // as of 2^8: 3 bit for hashes
  // 0: SHA-1
  // 1: RIPEMD-160
  // ... corresponds to shash::algorithms with offset in order to support
  // future hashes
  const static int kFlagPosHash             = 8;

 protected:
  /**
   *  take the meta data from the DirectoryEntry and transform it
   *  into a valid flags field ready to be saved in the database
   *  @param entry the DirectoryEntry to encode
   *  @return an integer containing the bitmap of the flags field
   */
  unsigned CreateDatabaseFlags(const DirectoryEntry &entry) const;
  void StoreHashAlgorithm(const shash::Algorithms algo, unsigned *flags) const;
  shash::Algorithms RetrieveHashAlgorithm(const unsigned flags) const;

  /**
   * The hardlink information (hardlink group ID and linkcount) is saved in one
   * uint_64t field in the CVMFS Catalogs. Therefore we need to do some minor
   * bitshifting in these helper methods.
   */
  uint32_t Hardlinks2Linkcount(const uint64_t hardlinks) const;
  uint32_t Hardlinks2HardlinkGroup(const uint64_t hardlinks) const;
  uint64_t MakeHardlinks(const uint32_t hardlink_group,
                         const uint32_t linkcount) const;

  /**
   *  replaces place holder variables in a symbolic link by actual
   *  path elements
   *  @param raw_symlink the raw symlink path (may) containing place holders
   *  @return the expanded symlink
   */
  void ExpandSymlink(LinkString *raw_symlink) const;
};


//------------------------------------------------------------------------------


class SqlDirentWrite : public SqlDirent {
 public:
   /**
    * To bind an entire DirectoryEntry
    * @param entry the DirectoryEntry to bind in the SQL statement
    * @return true on success, false otherwise
    */
  virtual bool BindDirent(const DirectoryEntry &entry) = 0;

 protected:
  bool BindDirentFields(const int hash_idx,
                        const int hardlinks_idx,
                        const int size_idx,
                        const int mode_idx,
                        const int mtime_idx,
                        const int flags_idx,
                        const int name_idx,
                        const int symlink_idx,
                        const int uid_idx,
                        const int gid_idx,
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
  std::string GetFieldsToSelect(const float schema_version) const;

 public:
  /**
   * Retrieves a DirectoryEntry from a freshly performed SqlLookup statement
   * @param catalog the catalog in which the DirectoryEntry resides
   * @return the retrieved DirectoryEntry
   */
  DirectoryEntry GetDirent(const Catalog *catalog,
                           const bool expand_symlink = true) const;

  /**
   * DirectoryEntrys do not contain their path hash.
   * This method retrieves the saved path hash from the database
   * @return the MD5 path hash of a freshly performed lookup
   */
  shash::Md5 GetPathHash() const;

  /**
   * DirectoryEntries do not contain their parent path hash.
   * This method retrieves the saved parent path hash from the database
   * @return the MD5 parent path hash of a freshly performed lookup
   */
  shash::Md5 GetParentPathHash() const;
};


//------------------------------------------------------------------------------


class SqlListing : public SqlLookup {
 public:
  SqlListing(const Database &database);
  bool BindPathHash(const struct shash::Md5 &hash);
};


//------------------------------------------------------------------------------


class SqlLookupPathHash : public SqlLookup {
 public:
  SqlLookupPathHash(const Database &database);
  bool BindPathHash(const struct shash::Md5 &hash);
};


//------------------------------------------------------------------------------


class SqlLookupInode : public SqlLookup {
 public:
  SqlLookupInode(const Database &database);
  bool BindRowId(const uint64_t inode);
};


//------------------------------------------------------------------------------


/**
 * Filesystem like _touch_ of a DirectoryEntry. Only file system specific meta
 * data will be modified. All CVMFS-specific administrative data stays unchanged.
 * NOTE: This is not a subclass of SqlDirent since it works on DirectoryEntryBase
 *       objects, which are restricted to file system meta data.
 */
class SqlDirentTouch : public Sql {
 public:
  SqlDirentTouch(const Database &database);

  bool BindDirentBase(const DirectoryEntryBase &entry);
  bool BindPathHash(const shash::Md5 &hash);
};


//------------------------------------------------------------------------------


class SqlNestedCatalogLookup : public Sql {
 public:
  SqlNestedCatalogLookup(const Database &database);
  bool BindSearchPath(const PathString &path);
  shash::Any GetContentHash() const;
  uint64_t GetSize() const;
};


//------------------------------------------------------------------------------


class SqlNestedCatalogListing : public Sql {
 public:
  SqlNestedCatalogListing(const Database &database);
  PathString GetMountpoint() const;
  shash::Any GetContentHash() const;
  uint64_t GetSize() const;
};


//------------------------------------------------------------------------------


class SqlDirentInsert : public SqlDirentWrite {
 public:
  SqlDirentInsert(const Database &database);
  bool BindPathHash(const shash::Md5 &hash);
  bool BindParentPathHash(const shash::Md5 &hash);
  bool BindDirent(const DirectoryEntry &entry);
};


//------------------------------------------------------------------------------


class SqlDirentUpdate : public SqlDirentWrite {
 public:
  SqlDirentUpdate(const Database &database);
  bool BindPathHash(const shash::Md5 &hash);
  bool BindDirent(const DirectoryEntry &entry);
};


//------------------------------------------------------------------------------


class SqlDirentUnlink : public Sql {
 public:
  SqlDirentUnlink(const Database &database);
  bool BindPathHash(const shash::Md5 &hash);
};


//------------------------------------------------------------------------------


/**
 * Changes the linkcount for all files in a hardlink group.
 */
class SqlIncLinkcount : public Sql {
 public:
  SqlIncLinkcount(const Database &database);
  bool BindPathHash(const shash::Md5 &hash);
  bool BindDelta(const int delta);
};


//------------------------------------------------------------------------------


class SqlChunkInsert : public Sql {
 public:
  SqlChunkInsert(const Database &database);
  bool BindPathHash(const shash::Md5 &hash);
  bool BindFileChunk(const FileChunk &chunk);
};


//------------------------------------------------------------------------------


class SqlChunksRemove : public Sql {
 public:
  SqlChunksRemove(const Database &database);
  bool BindPathHash(const shash::Md5 &hash);
};


//------------------------------------------------------------------------------


class SqlChunksListing : public Sql {
 public:
  SqlChunksListing(const Database &database);
  bool BindPathHash(const shash::Md5 &hash);
  FileChunk GetFileChunk(const shash::Algorithms interpret_hash_as) const;
};


//------------------------------------------------------------------------------


class SqlChunksCount : public Sql {
 public:
  SqlChunksCount(const Database &database);
  bool BindPathHash(const shash::Md5 &hash);
  int GetChunkCount() const;
};


//------------------------------------------------------------------------------


class SqlMaxHardlinkGroup : public Sql {
 public:
  SqlMaxHardlinkGroup(const Database &database);
  uint32_t GetMaxGroupId() const;
};


//------------------------------------------------------------------------------


class SqlGetCounter : public Sql {
 public:
  SqlGetCounter(const Database &database);
  bool BindCounter(const std::string &counter);
  uint64_t GetCounter() const;
 private:
  bool compat_;
};


//------------------------------------------------------------------------------


class SqlUpdateCounter : public Sql {
 public:
  SqlUpdateCounter(const Database &database);
  bool BindCounter(const std::string &counter);
  bool BindDelta(const int64_t delta);
};


//------------------------------------------------------------------------------


class SqlCreateCounter : public Sql {
 public:
  SqlCreateCounter(const Database &database);
  bool BindCounter(const std::string &counter);
  bool BindInitialValue(const int64_t value);
};


//------------------------------------------------------------------------------


class SqlAllChunks : public Sql {
 public:
  SqlAllChunks(const Database &database);
  bool Open();
  bool Next(shash::Any *hash, ChunkTypes *type);
  bool Close();
};

}  // namespace catalog

#endif  // CVMFS_CATALOG_SQL_H_
