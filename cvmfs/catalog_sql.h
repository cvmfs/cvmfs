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

#include "compression.h"
#include "directory_entry.h"
#include "file_chunk.h"
#include "hash.h"
#include "shortstring.h"
#include "sql.h"

class XattrList;

namespace catalog {

class Catalog;


class CatalogDatabase : public sqlite::Database<CatalogDatabase> {
 public:
  static const float kLatestSchema;
  static const float kLatestSupportedSchema;  // + 1.X catalogs (r/o)
  // Backwards-compatible schema changes
  static const unsigned kLatestSchemaRevision;

  bool CreateEmptyDatabase();
  bool InsertInitialValues(const std::string     &root_path,
                           const bool             volatile_content,
                           const std::string     &voms_authz,
                           const DirectoryEntry  &root_entry
                                             = DirectoryEntry(kDirentNegative));

  bool CheckSchemaCompatibility();
  bool LiveSchemaUpgradeIfNecessary();
  bool CompactDatabase() const;

  double GetRowIdWasteRatio() const;
  bool SetVOMSAuthz(const std::string&);

 protected:
  // TODO(rmeusel): C++11 - constructor inheritance
  friend class sqlite::Database<CatalogDatabase>;
  CatalogDatabase(const std::string  &filename,
                  const OpenMode      open_mode)
    : sqlite::Database<CatalogDatabase>(filename, open_mode) { }
};


//------------------------------------------------------------------------------


/**
 * Base class for all SQL statement classes.  It wraps a single SQL statement
 * and all neccessary calls of the sqlite3 API to deal with this statement.
 */
class SqlCatalog : public sqlite::Sql {
 public:
  /**
   * Basic constructor to use this class for a specific statement.
   * @param database the database to use the query on
   * @param statement the statement to prepare
   */
  SqlCatalog(const CatalogDatabase &database, const std::string &statement) {
    Init(database.sqlite_db(), statement);
  }

  /**
   * Wrapper for retrieving MD5-ified path names.
   * @param idx_high offset of most significant bits in database query
   * @param idx_low offset of least significant bits in database query
   * @result the retrieved MD5 hash
   */
  inline shash::Md5 RetrieveMd5(const int idx_high, const int idx_low) const {
    return shash::Md5(RetrieveInt64(idx_high), RetrieveInt64(idx_low));
  }

  /**
   * Wrapper for retrieving a cryptographic hash from a blob field.
   */
  inline shash::Any RetrieveHashBlob(
    const int                idx_column,
    const shash::Algorithms  hash_algo,
    const char               hash_suffix = shash::kSuffixNone) const
  {
    // Note: SQLite documentation advises to first define the data type of BLOB
    //       by calling sqlite3_column_XXX() on the column and _afterwards_ get
    //       the number of bytes using sqlite3_column_bytes().
    //
    //  See: https://www.sqlite.org/c3ref/column_blob.html
    const unsigned char *buffer = static_cast<const unsigned char *>(
      RetrieveBlob(idx_column));
    const int byte_count = RetrieveBytes(idx_column);
    return (byte_count > 0) ? shash::Any(hash_algo, buffer, hash_suffix)
                            : shash::Any(hash_algo);
  }

  /**
   * Wrapper for retrieving a cryptographic hash from a text field.
   */
  inline shash::Any RetrieveHashHex(
    const int  idx_column,
    const char hash_suffix = shash::kSuffixNone) const
  {
    const std::string hash_string = std::string(reinterpret_cast<const char *>(
      RetrieveText(idx_column)));
    return shash::MkFromHexPtr(shash::HexPtr(hash_string), hash_suffix);
  }

  /**
   * Wrapper for binding a MD5-ified path name.
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
  SqlCatalog() : sqlite::Sql() {}
};


//------------------------------------------------------------------------------


/**
 * Common ancestor of SQL statemnts that deal with directory entries.
 */
class SqlDirent : public SqlCatalog {
 public:
  // Definition of bit positions for the flags field of a DirectoryEntry
  // All other bit positions are unused
  static const int kFlagDir                 = 1;
  // Link in the parent catalog
  static const int kFlagDirNestedMountpoint = 2;
  // Link in the child catalog
  static const int kFlagDirNestedRoot       = 32;
  static const int kFlagFile                = 4;
  static const int kFlagLink                = 8;
  static const int kFlagFileSpecial         = 16;
  static const int kFlagFileChunk           = 64;
  /**
   * The file is not natively stored in cvmfs but on a different storage system,
   * for instance on HTTPS data federation services.
   * NOTE: used as magic number in SqlListContentHashes::SqlListContentHashes
   */
  static const int kFlagFileExternal        = 128;
  // as of 2^8: 3 bit for hashes
  //   - 0: SHA-1
  //   - 1: RIPEMD-160
  //   - ...
  // Corresponds to shash::algorithms with offset in order to support future
  // hashes
  static const int kFlagPosHash             = 8;
  // Compression methods, 3 bits starting at 2^11
  // Corresponds to zlib::Algorithms
  static const int kFlagPosCompression      = 11;
  /**
   * A transition point to a root catalog (instead of a nested catalog).  Used
   * to link previous snapshots into the catalog structure.
   */
  static const int kFlagDirBindMountpoint   = 0x4000;  // 2^14
  /**
   * An entry that should not appear in listings.  Used for the /.cvmfs
   * directory.
   */
  static const int kFlagHidden              = 0x8000;  // 2^15
  /**
   * For regular files, indicates that the file should be opened with direct I/O
   */
  static const int kFlagDirectIo            = 0x10000;  // 2^16


 protected:
  /**
   * Take the meta data from the DirectoryEntry and transform it
   * into a valid flags field ready to be saved in the database.
   * @param entry the DirectoryEntry to encode
   * @return an integer containing the bitmap of the flags field
   */
  unsigned CreateDatabaseFlags(const DirectoryEntry &entry) const;
  void StoreHashAlgorithm(const shash::Algorithms algo, unsigned *flags) const;
  shash::Algorithms RetrieveHashAlgorithm(const unsigned flags) const;
  zlib::Algorithms RetrieveCompressionAlgorithm(const unsigned flags) const;

  /**
   * The hardlink information (hardlink group ID and linkcount) is saved in one
   * uint_64t field in the CVMFS Catalogs. Therefore we need to do bitshifting
   * in these helper methods.
   */
  uint32_t Hardlinks2Linkcount(const uint64_t hardlinks) const;
  uint32_t Hardlinks2HardlinkGroup(const uint64_t hardlinks) const;
  uint64_t MakeHardlinks(const uint32_t hardlink_group,
                         const uint32_t linkcount) const;

  /**
   * Replaces place holder variables in a symbolic link by actual path elements.
   * @param raw_symlink the raw symlink path (may) containing place holders
   * @return the expanded symlink
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


class SqlListContentHashes : public SqlDirent {
 public:
  explicit SqlListContentHashes(const CatalogDatabase &database);
  shash::Any GetHash() const;
};


//------------------------------------------------------------------------------


class SqlLookup : public SqlDirent {
 public:
  /**
   * Retrieves a DirectoryEntry from a freshly performed SqlLookup statement.
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
  explicit SqlListing(const CatalogDatabase &database);
  bool BindPathHash(const struct shash::Md5 &hash);
};


//------------------------------------------------------------------------------


class SqlLookupPathHash : public SqlLookup {
 public:
  explicit SqlLookupPathHash(const CatalogDatabase &database);
  bool BindPathHash(const struct shash::Md5 &hash);
};


//------------------------------------------------------------------------------


class SqlLookupInode : public SqlLookup {
 public:
  explicit SqlLookupInode(const CatalogDatabase &database);
  bool BindRowId(const uint64_t inode);
};


//------------------------------------------------------------------------------


/**
 * This SQL statement is only used for legacy catalog migrations and has been
 * moved here as it needs to use a locally defined macro inside catalog_sql.cc
 *
 * Queries a single catalog and looks for DirectoryEntrys that have direct
 * children in the same catalog but are marked as 'nested catalog mountpoints'.
 * This is an inconsistent situation, since a mountpoint is supposed to be empty
 * and it's children are stored in the corresponding referenced nested catalog.
 *
 * Note: the user code needs to check if there is a corresponding nested catalog
 *       reference for the found dangling mountpoints. If so, we also have a
 *       bogus state, but it is not reliably fixable automatically. The child-
 *       DirectoryEntrys would be masked by the mounting nested catalog but it
 *       is not clear if we can simply delete them or if this would destroy data.
 */
class SqlLookupDanglingMountpoints : public catalog::SqlLookup {
 public:
  explicit SqlLookupDanglingMountpoints(const CatalogDatabase &database);
};


//------------------------------------------------------------------------------


/**
 * Filesystem like _touch_ of a DirectoryEntry. Only file system specific meta
 * data will be modified.  All CVMFS-specific administrative data stays
 * unchanged.
 * NOTE: This is not a subclass of SqlDirent since it works on
 *       DirectoryEntryBase objects, which are restricted to file system meta
 *       data.
 */
class SqlDirentTouch : public SqlCatalog {
 public:
  explicit SqlDirentTouch(const CatalogDatabase &database);

  bool BindDirentBase(const DirectoryEntryBase &entry);
  bool BindPathHash(const shash::Md5 &hash);
  bool BindXattr(const XattrList &xattrs);
  bool BindXattrEmpty();
};


//------------------------------------------------------------------------------


/**
 * Nested catalogs and bind mountpoints.
 */
class SqlNestedCatalogLookup : public SqlCatalog {
 public:
  explicit SqlNestedCatalogLookup(const CatalogDatabase &database);
  bool BindSearchPath(const PathString &path);
  shash::Any GetContentHash() const;
  uint64_t GetSize() const;
};


//------------------------------------------------------------------------------


/**
 * Nested catalogs and bind mountpoints.
 */
class SqlNestedCatalogListing : public SqlCatalog {
 public:
  explicit SqlNestedCatalogListing(const CatalogDatabase &database);
  PathString GetPath() const;
  shash::Any GetContentHash() const;
  uint64_t GetSize() const;
};


//------------------------------------------------------------------------------


/**
 * Only nested catalogs, no bind mountpoints. Used for replication and GC.
 */
class SqlOwnNestedCatalogListing : public SqlCatalog {
 public:
  explicit SqlOwnNestedCatalogListing(const CatalogDatabase &database);
  PathString GetPath() const;
  shash::Any GetContentHash() const;
  uint64_t GetSize() const;
};


//------------------------------------------------------------------------------


class SqlDirentInsert : public SqlDirentWrite {
 public:
  explicit SqlDirentInsert(const CatalogDatabase &database);
  bool BindPathHash(const shash::Md5 &hash);
  bool BindParentPathHash(const shash::Md5 &hash);
  bool BindDirent(const DirectoryEntry &entry);
  bool BindXattr(const XattrList &xattrs);
  bool BindXattrEmpty();
};


//------------------------------------------------------------------------------


class SqlDirentUpdate : public SqlDirentWrite {
 public:
  explicit SqlDirentUpdate(const CatalogDatabase &database);
  bool BindPathHash(const shash::Md5 &hash);
  bool BindDirent(const DirectoryEntry &entry);
};


//------------------------------------------------------------------------------


class SqlDirentUnlink : public SqlCatalog {
 public:
  explicit SqlDirentUnlink(const CatalogDatabase &database);
  bool BindPathHash(const shash::Md5 &hash);
};


//------------------------------------------------------------------------------


/**
 * Changes the linkcount for all files in a hardlink group.
 */
class SqlIncLinkcount : public SqlCatalog {
 public:
  explicit SqlIncLinkcount(const CatalogDatabase &database);
  bool BindPathHash(const shash::Md5 &hash);
  bool BindDelta(const int delta);
};


//------------------------------------------------------------------------------


class SqlChunkInsert : public SqlCatalog {
 public:
  explicit SqlChunkInsert(const CatalogDatabase &database);
  bool BindPathHash(const shash::Md5 &hash);
  bool BindFileChunk(const FileChunk &chunk);
};


//------------------------------------------------------------------------------


class SqlChunksRemove : public SqlCatalog {
 public:
  explicit SqlChunksRemove(const CatalogDatabase &database);
  bool BindPathHash(const shash::Md5 &hash);
};


//------------------------------------------------------------------------------


class SqlChunksListing : public SqlCatalog {
 public:
  explicit SqlChunksListing(const CatalogDatabase &database);
  bool BindPathHash(const shash::Md5 &hash);
  FileChunk GetFileChunk(const shash::Algorithms interpret_hash_as) const;
};


//------------------------------------------------------------------------------


class SqlChunksCount : public SqlCatalog {
 public:
  explicit SqlChunksCount(const CatalogDatabase &database);
  bool BindPathHash(const shash::Md5 &hash);
  int GetChunkCount() const;
};


//------------------------------------------------------------------------------


class SqlMaxHardlinkGroup : public SqlCatalog {
 public:
  explicit SqlMaxHardlinkGroup(const CatalogDatabase &database);
  uint32_t GetMaxGroupId() const;
};


//------------------------------------------------------------------------------


class SqlGetCounter : public SqlCatalog {
 public:
  explicit SqlGetCounter(const CatalogDatabase &database);
  bool BindCounter(const std::string &counter);
  uint64_t GetCounter() const;
 private:
  bool compat_;
};


//------------------------------------------------------------------------------


class SqlUpdateCounter : public SqlCatalog {
 public:
  explicit SqlUpdateCounter(const CatalogDatabase &database);
  bool BindCounter(const std::string &counter);
  bool BindDelta(const int64_t delta);
};


//------------------------------------------------------------------------------


class SqlCreateCounter : public SqlCatalog {
 public:
  explicit SqlCreateCounter(const CatalogDatabase &database);
  bool BindCounter(const std::string &counter);
  bool BindInitialValue(const int64_t value);
};


//------------------------------------------------------------------------------


class SqlAllChunks : public SqlCatalog {
 public:
  explicit SqlAllChunks(const CatalogDatabase &database);
  bool Open();
  bool Next(shash::Any *hash, zlib::Algorithms *compression_alg);
  bool Close();
};


//------------------------------------------------------------------------------


class SqlLookupXattrs : public SqlCatalog {
 public:
  explicit SqlLookupXattrs(const CatalogDatabase &database);
  bool BindPathHash(const shash::Md5 &hash);
  XattrList GetXattrs();
};

}  // namespace catalog

#endif  // CVMFS_CATALOG_SQL_H_
