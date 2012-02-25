/**
 * \file catalog.cc
 * \namespace catalog
 *
 * This contains wrapper functions for the SQLite-catalogs.
 * File system operations are translated into SQL queries.
 *
 * Most of the functions are locked by pthread mutexes.
 * We don't rely on SQLite's multithreading capabilities here
 * because we use static prepared statements.
 * "Unprotected" versions are meant to be enclosed in
 * a lock() unlock() pair.
 *
 * Developed by Jakob Blomer 2009 at CERN
 * jakob.blomer@cern.ch
 */

#define _FILE_OFFSET_BITS 64
#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "catalog.h"

#ifdef CVMFS_CLIENT
#include "cvmfs.h"
#endif

#include "platform.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <ctime>

#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <pthread.h>
#include <inttypes.h>

#include "hash.h"
#include "util.h"
#include "catalog_tree.h"
#include "sqlite3-duplex.h"
#include "logging.h"

using namespace std;

namespace catalog {

  const uint64_t DEFAULT_TTL = 3600; ///< Default TTL for a catalog is one hour.
  const uint64_t GROW_EPOCH = 1199163600;
  const int SQLITE_THREAD_MEM = 4; ///< SQLite3 heap limit per thread

  pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
  string root_prefix; ///< If we mount deep into a nested catalog, we need the full preceeding path to calculate the correct MD5 hash
  uid_t uid; ///< Required for converting a catalog entry into struct stat
  gid_t gid; ///< Required for converting a catalog entry into struct stat
  int num_catalogs = 0; ///< We support nested catalogs.  There are loaded by SQLite's ATTACH.  In the end, we operate on a set of multiple catalogs
  int current_catalog = 0; ///< We query the catalogs one after another until success.  We use a lazy approach, meaning after success we ask that catalog next time again.
	map<unsigned int, map<uint64_t, uint64_t> > hardlinkInodeMap; /// < mapping from catalog_id, hardlinkGroupId to inodeNumber (must be shifted by offset afterwards)

  vector<sqlite3 *> db;
  vector<sqlite3_stmt *> stmts_insert;
  vector<sqlite3_stmt *> stmts_update;
  vector<sqlite3_stmt *> stmts_update_inode;
  vector<sqlite3_stmt *> stmts_unlink;
  vector<sqlite3_stmt *> stmts_ls;
  vector<sqlite3_stmt *> stmts_lookup;
  vector<sqlite3_stmt *> stmts_lookup_inode;
  vector<sqlite3_stmt *> stmts_parent;
  vector<sqlite3_stmt *> stmts_lookup_nested;
  vector<bool> in_transaction;
  vector<bool> opened_read_only;
  vector<string> catalog_urls;
  vector<string> catalog_files;

  typedef struct {
    uint64_t offset;
    uint64_t size;
  } inode_chunk_t;

  uint64_t current_inode_offset = INITIAL_INODE_OFFSET;
	vector<inode_chunk_t> inode_chunks; ///< inodes are assigned at runtime... every catalog is assigned an inode chunk at attach time

	inode_chunk_t get_next_inode_chunk_of_size(uint64_t size) {
    inode_chunk_t new_chunk;
    new_chunk.offset = current_inode_offset;
    new_chunk.size = size;
    current_inode_offset += size;
    return new_chunk;
	}

	inode_chunk_t realign_inode_chunks_for_catalog(int cat_id, uint64_t size) {
    // create a new inode chunk out of the old ones
    inode_chunk_t old_chunk = inode_chunks[cat_id];
    inode_chunk_t new_chunk;
    new_chunk.offset = old_chunk.offset;
    new_chunk.size = size;

    // align the successing chunks according to the new one
    int64_t delta_offset = new_chunk.size - old_chunk.size;
    for (unsigned int i = cat_id + 1; i < inode_chunks.size(); ++i) {
      inode_chunks[i].offset += delta_offset;
    }

    // return the new chunk
    return new_chunk;
	}

  /* __thread This is actually necessary... but not allowed until C++0x */string sqlError;

	// thread local storage for both Mac OS X and Linux wrapped in a small simple abstraction API
#ifdef __APPLE__
  pthread_key_t pkey_sqlitemem;
#else
  __thread bool sqlite_mem_enforced = false;
#endif

	inline bool initTLS() {
#ifdef __APPLE__
    // creating a pthread-key and setting its value to TRUE
    if (pthread_key_create(&pkey_sqlitemem, NULL) != 0) return false;
    bool *value = (bool *)malloc(sizeof(bool));
    *value = false;
    return (pthread_setspecific(pkey_sqlitemem, (void *)value) == 0);
#else
    // nothing to do here
    return true;
#endif
	}

	inline void teardownTLS() {
#ifdef __APPLE__
    // retrieve the value from pthread-key, free it and delete the key
    void *value = pthread_getspecific(pkey_sqlitemem);
    pthread_setspecific(pkey_sqlitemem, NULL);
    free(value);
    pthread_key_delete(pkey_sqlitemem);
#else
    // nothing to do here
    return;
#endif
	}

	inline bool getMemoryEnforcedFlag() {
#ifdef __APPLE__
    // retrieve the value from the pthread-key and return it
    bool *value = (bool *)pthread_getspecific(pkey_sqlitemem);
    if (value == NULL) return false;
    return *value;
#else
    // simply return the thread specific variable
    return sqlite_mem_enforced;
#endif
	}

	inline void setMemoryEnforcedFlag(bool flag) {
#ifdef __APPLE__
    // retrieve the value from the pthread-key and reset it
    bool *value = (bool *)pthread_getspecific(pkey_sqlitemem);
    if (value != NULL) *value = flag;
#else
    // simply reset the thread specific variable
    sqlite_mem_enforced = flag;
#endif
	}


  /**
   * Since the functions in here can be called from multiple
   * threads, we have to enforce void sqlite3_soft_heap_limit(int)
   * the hard way (via TLS).
   */
  static void enforce_mem_limit() {
    if (!getMemoryEnforcedFlag()) {
      sqlite3_soft_heap_limit(SQLITE_THREAD_MEM*1024*1024);
      setMemoryEnforcedFlag(true);
    }
  }

  /**
   * Executes an SQL statement (that doesn't return a record set)
   *
   * @param[in] db SQLite database connection (opened)
   * @param[in] sql sql statement
   * \returns True on success, false otherwise.  Sets sqlError on failure.
   */
  static bool sql_exec(sqlite3 * const db, const string &sql) {
    enforce_mem_limit();

    char *errMsg = NULL;
    int err = sqlite3_exec(db, sql.c_str(), NULL, NULL, &errMsg);
    if (err != SQLITE_OK) {
      ostringstream str_err;
      str_err << errMsg << " (" << err << ")";
      sqlError = str_err.str();
      sqlite3_free(errMsg);
      return false;
    }
    if (errMsg) free(errMsg);
    return true;
  }


  /**
   * Creates catalog database schema and indexes if not exist.
   *
   * \return True on success, false otherwise
   */
  static bool create_db(const unsigned cat_id, const bool read_only) {
    const string sql_catalog = "CREATE TABLE IF NOT EXISTS catalog "
    "(md5path_1 INTEGER, md5path_2 INTEGER, parent_1 INTEGER, parent_2 INTEGER, inode INTEGER, "
    "hash BLOB, size INTEGER, mode INTEGER, mtime INTEGER, flags INTEGER, name TEXT, symlink TEXT, "
    "CONSTRAINT pk_catalog PRIMARY KEY (md5path_1, md5path_2));";
    if (!sql_exec(db[cat_id], sql_catalog))
      return false;

    const string sql_index = "CREATE INDEX IF NOT EXISTS idx_catalog_parent "
    "ON catalog (parent_1, parent_2);";
    if (!sql_exec(db[cat_id], sql_index))
      return false;

    const string sql_index_ino = "CREATE INDEX IF NOT EXISTS idx_catalog_inode "
    "ON catalog (inode);";
    if (!sql_exec(db[cat_id], sql_index_ino))
      return false;

    const string sql_props = "CREATE TABLE IF NOT EXISTS properties "
    "(key TEXT, value TEXT, CONSTRAINT pk_properties PRIMARY KEY (key));";
    if (!sql_exec(db[cat_id], sql_props))
      return false;

    const string sql_nested = "CREATE TABLE IF NOT EXISTS nested_catalogs "
    "(path TEXT, sha1 TEXT, CONSTRAINT pk_nested_catalogs PRIMARY KEY (path));";
    if (!sql_exec(db[cat_id], sql_nested))
      return false;

    if (!read_only) {
      const string sql_revision = "INSERT OR IGNORE INTO properties (key, value) VALUES ('revision', 0);";
      if (!sql_exec(db[cat_id], sql_revision))
        return false;
      const string sql_schema = "INSERT OR REPLACE INTO properties (key, value) VALUES ('schema', '1.2');";
      if (!sql_exec(db[cat_id], sql_schema))
        return false;
		} else {
      /* Check Schema */
      bool result = true;

      const string sql = "SELECT value FROM properties WHERE key='schema';";
      sqlite3_stmt *stmt;
      sqlite3_prepare_v2(db[cat_id], sql.c_str(), -1, &stmt, NULL);
      int err = sqlite3_step(stmt);
      if (err == SQLITE_ROW) {
        const string schema = string((char *)sqlite3_column_text(stmt, 0));
        if (schema.find("1.", 0) != 0)
          result = false;
      } else {
        //logmsg("No schema version in catalog");
        result = false;
      }
      sqlite3_finalize(stmt);

      return result;
    }

    return true;
  }


  /**
   * Sets the preceeding path for a specific catalog.
   * For the root catalog this is "", for a nested
   * catalog at /foo/bar/ this is "/foo/bar"
   *
   * \return True on success, false otherwise
   */
  bool set_root_prefix(const string &r_prefix, const unsigned cat_id) {
    const string sql = "INSERT OR REPLACE INTO properties "
    "(key, value) VALUES ('root_prefix', '" + r_prefix + "');";
    if (!sql_exec(db[cat_id], sql)) {
      return false;
    }

    if (cat_id == 0) root_prefix = r_prefix;
    return true;
  }


  /**
   * Gets the root prefix of the current root catalog.
   * This is the only one where it makes sense to ask for.
   */
  string get_root_prefix() {
    return root_prefix;
  }

  /**
   * Gets the root prefix of a specific catalog for information.
   */
  string get_root_prefix_specific(const unsigned cat_id) {
    enforce_mem_limit();

    string result;

    const string sql = "SELECT value FROM properties WHERE key='root_prefix';";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db[cat_id], sql.c_str(), -1, &stmt, NULL);
    int err = sqlite3_step(stmt);
    if (err == SQLITE_ROW)
      result = string((char *)sqlite3_column_text(stmt, 0));
    else
      result = "";
    sqlite3_finalize(stmt);

    return result;
  }


  /**
   * Gets the root catalog revision.
   */
  uint64_t get_revision() {
    enforce_mem_limit();

    uint64_t result;

    const string sql = "SELECT value FROM properties WHERE key='revision';";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db[0], sql.c_str(), -1, &stmt, NULL);
    int err = sqlite3_step(stmt);
    if (err == SQLITE_ROW)
      result = sqlite3_column_int64(stmt, 0);
    else
      result = 0;
    sqlite3_finalize(stmt);

    return result;
  }


  /**
   * Increases revision by one.
   */
  bool inc_revision(const int cat_id) {
    enforce_mem_limit();

    const string sql = "UPDATE properties SET value=value+1 WHERE key='revision';";
    return sql_exec(db[cat_id], sql);
  }


  /**
   * Gets the TTL in seconds of a catalog.
   */
  uint64_t get_ttl(const unsigned cat_id) {
    enforce_mem_limit();

    uint64_t result;

    const string sql = "SELECT value FROM properties WHERE key='TTL';";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db[cat_id], sql.c_str(), -1, &stmt, NULL);
    int err = sqlite3_step(stmt);
    if (err == SQLITE_ROW)
      result = sqlite3_column_int64(stmt, 0);
    else
      result = DEFAULT_TTL;
    sqlite3_finalize(stmt);

    return result;
  }


  /**
   * Sets the TTL in seconds of a catalog.
   *
   * \return True on success, false otherwise
   */
  bool set_ttl(const unsigned cat_id, const uint64_t ttl) {
    ostringstream sql;

    sql << "INSERT OR REPLACE INTO properties "
    "(key, value) VALUES ('TTL', " << ttl << ");";
    return sql_exec(db[cat_id], sql.str());
  }

  /**
   * Sets the current time in UTC.
   *
   * \return True on success, false otherwise
   */
  bool update_lastmodified(const unsigned cat_id) {
    time_t now = time(NULL);
    ostringstream sql;
    sql << "INSERT OR REPLACE INTO properties "
    "(key, value) VALUES ('last_modified', '" << now << "');";
    return sql_exec(db[cat_id], sql.str());;
  }


  /**
   * Gets the last modified timestamp (in UTC).
   *
   * \return True on success, false otherwise
   */
  time_t get_lastmodified(const unsigned cat_id) {
    enforce_mem_limit();

    uint64_t result;

    const string sql = "SELECT value FROM properties WHERE key='last_modified';";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db[cat_id], sql.c_str(), -1, &stmt, NULL);
    int err = sqlite3_step(stmt);
    if (err == SQLITE_ROW)
      result = sqlite3_column_int64(stmt, 0);
    else
      result = 0;
    sqlite3_finalize(stmt);

    return (time_t)result;
  }

  uint64_t get_inode_offset_for_catalog_id(int catalog_id) {
    if (catalog_id >=0 && catalog_id < (int)inode_chunks.size()) {
      return inode_chunks[catalog_id].offset;
    } else {
      return 0;
    }
  }

  /**
   * Sets the SHA1 of the previous snapshotted catalog in the properties table
   */
  bool set_previous_revision(const unsigned cat_id, const hash::t_sha1 &sha1) {
    ostringstream sql;
    sql << "INSERT OR REPLACE INTO properties "
    "(key, value) VALUES ('previous_revision', '" << sha1.to_string() << "');";
    return sql_exec(db[cat_id], sql.str());;
  }


  hash::t_sha1 get_previous_revision(const unsigned cat_id) {
    enforce_mem_limit();

    hash::t_sha1 result;

    const string sql = "SELECT value FROM properties WHERE key='previous_revision';";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db[cat_id], sql.c_str(), -1, &stmt, NULL);
    int err = sqlite3_step(stmt);
    if (err == SQLITE_ROW)
      result.from_hash_str((char *)sqlite3_column_text(stmt, 0));
    sqlite3_finalize(stmt);

    return result;
  }

  uint64_t get_max_rowid(const unsigned cat_id) {
    enforce_mem_limit();

    uint64_t result = 0;

    const string sql = "SELECT MAX(rowid) FROM catalog;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db[cat_id], sql.c_str(), -1, &stmt, NULL);
    int err = sqlite3_step(stmt);
    if (err == SQLITE_ROW)
      result = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);

    return result;
  }

  uint64_t get_num_dirent(const unsigned cat_id) {
    enforce_mem_limit();

    uint64_t result = 0;

    const string sql = "SELECT count(*) FROM catalog;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db[cat_id], sql.c_str(), -1, &stmt, NULL);
    int err = sqlite3_step(stmt);
    if (err == SQLITE_ROW)
      result = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);

    return result;
  }


  /**
   * Builds prepared statements for a catalog
   *
   * \return True on success, false otherwise
   */
  static bool build_prepared_stmts(const unsigned cat_id) {
    int err;

    /* SELECT LS */
    sqlite3_stmt *ls;
    ostringstream sql_ls;
    sql_ls << "SELECT " << cat_id << ", hash, inode, size, mode, mtime, flags, name, symlink, rowid "
    "FROM catalog WHERE (parent_1 = :p_1) AND (parent_2 = :p_2);";

    LogCvmfs(kLogCatalog, kLogDebug, "Prepared statement catalog %u: %s",
             cat_id, sql_ls.str().c_str());
    err = sqlite3_prepare_v2(db[cat_id], sql_ls.str().c_str(), -1, &ls, NULL);
    if (err != SQLITE_OK) {
      sqlError = "unable to prepare ls statement";
      return false;
    }

    /* SELECT LOOKUP */
    sqlite3_stmt *lookup;
    ostringstream sql_lookup;
    sql_lookup << "SELECT " << cat_id << ", hash, inode, size, mode, mtime, flags, name, symlink, rowid "
    "FROM catalog "
    "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);";
    LogCvmfs(kLogCatalog, kLogDebug, "Prepared statement catalog %u: %s",
             cat_id, sql_lookup.str().c_str());
    err = sqlite3_prepare_v2(db[cat_id], sql_lookup.str().c_str(), -1, &lookup, NULL);
    if (err != SQLITE_OK) {
      sqlError = "unable to prepare lookup statement";
      return false;
    }

    /* SELECT LOOKUP INODE */
    sqlite3_stmt *lookup_inode;
    ostringstream sql_lookup_inode;
    sql_lookup_inode << "SELECT " << cat_id << ", hash, inode, size, mode, mtime, flags, name, symlink, md5path_1, md5path_2, parent_1, parent_2 "
    "FROM catalog "
    "WHERE rowid = :rowid";
    LogCvmfs(kLogCatalog, kLogDebug, "Prepared statement catalog %u: %s",
             cat_id, sql_lookup_inode.str().c_str());
    err = sqlite3_prepare_v2(db[cat_id], sql_lookup_inode.str().c_str(), -1, &lookup_inode, NULL);
    if (err != SQLITE_OK) {
      sqlError = "unable to prepare lookup statement";
      return false;
    }

    /* SELECT PARENT */
    sqlite3_stmt *parent;
    ostringstream sql_parent;
    sql_parent << "SELECT " << cat_id << ", a.hash, a.inode, a.size, a.mode, a.mtime, a.flags, a.name, a.symlink, a.rowid "
    "FROM catalog a, catalog b "
    "WHERE (b.md5path_1 = :md5_1) AND (b.md5path_2 = :md5_2) AND (a.md5path_1 = b.parent_1) AND (a.md5path_2 = b.parent_2);";
    LogCvmfs(kLogCatalog, kLogDebug, "Prepared statement catalog %u: %s",
          cat_id, sql_parent.str().c_str());
    err = sqlite3_prepare_v2(db[cat_id], sql_parent.str().c_str(), -1, &parent, NULL);
    if (err != SQLITE_OK) {
      sqlError = "unable to prepare parent statement";
      return false;
    }

    /* SELECT LOOKUP NESTED CATAOG */
    sqlite3_stmt *lookup_nested;
    const string sql_lookup_nested = "SELECT sha1 FROM nested_catalogs WHERE path=:path;";
    LogCvmfs(kLogCatalog, kLogDebug, "Prepared statement catalog %u: %s",
             cat_id, sql_lookup_nested.c_str());
    err = sqlite3_prepare_v2(db[cat_id], sql_lookup_nested.c_str(), -1, &lookup_nested, NULL);
    if (err != SQLITE_OK) {
      sqlError = "unable to prepare nested catalog statement";
      return false;
    }

    /* INSERT */
    sqlite3_stmt *insert;
    const string sql_insert = "INSERT INTO catalog "
    "(md5path_1, md5path_2, parent_1, parent_2, hash, inode, size, mode, mtime, flags, name, symlink) "
    "VALUES (:md5_1, :md5_2, :p_1, :p_2, :hash, :ino, :size, :mode, :mtime, :flags, :name, :symlink);";
    LogCvmfs(kLogCatalog, kLogDebug, "Prepared statement catalog %u: %s",
             cat_id, sql_insert.c_str());
    err = sqlite3_prepare_v2(db[cat_id], sql_insert.c_str(), -1, &insert, NULL);
    if (err != SQLITE_OK) {
      sqlError = "unable to prepare insert statement";
      return false;
    }

    /* UPDATE */
    sqlite3_stmt *update;
    const string sql_update = "UPDATE catalog "
    "SET hash = :hash, size = :size, mode = :mode, mtime = :mtime, "
    "flags = :flags, name = :name, symlink = :symlink, inode = :inode "
    "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);";
    LogCvmfs(kLogCatalog, kLogDebug, "Prepared statement catalog %u: %s",
             cat_id, sql_update.c_str());
    err = sqlite3_prepare_v2(db[cat_id], sql_update.c_str(), -1, &update, NULL);
    if (err != SQLITE_OK) {
      sqlError = "unable to prepare update statement";
      return false;
    }

    /* UPDATE_INODE */
    sqlite3_stmt *update_inode;
    const string sql_update_inode = "UPDATE catalog "
    "SET hash = :hash, size = :size, mode = coalesce(:mode, mode), mtime = :mtime "
    "WHERE inode = :ino;";
    LogCvmfs(kLogCatalog, kLogDebug, "Prepared statement catalog %u: %s",
             cat_id, sql_update_inode.c_str());
    err = sqlite3_prepare_v2(db[cat_id], sql_update_inode.c_str(), -1, &update_inode, NULL);
    if (err != SQLITE_OK) {
      sqlError = "unable to prepare update statement";
      return false;
    }

    /* UNLINK */
    sqlite3_stmt *unlink;
    const string sql_unlink = "DELETE FROM catalog "
    "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);";
    LogCvmfs(kLogCatalog, kLogDebug, "Prepared statement catalog %u: %s",
             cat_id, sql_unlink.c_str());
    err = sqlite3_prepare_v2(db[cat_id], sql_unlink.c_str(), -1, &unlink, NULL);
    if (err != SQLITE_OK) {
      sqlError = "unable to prepare unlink statement";
      return false;
    }

    if (cat_id >= stmts_ls.size()) stmts_ls.push_back(ls);
    else stmts_ls[cat_id] = ls;
    if (cat_id >= stmts_lookup.size()) stmts_lookup.push_back(lookup);
    else stmts_lookup[cat_id] = lookup;
    if (cat_id >= stmts_lookup_inode.size()) stmts_lookup_inode.push_back(lookup_inode);
    else stmts_lookup_inode[cat_id] = lookup_inode;
    if (cat_id >= stmts_parent.size()) stmts_parent.push_back(parent);
    else stmts_parent[cat_id] = parent;
    if (cat_id >= stmts_lookup_nested.size()) stmts_lookup_nested.push_back(lookup_nested);
    else stmts_lookup_nested[cat_id] = lookup_nested;
    if (cat_id >= stmts_insert.size()) stmts_insert.push_back(insert);
    else stmts_insert[cat_id] = insert;
    if (cat_id >= stmts_update.size()) stmts_update.push_back(update);
    else stmts_update[cat_id] = update;
    if (cat_id >= stmts_update_inode.size()) stmts_update_inode.push_back(update_inode);
    else stmts_update_inode[cat_id] = update_inode;
    if (cat_id >= stmts_unlink.size()) stmts_unlink.push_back(unlink);
    else stmts_unlink[cat_id] = unlink;
    return true;
  }


  /**
   * Temporarily removes a catalog from the set of active catalogs.
   * Lock this with lock().  Reattach the catalog before unlock().
   *
   * \return True on success, false otherwise
   */
  bool detach_intermediate(const unsigned cat_id) {
    LogCvmfs(kLogCatalog, kLogDebug,  "detach intermediate catalog %d", cat_id);

    if (stmts_unlink[cat_id]) {
      sqlite3_finalize(stmts_unlink[cat_id]);
      stmts_unlink[cat_id] = NULL;
    }
    if (stmts_update_inode[cat_id]) {
      sqlite3_finalize(stmts_update_inode[cat_id]);
      stmts_update_inode[cat_id] = NULL;
    }
    if (stmts_update[cat_id]) {
      sqlite3_finalize(stmts_update[cat_id]);
      stmts_update[cat_id] = NULL;
    }
    if (stmts_insert[cat_id]) {
      sqlite3_finalize(stmts_insert[cat_id]);
      stmts_insert[cat_id] = NULL;
    }
    if (stmts_ls[cat_id]) {
      sqlite3_finalize(stmts_ls[cat_id]);
      stmts_ls[cat_id] = NULL;
    }
    if (stmts_lookup_nested[cat_id]) {
      sqlite3_finalize(stmts_lookup_nested[cat_id]);
      stmts_lookup_nested[cat_id] = NULL;
    }
    if (stmts_parent[cat_id]) {
      sqlite3_finalize(stmts_parent[cat_id]);
      stmts_parent[cat_id] = NULL;
    }
    if (stmts_lookup[cat_id]) {
      sqlite3_finalize(stmts_lookup[cat_id]);
      stmts_lookup[cat_id] = NULL;
    }
    if (stmts_lookup_inode[cat_id]) {
      sqlite3_finalize(stmts_lookup_inode[cat_id]);
      stmts_lookup_inode[cat_id] = NULL;
    }
    in_transaction[cat_id] = false;

    LogCvmfs(kLogCatalog, kLogDebug,
             "detach intermediate %d, statements removed, closing db", cat_id);
    const int result = sqlite3_close(db[cat_id]);
    if (result == SQLITE_OK) {
      db[cat_id] = NULL;
      return true;
    } else return false;
  }


  /**
   * Permanently removes a catalog from the set of active catalogs.
   * TODO: don't release file handle
   *
   * \return True on success, false otherwise
   */
  bool detach(const unsigned cat_id) {
    bool result = false;
    LogCvmfs(kLogCatalog, kLogDebug, "detaching catalog %d", cat_id);

    if (detach_intermediate(cat_id)) {
      num_catalogs--;
      current_catalog = 0;
      LogCvmfs(kLogCatalog, kLogDebug,
               "reorganising catalogs %d-%d", cat_id, num_catalogs);
      for (int i = cat_id; i < num_catalogs; ++i) {
        if (!detach_intermediate(i+1) ||
            !reattach(i, catalog_files[i+1], catalog_urls[i+1]))
				{
          return false;
				}
        catalog_files[i] = catalog_files[i+1];
        catalog_urls[i] = catalog_urls[i+1];
        opened_read_only[i] = opened_read_only[i+1];
        inode_chunks[i] = inode_chunks[i+1];
        in_transaction[i] = false;
      }
      stmts_unlink.pop_back();
      stmts_update_inode.pop_back();
      stmts_update.pop_back();
      stmts_insert.pop_back();
      stmts_ls.pop_back();
      stmts_lookup_nested.pop_back();
      stmts_parent.pop_back();
      stmts_lookup.pop_back();
      stmts_lookup_inode.pop_back();
      in_transaction.pop_back();
      inode_chunks.pop_back();
      opened_read_only.pop_back();
      catalog_files.pop_back();
      catalog_urls.pop_back();
      db.pop_back();
      result = true;
    }

    return result;
  }


  /**
   * Adds an SQLite file to the set of active catalogs.
   * If necessary, the database schema is created, i.e.
   * the file may be non-existant.
   *
   * @param[in] db_file Absolute path of SQLite file
   * @param[in] url Source url of the catalog file.  It is stored here
   *    for convenience in order to access it by a catalog id.
   * \return True on success, false otherwise
   */
  bool attach(const string &db_file, const string &url,
              const bool read_only, const bool open_transaction)
  {
		sqlite3 *new_db;
    int flags = SQLITE_OPEN_NOMUTEX;
    if (read_only)
      flags |= SQLITE_OPEN_READONLY;
    else
      flags |= SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;

    LogCvmfs(kLogCatalog, kLogDebug, "Attaching %s as id %d",
             db_file.c_str(), num_catalogs);
    if (sqlite3_open_v2(db_file.c_str(), &new_db, flags, NULL) != SQLITE_OK) {
      sqlite3_close(new_db);
      LogCvmfs(kLogCatalog, kLogDebug, "Cannot attach file %s as id %d",
               db_file.c_str(), num_catalogs);
      goto attach_return;
    }
    db.push_back(new_db);
    sqlite3_extended_result_codes(db[num_catalogs], 1);
    in_transaction.push_back(false);
    opened_read_only.push_back(read_only);

    if (!read_only) {
      if (!sql_exec(db[num_catalogs], "PRAGMA synchronous=0; PRAGMA auto_vacuum=1;"))
        goto attach_return;
    }

    if (!create_db(num_catalogs, read_only)) {
      sqlite3_close(db[num_catalogs]);
      goto attach_return;
    }

    if (!build_prepared_stmts(num_catalogs)) {
      sqlite3_close(db[num_catalogs]);
      goto attach_return;
    }

    /* For db0: get root prefix */
    if (num_catalogs == 0) {
      sqlite3_stmt *stmt_rprefix;
      int err = sqlite3_prepare(db[0], "SELECT value FROM properties WHERE key='root_prefix';",
                                -1, &stmt_rprefix, NULL);
      if (err != SQLITE_OK)
        goto attach_fail;
      err = sqlite3_step(stmt_rprefix);
      if (err == SQLITE_ROW) {
        root_prefix = string((char *)sqlite3_column_text(stmt_rprefix, 0));
        LogCvmfs(kLogCatalog, kLogDebug, "found root prefix %s",
                 root_prefix.c_str());
      }
      sqlite3_finalize(stmt_rprefix);
    }

    catalog_urls.push_back(url);
    catalog_files.push_back(db_file);

    current_catalog = num_catalogs;
    num_catalogs++;

    if (open_transaction) transaction(num_catalogs-1);

    inode_chunks.push_back(get_next_inode_chunk_of_size(get_max_rowid(current_catalog)));

    return true;

  attach_fail:
    detach_intermediate(num_catalogs);
  attach_return:
    return false;
  }


  /**
   * Re-attaches an updated catalog file.  Works like attach().
   *
   * \return True on success, false otherwise
   */
  bool reattach(const unsigned cat_id, const string &db_file, const string &url)
  {
    int flags = SQLITE_OPEN_NOMUTEX;
    if (opened_read_only[cat_id])
      flags |= SQLITE_OPEN_READONLY;
    else
      flags |= SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;

    LogCvmfs(kLogCatalog, kLogDebug, "Re-attaching %s as %u",
             db_file.c_str(), cat_id);
    if (sqlite3_open_v2(db_file.c_str(), &db[cat_id], flags, NULL) != SQLITE_OK) {
      sqlite3_close(db[cat_id]);
      return false;
    }
    in_transaction[cat_id] = false;
    sqlite3_extended_result_codes(db[cat_id], 1);

    if (!create_db(cat_id, opened_read_only[cat_id]) || !build_prepared_stmts(cat_id)) {
      detach_intermediate(cat_id);
      return false;
    }

    catalog_urls[cat_id] = url;
    catalog_files[cat_id] = db_file;
    inode_chunks[cat_id] = realign_inode_chunks_for_catalog(cat_id, get_max_rowid(cat_id));

    return true;
  }


  string get_catalog_url(const unsigned cat_id) {
    return catalog_urls[cat_id];
  }

  string get_catalog_file(const unsigned cat_id) {
    return catalog_files[cat_id];
  }

  int get_num_catalogs() {
    return num_catalogs;
  }


  /**
   * @param[in] puid uid to use for catalog entry to struct stat conversion
   * @param[in] pgid gid to use for catalog entry to struct stat conversion
   *
   * \return True on success, false otherwise
   */
  bool init(const uid_t puid, const gid_t pgid) {
    uid = puid;
    gid = pgid;
    root_prefix = "";
    num_catalogs = current_catalog = 0;
    return initTLS();
  }


  /**
   * Cleans up memory and open file handles.
   */
  void fini() {
    for (int i = 0; i < num_catalogs; ++i)
      detach_intermediate(i);

    stmts_unlink.clear();
    stmts_update_inode.clear();
    stmts_update.clear();
    stmts_insert.clear();
    stmts_ls.clear();
    stmts_lookup_nested.clear();
    stmts_parent.clear();
    stmts_lookup.clear();
    stmts_lookup_inode.clear();

    catalog_urls.clear();
    catalog_files.clear();

    root_prefix = "";
    uid = gid = 0;
    num_catalogs = current_catalog = 0;
    teardownTLS();
  }


  /**
   * Starts a database transaction.  Must not be nested.
   */
  void transaction(const unsigned cat_id) {
    if (!in_transaction[cat_id]) {
      in_transaction[cat_id] = true;
      sql_exec(db[cat_id], "BEGIN");
    }
  }


  /**
   * Checks if in transaction.
   */
  bool transaction_running(const unsigned cat_id) {
    return in_transaction[cat_id];
  }



  /**
   * Commits a database transaction.
   */
  void commit(const unsigned cat_id) {
    if (in_transaction[cat_id]) {
      sql_exec(db[cat_id], "COMMIT");
      in_transaction[cat_id] = false;
    }
  }


  /**
   * Rolls back a database transaction.
   */
  void rollback(const unsigned cat_id) {
    if (in_transaction[cat_id]) {
      sql_exec(db[cat_id], "ROLLBACK");
      in_transaction[cat_id] = false;
    }
  }


  /**
   * See insert().
   */
  bool insert_unprotected(const hash::t_md5 &name, const hash::t_md5 &parent,
                          const t_dirent &entry) {
    enforce_mem_limit();

    bool ret;

    sqlite3_stmt *insert = stmts_insert[entry.catalog_id];

    sqlite3_bind_int64(insert, 1, *((sqlite_int64 *)(&name.digest[0])));
    sqlite3_bind_int64(insert, 2, *((sqlite_int64 *)(&name.digest[8])));
    sqlite3_bind_int64(insert, 3, *((sqlite_int64 *)(&parent.digest[0])));
    sqlite3_bind_int64(insert, 4, *((sqlite_int64 *)(&parent.digest[8])));
    if (entry.checksum.is_null())
      sqlite3_bind_null(insert, 5);
    else
      sqlite3_bind_blob(insert, 5, entry.checksum.digest, 20, SQLITE_STATIC);
    sqlite3_bind_int64(insert, 6, entry.inode);
    sqlite3_bind_int64(insert, 7, entry.size);
    sqlite3_bind_int(insert, 8, entry.mode);
    sqlite3_bind_int64(insert, 9, entry.mtime);
    sqlite3_bind_int(insert, 10, entry.flags);
    sqlite3_bind_text(insert, 11, &entry.name[0], entry.name.length(), SQLITE_STATIC);
    sqlite3_bind_text(insert, 12, &entry.symlink[0], entry.symlink.length(), SQLITE_STATIC);
    const int rcode = sqlite3_step(insert);
    ret = ((rcode == SQLITE_DONE) || (rcode == SQLITE_OK));
    sqlite3_reset(insert);

    return ret;
  }


  /**
   * Inserts a catalog entry into the catalog of its parent.  This is always the
   * right decision, because we don't find the nested-link in the parent catalog as
   * parent entry.
   *
   * @param[in] name Hashed path of entry
   * @param[in] parent Hashed path of parent of entry
   *
   * \return True on success, false otherwise
   */
  bool insert(const hash::t_md5 &name, const hash::t_md5 &parent, const t_dirent &entry)
  {
    bool ret;

    pthread_mutex_lock(&mutex);
    ret = insert_unprotected(name, parent, entry);
    pthread_mutex_unlock(&mutex);

    return ret;
  }


  /**
   * See update().
   */
  bool update_unprotected(const hash::t_md5 &name, const t_dirent &entry) {
    enforce_mem_limit();

    bool ret = false;
    sqlite3_stmt *update;

    if (entry.flags & DIR_NESTED_ROOT) {
      //for (int i = 0; i < num_catalogs; ++i) transaction(i);

      for (int i = 0; i < num_catalogs; ++i) {
        update = stmts_update[i];

        if (entry.checksum.is_null())
          sqlite3_bind_null(update, 1);
        else
          sqlite3_bind_blob(update, 1, entry.checksum.digest, 20, SQLITE_STATIC);
        sqlite3_bind_int64(update, 2, entry.size);
        sqlite3_bind_int(update, 3, entry.mode);
        sqlite3_bind_int64(update, 4, entry.mtime);
        sqlite3_bind_int(update, 5, (i == entry.catalog_id) ? entry.flags : DIR | DIR_NESTED);
        sqlite3_bind_text(update, 6, &entry.name[0], entry.name.length(), SQLITE_STATIC);
        sqlite3_bind_text(update, 7, &entry.symlink[0], entry.symlink.length(), SQLITE_STATIC);
        sqlite3_bind_int64(update, 8, entry.inode);
        sqlite3_bind_int64(update, 9, *((sqlite_int64 *)(&name.digest[0])));
        sqlite3_bind_int64(update, 10, *((sqlite_int64 *)(&name.digest[8])));

        const int rcode = sqlite3_step(update);
        ret = ((rcode == SQLITE_DONE) || (rcode == SQLITE_OK));
        sqlite3_reset(update);

        if (!ret) break;
      }

      //if (ret) {
      //   for (int i = 0; i < num_catalogs; i++) commit(i);
      //} else {
      //   for (int i = 0; i < num_catalogs; i++) rollback(i);
      //}
    } else {
      update = stmts_update[entry.catalog_id];

      if (entry.checksum.is_null())
        sqlite3_bind_null(update, 1);
      else
        sqlite3_bind_blob(update, 1, entry.checksum.digest, 20, SQLITE_STATIC);
      sqlite3_bind_int64(update, 2, entry.size);
      sqlite3_bind_int(update, 3, entry.mode);
      sqlite3_bind_int64(update, 4, entry.mtime);
      sqlite3_bind_int(update, 5, entry.flags);
      sqlite3_bind_text(update, 6, &entry.name[0], entry.name.length(), SQLITE_STATIC);
      sqlite3_bind_text(update, 7, &entry.symlink[0], entry.symlink.length(), SQLITE_STATIC);
      sqlite3_bind_int64(update, 8, entry.inode);
      sqlite3_bind_int64(update, 9, *((sqlite_int64 *)(&name.digest[0])));
      sqlite3_bind_int64(update, 10, *((sqlite_int64 *)(&name.digest[8])));

      const int rcode = sqlite3_step(update);
      ret = ((rcode == SQLITE_DONE) || (rcode == SQLITE_OK));
      sqlite3_reset(update);
    }

    return ret;
  }


  /**
   * Updates the information for a path according to entry.
   * This is a bit messy, because if inode-information changes,
   * we actually have to use update inode.
   *
   * \return True on success, false otherwise
   */
  bool update(const hash::t_md5 &name, const t_dirent &entry) {
    bool ret;

    pthread_mutex_lock(&mutex);
    ret = update_unprotected(name, entry);
    pthread_mutex_unlock(&mutex);

    return ret;
  }


  /**
   * Updates all paths with the same inode.
   *
   * \return True on success, false otherwise
   */
  bool update_inode_internal(const uint64_t inode, const unsigned *mode,
                             const uint64_t size, const time_t mtime,
                             const hash::t_sha1 &checksum)
  {
    enforce_mem_limit();

    bool ret = true;

    pthread_mutex_lock(&mutex);

    //for (int i = 0; i < num_catalogs; ++i)
    //   transaction(i);

    for (int i = 0; i < num_catalogs; ++i) {
      sqlite3_stmt *update_inode = stmts_update_inode[i];

      if (checksum.is_null())
        sqlite3_bind_null(update_inode, 1);
      else
        sqlite3_bind_blob(update_inode, 1, checksum.digest, 20, SQLITE_STATIC);
      sqlite3_bind_int64(update_inode, 2, size);
      if (mode) sqlite3_bind_int(update_inode, 3, *mode);
      else sqlite3_bind_null(update_inode, 3);
      sqlite3_bind_int64(update_inode, 4, mtime);
      sqlite3_bind_int64(update_inode, 5, inode);
      const int rcode = sqlite3_step(update_inode);
      ret &= ((rcode == SQLITE_DONE) || (rcode == SQLITE_OK));
      sqlite3_reset(update_inode);
      //pmesg(D_CATALOG, "update inode %lld in catalog %d resulted in %d, ret is %d", inode, i, rcode, ret);
      if (!ret) break;
    }
    //if (ret) {
    //   for (int i = 0; i < num_catalogs; i++)
    //      commit(i);
    //} else {
    //   for (int i = 0; i < num_catalogs; i++)
    //      rollback(i);
    //}

    pthread_mutex_unlock(&mutex);

    return ret;
  }


  /**
   * See update_inode_internal().
   */
  bool update_inode(const uint64_t inode, const unsigned mode,
                    const uint64_t size, const time_t mtime, const hash::t_sha1 &checksum)
  {
    return update_inode_internal(inode, &mode, size, mtime, checksum);
  }


  /**
   * See update_inode_internal().
   */
  bool update_inode(const uint64_t inode, const uint64_t size,
                    const time_t mtime, const hash::t_sha1 &checksum)
  {
    return update_inode_internal(inode, NULL, size, mtime, checksum);
  }

  uint64_t get_root_inode() {
    return (inode_chunks.size() == 0) ? INITIAL_INODE_OFFSET + 1 : inode_chunks[0].offset + 1;
  }


  /**
   * See unlink().
   */
  bool unlink_unprotected(const hash::t_md5 &name, const unsigned cat_id) {
    enforce_mem_limit();

    bool ret;

    sqlite3_stmt * const unlink = stmts_unlink[cat_id];

    sqlite3_bind_int64(unlink, 1, *((sqlite_int64 *)(&name.digest[0])));
    sqlite3_bind_int64(unlink, 2, *((sqlite_int64 *)(&name.digest[8])));

    const int rcode = sqlite3_step(unlink);
    ret = ((rcode == SQLITE_DONE) || (rcode == SQLITE_OK));
    sqlite3_reset(unlink);

    return ret;
  }


  /**
   * Removes a path from catalog.
   *
   * \return True on success, false otherwise
   */
  bool unlink(const hash::t_md5 &name, const unsigned cat_id) {
    bool ret;

    pthread_mutex_lock(&mutex);
    ret = unlink_unprotected(name, cat_id);
    pthread_mutex_unlock(&mutex);

    return ret;
  }


  /**
   * Takes a path from FUSE and prepares it for MD5-hash, e.g. adds root prefix.
   * We have to be unique here, because we look for the hash.
   *
   * \return mangled path
   */
  string mangled_path(const string &path) {
    return root_prefix + (path == "/" ? "" : path);
  }


	/**
	 *  gets the inode for a root node of a nested catalog
	 *  it looks in the parent catalog and returns the inode of the mount point
	 *  for this function to work the catalog tree structure must be maintained
	 *  @param dirent the t_dirent structure of the root node (will be altered)
	 *  @param key the md5 key of the mount point and the root node (same)
	 */
	void get_inode_of_nested_catalog_mountpoint(t_dirent &dirent, const hash::t_md5 key) {
		if (not catalog_tree::isEnabled()) {
			return;
		}

		if (dirent.catalog_id == 0) {
			return;
		}

		int parentCatalogId = catalog_tree::get_parent(dirent.catalog_id)->catalog_id;
		struct t_dirent nestedLinkInParent;
		lookup_informed_unprotected(key, parentCatalogId, nestedLinkInParent);
		dirent.inode = nestedLinkInParent.inode;
	}


  /**
   * See ls().
   */
  vector<t_dirent> ls_unprotected(const hash::t_md5 &parent) {
    enforce_mem_limit();

    vector<t_dirent> result;

    for (int i = 0; i < num_catalogs; ++i) {
      sqlite3_stmt *stmt_ls = stmts_ls[(current_catalog + i) % num_catalogs];

      sqlite3_bind_int64(stmt_ls, 1, *((sqlite_int64 *)(&parent.digest[0])));
      sqlite3_bind_int64(stmt_ls, 2, *((sqlite_int64 *)(&parent.digest[8])));
      while (sqlite3_step(stmt_ls) == SQLITE_ROW) {
        int flags = sqlite3_column_int(stmt_ls, 6);
        //pmesg(D_CATALOG, "Found child entry in catalog %d, flags %d", sqlite3_column_int(stmt_ls, 0), flags);
        if (flags & DIR_NESTED_ROOT) break;

				struct t_dirent d;
				d.catalog_id = sqlite3_column_int(stmt_ls, 0);
				d.name = string((char *)sqlite3_column_text(stmt_ls, 7));
				d.symlink = string((char *)sqlite3_column_text(stmt_ls, 8));
				d.flags = sqlite3_column_int(stmt_ls, 6);
				d.inode = getInode(sqlite3_column_int64(stmt_ls, 9), sqlite3_column_int64(stmt_ls, 2), sqlite3_column_int(stmt_ls, 0));
				d.mode = sqlite3_column_int(stmt_ls, 4);
				d.size = sqlite3_column_int64(stmt_ls, 3);
				d.mtime = sqlite3_column_int64(stmt_ls, 5);
				d.checksum = ((sqlite3_column_bytes(stmt_ls, 1) > 0) ?
                      hash::t_sha1(sqlite3_column_blob(stmt_ls, 1), sqlite3_column_bytes(stmt_ls, 1)) :
                      hash::t_sha1());

				// if we encounter a root directory of a nested catalog we have to take a look
				// for its actual inode number in it's parent catalog (only possible with catalog_tree)
				if (d.flags & DIR_NESTED_ROOT) {
					get_inode_of_nested_catalog_mountpoint(d, parent);
				}

        result.push_back(d);
      }
      sqlite3_reset(stmt_ls);
      if (!result.empty()) {
        current_catalog = (current_catalog + i) % num_catalogs;
        break;
      }
    }

    return result;
  }


  /**
   * Gets files and directories under parent. Parent may be a file,
   * in this case the returned vector is empty.
   *
   * \return A vector of dirents directly under path parent
   */
  vector<t_dirent> ls(const hash::t_md5 &parent) {
    vector<t_dirent> result;

    pthread_mutex_lock(&mutex);
    result = ls_unprotected(parent);
    pthread_mutex_unlock(&mutex);

    return result;
  }


  /**
   * Gets mount points of all nested catalogs for a catalog.
   *
   * @param[out] ls Paths of mount points (set by register_nested()).
   * \return True on success, false otherwise
   */
  bool ls_nested(const unsigned cat_id, vector<string> &ls) {
    enforce_mem_limit();

    sqlite3_stmt *stmt_ls_nested;
    const string sql_ls_nested = "SELECT path FROM nested_catalogs;";
    const int err = sqlite3_prepare_v2(db[cat_id], sql_ls_nested.c_str(), -1, &stmt_ls_nested, NULL);
    if (err != SQLITE_OK) {
      sqlError = "unable to prepare update statement";
      return false;
    }

    while (sqlite3_step(stmt_ls_nested) == SQLITE_ROW) {
      ls.push_back(string((char *)sqlite3_column_text(stmt_ls_nested, 0)));
    }
    sqlite3_finalize(stmt_ls_nested);

    return true;
  }


  /**
   * Gets the SHA-1 key of a particular nested catalog
   *
   * @param[out] ls Paths of mount points (set by register_nested()).
   * \return True on success, false otherwise
   */
  bool lookup_nested_unprotected(const unsigned cat_id, const string &path, hash::t_sha1 &sha1) {
    enforce_mem_limit();

    sqlite3_stmt *stmt_lookup_nested = stmts_lookup_nested[cat_id];
    bool found = false;

		sqlite3_bind_text(stmt_lookup_nested, 1, &path[0], path.length(), SQLITE_STATIC);
		if (sqlite3_step(stmt_lookup_nested) == SQLITE_ROW) {
      const string sha1_str = string((char *)sqlite3_column_text(stmt_lookup_nested, 0));
      sha1.from_hash_str(sha1_str);
      found = true;
		}
		sqlite3_reset(stmt_lookup_nested);

    return found;
  }


  /**
   * Registers a path as mount point for a nested catalog.
   *
   * \return True on success, false otherwise
   */
  bool register_nested(const unsigned cat_id, const string &path) {
    enforce_mem_limit();

    int result = true;

    sqlite3_stmt *stmt_register;
    const string sql = "INSERT INTO nested_catalogs (path) VALUES (:p);";
    const int err = sqlite3_prepare_v2(db[cat_id], sql.c_str(), -1, &stmt_register, NULL);
    if (err != SQLITE_OK) {
      sqlError = "unable to prepare register nested catalog statement";
      result = false;
    } else {
      sqlite3_bind_text(stmt_register, 1, &path[0], path.length(), SQLITE_STATIC);
      const int rcode = sqlite3_step(stmt_register);
      result = ((rcode == SQLITE_DONE) || (rcode == SQLITE_OK));
      sqlite3_finalize(stmt_register);
    }

    return result;
  }


  bool unregister_nested(const unsigned cat_id, const string &path) {
    enforce_mem_limit();

    int result = true;

    sqlite3_stmt *stmt_unregister;
    const string sql = "DELETE FROM nested_catalogs WHERE path=:p;";
    const int err = sqlite3_prepare_v2(db[cat_id], sql.c_str(), -1, &stmt_unregister, NULL);
    if (err != SQLITE_OK) {
      sqlError = "unable to prepare unregister nested catalog statement";
      result = false;
    } else {
      sqlite3_bind_text(stmt_unregister, 1, &path[0], path.length(), SQLITE_STATIC);
      const int rcode = sqlite3_step(stmt_unregister);
      result = ((rcode == SQLITE_DONE) || (rcode == SQLITE_OK));
      sqlite3_finalize(stmt_unregister);
    }

    return result;
  }


  /**
   * Updates the nested catalog to a new sha1 value in the parent one
   */
  bool update_nested_sha1(const unsigned cat_id, const string path,
                          const hash::t_sha1 &sha1)
  {
    enforce_mem_limit();

    const string sql = "UPDATE nested_catalogs SET sha1 = :sha1 WHERE path = :path;";
    sqlite3_stmt *stmt;
    int retval = sqlite3_prepare_v2(db[cat_id], sql.c_str(), -1, &stmt, NULL);
    if (retval != SQLITE_OK) {
      sqlError = "unable to prepare update nested catalog sha1 key statement";
      return false;
    }

    const string sha1_str = sha1.to_string();
    sqlite3_bind_text(stmt, 1, &(sha1_str[0]), sha1_str.length(), SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, &(path[0]), path.length(), SQLITE_STATIC);

    retval = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return ((retval == SQLITE_DONE) || (retval == SQLITE_OK));
  }




  /**
   * Gets the deepest catalog hosting key.
   * \return catalog id on success, -1 else
   */
  int lookup_catalogid_unprotected(const hash::t_md5 &key) {
    enforce_mem_limit();

    int catalog_id = -1;

    for (int i = 0; i < num_catalogs; ++i) {
      sqlite3_stmt *stmt_lookup = stmts_lookup[(current_catalog + i) % num_catalogs];
      int flags = 0;

      sqlite3_bind_int64(stmt_lookup, 1, *((sqlite_int64 *)(&key.digest[0])));
      sqlite3_bind_int64(stmt_lookup, 2, *((sqlite_int64 *)(&key.digest[8])));
      if (sqlite3_step(stmt_lookup) == SQLITE_ROW) {
        flags = sqlite3_column_int(stmt_lookup, 6);
        catalog_id = sqlite3_column_int(stmt_lookup, 0);
      }
      sqlite3_reset(stmt_lookup);
      /* Always preferr root from nested catalog */
      if (flags & catalog::DIR_NESTED_ROOT)
        break;
    }

    return catalog_id;
  }

	/**
	 *  compute the inode number of a file
	 *  @param rowid the row id where the file is saved in it's catalog
	 *  @param hardlinkGroupId the hardlink group id of the file
	 *  @param catalog_id the catalog_id where the file was found
	 *  @return the inode for the file
	 */
	uint64_t getInode(unsigned int rowid, uint64_t hardlinkGroupId, unsigned int catalog_id) {
		uint64_t inode;

		if (hardlinkGroupId == 0) { // no hardlinks present
			inode = rowid;
		} else {
			map<unsigned int, map<uint64_t, uint64_t> >::iterator catalogSpecificHardlinkMap = hardlinkInodeMap.find(catalog_id);
			if (catalogSpecificHardlinkMap == hardlinkInodeMap.end()) { // no mapping found... create one
				map<uint64_t, uint64_t> newMapping;
				newMapping[hardlinkGroupId] = rowid;
				hardlinkInodeMap[catalog_id] = newMapping;
				inode = rowid;
			} else { // found a mapping... check if the hardlinkGroupId already showed up
				map<uint64_t, uint64_t>::iterator hardlinkGroupSpecificMap = catalogSpecificHardlinkMap->second.find(hardlinkGroupId);
				if (hardlinkGroupSpecificMap == catalogSpecificHardlinkMap->second.end()) { // hardlink group didn't show up before... create a mapping for it
					catalogSpecificHardlinkMap->second[hardlinkGroupId] = rowid;
					inode = rowid;
				} else {
					inode = catalogSpecificHardlinkMap->second[hardlinkGroupId];
				}
			}
		}

		if (inode_chunks.size() <= catalog_id) {
			LogCvmfs(kLogCatalog, kLogDebug,
               "catalog_id is not recognized by inode offset vector");
			exit(1);
		}

		return inode + inode_chunks[catalog_id].offset;
	}

  /**
   * See lookup().
   */
  bool lookup_unprotected(const hash::t_md5 &key, struct t_dirent &result) {
    enforce_mem_limit();

    bool found = false;

    int i;
    for (i = 0; i < num_catalogs; ++i) {
      sqlite3_stmt *stmt_lookup = stmts_lookup[(current_catalog + i) % num_catalogs];

      sqlite3_bind_int64(stmt_lookup, 1, *((sqlite_int64 *)(&key.digest[0])));
      sqlite3_bind_int64(stmt_lookup, 2, *((sqlite_int64 *)(&key.digest[8])));
      int flags = catalog::DIR_NESTED;
      if (sqlite3_step(stmt_lookup) == SQLITE_ROW) {
        flags = sqlite3_column_int(stmt_lookup, 6);
        result.catalog_id = sqlite3_column_int(stmt_lookup, 0);
        result.name = string((char *)sqlite3_column_text(stmt_lookup, 7));
        result.symlink = string((char *)sqlite3_column_text(stmt_lookup, 8));
        result.flags = flags;
				result.inode = getInode(sqlite3_column_int64(stmt_lookup, 9), sqlite3_column_int64(stmt_lookup, 2), sqlite3_column_int(stmt_lookup, 0));
        result.mode = sqlite3_column_int(stmt_lookup, 4);
        result.size = sqlite3_column_int64(stmt_lookup, 3);
        result.mtime = sqlite3_column_int64(stmt_lookup, 5);
        result.checksum = (sqlite3_column_bytes(stmt_lookup, 1) > 0) ?
        hash::t_sha1(sqlite3_column_blob(stmt_lookup, 1), sqlite3_column_bytes(stmt_lookup, 1)) :
        hash::t_sha1();
        found = true;
      }
      sqlite3_reset(stmt_lookup);

			// if we encounter a root directory of a nested catalog we have to take a look
			// for its actual inode number in it's parent catalog (only possible with catalog_tree)
			if (result.flags & DIR_NESTED_ROOT) {
				get_inode_of_nested_catalog_mountpoint(result, key);
			}

      /* Always prefer root from nested catalog */
      if (!(flags & catalog::DIR_NESTED)) {
        break;
			}
    }

    if (found)
      current_catalog = (current_catalog + i) % num_catalogs;

    return found;
  }

	bool lookup_informed_unprotected(const hash::t_md5 &key, const int catalog_id, t_dirent &result) {
    enforce_mem_limit();

		sqlite3_stmt *stmt_lookup = stmts_lookup[catalog_id];
    bool found = false;

		sqlite3_bind_int64(stmt_lookup, 1, *((sqlite_int64 *)(&key.digest[0])));
		sqlite3_bind_int64(stmt_lookup, 2, *((sqlite_int64 *)(&key.digest[8])));
		if (sqlite3_step(stmt_lookup) == SQLITE_ROW) {
			int flags = sqlite3_column_int(stmt_lookup, 6);
      //pmesg(D_CATALOG, "Found flags %d in catalog %d for path %s", flags, fid, path.c_str());
			result.catalog_id = catalog_id;
			result.name = string((char *)sqlite3_column_text(stmt_lookup, 7));
			result.symlink = string((char *)sqlite3_column_text(stmt_lookup, 8));
			result.flags = flags;
			result.inode = getInode(sqlite3_column_int64(stmt_lookup, 9), sqlite3_column_int64(stmt_lookup, 2), catalog_id);
			result.mode = sqlite3_column_int(stmt_lookup, 4);
			result.size = sqlite3_column_int64(stmt_lookup, 3);
			result.mtime = sqlite3_column_int64(stmt_lookup, 5);
			result.checksum = (sqlite3_column_bytes(stmt_lookup, 1) > 0) ?
      hash::t_sha1(sqlite3_column_blob(stmt_lookup, 1), sqlite3_column_bytes(stmt_lookup, 1)) :
      hash::t_sha1();
			current_catalog = catalog_id;
      found = true;

			// if we encounter a root directory of a nested catalog we have to take a look
			// for its actual inode number in it's parent catalog (only possible with catalog_tree)
			if (result.flags & DIR_NESTED_ROOT) {
				get_inode_of_nested_catalog_mountpoint(result, key);
			}

		}
		sqlite3_reset(stmt_lookup);

    return found;
  }

  int find_catalog_id_from_inode(const uint64_t inode) {
    vector<inode_chunk_t>::const_iterator i = inode_chunks.begin();
    vector<inode_chunk_t>::const_iterator iend = inode_chunks.end();
    int cat_id = 0; // not found

    // loop through the offset vector to find the catalog according to this offset
    for (; i != iend; ++i) {
      if (inode > i->offset && inode <= i->size + i->offset) {
        return cat_id;
      }
      cat_id++;
    }

    return -1;
  }

  bool lookup_inode_unprotected(const uint64_t inode, t_dirent &result, const bool lookup_parent) {
    enforce_mem_limit();

    int catalog_id = find_catalog_id_from_inode(inode);
    if (catalog_id < 0) {
      // inode not found
      return false;
    }

    sqlite3_stmt *stmt_lookup_inode = stmts_lookup_inode[catalog_id];
    int rowid = inode - inode_chunks[catalog_id].offset;

    bool found = false;

    sqlite3_bind_int64(stmt_lookup_inode, 1, rowid);
    if (sqlite3_step(stmt_lookup_inode) == SQLITE_ROW) {
      int flags = sqlite3_column_int(stmt_lookup_inode, 6);
      //pmesg(D_CATALOG, "Found flags %d in catalog %d for path %s", flags, fid, path.c_str());
      result.catalog_id = catalog_id;
      result.name = string((char *)sqlite3_column_text(stmt_lookup_inode, 7));
      result.symlink = string((char *)sqlite3_column_text(stmt_lookup_inode, 8));
      result.flags = flags;
      result.inode = inode;
      result.mode = sqlite3_column_int(stmt_lookup_inode, 4);
      result.size = sqlite3_column_int64(stmt_lookup_inode, 3);
      result.mtime = sqlite3_column_int64(stmt_lookup_inode, 5);
      result.checksum = (sqlite3_column_bytes(stmt_lookup_inode, 1) > 0) ?
      hash::t_sha1(sqlite3_column_blob(stmt_lookup_inode, 1), sqlite3_column_bytes(stmt_lookup_inode, 1)) :
      hash::t_sha1();

      // if we encounter a root directory of a nested catalog we have to take a look
      // for its actual inode number in it's parent catalog (only possible with catalog_tree)
      if (result.flags & DIR_NESTED_ROOT) {
        // retrieve md5 hash for path
        int64_t md5_1 = sqlite3_column_int64(stmt_lookup_inode, 9);
        int64_t md5_2 = sqlite3_column_int64(stmt_lookup_inode, 10);
        hash::t_md5 md5path(md5_1, md5_2);

        get_inode_of_nested_catalog_mountpoint(result, md5path);
      }

      found = true;

      // if the user needs to know the inode of the parent entry, we have to retrieve it
      if (lookup_parent) {
        // retrieve md5 hash for parent
        int64_t md5_1 = sqlite3_column_int64(stmt_lookup_inode, 11);
        int64_t md5_2 = sqlite3_column_int64(stmt_lookup_inode, 12);

        // check if we are dealing with the absolute root entry
        if (md5_1 == 0 && md5_2 == 0) {
          result.parentInode = INVALID_INODE; // there is nothing above root!
        } else {
          hash::t_md5 md5parent(md5_1, md5_2);

          struct t_dirent parentDirent;
          if (lookup_unprotected(md5parent, parentDirent)) {
            result.parentInode = parentDirent.inode;
          } else {
            found = false;
          }
        }
      }
    }
    sqlite3_reset(stmt_lookup_inode);

    return found;
  }

	bool lookup_inode(const uint64_t inode, t_dirent &result, const bool lookup_parent) {
		bool found;

    pthread_mutex_lock(&mutex);
    found = lookup_inode_unprotected(inode, result, lookup_parent);
    pthread_mutex_unlock(&mutex);

    return found;
	}

  /**
   * Looks for a specific names in active set of catalogs.
   *
   * @param[out] result Catalog entry of found name
   * \return True if key was found, false otherwise
   */
  bool lookup(const hash::t_md5 &key, t_dirent &result) {
    bool found;

    pthread_mutex_lock(&mutex);
    found = lookup_unprotected(key, result);
    pthread_mutex_unlock(&mutex);

    return found;
  }


  /**
   * Looks for the parent catalog entry of key.
   *
   * @param[out] result Catalog entry of found parent
   * \return True if parent was found, false otherwise
   */
  bool parent_unprotected(const hash::t_md5 &key, t_dirent &result) {
    enforce_mem_limit();

    bool found = false;

    for (int i = 0; i < num_catalogs; ++i) {
      sqlite3_stmt *stmt_parent = stmts_parent[(current_catalog + i) % num_catalogs];

      sqlite3_bind_int64(stmt_parent, 1, *((sqlite_int64 *)(&key.digest[0])));
      sqlite3_bind_int64(stmt_parent, 2, *((sqlite_int64 *)(&key.digest[8])));
      if (sqlite3_step(stmt_parent) == SQLITE_ROW) {
        int flags = sqlite3_column_int(stmt_parent, 6);
        /* If we hit the nested catalog entry, we are in the wrong catalog. Too bad. */
        if (!(flags & catalog::DIR_NESTED)) {
          result.catalog_id = sqlite3_column_int(stmt_parent, 0);
          result.name = string((char *)sqlite3_column_text(stmt_parent, 7));
          result.symlink = string((char *)sqlite3_column_text(stmt_parent, 8));
          result.flags = flags;
          result.inode = getInode(sqlite3_column_int64(stmt_parent, 9), sqlite3_column_int64(stmt_parent, 2), sqlite3_column_int(stmt_parent, 0));
          result.mode = sqlite3_column_int(stmt_parent, 4);
          result.size = sqlite3_column_int64(stmt_parent, 3);
          result.mtime = sqlite3_column_int64(stmt_parent, 5);
          result.checksum = (sqlite3_column_bytes(stmt_parent, 1) > 0) ?
          hash::t_sha1(sqlite3_column_blob(stmt_parent, 1), sqlite3_column_bytes(stmt_parent, 1)) :
          hash::t_sha1();
          found = true;
        }
      }
      sqlite3_reset(stmt_parent);
      if (found) {
        current_catalog = (current_catalog + i) % num_catalogs;
        break;
      }
    }

    return found;
  }

  bool parent(const hash::t_md5 &key, t_dirent &result) {
    bool found;

    pthread_mutex_lock(&mutex);
    found = parent_unprotected(key, result);
    pthread_mutex_unlock(&mutex);

    return found;
  }


  /**
   * Wrapper around SQLite VACUUM, applies to all active catalogs.
   *
   * \return True on success, false otherwise
   */
  bool vacuum() {
    int result = true;

    for (int i = 0; i < num_catalogs; i++) {
      if (!sql_exec(db[i], "VACUUM;"))
        result = false;
    }

    return result;
  }


  /**
   * See relink().
   */
  bool relink_unprotected(const string &from_dir, const string &to_dir) {
    enforce_mem_limit();

    bool result = true;
    const hash::t_md5 p_md5(to_dir);

    t_dirent p;
    if (lookup_unprotected(p_md5, p)) {
      vector<t_dirent> children = ls_unprotected(hash::t_md5(from_dir));
      for (vector<t_dirent>::const_iterator i = children.begin(), iEnd = children.end();
           i != iEnd; ++i)
      {
        t_dirent d = *i;
        hash::t_md5 md5(from_dir + "/" + d.name);

        if (!unlink_unprotected(md5, d.catalog_id)) {
          result = false;
          break;
        }

        d.catalog_id = p.catalog_id;
        md5 = hash::t_md5(to_dir + "/" + d.name);
        if (!insert_unprotected(md5, p_md5, d)) {
          result = false;
          break;
        }

        if ((d.flags & DIR) && (!(d.flags & DIR_NESTED))) {
          if (!relink_unprotected(from_dir + "/" + d.name, to_dir + "/" + d.name)) {
            result = false;
            break;
          }
        }
      }
    } else
      result = false;

    return result;
  }


  /**
   * Recursively walks through the directory tree in from_dir,
   * removes it and attaches it to to_dir.
   *
   * \return True on success, false otherwise
   */
  bool relink(const string &from_dir, const string &to_dir) {
    bool result;

    pthread_mutex_lock(&mutex);
    result = relink_unprotected(from_dir, to_dir);
    pthread_mutex_unlock(&mutex);

    return result;
  }


  /**
   * Merge a nested catalog with its parent catalog into
   * the parent catalog.  The nested catalog is empty afterwards.
   *
   * TODO: Transaction
   *
   * \return True on success, false otherwise
   */
  bool merge(const string &nested_dir) {
    enforce_mem_limit();

    int result = false;
    t_dirent dir;
    t_dirent nest;
    int src_id = -1;
    const hash::t_md5 md5(nested_dir);

    pthread_mutex_lock(&mutex);

    LogCvmfs(kLogCatalog, kLogDebug, "merging at path %s",
             mangled_path(nested_dir).c_str());

    /* First: nested root becomes a normal directory */
    if (lookup_unprotected(md5, dir) &&
        unlink_unprotected(md5, dir.catalog_id) &&
        lookup_unprotected(md5, nest))
    {
      src_id = dir.catalog_id;
      dir.catalog_id = nest.catalog_id;
      dir.flags = DIR;
      LogCvmfs(kLogCatalog, kLogDebug,
               "figured out parent catalog (%d) and nested catalog (%d)",
               nest.catalog_id, src_id);
      if (update_unprotected(md5, dir)) {
        const string cat_file = get_catalog_file(src_id);
        detach(src_id);
        if (src_id < dir.catalog_id) dir.catalog_id--;
        LogCvmfs(kLogCatalog, kLogDebug,
                  "try to merge from %s into id %d",
                 cat_file.c_str(), dir.catalog_id);
        if (sql_exec(db[dir.catalog_id], "ATTACH '" + cat_file + "' AS nested;"))
        {
          const string sql_merge = "INSERT INTO main.catalog "
          "SELECT * FROM nested.catalog; "
          "DELETE FROM nested.catalog; "
          "DELETE FROM main.nested_catalogs "
          "WHERE path = '" + nested_dir + "'; "
          "INSERT INTO main.nested_catalogs "
          "SELECT * FROM nested.nested_catalogs; "
          "DELETE FROM nested.nested_catalogs;";

          if (sql_exec(db[dir.catalog_id], sql_merge)) {
            LogCvmfs(kLogCatalog, kLogDebug, "merge data moved");
            result = sql_exec(db[dir.catalog_id], "DETACH nested;");
          }
        } else {
          LogCvmfs(kLogCatalog, kLogDebug, "Attach failed: %s",
                   get_sql_error().c_str());
        }
      }
    }

    pthread_mutex_unlock(&mutex);
    return result;
  }


  /**
   * Creates a mini-database with just a single directory.
   */
  bool make_ls(const string &path, const string &filename) {
    vector<t_dirent> entries = ls_unprotected(path);

    if (!attach(filename, "", false, true))
      return false;

    const hash::t_md5 md5_parent(path);
    unsigned i;
    for (i = 0; i < entries.size(); ++i) {
      const hash::t_md5 md5(path + "/" + entries[i].name);
      entries[i].catalog_id = num_catalogs-1;
      if (!insert_unprotected(md5, md5_parent, entries[i]))
        break;
    }
    if ((entries.size() > 0) && (i != entries.size()))
      return false;

    commit(num_catalogs-1);
    if (!detach(num_catalogs-1))
      return false;

    return true;
  }


  /**
   * Removes everything from catalog id_dest and copies table
   * by table from catalog_source to catalog_dest.
   * NOT USED CURRENTLY, UNTESTED
   * \return true on success, false otherwise
   */
  /*bool clone(const unsigned id_src, const string &snapshot) {
   enforce_mem_limit();

   if (!attach(snapshot, "", false, false)) return false;
   const string sql_wipe = "DELETE FROM catalog; "
   "DELETE FROM nested_catalogs; DELETE FROM properties;";
   if (!sql_exec(db[get_num_catalogs()-1], sql_wipe)) {
   detach(get_num_catalogs()-1);
   return false;
   }
   if (!detach(get_num_catalogs()-1)) return false;

   */  /* Now we attach it by means of SQLite */
  /*  if (sql_exec(db[id_src], "ATTACH '" + snapshot + "' AS snapshot;")) {
   const string sql_clone = "INSERT INTO snapshot.catalog "
   "SELECT * FROM main.catalog; "
   "INSERT INTO snapshot.nested_catalogs SELECT * FROM main.nested_catalogs; "
   "INSERT INTO snapshot.properties SELECT * FROM main.properties;";
   bool result = sql_exec(db[id_src], sql_clone);
   result &= sql_exec(db[id_src], "DETACH snapshot;");
   return result;
   } else {
   return false;
   }
   }*/


  string get_sql_error() {
    return sqlError;
  }

#ifdef CVMFS_CLIENT
  string get_db_memory_usage() {
    ostringstream result;
    int current = 0;
    int highwater = 0;

    for (int i = 0; i < num_catalogs; ++i) {
      result << cvmfs::root_url << catalog_urls[i] << ":" << endl;

      sqlite3_db_status(db[i], SQLITE_DBSTATUS_LOOKASIDE_USED, &current, &highwater, 0);
      result << "  Number of lookaside slots used " << current << " / " << highwater << endl;

      sqlite3_db_status(db[i], SQLITE_DBSTATUS_CACHE_USED, &current, &highwater, 0);
      result << "  Page cache used " << current/1024 << " KB" << endl;

      sqlite3_db_status(db[i], SQLITE_DBSTATUS_SCHEMA_USED, &current, &highwater, 0);
      result << "  Schema memory used " << current/1024 << " KB" << endl;

      sqlite3_db_status(db[i], SQLITE_DBSTATUS_STMT_USED, &current, &highwater, 0);
      result << "  Prepared statements memory used " << current/1024 << " KB" << endl;
    }

    return result.str();
  }
#endif

	bool getMaximalHardlinkGroupId(const unsigned cat_id, unsigned int &maxId) {
		enforce_mem_limit();

		bool result;
		const string sql = "SELECT max(inode) FROM catalog;";
		sqlite3_stmt *stmt;
		sqlite3_prepare_v2(db[cat_id], sql.c_str(), -1, &stmt, NULL);
		int err = sqlite3_step(stmt);
		if (err == SQLITE_ROW) {
			maxId = (unsigned int)atoi((char *)sqlite3_column_text(stmt, 0));
			result = true;
		} else {
			maxId = 0; // returning default value
			result = false;
		}
		sqlite3_finalize(stmt);

		return result;
	}

  void t_dirent::to_stat(struct stat *s) const {
    memset(s, 0, sizeof(*s));
    s->st_dev = 1;
    s->st_ino = inode;
    s->st_mode = mode;
    s->st_nlink = getLinkcountInFlags(flags);
    s->st_uid = uid;
    s->st_gid = gid;
    s->st_rdev = 1;
    //      s->st_size = (flags & catalog::FILE_LINK) ? expand_env(symlink).length() : size;
    s->st_blksize = 4096; /* will be ignored by Fuse */
    s->st_blocks = 1+size/512;
    s->st_atime = mtime;
    s->st_mtime = mtime;
    s->st_ctime = mtime;
  }

}
