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
#include <xlocale.h>

#include "hash.h"
#include "util.h"
extern "C" {
   #include "sqlite3-duplex.h"
   #include "md5.h"
   #include "debug.h"
   #include "compression.h"
}

using namespace std;

namespace catalog {

   const uint64_t DEFAULT_TTL = 86400; ///< Default TTL for a catalog is one day.
   const uint64_t GROW_EPOCH = 1199163600;
   const int SQLITE_THREAD_MEM = 4; ///< SQLite3 heap limit per thread

   pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
   string root_prefix; ///< If we mount deep into a nested catalog, we need the full preceeding path to calculate the correct MD5 hash
   uid_t uid; ///< Required for converting a catalog entry into struct stat
   gid_t gid; ///< Required for converting a catalog entry into struct stat
   int num_catalogs = 0; ///< We support nested catalogs.  There are loaded by SQLite's ATTACH.  In the end, we operate on a set of multiple catalogs
   int current_catalog = 0; ///< We query the catalogs one after another until success.  We use a lazy approach, meaning after success we ask that catalog next time again.

   vector<sqlite3 *> db;
   vector<sqlite3_stmt *> stmts_insert;
   vector<sqlite3_stmt *> stmts_update;
   vector<sqlite3_stmt *> stmts_update_inode;
   vector<sqlite3_stmt *> stmts_unlink;
   vector<sqlite3_stmt *> stmts_ls;
   vector<sqlite3_stmt *> stmts_lookup;
   vector<sqlite3_stmt *> stmts_lookup_inode;
   vector<sqlite3_stmt *> stmts_parent;
   vector<sqlite3_stmt *> stmts_ins_chunk;
   vector<sqlite3_stmt *> stmts_rm_chunk;
   vector<sqlite3_stmt *> stmts_rmino_chunk;
   vector<sqlite3_stmt *> stmts_lookup_chunk;
   vector<bool> in_transaction;
   vector<bool> opened_read_only;
   vector<string> catalog_urls;
   vector<string> catalog_files;
   /* __thread This is actually necessary... but not allowed until C++0x */string sqlError;
   pthread_key_t pkey_sqlitemem;
   __thread bool sqlite_mem_enforced = false;
   
   
   /**
    * Since the functions in here can be called from multiple
    * threads, we have to enforce void sqlite3_soft_heap_limit(int)
    * the hard way (via TLS).
    */
   static void enforce_mem_limit() {
      if (!sqlite_mem_enforced) {
         sqlite3_soft_heap_limit(SQLITE_THREAD_MEM*1024*1024);
         sqlite_mem_enforced = true;
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
      
      /* Schema extensions */
      if (!read_only) {
         /* Added from schema 1.0 --> 2.0 */
         const string sql_chunks = "CREATE TABLE IF NOT EXISTS chunks "
            "(md5path_1 INTEGER, md5path_2 INTEGER, inode INTEGER, offset INTEGER, hash BLOB,"
            "CONSTRAINT pk_chunks PRIMARY KEY (md5path_1, md5path_2, offset));";
         if (!sql_exec(db[cat_id], sql_chunks))
            return false;
         
         const string sql_index_chunks_ino = "CREATE INDEX IF NOT EXISTS idx_chunks_inode "
            "ON chunks (inode);";
         if (!sql_exec(db[cat_id], sql_index_chunks_ino))
            return false;
      
         const string sql_schema = "INSERT OR REPLACE INTO properties (key, value) VALUES ('schema', '2.0');";
         if (!sql_exec(db[cat_id], sql_schema))
            return false;   
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
   
   
   static double get_schema_version(const unsigned cat_id) {
      enforce_mem_limit();
      
      double result = 0.0;
      
      const string sql = "SELECT value FROM properties WHERE key='schema';";
      sqlite3_stmt *stmt;
      sqlite3_prepare_v2(db[cat_id], sql.c_str(), -1, &stmt, NULL);
      int err = sqlite3_step(stmt);
      if (err == SQLITE_ROW) {
         result = sqlite3_column_double(stmt, 0);
      }
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
      sql_ls << "SELECT " << cat_id << ", hash, inode, size, mode, mtime, flags, name, symlink "
         "FROM catalog WHERE (parent_1 = :p_1) AND (parent_2 = :p_2);";
      pmesg(D_CATALOG, "Prepared statement catalog %u: %s", cat_id, sql_ls.str().c_str());
      err = sqlite3_prepare_v2(db[cat_id], sql_ls.str().c_str(), -1, &ls, NULL);
      if (err != SQLITE_OK) {
         sqlError = "unable to prepare ls statement";
         return false;
      }
      
      /* SELECT LOOKUP */
      sqlite3_stmt *lookup;
      ostringstream sql_lookup;
      sql_lookup << "SELECT " << cat_id << ", hash, inode, size, mode, mtime, flags, name, symlink "
         "FROM catalog "
         "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);";
      pmesg(D_CATALOG, "Prepared statement catalog %u: %s", cat_id, sql_lookup.str().c_str());
      err = sqlite3_prepare_v2(db[cat_id], sql_lookup.str().c_str(), -1, &lookup, NULL);
      if (err != SQLITE_OK) {
         sqlError = "unable to prepare lookup statement";
         return false;
      }
      
      /* SELECT LOOKUP INODE */
      sqlite3_stmt *lookup_inode;
      ostringstream sql_lookup_inode;
      sql_lookup_inode << "SELECT " << cat_id << " FROM catalog "
         "WHERE (inode = :ino);";
      pmesg(D_CATALOG, "Prepared statement catalog %u: %s", cat_id, sql_lookup_inode.str().c_str());
      err = sqlite3_prepare_v2(db[cat_id], sql_lookup_inode.str().c_str(), -1, &lookup_inode, NULL);
      if (err != SQLITE_OK) {
         sqlError = "unable to prepare lookup statement";
         return false;
      }
      
      /* SELECT PARENT */
      sqlite3_stmt *parent;
      ostringstream sql_parent;
      sql_parent << "SELECT " << cat_id << ", a.hash, a.inode, a.size, a.mode, a.mtime, a.flags, a.name, a.symlink "
         "FROM catalog a, catalog b "
         "WHERE (b.md5path_1 = :md5_1) AND (b.md5path_2 = :md5_2) AND (a.md5path_1 = b.parent_1) AND (a.md5path_2 = b.parent_2);";
      pmesg(D_CATALOG, "Prepared statement catalog %u: %s", cat_id, sql_parent.str().c_str());
      err = sqlite3_prepare_v2(db[cat_id], sql_parent.str().c_str(), -1, &parent, NULL);
      if (err != SQLITE_OK) {
         sqlError = "unable to prepare parent statement";
         return false;
      }
      
      /* INSERT */
      sqlite3_stmt *insert;
      const string sql_insert = "INSERT INTO catalog "
         "(md5path_1, md5path_2, parent_1, parent_2, hash, inode, size, mode, mtime, flags, name, symlink) "
         "VALUES (:md5_1, :md5_2, :p_1, :p_2, :hash, :ino, :size, :mode, :mtime, :flags, :name, :symlink);"; 
      pmesg(D_CATALOG, "Prepared statement catalog %u: %s", cat_id, sql_insert.c_str());
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
      pmesg(D_CATALOG, "Prepared statement catalog %u: %s", cat_id, sql_update.c_str());
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
      pmesg(D_CATALOG, "Prepared statement catalog %u: %s", cat_id, sql_update_inode.c_str());
      err = sqlite3_prepare_v2(db[cat_id], sql_update_inode.c_str(), -1, &update_inode, NULL);
      if (err != SQLITE_OK) {
         sqlError = "unable to prepare update statement";
         return false;
      }
      
      /* UNLINK */
      sqlite3_stmt *unlink;
      const string sql_unlink = "DELETE FROM catalog "
         "WHERE (md5path_1 = :md5_1) AND (md5path_2 = :md5_2);"; 
      pmesg(D_CATALOG, "Prepared statement catalog %u: %s", cat_id, sql_unlink.c_str());
      err = sqlite3_prepare_v2(db[cat_id], sql_unlink.c_str(), -1, &unlink, NULL);
      if (err != SQLITE_OK) {
         sqlError = "unable to prepare unlink statement";
         return false;
      }
      
      double schema = get_schema_version(cat_id);
      
      /* INSERT CHUNKS */
      sqlite3_stmt *ins_chunk;
      const string sql_ins_chunk = (schema >= 2.0 ) ? "INSERT INTO chunks "
         "(md5path_1, md5path_2, inode, offset, hash) "
         "VALUES (:md5_1, :md5_2, :ino, :offset, :hash);" : ";"; 
      pmesg(D_CATALOG, "Prepared statement catalog %u: %s", cat_id, sql_ins_chunk.c_str());
      err = sqlite3_prepare_v2(db[cat_id], sql_ins_chunk.c_str(), -1, &ins_chunk, NULL);
      if (err != SQLITE_OK) {
         sqlError = "unable to prepare insert statement";
         return false;
      }
      
      /* REMOVE CHUNKS */
      sqlite3_stmt *rm_chunk;
      const string sql_rm_chunk = (schema >= 2.0 ) ? "DELETE FROM chunks "
         "WHERE md5path_1=:md5_1 AND md5path_2=:md5_2;" : ";"; 
      pmesg(D_CATALOG, "Prepared statement catalog %u: %s", cat_id, sql_rm_chunk.c_str());
      err = sqlite3_prepare_v2(db[cat_id], sql_rm_chunk.c_str(), -1, &rm_chunk, NULL);
      if (err != SQLITE_OK) {
         sqlError = "unable to prepare insert statement";
         return false;
      }
      
      /* REMOVE CHUNKS (INODE) */
      sqlite3_stmt *rmino_chunk;
      const string sql_rmino_chunk = (schema >= 2.0 ) ? "DELETE FROM chunks "
         "WHERE inode=:ino;" : ";"; 
      pmesg(D_CATALOG, "Prepared statement catalog %u: %s", cat_id, sql_rmino_chunk.c_str());
      err = sqlite3_prepare_v2(db[cat_id], sql_rmino_chunk.c_str(), -1, &rmino_chunk, NULL);
      if (err != SQLITE_OK) {
         sqlError = "unable to prepare insert statement";
         return false;
      }
      
      /* LOOKUP CHUNKS */
      sqlite3_stmt *lookup_chunk;
      const string sql_lookup_chunk = (schema >= 2.0 ) ? "SELECT inode, offset, hash FROM chunks "
         "WHERE inode=:ino;" : ";"; 
      pmesg(D_CATALOG, "Prepared statement catalog %u: %s", cat_id, sql_lookup_chunk.c_str());
      err = sqlite3_prepare_v2(db[cat_id], sql_lookup_chunk.c_str(), -1, &lookup_chunk, NULL);
      if (err != SQLITE_OK) {
         sqlError = "unable to prepare insert statement";
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
      if (cat_id >= stmts_insert.size()) stmts_insert.push_back(insert);
      else stmts_insert[cat_id] = insert;
      if (cat_id >= stmts_update.size()) stmts_update.push_back(update);
      else stmts_update[cat_id] = update;
      if (cat_id >= stmts_update_inode.size()) stmts_update_inode.push_back(update_inode);
      else stmts_update_inode[cat_id] = update_inode;
      if (cat_id >= stmts_unlink.size()) stmts_unlink.push_back(unlink);
      else stmts_unlink[cat_id] = unlink;
      if (cat_id >= stmts_ins_chunk.size()) stmts_ins_chunk.push_back(ins_chunk);
      else stmts_ins_chunk[cat_id] = ins_chunk;
      if (cat_id >= stmts_rm_chunk.size()) stmts_rm_chunk.push_back(rm_chunk);
      else stmts_rm_chunk[cat_id] = rm_chunk;
      if (cat_id >= stmts_rmino_chunk.size()) stmts_rmino_chunk.push_back(rmino_chunk);
      else stmts_rmino_chunk[cat_id] = rmino_chunk;
      if (cat_id >= stmts_lookup_chunk.size()) stmts_lookup_chunk.push_back(lookup_chunk);
      else stmts_lookup_chunk[cat_id] = lookup_chunk;
      return true;
   }
   
   
   /**
    * Temporarily removes a catalog from the set of active catalogs.
    * Lock this with lock().  Reattach the catalog before unlock(). 
    *
    * \return True on success, false otherwise
    */
   bool detach_intermediate(const unsigned cat_id) {
      pmesg(D_CATALOG, "detach intermediate catalog %d", cat_id);
      
      if (stmts_lookup_chunk[cat_id]) {
         sqlite3_finalize(stmts_lookup_chunk[cat_id]);
         stmts_lookup_chunk[cat_id] = NULL;
      }
      if (stmts_rmino_chunk[cat_id]) {
         sqlite3_finalize(stmts_rmino_chunk[cat_id]);
         stmts_rmino_chunk[cat_id] = NULL;
      }
      if (stmts_rm_chunk[cat_id]) {
         sqlite3_finalize(stmts_rm_chunk[cat_id]);
         stmts_rm_chunk[cat_id] = NULL;
      }
      if (stmts_ins_chunk[cat_id]) {
         sqlite3_finalize(stmts_ins_chunk[cat_id]);
         stmts_ins_chunk[cat_id] = NULL;
      }
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

      pmesg(D_CATALOG, "detach intermediate %d, statements removed, closing db", cat_id);
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
      pmesg(D_CATALOG, "detaching catalog %d", cat_id);
   
      if (detach_intermediate(cat_id)) {
         num_catalogs--;
         current_catalog = 0;
         pmesg(D_CATALOG, "reorganising catalogs %d-%d", cat_id, num_catalogs);
         for (int i = cat_id; i < num_catalogs; ++i) {
            if (!detach_intermediate(i+1) || 
					 !reattach(i, catalog_files[i+1], catalog_urls[i+1]))
				{
               return false;
				}
            catalog_files[i] = catalog_files[i+1];
            catalog_urls[i] = catalog_urls[i+1];
            opened_read_only[i] = opened_read_only[i+1];
            in_transaction[i] = false;
         }
         stmts_lookup_chunk.pop_back();
         stmts_rmino_chunk.pop_back();
         stmts_rm_chunk.pop_back();
         stmts_ins_chunk.pop_back();
         stmts_unlink.pop_back();
         stmts_update_inode.pop_back();
         stmts_update.pop_back();
         stmts_insert.pop_back();
         stmts_ls.pop_back();
         stmts_parent.pop_back();
         stmts_lookup.pop_back();
         stmts_lookup_inode.pop_back();
         in_transaction.pop_back();
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
      
      pmesg(D_CATALOG, "Attaching %s as id %d", db_file.c_str(), num_catalogs);
      if (sqlite3_open_v2(db_file.c_str(), &new_db, flags, NULL) != SQLITE_OK) {
         sqlite3_close(new_db);
         pmesg(D_CATALOG, "Cannot attach file %s as id %d", db_file.c_str(), num_catalogs);
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
      
      pmesg(D_CATALOG, "Creating / Checking DB in %s", db_file.c_str());
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
            pmesg(D_CATALOG, "found root prefix %s", root_prefix.c_str());
         }
         sqlite3_finalize(stmt_rprefix);
      }
   
      catalog_urls.push_back(url);
      catalog_files.push_back(db_file);
   
      current_catalog = num_catalogs;
      num_catalogs++;
      
      if (open_transaction) transaction(num_catalogs-1);
      
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
      
      pmesg(D_CATALOG, "Re-attaching %s as %u", db_file.c_str(), cat_id);
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
      return (pthread_key_create(&pkey_sqlitemem, NULL) == 0);
   }


   /**
    * Cleans up memory and open file handles.
    */
   void fini() {
      for (int i = 0; i < num_catalogs; ++i)
         detach_intermediate(i);
           
      stmts_lookup_chunk.clear();
      stmts_rmino_chunk.clear();
      stmts_rm_chunk.clear();
      stmts_ins_chunk.clear();
      stmts_unlink.clear();
      stmts_update_inode.clear();
      stmts_update.clear();
      stmts_insert.clear();
      stmts_ls.clear();
      stmts_parent.clear();
      stmts_lookup.clear();
      stmts_lookup_inode.clear();
      
      catalog_urls.clear();
      catalog_files.clear();
      
      root_prefix = "";
      uid = gid = 0;
      num_catalogs = current_catalog = 0;
      pthread_key_delete(pkey_sqlitemem);
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
   
   
   bool insert_chunks_unprotected(const hash::t_md5 &name, const uint64_t inode, const int cat_id,
                                  const vector<uint64_t> offsets, 
                                  const vector<hash::t_sha1> &checksums)
   {
      if (offsets.size() != checksums.size())
         return false;
      
      unsigned size = offsets.size();
      if (size == 0)
         return true;
      
      bool result;
      sqlite3_stmt *ins_chunk = stmts_ins_chunk[cat_id];
      for (unsigned i = 0; i < size; ++i) {
         sqlite3_bind_int64(ins_chunk, 1, *((sqlite_int64 *)(&name.digest[0])));
         sqlite3_bind_int64(ins_chunk, 2, *((sqlite_int64 *)(&name.digest[8])));
         sqlite3_bind_int64(ins_chunk, 3, inode);
         sqlite3_bind_int64(ins_chunk, 4, offsets[i]);
         sqlite3_bind_blob(ins_chunk, 5, (checksums[i]).digest, 20, SQLITE_STATIC);
         
         const int rcode = sqlite3_step(ins_chunk);
         result = ((rcode == SQLITE_DONE) || (rcode == SQLITE_OK));
         sqlite3_reset(ins_chunk);
         
         if (!result) break;
      }
      
      return result;
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
         for (int i = 0; i < num_catalogs; ++i) transaction(i);
         
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
         
         if (ret) {
            for (int i = 0; i < num_catalogs; i++) commit(i);
         } else { 
            for (int i = 0; i < num_catalogs; i++) rollback(i);
         }
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
         int rcode = sqlite3_step(update_inode); 
         ret &= ((rcode == SQLITE_DONE) || (rcode == SQLITE_OK));
         sqlite3_reset(update_inode);
         //pmesg(D_CATALOG, "update inode %lld in catalog %d resulted in %d, ret is %d", inode, i, rcode, ret);
         if (!ret) break;
         
         sqlite3_stmt *rm_chunk = stmts_rmino_chunk[i];
         sqlite3_bind_int64(rm_chunk, 1, inode);
         rcode = sqlite3_step(rm_chunk); 
         ret &= ((rcode == SQLITE_DONE) || (rcode == SQLITE_OK));
         sqlite3_reset(rm_chunk);
         if (!ret) break;
      }
      /*if (ret) {
         for (int i = 0; i < num_catalogs; i++)
            commit(i);
      } else { 
         for (int i = 0; i < num_catalogs; i++)
            rollback(i);
      }*/
      
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
   
   
   /**
    * See unlink().
    */
   bool unlink_unprotected(const hash::t_md5 &name, const unsigned cat_id) {
      enforce_mem_limit();
      
      bool ret;
      
      sqlite3_stmt * const unlink = stmts_unlink[cat_id];
      sqlite3_bind_int64(unlink, 1, *((sqlite_int64 *)(&name.digest[0])));
      sqlite3_bind_int64(unlink, 2, *((sqlite_int64 *)(&name.digest[8])));
      
      int rcode = sqlite3_step(unlink);
      ret = ((rcode == SQLITE_DONE) || (rcode == SQLITE_OK));
      sqlite3_reset(unlink);
      
      sqlite3_stmt * const rm_chunk = stmts_rm_chunk[cat_id];
      sqlite3_bind_int64(rm_chunk, 1, *((sqlite_int64 *)(&name.digest[0])));
      sqlite3_bind_int64(rm_chunk, 2, *((sqlite_int64 *)(&name.digest[8])));
      rcode = sqlite3_step(rm_chunk);
      ret &= ((rcode == SQLITE_DONE) || (rcode == SQLITE_OK));
      sqlite3_reset(rm_chunk);
      
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
            result.push_back(t_dirent(sqlite3_column_int(stmt_ls, 0), /* catalog id */
                                      string((char *)sqlite3_column_text(stmt_ls, 7)), 
                                      string((char *)sqlite3_column_text(stmt_ls, 8)),
                                      sqlite3_column_int(stmt_ls, 6),
                                      sqlite3_column_int64(stmt_ls, 2),
                                      sqlite3_column_int(stmt_ls, 4),
                                      sqlite3_column_int64(stmt_ls, 3),
                                      sqlite3_column_int64(stmt_ls, 5),
                                      ((flags & FILE) ? 
                                        hash::t_sha1(sqlite3_column_blob(stmt_ls, 1), sqlite3_column_bytes(stmt_ls, 1)) :
                                        hash::t_sha1())));
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
    * See lookup().
    */
   bool lookup_unprotected(const hash::t_md5 &key, t_dirent &result) {
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
            result.inode = sqlite3_column_int64(stmt_lookup, 2); 
            result.mode = sqlite3_column_int(stmt_lookup, 4);
            result.size = sqlite3_column_int64(stmt_lookup, 3);
            result.mtime = sqlite3_column_int64(stmt_lookup, 5);
            if (flags & FILE) result.checksum = hash::t_sha1(sqlite3_column_blob(stmt_lookup, 1), sqlite3_column_bytes(stmt_lookup, 1));
            found = true;
         }
         sqlite3_reset(stmt_lookup);
         /* Always preferr root from nested catalog */
         if (!(flags & catalog::DIR_NESTED))
            break;
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
			result.inode = sqlite3_column_int64(stmt_lookup, 2); 
			result.mode = sqlite3_column_int(stmt_lookup, 4);
			result.size = sqlite3_column_int64(stmt_lookup, 3);
			result.mtime = sqlite3_column_int64(stmt_lookup, 5);
			if (flags & FILE) result.checksum = hash::t_sha1(sqlite3_column_blob(stmt_lookup, 1), sqlite3_column_bytes(stmt_lookup, 1));
			current_catalog = catalog_id;
         found = true;
		}
		sqlite3_reset(stmt_lookup);
      
      return found;
   }
   
   bool lookup_inode_unprotected(const uint64_t inode) {
      enforce_mem_limit();
      
      bool found = false;
      
      int i;
      for (i = 0; i < num_catalogs; ++i) {
         sqlite3_stmt *stmt_lookup_inode = stmts_lookup_inode[(current_catalog + i) % num_catalogs];
      
         sqlite3_bind_int64(stmt_lookup_inode, 1, inode);
         if (sqlite3_step(stmt_lookup_inode) == SQLITE_ROW) {
            found = true;
         }
         sqlite3_reset(stmt_lookup_inode);
      
         if (found) break;
      }
      
      if (found)
         current_catalog = (current_catalog + i) % num_catalogs;
               
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
               result.inode = sqlite3_column_int64(stmt_parent, 2); 
               result.mode = sqlite3_column_int(stmt_parent, 4);
               result.size = sqlite3_column_int64(stmt_parent, 3);
               result.mtime = sqlite3_column_int64(stmt_parent, 5);
               //result.checksum = hash::t_sha1();
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
         pmesg(D_CATALOG, "trying vacuum %s", get_catalog_file(i).c_str());
         if (!sql_exec(db[i], "VACUUM;"))
            result = false;
      }
      
      return result;
   }
    
   
   /**
    * See relink(). TODO: migrate chunks
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
         
            if (d.flags & DIR) {
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
      
      pmesg(D_CATALOG, "merging at path %s", mangled_path(nested_dir).c_str());
      
      /* First: nested root becomes a normal directory */
      if (lookup_unprotected(md5, dir) &&
          unlink_unprotected(md5, dir.catalog_id) &&
          lookup_unprotected(md5, nest))
      {
         src_id = dir.catalog_id;
         dir.catalog_id = nest.catalog_id;
         dir.flags = DIR;
         pmesg(D_CATALOG, "figured out parent catalog (%d) and nested catalog (%d)", nest.catalog_id, src_id);
         if (update_unprotected(md5, dir)) {
            const string cat_file = get_catalog_file(src_id);
            detach(src_id);
            if (src_id < dir.catalog_id) dir.catalog_id--;
            pmesg(D_CATALOG, "try to merge from %s into id %d", cat_file.c_str(), dir.catalog_id);
            if (sql_exec(db[dir.catalog_id], "ATTACH '" + cat_file + "' AS nested;")) 
            {
               const string sql_merge = "INSERT INTO main.catalog "
                  "SELECT * FROM nested.catalog; "
                  "DELETE FROM nested.catalog; "
                  "INSERT INTO main.chunks "
                  "SELECT * FROM main.chunks; "
                  "DELETE FROM nested.chunks; " 
                  "DELETE FROM main.nested_catalogs "
                     "WHERE path = '" + nested_dir + "'; "
                  "INSERT INTO main.nested_catalogs "
                     "SELECT * FROM nested.nested_catalogs; "
                  "DELETE FROM nested.nested_catalogs;";
               pmesg(D_CATALOG, "merge prerquisits sucessful, moving mere data:\n%s", sql_merge.c_str());

               if (sql_exec(db[dir.catalog_id], sql_merge)) {
                  pmesg(D_CATALOG, "merge data moved");
                  result = sql_exec(db[dir.catalog_id], "DETACH nested;");
               }
            } else {
               pmesg(D_CATALOG, "Attache failed: %s", get_sql_error().c_str());
            }
         }
      }
      
      pthread_mutex_unlock(&mutex);
      return result;
   }
   

   bool create_compat_internal(const string &dir, ::FILE *f) {
      const vector<t_dirent> entries = ls_unprotected(hash::t_md5(mangled_path(dir)));
      for (vector<t_dirent>::const_iterator i = entries.begin(), iEnd = entries.end(); 
           i != iEnd; ++i) 
      {
         if ((i->name == ".growfsdir") || (i->name == ".growfschecksum") ||
             (i->name == ".growfsdir.zgfs"))
         {
            continue;
         }
         
         char type = 'X';
         if (i->flags & FILE_LINK) type = 'L';
         else if (i->flags & FILE) type = 'F';
         else if (i->flags & (DIR | DIR_NESTED)) type = 'D';
         
         string postfix;
         if (i->flags & FILE_LINK) postfix = "0 " + i->symlink;
         else if (i->flags & FILE) postfix = i->checksum.to_string();
         else postfix = "0";
         fprintf(f, "%c %s\t%u %" PRIu64 " %" PRIu64 " %s\n", type, i->name.c_str(), i->mode, i->size, i->mtime-GROW_EPOCH, 
                 postfix.c_str());
         if (i->flags & DIR_NESTED) fprintf(f, "E S\n");
         else if (i->flags & DIR) {
            if (!create_compat_internal(dir + "/" + i->name, f))
               return false;
         }
      }
      
      fprintf(f, "E\n");
      
      return true;
   }


   /**
    * Writes the current catalog as CVMFSv1 catalog into.
    * <growfsdir>/.growfsdir (together with .growfsdir.zgfs, .growfschecksum)
    *
    * In the catalog at directory <rootdir>, these files are added.
    * (Which means, the directories should match in the end, resp. of caller.)
    *
    * Specify both dirs *without* trailing slash.
    *
    * \return True on success, false otherwise
    */ 
   bool create_compat(const string &growfsdir, const string &rootdir) {
      int result = false;
      ::FILE *f = NULL;
      if ((f = fopen((growfsdir + "/.growfsdir").c_str(), "w")) == NULL) return false;
      
      t_dirent root;
      result = lookup_unprotected(hash::t_md5(mangled_path(rootdir)), root);
      if (result) {
         fprintf(f, "D root\t%u %" PRIu64 " %" PRIu64 " 0\n", root.mode, root.size, root.mtime-GROW_EPOCH); 
         result = create_compat_internal(rootdir, f);
      }
      fclose(f);
      if (result) {
         string gfs = growfsdir + "/.growfsdir";
         string gzgfs = growfsdir + "/.growfsdir.zgfs";
         string gcs = growfsdir + "/.growfschecksum";
         if ((system(("gzip -c \"" + gfs + "\" > \"" + gzgfs + "\"").c_str()) != 0) ||
             (system(("sha1sum < \"" + gfs + "\" > \"" + gcs + "\"").c_str()) != 0))
         {
            result = false;
         } else {
            string compat_files[] = {".growfsdir", ".growfsdir.zgfs", ".growfschecksum"};
            for (int i = 0; i < 3; ++i) {
               struct stat64 info;
               struct stat64 cinfo;
               if (stat64((growfsdir + "/" + compat_files[i]).c_str(), &info) != 0) {
                  result = false;
                  break;
               }
               
               hash::t_sha1 sha1;
               if (compress_file_sha1((growfsdir + "/" + compat_files[i]).c_str(),
                                      (growfsdir + "/" + compat_files[i] + ".compressed").c_str(),
                                      sha1.digest) != 0) 
               {
                  result = false;
                  break;
               }
               
               if (rename((growfsdir + "/" + compat_files[i] + ".compressed").c_str(),
                          (growfsdir + "/" + compat_files[i]).c_str()) != 0)
               {
                  result = false;
                  break;
               }
               
               if (stat64((growfsdir + "/" + compat_files[i]).c_str(), &cinfo) != 0) {
                  result = false;
                  break;
               }
               
               t_dirent d(root.catalog_id, compat_files[i], "", FILE, cinfo.st_ino, 
                          info.st_mode, info.st_size, info.st_mtime, sha1);
               t_dirent exists;
               if (lookup_unprotected(mangled_path(rootdir + "/" + compat_files[i]), exists)) {
                  unlink_unprotected(hash::t_md5(mangled_path(rootdir + "/" + compat_files[i])), d.catalog_id);
               }
               result = insert_unprotected(hash::t_md5(mangled_path(rootdir + "/" + compat_files[i])),
                                              hash::t_md5(mangled_path(rootdir)), d);
               if (!result) break;
            }
         }
      }
      
      return result;
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
   
   
   void t_dirent::to_stat(struct stat * const s) const {
      memset(s, 0, sizeof(*s));
      s->st_dev = 1;
      s->st_ino = inode;
      s->st_mode = mode;
      s->st_nlink = 1;
      s->st_uid = uid;
      s->st_gid = gid;
      s->st_rdev = 1;
      s->st_size = size;
      s->st_blksize = 4096; /* will be ignored by Fuse */
      s->st_blocks = 1+size/512;
      s->st_atime = mtime;
      s->st_mtime = mtime;
      s->st_ctime = mtime;
   }

}
