/**
 * \file lru.cc
 * \namespace lru
 *
 * This module implements a "managed local cache".
 * This way, we are able to track access times of files in the cache
 * and remove files based on least recently used strategy.
 *
 * We setup another SQLite catalog, a "cache catalog", that helps us
 * to keep files, file sizes and access times.
 *
 * We might choose to not manage the local cache.  This is indicated
 * by limit == 0 and everything succeeds in that case.
 *
 * Developed by Jakob Blomer 2009 at CERN
 * jakob.blomer@cern.ch
 */

#define __STDC_LIMIT_MACROS
#define _FILE_OFFSET_BITS 64

#include "config.h"
#include "lru.h"

#include <string>
#include <sstream>
#include <stack>
#include <map>
#include <set>

#include <cstdlib>
#include <cstdio>
#include <stdint.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <sys/wait.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>

extern "C" {
   #include "sqlite3-duplex.h"
   #include "debug.h"
   #include "log.h"
}

using namespace std;

namespace lru {

   const int SQLITE_THREAD_MEM = 2;
   
   const string FTYPESTR_REG = "0";
   const string FTYPESTR_CLG = "1";

   pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
   pthread_t thread_touch;
   pthread_t thread_insert;
   bool running;
   int pipe_touch[2];
   int pipe_insert[2];
   map<string, string> key2paths; ///< maps SHA1 chunks to their file name on insert
   pthread_mutex_t mutex_key2paths = PTHREAD_MUTEX_INITIALIZER;
   set<hash::t_sha1> pinned_chunks;
   pthread_mutex_t mutex_pinned = PTHREAD_MUTEX_INITIALIZER;
   
   uint64_t limit; ///< If the cache grows above this size, we clean up until cleanup_threshold.
   uint64_t pinned; ///< Size of pinned files (file catalogs).
   uint64_t cleanup_threshold; ///< When cleaning up, stop when size is below cleanup_threshold. This way, the current working set stays in cache.
   uint64_t gauge; ///< Current size of cache.
   uint64_t seq; ///< Current access sequence number.  Gets increased on every access/insert operation.
   string cache_dir;
   
   sqlite3 *db = NULL;
   sqlite3_stmt *stmt_touch = NULL;
   sqlite3_stmt *stmt_new = NULL;
   sqlite3_stmt *stmt_lru = NULL;
   sqlite3_stmt *stmt_size = NULL;
   sqlite3_stmt *stmt_rm = NULL;
   sqlite3_stmt *stmt_list = NULL;
   sqlite3_stmt *stmt_list_pinned = NULL; /* loaded catalogs are pinned */
   sqlite3_stmt *stmt_list_catalogs = NULL;


   static void *tf_touch(void *data __attribute__((unused))) {
      pmesg(D_LRU, "starting touch thread");
      sqlite3_soft_heap_limit(SQLITE_THREAD_MEM*1024*1024);
      
      hash::t_sha1 sha1;
      
      while (read(pipe_touch[0], sha1.digest, 20) == 20) {
         const string sha1_str = sha1.to_string();
         pmesg(D_LRU, "touching %s", sha1_str.c_str());
      
         pthread_mutex_lock(&mutex);
      
         sqlite3_bind_int64(stmt_touch, 1, seq++);
         sqlite3_bind_text(stmt_touch, 2, &sha1_str[0], sha1_str.length(), SQLITE_STATIC);
         int result = sqlite3_step(stmt_touch);
         pmesg(D_LRU, "touching %s (%ld): %d", sha1_str.c_str(), seq-1, result);
         errno = result;
         assert(((result == SQLITE_DONE) || (result == SQLITE_OK)) && "LRU touch failed");
         sqlite3_reset(stmt_touch);
      
         pthread_mutex_unlock(&mutex);
      }
      
      close(pipe_touch[0]);
      pmesg(D_LRU, "ending touch thread");
      return NULL;
   }

   /* Insert normal files */
   static void *tf_insert(void *data __attribute__((unused))) {
      pmesg(D_LRU, "starting insert thread");
      sqlite3_soft_heap_limit(SQLITE_THREAD_MEM*1024*1024);
      
      hash::t_sha1 sha1;
      uint64_t size;
      unsigned char buf[20+sizeof(size)];
      
      while (read(pipe_insert[0], buf, 20+sizeof(size)) == 20+sizeof(size)) {
         memcpy(sha1.digest, buf, 20);
         memcpy(&size, buf+20, sizeof(size));
         const string sha1_str = sha1.to_string();
         pmesg(D_LRU, "insert thread, got sha1 %s", sha1_str.c_str());
         string path = "(UNKNOWN)";
         
         pthread_mutex_lock(&mutex_key2paths);
         map<string, string>::iterator i = key2paths.find(sha1_str);
         if (i != key2paths.end()) {
            path = i->second;
            key2paths.erase(i);
         }
         pthread_mutex_unlock(&mutex_key2paths);
         
         pthread_mutex_lock(&mutex);
      
         /* cleanup, move to trash and unlink when unlocked */
         if (gauge + size > limit) {
            pmesg(D_LRU, "over limit, gauge %lu, file size %lu", gauge, size);
            if (!cleanup_unprotected(cleanup_threshold)) {
               pthread_mutex_unlock(&mutex);
               continue;
            }
         }
      
         /* insert */
         sqlite3_bind_text(stmt_new, 1, &(sha1_str[0]), hash::t_sha1::CHAR_SIZE, SQLITE_STATIC);
         sqlite3_bind_int64(stmt_new, 2, size);
         sqlite3_bind_int64(stmt_new, 3, seq++);
         sqlite3_bind_text(stmt_new, 4, &(path[0]), path.length(), SQLITE_STATIC);
         sqlite3_bind_int64(stmt_new, 5, FTYPE_REG);
         sqlite3_bind_int64(stmt_new, 6, 0);
         int result = sqlite3_step(stmt_new);
         assert(((result == SQLITE_DONE) || (result == SQLITE_OK)) && "LRU insert failed");
         sqlite3_reset(stmt_new);
      
         gauge += size;

         pthread_mutex_unlock(&mutex);
      }
      
      close(pipe_insert[0]);
      pmesg(D_LRU, "ending insert thread");
      return NULL;
   }

   /**
    * Sets up parameters.  We don't check here, if cache is already too big.
    *
    * @param[in] dont_build Specifies if SQLite cache catalog has to be rebuild based on chache directory.
    *    This is done anyway, if the catalog is empty.
    * \return True on success, false otherwise.
    */
   bool init(const string &cache_dir, const uint64_t limit, 
             const uint64_t cleanup_threshold, const bool dont_build) 
   {
      if ((cleanup_threshold >= limit) && (limit > 0)) {
         pmesg(D_LRU, "invalid parameters: limit %llu, cleanup_threshold %llu", limit, cleanup_threshold);
         return false;
      }
      
      string sql;
      sqlite3_stmt *stmt;
      
      running = false;
            
      lru::limit = limit;
      pinned = 0;
      lru::cleanup_threshold = cleanup_threshold;
      lru::cache_dir = cache_dir;
      
      /* Initialize cache catalog */
      bool retry = false;
   init_recover:
      const string db_file = cache_dir + "/cvmfscatalog.cache";
      int err = sqlite3_open(db_file.c_str(), &db);
      if (err != SQLITE_OK) {
         pmesg(D_LRU, "could not open cache database (%d)", err);
         return false;
      }
      sql = "PRAGMA synchronous=0; PRAGMA locking_mode=EXCLUSIVE; PRAGMA auto_vacuum=1; "
         "CREATE TABLE IF NOT EXISTS cache_catalog (sha1 TEXT, size INTEGER, acseq INTEGER, path TEXT, type INTEGER, pinned INTEGER, "
         "CONSTRAINT pk_cache_catalog PRIMARY KEY (sha1)); "
         "CREATE UNIQUE INDEX IF NOT EXISTS idx_cache_catalog_acseq ON cache_catalog (acseq); "
         "CREATE TEMP TABLE fscache (sha1 TEXT, size INTEGER, actime INTEGER, "
         "CONSTRAINT pk_fscache PRIMARY KEY (sha1)); "
         "CREATE INDEX idx_fscache_actime ON fscache (actime); "
         "CREATE TABLE IF NOT EXISTS properties (key TEXT, value TEXT, CONSTRAINT pk_properties PRIMARY KEY(key));";
      err = sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL);
      if (err != SQLITE_OK) {
         if (!retry) {
            retry = true;
            unlink(db_file.c_str());
            logmsg("LRU database corrupted, re-building");
            goto init_recover;
         }
         pmesg(D_LRU, "could not init cache database (failed: %s)", sql.c_str());
         return false;
      }
         
      
      /* If this an old cache catalog, add and initialize new columns to cache_catalog */
      sql = "ALTER TABLE cache_catalog ADD type INTEGER; "
         "ALTER TABLE cache_catalog ADD pinned INTEGER";
      err = sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL);
      if (err == SQLITE_OK) {
         sql = "UPDATE cache_catalog SET type=" + FTYPESTR_REG + ";";
         err = sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL);
         if (err != SQLITE_OK) {
            pmesg(D_LRU, "could not init cache database (failed: %s)", sql.c_str());
            return false;
         }
      }
      
      /* Set pinned back */
      sql = "UPDATE cache_catalog SET pinned=0;";
      err = sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL);
      if (err != SQLITE_OK) {
         pmesg(D_LRU, "could not init cache database (failed: %s)", sql.c_str());
         return false;
      }
      
      /* Set schema version */
      sql = "INSERT OR REPLACE INTO properties (key, value) VALUES ('schema', '1.0')";
      err = sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL);
      if (err != SQLITE_OK) {
         pmesg(D_LRU, "could not init cache database (failed: %s)", sql.c_str());
         return false;
      }
      
      /* Easy way out, no quota restrictions */
      if (limit == 0) {
         gauge = 0;
         return true;
      }
      
      /* If cache catalog is empty, recreate from file system */
      if (!dont_build) {
         sql = "SELECT count(*) FROM cache_catalog;";
         sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL);
         if (sqlite3_step(stmt) == SQLITE_ROW) {
            if ((sqlite3_column_int64(stmt, 0)) == 0 && !build()) {
               pmesg(D_LRU, "could not build cache database from file system");
               return false;
            }
         } else {
            pmesg(D_LRU, "could not select on cache catalog");
            sqlite3_finalize(stmt);
            return false;
         }
         sqlite3_finalize(stmt);
      }
      
      /* How many bytes do we already have in cache? */
      sql = "SELECT sum(size) FROM cache_catalog;";
      sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL);
      if (sqlite3_step(stmt) == SQLITE_ROW) {
         gauge = sqlite3_column_int64(stmt, 0);
      } else {
         pmesg(D_LRU, "could not determine cache size");
         sqlite3_finalize(stmt);
         return false;
      }
      sqlite3_finalize(stmt);
      
      /* Highest seq-no? */
      sql = "SELECT coalesce(max(acseq), 0) FROM cache_catalog;";
      sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL);
      if (sqlite3_step(stmt) == SQLITE_ROW) {
         seq = sqlite3_column_int64(stmt, 0)+1;
      } else {
         pmesg(D_LRU, "could not determine highest seq-no");
         sqlite3_finalize(stmt);
         return false;
      }
      sqlite3_finalize(stmt);
      
      /* Prepare touch and new statements */
      sqlite3_prepare_v2(db, "UPDATE cache_catalog SET acseq=:seq " 
         "WHERE sha1=:sha1;", -1, &stmt_touch, NULL); 
      sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO cache_catalog (sha1, size, acseq, path, type, pinned) "
         "VALUES (:sha1, :s, :seq, :p, :t, :pin);", -1, &stmt_new, NULL);
      sqlite3_prepare_v2(db, "SELECT size, pinned FROM cache_catalog WHERE sha1=:sha1;", -1, &stmt_size, NULL);
      sqlite3_prepare_v2(db, "DELETE FROM cache_catalog WHERE sha1=:sha1;", -1, &stmt_rm, NULL);
      sqlite3_prepare_v2(db, "SELECT sha1, size FROM cache_catalog WHERE acseq=(SELECT min(acseq) FROM cache_catalog WHERE pinned=0);", 
         -1, &stmt_lru, NULL);
      sqlite3_prepare_v2(db, 
         ("SELECT path FROM cache_catalog WHERE type=" + FTYPESTR_REG + ";").c_str(), 
         -1, &stmt_list, NULL);
      sqlite3_prepare_v2(db, 
         "SELECT path FROM cache_catalog WHERE pinned=1;", 
         -1, &stmt_list_pinned, NULL);
      sqlite3_prepare_v2(db, 
         ("SELECT path FROM cache_catalog WHERE type=" + FTYPESTR_CLG + ";").c_str(), 
         -1, &stmt_list_catalogs, NULL);
         
      if ((pipe(pipe_touch) != 0) || (pipe(pipe_insert) != 0)) {
         pmesg(D_LRU, "could not create pipes");
         return false;
      }

      return true;
   }
   
   
   /**
    * Spawns touch and insert threads
    */
   void spawn() {
      if (limit == 0)
         return;
      
      if (pthread_create(&thread_touch, NULL, tf_touch, NULL) != 0) {
         pmesg(D_LRU, "could not create touch thread");
         abort();
      }
      if (pthread_create(&thread_insert, NULL, tf_insert, NULL) != 0) {
         pmesg(D_LRU, "could not create insert thread");
         abort();
      }
      running = true;
   }

   
   /**
    * Cleanup, closes SQLite connections.
    */
   void fini() {
      if (running) {
         /* unpin */
         for (set<hash::t_sha1>::const_iterator i = pinned_chunks.begin(), iEnd = pinned_chunks.end();
              i != iEnd; ++i) 
         {
            touch(*i);
         }
      
         char fin = 0;
         int r;
         r = write(pipe_touch[1], &fin, 1);
         assert(r==1);
         r = write(pipe_insert[1], &fin, 1);
         assert(r==1);
         close(pipe_touch[1]);
         close(pipe_insert[1]);
         pthread_join(thread_touch, NULL);
         pthread_join(thread_insert, NULL);
      } else {
         close(pipe_touch[0]);
         close(pipe_insert[0]);
         close(pipe_touch[1]);
         close(pipe_insert[1]);
      }
      
      if (stmt_list_catalogs) sqlite3_finalize(stmt_list_catalogs);
      if (stmt_list_pinned) sqlite3_finalize(stmt_list_pinned);
      if (stmt_list) sqlite3_finalize(stmt_list);
      if (stmt_lru) sqlite3_finalize(stmt_lru);
      if (stmt_rm) sqlite3_finalize(stmt_rm);
      if (stmt_size) sqlite3_finalize(stmt_size);
      if (stmt_touch) sqlite3_finalize(stmt_touch);
      if (stmt_new) sqlite3_finalize(stmt_new);
      if (db) sqlite3_close(db);
   }
   
   
   /**
    * Rebuilds the SQLite cache catalog based on the stat-information of files
    * in the cache directory.
    *
    * \return True on success, false otherwise
    */
   bool build() {
      bool result = false;
      string sql;
      sqlite3_stmt *stmt_select = NULL;
      sqlite3_stmt *stmt_insert = NULL;
      int sqlerr;
      int seq = 0;
      char hex[3];
      struct stat info;
      dirent *d;
      DIR *dirp = NULL;
      string path;
      set<string> catalogs;
      
      pmesg(D_LRU, "re-building cache-database");
      
      pthread_mutex_lock(&mutex);
      
      /* Empty cache catalog and fscache */
      sql = "DELETE FROM cache_catalog; DELETE FROM fscache;";
      sqlerr = sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL);
      if (sqlerr != SQLITE_OK) {
         pmesg(D_LRU, "could not clear cache database");
         goto build_return;
      }
      
      gauge = 0;
      
      /* Gather file catalog SHA1 values */
      if ((dirp = opendir(cache_dir.c_str())) == NULL) {
         pmesg(D_LRU, "failed to open directory %s", path.c_str());
         goto build_return;
      }
      while ((d = readdir(dirp)) != NULL) {
         if (d->d_type != DT_REG) continue;
         
         const string name = d->d_name;
         if (name.substr(0, 14) == "cvmfs.checksum") {
            FILE *f = fopen((cache_dir + "/" + name).c_str(), "r");
            if (f != NULL) {
               char sha1[40];
               if (fread(sha1, 1, 40, f) == 40) {
                  pmesg(D_LRU, "added %s to catalog list", string(sha1, 40).c_str());
                  catalogs.insert(string(sha1, 40).c_str());
               }
               fclose(f);
            }
         }
      }
      closedir(dirp);
      
      /* Insert files from cache sub-directories 00 - ff */
      sqlite3_prepare_v2(db, "INSERT INTO fscache (sha1, size, actime) VALUES (:sha1, :s, :t);", 
         -1, &stmt_insert, NULL);
      
      for(int i = 0; i <= 0xff; i++) {
         snprintf(hex, 3, "%02x", i);
         path = cache_dir + "/" + string(hex);
         if ((dirp = opendir(path.c_str())) == NULL) {
            pmesg(D_LRU, "failed to open directory %s", path.c_str());
            goto build_return;
         }
         while ((d = readdir(dirp)) != NULL) {
            if (d->d_type != DT_REG) continue;
            
            if (stat((path + "/" + string(d->d_name)).c_str(), &info) == 0) {
               string sha1 = string(hex) + string(d->d_name);
               sqlite3_bind_text(stmt_insert, 1, sha1.c_str(), sha1.length(), SQLITE_STATIC);
               sqlite3_bind_int64(stmt_insert, 2, info.st_size);
               sqlite3_bind_int64(stmt_insert, 3, info.st_atime);
               if (sqlite3_step(stmt_insert) != SQLITE_DONE) {
                  pmesg(D_LRU, "could not insert into temp table");
                  goto build_return;
               }
               sqlite3_reset(stmt_insert);

               gauge += info.st_size;
            } else {
               pmesg(D_LRU, "could not stat %s/%s", path.c_str(), d->d_name);
            }
         }
         closedir(dirp);
         dirp = NULL;
      }
      sqlite3_finalize(stmt_insert);
      stmt_insert = NULL;
      
      /* Transfer from temp table in cache catalog */
      sqlite3_prepare_v2(db, "SELECT sha1, size FROM fscache ORDER BY actime;", 
         -1, &stmt_select, NULL);
      sqlite3_prepare_v2(db, "INSERT INTO cache_catalog (sha1, size, acseq, path, type, pinned) "
         "VALUES (:sha1, :s, :seq, 'unknown (automatic rebuild)', :t, 0);", 
         -1, &stmt_insert, NULL);
      while (sqlite3_step(stmt_select) == SQLITE_ROW) {
         const string sha1 = string((const char *)sqlite3_column_text(stmt_select, 0));
         sqlite3_bind_text(stmt_insert, 1, &(sha1[0]), sha1.length(), SQLITE_STATIC);
         sqlite3_bind_int64(stmt_insert, 2, sqlite3_column_int64(stmt_select, 1));
         sqlite3_bind_int64(stmt_insert, 3, seq++);
         if (catalogs.find(sha1) != catalogs.end())
            sqlite3_bind_int64(stmt_insert, 4, FTYPE_CLG);
         else
            sqlite3_bind_int64(stmt_insert, 4, FTYPE_REG);
         
         if (sqlite3_step(stmt_insert) != SQLITE_DONE) {
            pmesg(D_LRU, "could not insert into cache catalog");
            goto build_return;
         }
         sqlite3_reset(stmt_insert);
      }
      
      /* Delete temporary table */
      sql = "DELETE FROM fscache;";
      sqlerr = sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL);
      if (sqlerr != SQLITE_OK) {
         pmesg(D_LRU, "could not clear temporary table (%d)", sqlerr);
         goto build_return;
      }
      
      lru::seq = seq;
      result = true;
      
   build_return:
      if (stmt_insert) sqlite3_finalize(stmt_insert);
      if (stmt_select) sqlite3_finalize(stmt_select);
      if (dirp) closedir(dirp);
      pthread_mutex_unlock(&mutex);
      return result;
   }
   
   
   /**
    * See cleanup(). 
    */
   bool cleanup_unprotected(const uint64_t leave_size) {
      if ((limit == 0) || (gauge <= leave_size))
         return true;
         
      bool result;
      stack<string> trash;
      string sha1;
      
      do {
         sqlite3_reset(stmt_lru);
         if (sqlite3_step(stmt_lru) != SQLITE_ROW) {
            pmesg(D_LRU, "could not get lru-entry");
            break;
         }
         
         sha1 = string((char *)sqlite3_column_text(stmt_lru, 0));
         trash.push(cache_dir + "/" + sha1.substr(0, 2) + "/" +
            sha1.substr(sha1.length() - (hash::t_sha1::CHAR_SIZE - 2)));
         
         gauge -= sqlite3_column_int64(stmt_lru, 1);
         pmesg(D_LRU, "lru cleanup %s", sha1.c_str());
   
         sqlite3_bind_text(stmt_rm, 1, sha1.c_str(), sha1.length(), SQLITE_STATIC);
         result = (sqlite3_step(stmt_rm) == SQLITE_DONE);
         sqlite3_reset(stmt_rm);
         
         if (!result) {
            pmesg(D_LRU, "could not remove lru-entry");
            return false;
         }
      } while (gauge > leave_size);
      
      /* Double fork avoids zombie */
      if (!trash.empty()) {
         pid_t pid;
         int statloc;
         if ((pid = fork()) == 0) {
            if (fork() == 0) {
               while (!trash.empty()) {
                  pmesg(D_LRU, "unlink %s", trash.top().c_str());
                  unlink(trash.top().c_str());
                  trash.pop();
               }   
               exit(0);
            }
            exit(0);
         } else {
            if (pid > 0) waitpid(pid, &statloc, 0);
            else return false;
         }
      }
      
      return gauge <= leave_size;
   }
   
   /**
    * Cleans up in data cache, until cache size is below leave_size.  The actual unlinking 
    * is done in a separate process (fork).
    *
    * \return True on success, false otherwise
    */
   bool cleanup(const uint64_t leave_size) {
      bool result;
      
      pthread_mutex_lock(&mutex);
      result = cleanup_unprotected(leave_size);
      pthread_mutex_unlock(&mutex);
      
      return result;
   }
   
   
   /**
    * Inserts a new file into cache catalog.  This file gets a new, highest sequence number.
    * Does cache cleanup if necessary.
    *
    * \return True on success, false otherwise 
    */
   bool insert(const hash::t_sha1 &sha1, const uint64_t size, 
               const string &cvmfs_path) 
   {
      if (limit == 0) return true;
            
      const string sha1_str = sha1.to_string();
      pmesg(D_LRU, "insert into lru %s, path %s", sha1_str.c_str(), cvmfs_path.c_str());
         
      pthread_mutex_lock(&mutex_key2paths);
      key2paths.insert(make_pair(sha1_str, cvmfs_path));
      pthread_mutex_unlock(&mutex_key2paths);
      
      unsigned char buf[20 + sizeof(size)];
      memcpy(buf, sha1.digest, 20);
      memcpy(buf+20, &size, sizeof(size));
      int r = write(pipe_insert[1], buf, 20+sizeof(size));
      assert(r==20+sizeof(size));
      
      return true;
   }


   /**
    * Immediately inserts a new pinned catalog.
    * Does cache cleanup if necessary.
    *
    * \return True on success, false otherwise 
    */
   bool pin(const hash::t_sha1 &sha1, const uint64_t size, 
            const string &cvmfs_path) 
   {
      if (limit == 0) return true;
            
      const string sha1_str = sha1.to_string();
      pmesg(D_LRU, "pin into lru %s, path %s", sha1_str.c_str(), cvmfs_path.c_str());
         
      pthread_mutex_lock(&mutex);
      pthread_mutex_lock(&mutex_pinned);
      if (pinned_chunks.find(sha1) == pinned_chunks.end()) {
         if ((cleanup_threshold > 0) && (pinned + size > cleanup_threshold)) {
            pthread_mutex_unlock(&mutex_pinned);
            pthread_mutex_unlock(&mutex);
            pmesg(D_LRU, "failed to insert %s (pinned), no space", sha1_str.c_str());
            return false;
         }
         pinned_chunks.insert(sha1);
         pinned += size;
      } else {
         /* Already in, nothing to do */
         pthread_mutex_unlock(&mutex_pinned);
         pthread_mutex_unlock(&mutex);
         return true;
      }
      pthread_mutex_unlock(&mutex_pinned);
           
      /* It could already be in unpinned, check */
      bool exists = false;
      sqlite3_bind_text(stmt_size, 1, &(sha1_str[0]), sha1_str.length(), SQLITE_STATIC);
      if (sqlite3_step(stmt_size) == SQLITE_ROW) {
         exists = true;
      }
      sqlite3_reset(stmt_size);
      
      /* cleanup, move to trash and unlink when unlocked */
      if (!exists && (gauge + size > limit)) {
         pmesg(D_LRU, "over limit, gauge %lu, file size %lu", gauge, size);
         if (!cleanup_unprotected(cleanup_threshold)) {
            pthread_mutex_unlock(&mutex);
            return false;
         }
      }
      
      /* insert */
      sqlite3_bind_text(stmt_new, 1, &(sha1_str[0]), hash::t_sha1::CHAR_SIZE, SQLITE_STATIC);
      sqlite3_bind_int64(stmt_new, 2, size);
      sqlite3_bind_int64(stmt_new, 3, seq++);
      sqlite3_bind_text(stmt_new, 4, &(cvmfs_path[0]), cvmfs_path.length(), SQLITE_STATIC);
      sqlite3_bind_int64(stmt_new, 5, FTYPE_CLG);
      sqlite3_bind_int64(stmt_new, 6, 1);
      int result = sqlite3_step(stmt_new);
      assert(((result == SQLITE_DONE) || (result == SQLITE_OK)) && "LRU pin failed");
      sqlite3_reset(stmt_new);
      
      if (!exists)
         gauge += size;
      
      pthread_mutex_unlock(&mutex);
      
      return true;
   }

   
   
   /**
    * Updates the sequence number of the file specified by an SHA1 hash.
    * Actual work is done by touch thread.
    */
   void touch(const hash::t_sha1 &file) {
      if (limit == 0) return;
      
      int r = write(pipe_touch[1], file.digest, 20);
      assert(r==20);
   }
   
   
   
   /**
    * Removes a SHA1 chunk from cache, if it exists.
    */ 
   void remove(const hash::t_sha1 &file) {
      string sha1 = file.to_string();
      
      if (limit != 0) {
         sqlite3_soft_heap_limit(SQLITE_THREAD_MEM*1024*1024);
         int result;
      
         pmesg(D_LRU, "manually removing %s", sha1.c_str());
         pthread_mutex_lock(&mutex);
         sqlite3_bind_text(stmt_size, 1, &(sha1[0]), sha1.length(), SQLITE_STATIC);
         if ((result = sqlite3_step(stmt_size)) == SQLITE_ROW) {
            uint64_t size = sqlite3_column_int64(stmt_size, 0);
            uint64_t is_pinned = sqlite3_column_int64(stmt_size, 1);
         
            sqlite3_bind_text(stmt_rm, 1, &(sha1[0]), sha1.length(), SQLITE_STATIC);
            result = sqlite3_step(stmt_rm);
            if ((result == SQLITE_DONE) || (result == SQLITE_OK)) {
               gauge -= size;
               if (is_pinned) {
                  pthread_mutex_lock(&mutex_pinned);
                  pinned_chunks.erase(file);
                  pinned -= size;
                  pthread_mutex_unlock(&mutex_pinned);
               }
            } else {
               pmesg(D_LRU, "could not delete %s, error %d", sha1.c_str(), result);
            }
         
            sqlite3_reset(stmt_rm);
         }
         sqlite3_reset(stmt_size);
         pthread_mutex_unlock(&mutex);
      }
      
      unlink((cache_dir + "/" + sha1.substr(0, 2) + "/" +
              sha1.substr(sha1.length() - (hash::t_sha1::CHAR_SIZE - 2))).c_str());
   }
   
   
   /**
    * Lists all path names from the cache db.
    */ 
   vector<string> list() {
      sqlite3_soft_heap_limit(SQLITE_THREAD_MEM*1024*1024);
      vector<string> result;
      
      pthread_mutex_lock(&mutex);
      while (sqlite3_step(stmt_list) == SQLITE_ROW) {
         if (sqlite3_column_type(stmt_list, 0) == SQLITE_NULL) {
            result.push_back("(NULL)");
         } else {
            result.push_back(string((char *)sqlite3_column_text(stmt_list, 0)));
         }
      }
      sqlite3_reset(stmt_list);
      pthread_mutex_unlock(&mutex);
      
      return result;
   }
   
   /**
    * Lists all pinned files from the cache db.
    */ 
   vector<string> list_pinned() {
      sqlite3_soft_heap_limit(SQLITE_THREAD_MEM*1024*1024);
      vector<string> result;
      
      pthread_mutex_lock(&mutex);
      while (sqlite3_step(stmt_list_pinned) == SQLITE_ROW) {
         if (sqlite3_column_type(stmt_list_pinned, 0) == SQLITE_NULL) {
            result.push_back("(NULL)");
         } else {
            result.push_back(string((char *)sqlite3_column_text(stmt_list_pinned, 0)));
         }
      }
      sqlite3_reset(stmt_list_pinned);
      pthread_mutex_unlock(&mutex);
      
      return result;
   }
   
   
   /**
    * Lists all catalog files from the cache db.
    */ 
   vector<string> list_catalogs() {
      sqlite3_soft_heap_limit(SQLITE_THREAD_MEM*1024*1024);
      vector<string> result;
      
      pthread_mutex_lock(&mutex);
      while (sqlite3_step(stmt_list_catalogs) == SQLITE_ROW) {
         if (sqlite3_column_type(stmt_list_catalogs, 0) == SQLITE_NULL) {
            result.push_back("(NULL)");
         } else {
            result.push_back(string((char *)sqlite3_column_text(stmt_list_catalogs, 0)));
         }
      }
      sqlite3_reset(stmt_list_catalogs);
      pthread_mutex_unlock(&mutex);
      
      return result;
   }

   
   
   /**
    * Since we only cleanup until cleanup_threshold, we can only add
    * files smaller than limit-cleanup_threshold.
    */
   uint64_t max_file_size() {
      if (limit == 0) return INT64_MAX;
      return limit - cleanup_threshold;
   }
   
   uint64_t capacity() {
      return limit;
   }
   
   uint64_t size() {
      uint64_t result;
      
      pthread_mutex_lock(&mutex);
      result = gauge;
      pthread_mutex_unlock(&mutex);
      
      return result;
   }
   
   uint64_t size_pinned() {
      uint64_t result;
      
      pthread_mutex_lock(&mutex_pinned);
      result = pinned;
      pthread_mutex_unlock(&mutex_pinned);
      
      return result;
   }
   
   void lock() {
      pthread_mutex_lock(&mutex);
   }
   
   void unlock() {
      pthread_mutex_unlock(&mutex);
   }
   
   
   string get_memory_usage() {
      if (limit == 0)
         return "LRU not active\n";
      
      ostringstream result;
      int current = 0;
      int highwater = 0;
      
      result << "LRU:" << endl;
      
      sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_USED, &current, &highwater, 0);
      result << "  Number of lookaside slots used " << current << " / " << highwater << endl;
      
      sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_USED, &current, &highwater, 0);
      result << "  Page cache used " << current/1024 << " KB" << endl;
      
      sqlite3_db_status(db, SQLITE_DBSTATUS_SCHEMA_USED, &current, &highwater, 0);
      result << "  Schema memory used " << current/1024 << " KB" << endl;
      
      sqlite3_db_status(db, SQLITE_DBSTATUS_STMT_USED, &current, &highwater, 0);
      result << "  Prepared statements memory used " << current/1024 << " KB" << endl;
      
      return result.str();
   }
   
}
