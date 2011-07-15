#ifndef CATALOG_H
#define CATALOG_H 1

#include "config.h"

#include "hash.h"

#include <string>
#include <vector>
#include <cstdio>
#include <ctime>
#include <sys/stat.h>
#include <stdint.h>

extern "C" {
   #include "sqlite3-duplex.h"
}

namespace catalog {

   const int DIR = 1;
   const int DIR_NESTED = 2; /* Link in the parent catalog */
   const int DIR_NESTED_ROOT = 32; /* Link in the child catalog */
   const int FILE = 4;
   const int FILE_LINK = 8;
   const int FILE_STAT = 16;
   const int FILE_CHUNK = 64;
   
   struct t_dirent {
      t_dirent() : catalog_id(0), flags(0), inode(0), mode(0), size(0), mtime(0) {}
      t_dirent(const int cat_id,
               const std::string &n, 
               const std::string &sym, 
               const int f, 
               const uint64_t ino,
               const unsigned m, 
               const uint64_t s, 
               const time_t t, 
               const hash::t_sha1 &c) : 
                   catalog_id(cat_id), flags(f), inode(ino), mode(m), size(s), mtime(t), checksum(c), name(n), symlink(sym) {}

      int catalog_id;
      int flags;
      uint64_t inode;
      unsigned mode;
      uint64_t size;
      time_t mtime;
      hash::t_sha1 checksum;
      std::string name;
      std::string symlink;

      void to_stat(struct stat * const s) const;
      bool operator <(const t_dirent &other) const {
         return this->name < other.name;
      }
   };

   bool init(const uid_t puid, const gid_t pgid);
   void fini();
   
   bool attach(const std::string &db_file, const std::string &url, 
					const bool read_only, const bool open_transaction); /* Lock this manually! */
   bool reattach(const unsigned cat_id, const std::string &db_file, const std::string &url); /* Lock this manually! */
   std::string get_catalog_url(const unsigned cat_id); /* Lock this manually! */
   std::string get_catalog_file(const unsigned cat_id); /* Lock this manually! */
   int get_num_catalogs(); /* Lock this manually! */
   bool detach(const unsigned cat_id); /* Lock this manually */
   bool detach_intermediate(const unsigned cat_id); /* Lock this manually, use only with reattach! */
   
   bool set_root_prefix(const std::string &r_prefix, const unsigned cat_id);
   std::string get_root_prefix();
   std::string get_root_prefix_specific(const unsigned cat_id);
   std::string mangled_path(const std::string &path);
   uint64_t get_ttl(const unsigned cat_id); /* Lock this manually! */
   bool set_ttl(const unsigned cat_id, const uint64_t ttl); /* Lock this manually! */
   bool update_lastmodified(const unsigned cat_id); /* unlocked */
   time_t get_lastmodified(const unsigned cat_id); /* unlocked */
   bool set_previous_revision(const unsigned cat_id, const hash::t_sha1 &sha1); /* unlocked */
   hash::t_sha1 get_previous_revision(const unsigned cat_id); /* unlocked */
   uint64_t get_num_dirent(const unsigned cat_id); /* unlocked */
   
   void transaction(const unsigned cat_id);
   bool transaction_running(const unsigned cat_id);
   void commit(const unsigned cat_id);
   void rollback(const unsigned cat_id);
   
   bool insert_unprotected(const hash::t_md5 &name, const hash::t_md5 &parent, const t_dirent &entry);
   bool insert(const hash::t_md5 &name, const hash::t_md5 &parent, const t_dirent &entry); /* Locked */
   bool insert_chunks_unprotected(const hash::t_md5 &name, const uint64_t inode, const int cat_id, 
                                  const std::vector<uint64_t> offsets, 
                                  const std::vector<hash::t_sha1> &checksums);
   bool update_unprotected(const hash::t_md5 &name, const t_dirent &entry);
   bool update(const hash::t_md5 &name, const t_dirent &entry); /* Locked */
   bool update_inode(const uint64_t inode, const unsigned mode, 
                     const uint64_t size, const time_t mtime, const hash::t_sha1 &checksum); /* Locked */
   bool update_inode(const uint64_t inode, const uint64_t size, 
                     const time_t mtime, const hash::t_sha1 &checksum); /* Locked */
   bool unlink_unprotected(const hash::t_md5 &name, const unsigned cat_id);
   bool unlink(const hash::t_md5 &name, const unsigned cat_id); /* Locked */
   bool lookup_unprotected(const hash::t_md5 &key, t_dirent &result);
	bool lookup_informed_unprotected(const hash::t_md5 &key, const int catalog_id, t_dirent &result);
   int lookup_catalogid_unprotected(const hash::t_md5 &key);
   bool lookup(const hash::t_md5 &key, t_dirent &result);  /* Locked */
   bool lookup_inode_unprotected(const uint64_t inode);
   bool parent(const hash::t_md5 &key, t_dirent &result);  /* Locked */
   bool parent_unprotected(const hash::t_md5 &key, t_dirent &result);
   std::vector<t_dirent> ls_unprotected(const hash::t_md5 &parent);
   std::vector<t_dirent> ls(const hash::t_md5 &parent);  /* Locked */
   bool ls_nested(const unsigned cat_id, std::vector<std::string> &ls); 
   bool register_nested(const unsigned cat_id, const std::string &path);
   bool update_nested_sha1(const unsigned cat_id, const std::string path, 
                           const hash::t_sha1 &sha1);

   
   bool vacuum(); /* Lock this manually */
   bool relink_unprotected(const std::string &from_dir, const std::string &to_dir);
   bool relink(const std::string &from_dir, const std::string &to_dir); /* Locked */
   bool merge(const std::string &nested_dir); /* Locked */
   bool create_compat(const std::string &growfsdir, const std::string &rootdir);
   //bool clone(const unsigned id_src, const std::string &snapshot); 
   
   std::string get_sql_error();
#ifdef CVMFS_CLIENT
   std::string get_db_memory_usage(); /* Lock manually */
#endif

   /* 
      External access to internal mutex.
      Be careful! Mutex is not nested.
      We use it for on-the-fly catalog replacement.
      This goes like fini - reload - init.
      Look into cvmfs.cc
   */
   extern pthread_mutex_t mutex;
   void inline lock() {
      pthread_mutex_lock(&mutex);
   }
   void inline unlock() {
      pthread_mutex_unlock(&mutex);
   }
}

#endif
