#ifndef CATALOG_H
#define CATALOG_H 1

#include "cvmfs_config.h"

#include "hash.h"

#include <string>
#include <vector>
#include <cstdio>
#include <ctime>
#include <sys/stat.h>
#include <stdint.h>
#include "sqlite3-duplex.h"

namespace catalog {

   const int DIR = 1;
   const int DIR_NESTED = 2; /* Link in the parent catalog */
   const int DIR_NESTED_ROOT = 32; /* Link in the child catalog */
   const int FILE = 4;
   const int FILE_LINK = 8;
   const int FILE_STAT = 16;
   const int FILE_CHUNK = 64;
   const int NLINK_COUNT_0 = 256; // 8 bit for link count of file
   const int NLINK_COUNT_1 = NLINK_COUNT_0 << 1;
   const int NLINK_COUNT_2 = NLINK_COUNT_1 << 1;
   const int NLINK_COUNT_3 = NLINK_COUNT_2 << 1;
   const int NLINK_COUNT_4 = NLINK_COUNT_3 << 1;
   const int NLINK_COUNT_5 = NLINK_COUNT_4 << 1;
   const int NLINK_COUNT_6 = NLINK_COUNT_5 << 1;
   const int NLINK_COUNT_7 = NLINK_COUNT_6 << 1;
   const int NLINK_COUNT = NLINK_COUNT_0 | NLINK_COUNT_1 | NLINK_COUNT_2 | NLINK_COUNT_3 | NLINK_COUNT_4 | NLINK_COUNT_5 | NLINK_COUNT_6 | NLINK_COUNT_7;

   const unsigned int INITIAL_INODE_OFFSET = 255;

   const unsigned int INVALID_INODE = 0;

	/**
	 *  saves the linkcount into the reserved 8-Bit area of the flags bitmap
	 *  !! the flags bitmap is not changed, but a new bitmap is returned !!
	 *  @param flags a pointer to the flags bitmap
	 *  @param linkcount the linkcount to be saved
	 *  @return the bitmap
	 */
	inline unsigned int setLinkcountInFlags(const unsigned int flags, const unsigned char linkcount) {
		unsigned int cleanFlags = (flags & catalog::NLINK_COUNT) ^ flags; // zero the designated area
		return cleanFlags | ((linkcount * catalog::NLINK_COUNT_0) & NLINK_COUNT);
	}

	/**
	 *  retrieves the linkcount from the 8-Bit area of the flags bitmap
	 *  @param flags a pointer to the flags bitmap
	 *  @return the linkcount of the file belonging to the given flags
	 */
	inline unsigned char getLinkcountInFlags(const unsigned int flags) {
		return (flags & NLINK_COUNT) / NLINK_COUNT_0;
	}

	bool getMaximalHardlinkGroupId(const unsigned cat_id, unsigned int &maxId);

	uint64_t getInode(unsigned int rowid, uint64_t hardlinkGroupId, unsigned int catalog_id);

   struct t_dirent {
      t_dirent() : catalog_id(0), flags(setLinkcountInFlags(0, 1)), inode(0), mode(0), size(0), mtime(0) {
         parentInode = INVALID_INODE;
      }
      t_dirent(const int cat_id,
               const std::string &n,
               const std::string &sym,
               const int f,
               const uint64_t hardlinkGroup,
               const unsigned m,
               const uint64_t s,
               const time_t t,
               const hash::t_sha1 &c) :
						catalog_id(cat_id), flags(f), inode(hardlinkGroup),  mode(m), size(s), mtime(t), checksum(c), name(n), symlink(sym) {

							// the linkcount defaults to 1 !!
							if (getLinkcountInFlags(f) == 0) {
								flags = setLinkcountInFlags(f, 1);
							}

                     parentInode = INVALID_INODE;
						}

	    t_dirent(const int cat_id,
	            const std::string &n,
	            const std::string &sym,
	            const int f,
	            const uint64_t ino,
	            const unsigned m,
	            const uint64_t s,
	            const time_t t,
	            const hash::t_sha1 &c,
					const unsigned int rowId) :
						catalog_id(cat_id), flags(f), mode(m), size(s), mtime(t), checksum(c), name(n), symlink(sym) {
							inode = getInode(rowId, ino, cat_id);
                     parentInode = INVALID_INODE;
						}

      int catalog_id;
      int flags;
      uint64_t inode;
      uint64_t parentInode; // will be set on demand
      unsigned mode;
      uint64_t size;
      time_t mtime;
      hash::t_sha1 checksum;
      std::string name;
      std::string symlink;

      void to_stat(struct stat *s) const;
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
   uint64_t get_revision(); /* Lock this manually */
   bool inc_revision(const int cat_id); /* Lock this manually */
   uint64_t get_ttl(const unsigned cat_id); /* Lock this manually! */
   bool set_ttl(const unsigned cat_id, const uint64_t ttl); /* Lock this manually! */
   bool update_lastmodified(const unsigned cat_id); /* unlocked */
   time_t get_lastmodified(const unsigned cat_id); /* unlocked */
   bool set_previous_revision(const unsigned cat_id, const hash::t_sha1 &sha1); /* unlocked */
   hash::t_sha1 get_previous_revision(const unsigned cat_id); /* unlocked */
   uint64_t get_num_dirent(const unsigned cat_id); /* unlocked */
   uint64_t get_root_inode(); /* unlocked */

   void transaction(const unsigned cat_id);
   bool transaction_running(const unsigned cat_id);
   void commit(const unsigned cat_id);
   void rollback(const unsigned cat_id);

   bool insert_unprotected(const hash::t_md5 &name, const hash::t_md5 &parent, const t_dirent &entry);
   bool insert(const hash::t_md5 &name, const hash::t_md5 &parent, const t_dirent &entry); /* Locked */
   bool update_unprotected(const hash::t_md5 &name, const t_dirent &entry);
   bool update(const hash::t_md5 &name, const t_dirent &entry); /* Locked */

   // bool update_inode(const uint64_t inode, const unsigned mode,
   //                   const uint64_t size, const time_t mtime, const hash::t_sha1 &checksum); /* Locked */
   //
   // bool update_inode(const uint64_t inode, const uint64_t size,
   //                   const time_t mtime, const hash::t_sha1 &checksum); /* Locked */
   // not supported anymore

   bool unlink_unprotected(const hash::t_md5 &name, const unsigned cat_id);
   bool unlink(const hash::t_md5 &name, const unsigned cat_id); /* Locked */
   bool lookup_unprotected(const hash::t_md5 &key, t_dirent &result);
	bool lookup_informed_unprotected(const hash::t_md5 &key, const int catalog_id, t_dirent &result);
   int lookup_catalogid_unprotected(const hash::t_md5 &key);
   bool lookup(const hash::t_md5 &key, t_dirent &result);  /* Locked */
   bool lookup_inode_unprotected(const uint64_t inode, t_dirent &result, const bool lookup_parent);
	bool lookup_inode(const uint64_t inode, t_dirent &result, const bool lookup_parent); /* Locked */
   int find_catalog_id_from_inode(const uint64_t inode);
   uint64_t get_inode_offset_for_catalog_id(const int catalog_id);

   bool parent(const hash::t_md5 &key, t_dirent &result);  /* Locked */
   bool parent_unprotected(const hash::t_md5 &key, t_dirent &result);
   std::vector<t_dirent> ls_unprotected(const hash::t_md5 &parent);
   std::vector<t_dirent> ls(const hash::t_md5 &parent);  /* Locked */
   bool ls_nested(const unsigned cat_id, std::vector<std::string> &ls);
   bool register_nested(const unsigned cat_id, const std::string &path);
   bool unregister_nested(const unsigned cat_id, const std::string &path);
   bool update_nested_sha1(const unsigned cat_id, const std::string path,
                           const hash::t_sha1 &sha1);
   bool lookup_nested_unprotected(const unsigned cat_id, const std::string &path,
                                  hash::t_sha1 &sha1);


   bool vacuum(); /* Lock this manually */
   bool relink_unprotected(const std::string &from_dir, const std::string &to_dir);
   bool relink(const std::string &from_dir, const std::string &to_dir); /* Locked */
   bool merge(const std::string &nested_dir); /* Locked */
   bool make_ls(const std::string &path, const std::string &filename); /* Lock manually! */
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
