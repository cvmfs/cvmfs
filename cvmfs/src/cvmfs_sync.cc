/**
 * \file cvmfs_sync.cc
 *
 * This tool makes the changes to a repository based on the cvmfsflt
 * kernel module log.
 * We call the user's working directoy "shadow directory".  This shadow 
 * directory is synchronized with a CVMFS2 repository.  The .cvmfscatalog
 * magic file is translated into nested catalogs.
 *
 * On the repository side we have a catalogs directory that mimicks the
 * shadow directory structure and stores compressed and uncompressed
 * versions of all catalogs.  The raw data are stored in the data 
 * subdirectory in zlib-compressed form.  They are named with their SHA1
 * hash of the compressed file (like in CVMFS client cache, but with a 
 * 2-level cache hierarchy).  Symlinks from the catalog directory to the 
 * data directory form the connection. If necessary, add a .htaccess file 
 * to allow Apache to follow the symlinks.
 *
 * Developed by Jakob Blomer 2010 at CERN
 * jakob.blomer@cern.ch
 */


#define _FILE_OFFSET_BITS 64

#include "cvmfs_config.h"

#include "cvmfs_sync_aufs.h"

#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <set>
#include <vector>
#include <map>
#include <cstdio>
#include <cstring>

/* from http://root.cern.ch/viewvc/trunk/cint/reflex/src/stl_hash.h */
/*#ifndef __GNU_CXX_HASH_H
#define __GNU_CXX_HASH_H
#if defined(__GNUC__)
# if defined(__INTEL_COMPILER) && (__INTEL_COMPILER <= 800)
#  define __gnu_cxx std
# endif
# if (__GNUC__ < 4) || ((__GNUC__ == 4) && (__GNUC_MINOR__ < 3))
// For gcc, the hash_map and hash_set classes are in the extensions area
#  include <ext/hash_set>
#  include <ext/hash_map>
# else
// GCC >= 4.3:
// silence warning
#  define _BACKWARD_BACKWARD_WARNING_H
#  include <backward/hash_set>
#  include <backward/hash_map>
# endif
# endif
# endif*/

#include <sys/stat.h>
#ifdef __APPLE__
	#include <limits.h>
#else
	#include <linux/limits.h>
#endif
#include <dirent.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <omp.h>

#include "catalog.h"
#include "hash.h"
#include "util.h"
#include "monitor.h"

#include "compat.h"

extern "C" {
   #include "compression.h"
   #include "smalloc.h"
}

using namespace std;

enum file_type_t {FT_DIR, FT_REG, FT_SYM, FT_ERR};

struct t_catalog_info {
   bool dirty;
   int id;
   int parent_id;
};

struct t_cas_file {
   string path;
   hash::t_md5 md5_path;
   hash::t_md5 md5_parent;
   catalog::t_dirent dirent;
};

map<string, t_catalog_info> open_catalogs; ///< bool is a dirty flag that shows which catalog to snapshot

/* path sets (absolute) */
set<string> immutables;
set<string> move_in;
set<string> move_out;
set<string> dir_add;
set<string> dir_touch;
set<string> dir_rem;
set<string> reg_add;
set<string> reg_touch; ///< might be add if file is opened with O_CREAT
set<string> sym_add;
set<string> replace_candidate; ///< file might get replaced by rename, has to be deleted first
set<string> fil_add; ///< We don't know if this is hard link to regular file or hard link to symlink
set<string> fil_rem; ///< We don't know if this is regular file or symlink
set<string> clg_add;
set<string> clg_rem;

set<string> prels; ///< Modified directories we need new mucro catalogs for


static bool rem_path(const string &path, set<string> &from) {
   set<string>::iterator itr;

   itr = from.find(path);
   if (itr != from.end()) {
      from.erase(itr);
      return true;
   }
   
   return false;
}


static bool in_subtree(const string &path_tree, const string &path_check) {
   return ((path_check.length() > path_tree.length()) &&
           (path_check.find(path_tree) == 0) &&
           (path_check[path_tree.length()] == '/'));
}


static void set_dirty(const string &path) {
   /* find hosting catalog of path (and all parent ones on the way) */
   bool found = false;
   for (map<string, t_catalog_info>::iterator i = open_catalogs.begin(), iEnd = open_catalogs.end();
        i != iEnd; ++i)
   {
      if (path.find(i->first) == 0) {
         i->second.dirty = true;
         found = true;
      }
   }
   
   if (!found) {
      cerr << "Warning: path " << path << " is not on any open catalog" << endl;
   }
}


static void print_set(const set<string> &s) {
   for (set<string>::const_iterator i = s.begin(), iEnd = s.end();
        i != iEnd; ++i)
   {
      cout << (*i) << endl;
   }
   cout << endl;
}


static file_type_t get_file_type(const string &path) {
   PortableStat64 info;
   if (portableLinkStat64(path.c_str(), &info) != 0)
      return FT_ERR;
      
   if (S_ISDIR(info.st_mode)) return FT_DIR;
   else if (S_ISREG(info.st_mode)) return FT_REG;
   else if (S_ISLNK(info.st_mode)) return FT_SYM;
   
   return FT_ERR;
}


static bool get_file_info(const string &path, PortableStat64 *info) {
   if (portableLinkStat64(path.c_str(), info) != 0) {
      cerr << "Warning: could not stat " << path << endl;
      return false;
   }
   
   return true;
}



static string abs2clg_path(const string &path, const string &dir_shadow) {
   return path.substr(dir_shadow.length());
}


static void hieve_add(const string &path) 
{
   DIR *dir = opendir(path.c_str());
   if (!dir) {
      cerr << "Warning: could not open directory " << path << endl;
      return;
   }
   
   dir_add.insert(path);
   
   PortableDirent *d;
   while ((d = portableReaddir(dir)) != NULL) {
      const string name = string(d->d_name);
      if ((name == ".") || (name == ".."))
         continue;
         
      const string itr = path + "/" + name;
            
      switch (get_file_type(itr)) {
         case FT_REG:
            reg_add.insert(itr);
            break;
         case FT_SYM:
            sym_add.insert(itr);
            break;
         case FT_DIR:
            hieve_add(itr);
            break;
         default:
            cerr << "Warning: unexpected file type of " << itr << endl;
      }
   }
   
   closedir(dir);
}


static void squeeze_out(const string &path, const string &dir_shadow) {
   hash::t_md5 md5(catalog::mangled_path(abs2clg_path(path, dir_shadow)));
   catalog::t_dirent d;
   if (!catalog::lookup(md5, d)) {
      cerr << "Warning: could not find directory " << path << " to delete it" << endl;
      return;
   }
   
   dir_rem.insert(path);
   if (d.flags & catalog::DIR_NESTED_ROOT)
      clg_rem.insert(path);
   
   vector<catalog::t_dirent> lsdir = catalog::ls(md5);
   for (vector<catalog::t_dirent>::const_iterator i = lsdir.begin(), iEnd = lsdir.end();
        i != iEnd; ++i)
   {
      const string name = path + "/" + i->name;
      if (i->flags & catalog::FILE) {
         /* Regular files and symlinks */
         fil_rem.insert(name);
      } else if (i->flags & catalog::DIR) {
         squeeze_out(name, dir_shadow);
      } else {
         cerr << "Warning: unexpected catalog flags of " << name << endl;
      }
   }
}


bool attach_nested(const string &dir_catalogs, const string &dir_shadow,
                   const unsigned cat_id, const bool dirty) 
{
   vector<string> ls;
   
   if (catalog::ls_nested(cat_id, ls)) {
      vector<string>::iterator i;
      for (i = ls.begin(); i != ls.end(); ++i) {
         bool skip = false;
         for (set<string>::const_iterator j = immutables.begin(), jEnd = immutables.end(); 
              j != jEnd; ++j) 
         {
            size_t cut = dir_shadow.length();
            if ((j->length() >= cut) && (i->find(j->substr(cut), 0) == 0)) {
               skip = true;
               break;
            }
         }
         if (skip) {
            cout << "Skipping catalog in immutable directory " << *i << endl;
            continue;
         }
         
         const string clg_path = dir_catalogs + 
                                 i->substr(catalog::get_root_prefix().length()) + 
                                 "/.cvmfscatalog.working";
         t_catalog_info ci;
         ci.dirty = dirty;
         ci.id = catalog::get_num_catalogs();
         ci.parent_id = cat_id;
         open_catalogs.insert(make_pair(dir_shadow + i->substr(catalog::get_root_prefix().length()), ci));
         
         cout << "Attaching " << clg_path << endl;
         if (!catalog::attach(clg_path, "", false, false))
         {
            cerr << "unable to attach nested catalog " << (*i) << endl;
            return false;
         }

         if (!attach_nested(dir_catalogs, dir_shadow, catalog::get_num_catalogs() - 1, dirty))
            return false;
      }
      return true;
   } else {
      return false;
   }
}


static bool init_catalogs(const string &dir_catalogs, const string &dir_shadow,
                          const bool attach_all) 
{
   if (!catalog::init(getuid(), getgid())) {
      cerr << "could not init SQLite" << endl;
      return false;
   }
   
   const string clg_path = dir_catalogs + "/.cvmfscatalog.working";
   
   cout << "Attaching " << clg_path << endl;
   if (!catalog::attach(clg_path, "", false, false)) {
      cerr << "could not init root catalog" << endl;
      return false;
   }
   
   /* If this is a new catalog, insert root node */
   hash::t_md5 rhash(catalog::mangled_path(""));
   catalog::t_dirent d;
   if (!catalog::lookup(rhash, d)) {
      cout << "creating root entry" << endl;

      PortableStat64 info;
      if (!get_file_info(dir_shadow, &info))
         return false;
      
      d = catalog::t_dirent(0, "", "", catalog::DIR, info.st_ino, info.st_mode, info.st_size, 
                            info.st_mtime, hash::t_sha1());
      if (!catalog::insert(rhash, hash::t_md5(), d)) {
         cerr << "could not insert root hash" << endl;
         return false;
      }
   }
   
   t_catalog_info ci;
   ci.dirty = false;
   ci.id = 0;
   ci.parent_id = -1;
   open_catalogs.insert(make_pair(dir_shadow, ci));
   
   if (attach_all) {
      if (!attach_nested(dir_catalogs, dir_shadow, 0, false)) {
         cerr << "could not init all nested catalogs" << endl;
         return false;
      }
   }
   
   return true;
}


static void clg_merge(const string &path, 
                      const string &dir_shadow, const string &dir_catalogs) 
{
   struct t_catalog_info ci_del;
   
   /* Check if catalog is loaded */
   for (map<string, t_catalog_info>::const_iterator i = open_catalogs.begin(), iEnd = open_catalogs.end(); 
        i != iEnd; ++i) 
   {
      if (i->first == path) {
         ci_del = i->second;
         goto clg_merge_continue;
      }
   }
   cerr << "Warning: there is no catalog loaded at " << path << endl;
   return;
   
clg_merge_continue:
   const string clg_path = abs2clg_path(path, dir_shadow);
   string mimick_path = dir_catalogs + clg_path;
   if (catalog::merge(catalog::mangled_path(clg_path))) {
      unlink((mimick_path + "/.cvmfscatalog").c_str());
      unlink((mimick_path + "/.cvmfscatalog.working").c_str());
      unlink((mimick_path + "/.cvmfschecksum").c_str());
      unlink((mimick_path + "/.cvmfschecksum.sig").c_str());
      unlink((mimick_path + "/.cvmfscatalog.publisher.x509.pem").c_str());
      unlink((mimick_path + "/.growfschecksum").c_str());
      unlink((mimick_path + "/.growfsdir").c_str());
      unlink((mimick_path + "/.growfsdir.zgfs").c_str());
      unlink((mimick_path + "/data").c_str());
      unlink((mimick_path + "/.cvmfswhitelist").c_str());
      unlink((mimick_path + "/.cvmfspublished").c_str());
      open_catalogs.erase(path);
      
      /* Fix open_catalogs ids */
      for (map<string, t_catalog_info>::iterator i = open_catalogs.begin(), iEnd = open_catalogs.end(); 
           i != iEnd; ++i) 
      {
         if (i->second.id > ci_del.id)
            i->second.id = i->second.id - 1;
         
         if (i->second.parent_id > ci_del.id)
            i->second.parent_id = i->second.parent_id - 1;
         else if (i->second.parent_id == ci_del.id)
            i->second.parent_id = ci_del.parent_id;
      }

      /* Remove mimick directories */
      while (is_empty_dir(mimick_path)) {
         if (rmdir(mimick_path.c_str()) != 0) {
            cerr << "Warning: could not delete empty path " << mimick_path << endl;
            return;
         }
         mimick_path = get_parent_path(mimick_path);
      }
   } else {
      cerr << "Warning: could not merge catalogs at " << path << endl;
   }
}


static void clg_snapshot(const string &path, 
                         const string &dir_shadow, const string &dir_catalogs, const string &dir_data,
                         const bool compat_catalog, const string &keyfile, const t_catalog_info &ci)
{
   cout << "Creating catalog snapshot at " << path << endl;
   
   const string clg_path = abs2clg_path(path, dir_shadow);
   const string cat_path = dir_catalogs + clg_path;
   
   /* Data symlink, whitelist symlink */  
   string backlink = "../";
   string parent = get_parent_path(cat_path);
   while (parent != get_parent_path(dir_data)) {
      if (parent == "") {
         cerr << "Warning: cannot find data dir" << endl;
         break;
      }
      parent = get_parent_path(parent);
      backlink += "../";
   }
   
   const string lnk_path_data = cat_path + "/data";
   const string lnk_path_whitelist = cat_path + "/.cvmfswhitelist";
   const string backlink_data = backlink + get_file_name(dir_data);
   const string backlink_whitelist = backlink + get_file_name(dir_catalogs) + "/.cvmfswhitelist";
   
   PortableStat64 info;
   if (portableLinkStat64(lnk_path_data.c_str(), &info) != 0) 
   {
      if (symlink(backlink_data.c_str(), lnk_path_data.c_str()) != 0) {
         cerr << "Warning: cannot create catalog store -> data store symlink" << endl;
      }
   }
   /* Don't make the symlink for the root catalog */
   if ((portableLinkStat64(lnk_path_whitelist.c_str(), &info) != 0) && 
       (get_parent_path(cat_path) != get_parent_path(dir_data)))
   {
      if (symlink(backlink_whitelist.c_str(), lnk_path_whitelist.c_str()) != 0) {
         cerr << "Warning: cannot create whitelist symlink" << endl;
      }
   }

   
   /* Compat catalog */
   if (compat_catalog) {
      cout << "Creating growfscatalog..." << endl;
      if (!catalog::create_compat(cat_path, clg_path)) {
         cerr << "Warning: could not create CVMFS1 catalog at " << cat_path << endl;
      } else {
         /* Copy compat catalog into data store */
         const string gfs_files[3] = {"/.growfsdir", "/.growfsdir.zgfs", "/.growfschecksum"};
         hash::t_sha1 sha1;
         for (unsigned i = 0; i < 3; ++i) {
            const string src_path = cat_path + gfs_files[i];
            if (sha1_file(src_path.c_str(), sha1.digest) == 0) {
               const string dst_path = dir_data + "/" + sha1.to_string().substr(0, 2) + "/" +
               sha1.to_string().substr(2);
               if (file_copy(src_path.c_str(), dst_path.c_str()) != 0) {
                  cerr << "Warning: could not store " << src_path << " in " << dst_path << endl;
               }
            } else {
               cerr << "Warning: could not checksum " << src_path << endl;
            }
         }
      }
   }
   
   /* Last-modified time stamp */
   if (!catalog::update_lastmodified(ci.id)) {
      cerr << "Warning, failed to update last modified time stamp" << endl;
   }
   /* Current revision */
   if (!catalog::inc_revision(ci.id)) {
      cerr << "Warning, failed to increase revision" << endl;
   }
   /* Previous revision */
   map<char, string> ext_chksum;
   if (parse_keyval(cat_path + "/.cvmfspublished", ext_chksum)) {
      map<char, string>::const_iterator i = ext_chksum.find('C');
      if (i != ext_chksum.end()) {
         hash::t_sha1 sha1_previous;
         sha1_previous.from_hash_str(i->second);
         if (!catalog::set_previous_revision(ci.id, sha1_previous)) {
            cerr << "Warning, failed store previous catalog revision " << sha1_previous.to_string() 
                 << endl;
         }
      } else {
         cerr << "Warning, failed to find catalog SHA1 key in .cvmfspublished" << endl;
      }
   }
      
   /* Compress catalog */
   const string src_path = cat_path + "/.cvmfscatalog.working";
   const string dst_path = dir_data + "/txn/compressing.catalog";
   //const string dst_path = cat_path + "/.cvmfscatalog";
   hash::t_sha1 sha1;
   FILE *fsrc = NULL, *fdst = NULL;
   int fd_dst;
   if (!(fsrc = fopen(src_path.c_str(), "r")) ||
       ((fd_dst = open(dst_path.c_str(), O_CREAT | O_TRUNC | O_RDWR, plain_file_mode)) < 0) ||
       !(fdst = fdopen(fd_dst, "w")) ||
       (compress_file_fp_sha1(fsrc, fdst, sha1.digest) != 0))
   {
      cerr << "Warning: could not compress catalog " << src_path << endl;
   } else {
      const string sha1str = sha1.to_string();
      const string hash_name = sha1str.substr(0, 2) + "/" + sha1str.substr(2) + "C";
      const string cache_path = dir_data + "/" + hash_name;
      if (rename(dst_path.c_str(), cache_path.c_str()) != 0) {
         cerr << "Warning: could not store catalog in data store as " << cache_path << endl;
      }
      const string entry_path = cat_path + "/.cvmfscatalog"; 
      unlink(entry_path.c_str());
      if (symlink(("data/" + hash_name).c_str(), entry_path.c_str()) != 0) {
         cerr << "Warning: could not create symlink to catalog " << cache_path << endl;
      }
   }
   if (fsrc) fclose(fsrc);
   if (fdst) fclose(fdst);
   
   /* Remove pending certificate */
   unlink((cat_path + "/.cvmfspublisher.x509").c_str());   
   
   /* Create extended checksum */
   FILE *fpublished = fopen((cat_path + "/.cvmfspublished").c_str(), "w");
   if (fpublished) {
      string fields = "C" + sha1.to_string() + "\n";
      hash::t_md5 md5(catalog::mangled_path(clg_path));
      fields += "R" + md5.to_string() + "\n";
      
      /* Mucro catalogs */
      catalog::t_dirent d;
      if (!catalog::lookup_unprotected(md5, d))
         cerr << "Warning, failed to find root entry" << endl;
      fields += "L" + d.checksum.to_string() + "\n";
      const uint64_t ttl = catalog::get_ttl(catalog::lookup_catalogid_unprotected(md5));
      ostringstream strm_ttl;
      strm_ttl << ttl;
      fields += "D" + strm_ttl.str() + "\n";
      
      /* Revision */
      ostringstream strm_revision;
      strm_revision << catalog::get_revision();
      fields += "S" + strm_revision.str() + "\n";
      
      if (fwrite(&(fields[0]), 1, fields.length(), fpublished) != fields.length())
         cerr << "Warning, failed to write extended checksum" << endl;
      fclose(fpublished);
   } else {
      cerr << "Warning, failed to write extended checksum" << endl;
   }
   
   /* Update registered catalog SHA1 in nested catalog */
   if (ci.parent_id >= 0) {
      if (!catalog::update_nested_sha1(ci.parent_id, catalog::mangled_path(clg_path), sha1)) {
         cerr << "Warning, failed to register modified catalog at " << clg_path
              << " in parent catalog" << endl;
      }
   }

   /* Compress and write SHA1 checksum */
   char chksum[40];
   int lchksum = 40;
   memcpy(chksum, &((sha1.to_string())[0]), 40);
   void *compr_buf = NULL;
   size_t compr_size;
   if (compress_mem(chksum, lchksum, &compr_buf, &compr_size) != 0) {
      cerr << "Warning: could not compress catalog checksum" << endl;
   }
   
   FILE *fsha1 = NULL;
   int fd_sha1;
   if (((fd_sha1 = open((cat_path + "/.cvmfschecksum").c_str(), O_CREAT | O_TRUNC | O_RDWR, plain_file_mode)) < 0) ||
       !(fsha1 = fdopen(fd_sha1, "w")) ||
       (fwrite(compr_buf, 1, compr_size, fsha1) != compr_size))
   {
      cerr << "Warning: could not store checksum at " <<  cat_path << endl;
   }
   
   if (fsha1) fclose(fsha1);
   if (compr_buf) free(compr_buf);
}


static void add_path_with_parent(string clg_path, set<string> &to) {
   while (to.insert(clg_path).second) {
      clg_path = get_parent_path(clg_path);
   }
}


static bool move_to_datastore(const string &source, const string &suffix, 
                              const string &dir_data, hash::t_sha1 &hash) 
{
   bool result = false;

   /* Create temporary file */
   const string templ = dir_data + "/txn/compressing.XXXXXX";
   char *tmp_path = (char *)smalloc(templ.length() + 1);
   strncpy(tmp_path, templ.c_str(), templ.length() + 1);
   int fd_dst = mkstemp(tmp_path);
   
   if ((fd_dst >= 0) && (fchmod(fd_dst, plain_file_mode) == 0)) {
      /* Compress and calculate SHA1 */
      FILE *fsrc = NULL, *fdst = NULL;
      if ( (fsrc = fopen(source.c_str(), "r")) && 
          (fdst = fdopen(fd_dst, "w")) &&
          (compress_file_fp_sha1(fsrc, fdst, hash.digest) == 0) )
      {
         const string sha1str = hash.to_string();
         const string cache_path = dir_data + "/" + sha1str.substr(0, 2) + "/" + 
                                   sha1str.substr(2) + suffix;
         fflush(fdst);
         if (rename(tmp_path, cache_path.c_str()) != 0) {
            unlink(tmp_path);
            cerr << "Warning: could not rename " << tmp_path << " to " << cache_path << endl;
         } else {
            result = true;
         }
      } else {
         cerr << "Warning: could not compress " << source << endl;
      }
      if (fsrc) fclose(fsrc);
      if (fdst) fclose(fdst);
   } else {
      cerr << "Warning: could not create temporary file " << templ << endl;
      result = false;
   }
   free(tmp_path); 
   
   return result;
}
               
void createChangesetFromChangelog(ifstream &fjournal) {
   /* Main loop, walk through journal lines and build change sets */
   cout << "Parsing file system change log... " << flush;
   string line;
   int no_lines = 0;
   while (getline(fjournal, line)) {
      no_lines++;

      /* Parse line */
      if (line.length() < 4) {
         cerr << "Warning: parse error in line " << no_lines << endl;
         continue;
      }

      char object;
      char operation;
      char result;
      string path1;
      string path2;

      object = line[0];
      operation = line[1];
      result = line[2];
      if (result == 'F') continue; ///< We process only sucessful calls

      unsigned i;
      for (i = 3; i < line.length(); ++i) {
         if (line[i] == '\0') break;
      }
      if ((i == 3) || (i > line.length()-2)) {
         cerr << "Warning: parse error in line " << no_lines << endl;
         continue;
      }
      path1 = line.substr(3, i-3);
      path2 = line.substr(i+1, line.length()-(i+2));

      /* skip operations inside moved-in directories */
      bool skip = false;
      for (set<string>::const_iterator i = move_in.begin(), iEnd=move_in.end();
           i != iEnd; ++i)
      {
         if ( ((operation == 'I') && (in_subtree(*i, path2))) ||
              ((operation != 'I') && (in_subtree(*i, path1))) )
         {
            skip = true;
            break;
         }
      }
      if (skip) continue;

      /* Fill change sets, walk through all possible events */
      switch (operation) {
         case 'C':
            switch (object) {
               case 'D':
                  dir_add.insert(path1);
                  break;
               case 'R':
                  reg_add.insert(path1);
                  break;
               case 'L':
                  sym_add.insert(path1);
                  break;
               case 'U':
                  fil_add.insert(path1);
                  break;
               default:
                  cerr << "Warning: unsupported file type in line " << no_lines << endl;
            }
            break;
         case 'D':
            switch (object) {
               case 'D':
                  rem_path(path1, dir_touch);
                  if (!rem_path(path1, dir_add) && !rem_path(path1, move_in))
                     dir_rem.insert(path1);
                  break;
               case 'R':
               case 'L':
               case 'U':
                  rem_path(path1, reg_touch);
                  if (!rem_path(path1, reg_add) && !rem_path(path1, sym_add) && 
                      !rem_path(path1, fil_add) && !rem_path(path1, move_in))
                  {
                     fil_rem.insert(path1);
                  }
                  break;
               default:
                  cerr << "Warning: unsupported file type in line " << no_lines << endl;
            }
            break;
         case 'A':
         case 'T':
            switch (object) {
               case 'D':
                  if ((dir_add.find(path1) == dir_add.end()) && 
                      (move_in.find(path1) == move_in.end()))
                  {
                     dir_touch.insert(path1);
                  }
                  break;
               case 'R':
                  if ((reg_add.find(path1) == reg_add.end()) && 
                      (fil_add.find(path1) == fil_add.end()) &&
                      (move_in.find(path1) == move_in.end()))
                  {
                     reg_touch.insert(path1);
                  }
                  break;
               case 'L':
                  /* remove and add */
                  if ((sym_add.find(path1) == sym_add.end()) && 
                      (fil_add.find(path1) == fil_add.end()) &&
                      (move_in.find(path1) == move_in.end()))
                  {
                     fil_rem.insert(path1);
                     sym_add.insert(path1);
                  }
                  break;
               default:
                  cerr << "Warning: unsupported file type in line " << no_lines << endl;
            }
            break;
         case 'I':
            switch (object) {
               case 'R':
                  reg_add.insert(path2);
                  replace_candidate.insert(path2);
                  break;
               case 'L':
                  sym_add.insert(path2);
                  replace_candidate.insert(path2);
                  break;
               default:
                  move_in.insert(path2);
                  break;
            }
            break;
         case 'O': {
            switch (object) {
               case 'R':
                  rem_path(path1, reg_touch);
                  if (!rem_path(path1, reg_add) && !rem_path(path1, fil_add) &&
                      !rem_path(path1, move_in)) 
                  {
                     fil_rem.insert(path1);
                  }
                  break;
               case 'L':
                  if (!rem_path(path1, sym_add) && !rem_path(path1, fil_add) &&
                      !rem_path(path1, move_in)) 
                  {
                     fil_rem.insert(path1);
                  }
                  break;
               default:
                  if (!rem_path(path1, move_in))
                     move_out.insert(path1);

                  /* Remove all previous operations on that path */
                  set<string> *s[] = {&move_in, &dir_add, &dir_touch, &dir_rem, &reg_add, &reg_touch,
                                      &sym_add, &fil_add, &fil_rem};
                  for (unsigned j = 0; j < sizeof(s)/sizeof(s[0]); ++j) {
                     for (set<string>::iterator k = s[j]->begin();
                          k != s[j]->end(); )
                     {
                        if ((path1 == *k) || (in_subtree(path1, *k)))
                           s[j]->erase(k++);
                        else
                           ++k;
                     }
                  }
                  break;
            }
            break;
         }
         default:
            cerr << "Warning: unsupported operation in line " << no_lines << endl;
      }
   }
   cout << no_lines << " lines" << endl;
}

void createChangesetFromOverlayDirectory(string dir_overlay) {
	cvmfs::UnionFilesystemSync *worker = new cvmfs::SyncAufs1("/cvmfs", dir_overlay);
	
	cout << "Traversing copy on write overlay directory... " << endl;

	if (not worker->goGetIt()) {
		cerr << "something went wrong while creating changeset" << endl;
	}
	
	cvmfs::Changeset myChangeset = worker->getChangeset();
	
	dir_add   = myChangeset.dir_add;
	dir_touch = myChangeset.dir_touch;
	dir_rem   = myChangeset.dir_rem;
	reg_add   = myChangeset.reg_add;
	reg_touch = myChangeset.reg_touch;
	sym_add   = myChangeset.sym_add;
	fil_add   = myChangeset.fil_add;
	fil_rem   = myChangeset.fil_rem;
	
	delete worker;
}

static void usage() {
   cout << "CernVM-FS sync shadow tree with repository" << endl;
   cout << "Usage: cvmfs_sync -s <shadow dir> -r <repository store> -l <file system change log file>" << endl
        << "                  [-p(rint change set)] [-d(ry run)] [-i <immutable dir(,dir)*>] [-c(ompat catalog)]" << endl 
        << "                  [-k(ey file)] [-z (lazy attach of catalogs)] [-b(ookkeeping of dirty catalogs)]" << endl
        << "                  [-t <threads>] [-m(ucatalogs)]" << endl << endl
        << "Make sure that a 'data' and a 'catalogs' subdirectory exist in your repository store." << endl
        << "Also, your webserver must be able to follow symlinks in the catalogs subdirectory." << endl
        << "For Apache, you can add 'Options +FollowSymLinks' to a '.htaccess' file." 
        << endl << endl;
}

int main(int argc, char **argv) {
   if ((argc < 2) || (string(argv[1]) == "-h") || (string(argv[1]) == "--help") ||
       (string(argv[1]) == "-v") || (string(argv[1]) == "--version"))
   {
      usage();
      return 0;
   }
   
   string dir_shadow;
   string dir_data;
   string dir_catalogs;
   string dir_overlay; // << path to a union file system overlay directory (copy on write)
   string keyfile;
   ifstream fjournal;
   ofstream fbookkeeping;
   set<string> bookkeeping;
   bool useJournal = false;
   bool useOverlay = false;
   bool print_cs = false;
   bool dry_run = false;
   bool compat_catalog = false;
   bool lazy_attach = false;
   int sync_threads = 0;
   bool mucatalogs = false;
   
   umask(022);
   
   if (!monitor::init(".", false)) {
      cerr << "Failed to init watchdog" << cerr;
   }
   monitor::spawn();
   
   char c;
   while ((c = getopt(argc, argv, "s:r:l:o:pdi:ck:zb:t:m")) != -1) {
      switch (c) {
         case 's':
            dir_shadow = canonical_path(optarg);
            break;
         case 'r': {
            const string path = canonical_path(optarg);
            dir_data = path + "/data";
            dir_catalogs = path + "/catalogs";
            break;
         }
         case 'l':
            fjournal.open(optarg);
			useJournal = true;
            break;
         case 'o':
            dir_overlay = canonical_path(optarg);
			useOverlay = true;
			break;
         case 'p':
            print_cs = true;
            break;
         case 'd':
            dry_run = true;
            break;
         case 'i': {
            char *token = strtok(optarg, ",");
            while (token != NULL) {
               immutables.insert(canonical_path(token));
               token = strtok(NULL, ",");
            }
            break;
         }
         case 'c':
            compat_catalog = true;
            break;
         case 'k':
            keyfile = optarg;
            break;
         case 'z':
            lazy_attach = true;
            break;
         case 'b': {
            ifstream input_dirty_clg;
            input_dirty_clg.open(optarg, ios_base::in);
            if (!input_dirty_clg.is_open()) {
               cerr << "failed to open bookkeeping file for reading " << optarg << 
                       " (" << errno << ")" << endl;
               return 2;
            }
            string line;
            while (getline(input_dirty_clg, line))
               bookkeeping.insert(line);
            input_dirty_clg.close();
            fbookkeeping.open(optarg, ios_base::out | ios_base::app);
            if (!fbookkeeping.is_open()) {
               cerr << "failed to open bookkeeping file for appending " << optarg << 
                       " (" << errno << ")" << endl;
               return 2;
            }
            break;
         }
         case 't':
            sync_threads = atoi(optarg);
            break;
         case 'm':
            mucatalogs = true;
            break;
         case '?':
         default:
            usage();
            return 1;
      }
   }
   
   /* Sanity checks */
   if (useJournal && !fjournal.is_open()) {
      cerr << "specified change log file not found" << endl;
      return 2;
   }
   if (useOverlay && get_file_type(dir_overlay) != FT_DIR) {
      cerr << "overlay (copy on write) directory does not exist" << endl;
      return 2;
   }
   if (get_file_type(dir_shadow) != FT_DIR) {
      cerr << "shadow directory does not exist" << endl;
      return 2;
   }
   if (get_file_type(dir_data) != FT_DIR) {
      cerr << "data store directory does not exist" << endl;
      return 2;
   }
   if (get_file_type(dir_catalogs) != FT_DIR) {
      cerr << "catalog store directory does not exist" << endl;
      return 2;
   }
      
   /* Init stuff */
   if (!make_cache_dir(dir_data, 0755)) {
      cerr << "could not initialize data store" << endl;
      return 3;
   }
   if (!init_catalogs(dir_catalogs, dir_shadow, !lazy_attach)) {
      cerr << "could not initialize catalog store" << endl;
      return 3;
   }
   
   /* build up a change set */
   if (useJournal) {
		createChangesetFromChangelog(fjournal);
   } else if (useOverlay) {
		createChangesetFromOverlayDirectory(dir_overlay);
   } else {
		cerr << "no changes in filesystem provided" << endl;
		return 1;
   } 
   
   /* Lazy attach of catalogs, just load the subtree where things happen.
      Careful, breaks cross-catalog links! */
   if (lazy_attach) {
      /* Initially: nested paths of root catalog */
      cout << "Loading required file catalogs..." << endl;
      open_catalogs[dir_shadow].dirty = true;
      map<string, int> all_nested_paths; /* This map is path, parent id */
      vector<string> current_nested_paths;
      if (!catalog::ls_nested(0, current_nested_paths)) {
         cerr << "Error: failed to list nested catalogs" << endl;
         return 3;
      }
      for (vector<string>::const_iterator i = current_nested_paths.begin(), iEnd = current_nested_paths.end();
           i != iEnd; ++i)
      {
         all_nested_paths[*i] = 0;
      }
      
      if (all_nested_paths.empty())
         goto catalogs_attached;

      set<string> *s[] = {&move_out, &move_in, &dir_add, &dir_touch, &dir_rem, &reg_add, 
                          &reg_touch, &sym_add, &fil_add, &fil_rem};
      for (unsigned i = 0; i < sizeof(s)/sizeof(s[0]); ++i) {
         for (set<string>::const_iterator j = s[i]->begin(), jEnd = s[i]->end();
              j != jEnd; ++j)
         {
            /* Strip shadow dir */
            const string spot_path = j->substr(dir_shadow.length()) + "/";
            
            /* Is the path on a nested subtree? */
            pair<string, int> on_nested; /* This map is path, parent id */
            do {
               on_nested.first = "";
               on_nested.second = -1;
               
               for (map<string, int>::const_iterator k = all_nested_paths.begin(), kEnd = all_nested_paths.end();
                    k != kEnd; ++k)
               {
                  //cout << "Checking path " << spot_path << " on nested path " << k->first << endl;
                  if (spot_path.find(k->first + "/", 0) == 0) {
                     on_nested = *k;
                     break;
                  }
               }
               
               if (on_nested.first != "") {
                  /* Attach nested catalog */
                  const string nested_path =  dir_catalogs + on_nested.first + "/.cvmfscatalog.working";
                  cout << "Attaching " << nested_path << endl;
                  if (!catalog::attach(nested_path, "", false, false)) {
                     cerr << "Error: failed to load nested catalog at " << nested_path << endl;
                     return false;
                  }
                  t_catalog_info ci;
                  ci.dirty = true;
                  ci.id = catalog::get_num_catalogs()-1;
                  ci.parent_id = on_nested.second;
                  open_catalogs[dir_shadow + on_nested.first] = ci;
                  
                  /* Re-organize all_nested_paths */
                  all_nested_paths.erase(on_nested.first);
                  current_nested_paths.clear();
                  if (!catalog::ls_nested(catalog::get_num_catalogs()-1, current_nested_paths)) {
                     cerr << "Error: failed to list nested catalogs" << endl;
                     return 3;
                  }
                  for (vector<string>::const_iterator i = current_nested_paths.begin(), iEnd = current_nested_paths.end();
                       i != iEnd; ++i)
                  {
                     all_nested_paths[*i] = catalog::get_num_catalogs()-1;
                  }
                  
                  /* Short way out, all catalogs attached */
                  if (all_nested_paths.empty())
                     goto catalogs_attached;
               }
            } while (on_nested.first != "");
            
            /* For move-out paths: load all remaining nested catalogs on this subtree */
            if (i == 0) {
               map<string, int> remaining; /* This maps path, catalog id */
               for (map<string, int>::const_iterator k = all_nested_paths.begin(), kEnd = all_nested_paths.end();
                    k != kEnd; ++k)
               {
                  if (k->first.find(spot_path, 0) == 0) {
                     const string nested_path = dir_catalogs + k->first + "/.cvmfscatalog.working";
                     cout << "Attaching " << nested_path << endl;
                     if (!catalog::attach(nested_path, "", false, false)) {
                        cerr << "Error: failed to load nested catalog at " << nested_path << endl;
                        return false;
                     }
                     t_catalog_info ci;
                     ci.dirty = true;
                     ci.id = catalog::get_num_catalogs()-1;
                     ci.parent_id = k->second;
                     open_catalogs[dir_shadow + k->first] = ci;
                     
                     remaining[k->first] = catalog::get_num_catalogs()-1;
                  }
               }
               
               for (map<string, int>::const_iterator k = remaining.begin(), kEnd = remaining.end();
                    k != kEnd; ++k)
               {
                  all_nested_paths.erase(k->first);
                  if (!attach_nested(dir_catalogs, dir_shadow, 
                                     k->second, true))
                  {
                     cerr << "Failed to attach nested catalogs" << endl;
                     return 3;
                  }
               }
            }
         }
      }
   }
catalogs_attached:

   cout << "Post-processing file system change log..." << endl;

   /* Squeeze out move out paths */
   for (set<string>::const_iterator i = move_out.begin(), iEnd = move_out.end();
        i != iEnd; ++i)
   {
      hash::t_md5 md5(catalog::mangled_path(abs2clg_path(*i, dir_shadow)));
      catalog::t_dirent d;
      if (catalog::lookup_unprotected(md5, d)) {
         if (d.flags & catalog::DIR) {
            cout << "Collecting file system entries from move-out path " << *i << endl;
            squeeze_out(*i, dir_shadow);
         } else if (d.flags & catalog::FILE) {
            fil_rem.insert(*i);
         } else {
            cerr << "Warning: unexpected file type of " << (*i) << endl;
         }
      }
   }
   move_out.clear();

         
   /* Process move in paths */
   for (set<string>::const_iterator i = move_in.begin(), iEnd = move_in.end();
        i != iEnd; ++i)
   {
      switch (get_file_type(*i)) {
         case FT_DIR:
            cout << "Inspecting move-in path " << *i << endl;
            hieve_add(*i);
            break;
         case FT_REG:
            reg_add.insert(*i);
            break;
         case FT_SYM:
            sym_add.insert(*i);
            break;
         default:
            cerr << "Warning: unexpected file type of " << (*i) << endl;
      }
   }
   move_in.clear();
   
   /* Figure out replaced files */
   for (set<string>::iterator i = replace_candidate.begin(), iEnd = replace_candidate.end();
        i != iEnd; ++i)
   {
      hash::t_md5 md5(catalog::mangled_path(abs2clg_path(*i, dir_shadow)));
      catalog::t_dirent d;
      if (catalog::lookup_unprotected(md5, d)) {
         fil_rem.insert(*i);
      }
   }
   replace_candidate.clear();
   
   /* Separate touched new files from touched existing files */
   for (set<string>::iterator i = reg_touch.begin();
        i != reg_touch.end(); )
   {
	
      if (get_file_name(*i) == ".cvmfscatalog") {
         /* Separate new catalog from touched ones (force dirty) */
         const string p = get_parent_path(*i);
         map<string, t_catalog_info>::iterator s = open_catalogs.find(p);
         if (s == open_catalogs.end())
            clg_add.insert(p);
         else
            s->second.dirty = true;
         
         reg_touch.erase(i++);
         continue;
      }
      
      hash::t_md5 md5(catalog::mangled_path(abs2clg_path(*i, dir_shadow)));
      catalog::t_dirent d;
		
      if (!catalog::lookup_unprotected(md5, d)) {
         reg_add.insert(*i);
         reg_touch.erase(i++);
      } else {
         ++i;
      }
   }
   
   /* Separate hard links to symlinks from hard links to regular files */
   for (set<string>::const_iterator i = fil_add.begin(), iEnd = fil_add.end();
        i != iEnd; ++i)
   {
      switch (get_file_type(*i)) {
         case FT_REG:
            reg_add.insert(*i);
            break;
         case FT_SYM:
            sym_add.insert(*i);
            break;
         default:
            cerr << "Warning: unexpected file type of " << (*i) << endl;
      }
   }
   fil_add.clear();
   
   /* Find out about new/removed/dirty catalogs */
   for (set<string>::iterator i = reg_add.begin();
        i != reg_add.end(); )
   {
      if (get_file_name(*i) == ".cvmfscatalog") {
         const string p = get_parent_path(*i);
         if (p != dir_shadow) {
            clg_add.insert(p);
            set_dirty(get_parent_path(p));
         }
         reg_add.erase(i++);
      } else {
         ++i;
      }
   }
   for (set<string>::iterator i = fil_rem.begin();
        i != fil_rem.end(); )
   {
      if (get_file_name(*i) == ".cvmfscatalog") {
         const string p = get_parent_path(*i);
         if (p != dir_shadow) {
            clg_rem.insert(p);
            set_dirty(get_parent_path(p));
         } else {
            cerr << "Warning: will not delete root catalog" << endl;
         }
         fil_rem.erase(i++);
      } else {
         ++i;
      }
   }
   /* For lazy attach, this is already done */
   if (!lazy_attach) {
      set<string> *s[] = {&move_in, &move_out, &dir_add, &dir_touch, &dir_rem, &reg_add, 
                          &reg_touch, &sym_add, &fil_rem};
      for (unsigned i = 0; i < sizeof(s)/sizeof(s[0]); ++i) {
         for (set<string>::const_iterator j = s[i]->begin(), jEnd = s[i]->end();
              j != jEnd; ++j)
         {
            set_dirty(*j);
         }
      }
   }

   /* Everything collected, print change sets */
   if (print_cs) {
      cout << endl; 
      cout << "New directories: " << endl;
      print_set(dir_add);
      cout << "New regular files: " << endl;
      print_set(reg_add);
      cout << "New symlinks: " << endl;
      print_set(sym_add);
      
      cout << "Touched directories: " << endl;
      print_set(dir_touch);
      cout << "Touched regular files: " << endl;
      print_set(reg_touch);
      
      cout << "Removed directories: " << endl;
      print_set(dir_rem);
      cout << "Removed files: " << endl;
      print_set(fil_rem);
      
      cout << "New catalogs: " << endl;
      print_set(clg_add);
      cout << "Removed catalogs: " << endl;
      print_set(clg_rem);
      cout << "Dirty catalogs: " << endl;
      for (map<string, t_catalog_info>::const_iterator i = open_catalogs.begin(), iEnd = open_catalogs.end();
           i != iEnd; ++i)
      {
         if (i->second.dirty) {
            cout << i->first << endl;
         }
      }
      cout << endl;
   }
   
   /* Real work: make changes to the catalog, compress files */
   if (!dry_run) {
      long count = 0;
      catalog::t_dirent d;
      PortableStat64 info;
      prels.insert("");
   
      /* Merge obsolete catalogs */
      for (set<string>::const_iterator i = clg_rem.begin(), iEnd = clg_rem.end();
           i != iEnd; ++i)
      {
         cout << "Merging catalogs at " << *i << endl;
         clg_merge(*i, dir_shadow, dir_catalogs);
      }
      
      for (int i = 0; i < catalog::get_num_catalogs(); ++i) 
         catalog::transaction(i);
      
      
      /* Delete obsolete entries */
      cout << "Step 1 - Deleting obsolete file and directory entries " 
           << "(" << (dir_rem.size()+fil_rem.size()) << " entries): " << flush;
      set<string>::const_iterator iRem = dir_rem.empty() ? fil_rem.begin() : dir_rem.begin();
      const set<string>::const_iterator iEndDirRem = dir_rem.end();
      const set<string>::const_iterator iEndFilRem = fil_rem.end();
      while (iRem != iEndFilRem) {
         const string clg_path = abs2clg_path(*iRem, dir_shadow);
         hash::t_md5 md5(catalog::mangled_path(clg_path));
         if (!catalog::lookup_unprotected(md5, d)) {
            cerr << "Warning: " << *iRem << " is not in the catalogs" << endl;
         } else {
            if (!catalog::unlink_unprotected(md5, d.catalog_id)) {
               cerr << "Warning: " << "could not remove " << *iRem << " from catalog" << endl;
            }
         }
         if ((count % 1000) == 0) cout << "." << flush;
         ++count;
         if (++iRem == iEndDirRem) iRem = fil_rem.begin();
         
         add_path_with_parent(get_parent_path(clg_path), prels);
      }
      fil_rem.clear();
      /* Correct LS precalculation list, removed directories shouldn't be in there */
      for (set<string>::const_iterator i = dir_rem.begin(), iEnd = dir_rem.end();
           i != iEnd; ++i)
      {
         prels.erase(abs2clg_path(*i, dir_shadow));
      }
      dir_rem.clear();
      cout << endl;
      
      
      /* Insert/Update directories and symlinks */
      cout << "Step 2 - Inserting new directories and symlinks " 
           << "(" << dir_add.size() + sym_add.size() << " entries): " << flush;
      count = 0;
      set<string>::const_iterator iAdd = dir_add.empty() ? sym_add.begin() : dir_add.begin();
      const set<string>::const_iterator iEndDirAdd = dir_add.end();
      const set<string>::const_iterator iEndSymAdd = sym_add.end();
      bool in_symlinks = dir_add.empty();
      while (iAdd != iEndSymAdd) {
         const string clg_path = abs2clg_path(*iAdd, dir_shadow);
         if (get_file_info(*iAdd, &info)) {
            hash::t_md5 md5(catalog::mangled_path(clg_path));
            hash::t_md5 p_md5(catalog::mangled_path(get_parent_path(clg_path)));
            if (!catalog::lookup_unprotected(md5, d)) {
               if (catalog::lookup_unprotected(p_md5, d)) {
                  catalog::t_dirent new_d(d.catalog_id, get_file_name(clg_path), "", catalog::DIR, info.st_ino, info.st_mode, 
                                          info.st_size, info.st_mtime, hash::t_sha1());
                  if (S_ISLNK(info.st_mode)) {
                     new_d.flags = catalog::FILE | catalog::FILE_LINK;
                     
                     char slnk[PATH_MAX+1];
                     ssize_t l = readlink((*iAdd).c_str(), slnk, PATH_MAX);
                     if (l >= 0) {
                        slnk[l] = '\0';
                        new_d.symlink = slnk;
                     } else {
                        cerr << "Warning: could not read link " << *iAdd << endl;
                        continue;
                     }
                  }
                  if (!catalog::insert_unprotected(md5, p_md5, new_d)) {
                     cerr << "Warning: could not insert directory " << *iAdd << endl;
                  }
               } else {
                  cerr << "Warning: " << *iAdd << " is dangling" << endl;
               }
            } else {
               cerr << "Warning: " << *iAdd << " is already in catalog" << endl;
            }
         }
         if ((count % 1000) == 0) cout << "." << flush;
         ++count;
         
         if (in_symlinks)
            add_path_with_parent(get_parent_path(clg_path), prels);
         else 
            add_path_with_parent(clg_path, prels);
         
         if (++iAdd == iEndDirAdd) {
            iAdd = sym_add.begin();
            in_symlinks = true;
         }
      }
      dir_add.clear();
      sym_add.clear();
      cout << endl;
      
      cout << "Step 3 - Updating touched directories " 
           << "(" << dir_touch.size() << " entries): " << flush;
      count = 0;
      for (set<string>::const_iterator i = dir_touch.begin(), iEnd = dir_touch.end(); 
           i != iEnd; ++i, ++count)
      {
         const string clg_path = abs2clg_path(*i, dir_shadow);
         if (get_file_info(*i, &info)) {
            hash::t_md5 md5(catalog::mangled_path(clg_path));
            if (catalog::lookup_unprotected(md5, d)) {
               d.inode = info.st_ino;
               d.mode = info.st_mode;
               d.size = info.st_size;
               d.mtime = info.st_mtime;
               if (!catalog::update_unprotected(md5, d)) { 
                  cerr << "Warning: could not update directory " << *i << endl;
               }
            } else {
               cerr << "Warning: directory " << *i << " was not in catalogs" << endl;
            }
         }
         if ((count % 1000) == 0) cout << "." << flush;
         
         add_path_with_parent(clg_path, prels);
      }
      dir_touch.clear();
      cout << endl;
      
      
      
      /* Create nested catalogs */
      for (set<string>::const_iterator i = clg_add.begin(), iEnd = clg_add.end();
           i != iEnd; ++i)
      {     
         /* Mimick directory structure in /pub/catalogs/ */
         if (!mkdir_deep(dir_catalogs + abs2clg_path(*i, dir_shadow), plain_dir_mode)) {
            cerr << "Warning: cannot create catalog directory structure " << *i << endl;
            continue;
         }
         
         const string clg_path = abs2clg_path(*i, dir_shadow);
         const hash::t_md5 md5(catalog::mangled_path(clg_path));
         const hash::t_md5 p_md5(catalog::mangled_path(get_parent_path(clg_path)));
         catalog::t_dirent d;
         catalog::t_dirent n;
         
         /* Find the path in current catalogs */
         if (!catalog::lookup_unprotected(md5, d)) {
            cerr << "Warning: cannot create nested catalog in " << *i << ", directory is dangling" << endl;
            continue;
         }
         n = d;
         d.flags |= catalog::DIR_NESTED;
         n.catalog_id = catalog::get_num_catalogs();
         n.flags |= catalog::DIR_NESTED_ROOT;
         const string cat_path = dir_catalogs + clg_path + "/.cvmfscatalog.working";
         cout << "Creating new nested catalog " << cat_path << endl;
         
         /* Move entries in nested catalog */
         if (!catalog::update_unprotected(md5, d) ||
             !catalog::attach(cat_path, "", false, true) ||
             !catalog::set_root_prefix(clg_path, n.catalog_id),
             !catalog::insert_unprotected(md5, p_md5, n) ||
             !catalog::relink_unprotected(catalog::mangled_path(clg_path), catalog::mangled_path(clg_path)) ||
             !catalog::register_nested(d.catalog_id, catalog::mangled_path(clg_path)))
         {
            cerr << "Warning: error while creating nested catalog " << cat_path << endl;
            continue;
         }
         
         /* New one is dirty, we want to snapshot it later */
         t_catalog_info ci;
         ci.dirty = true;
         ci.id = catalog::get_num_catalogs()-1;
         ci.parent_id = d.catalog_id;
         
         /* Move registerd catalogs from parent to nested */
         vector<string> parent_nested;
         if (!catalog::ls_nested(ci.parent_id, parent_nested)) {
            cerr << "Warning: failed to list nested catalogs of parent catalog" << endl;
            continue;
         }
         for (unsigned j = 0; j < parent_nested.size(); ++j) {
            if (parent_nested[j].find(abs2clg_path(*i, dir_shadow) + "/", 0) == 0) {
               hash::t_sha1 nested_sha1;
               if (!catalog::lookup_nested_unprotected(ci.parent_id, parent_nested[j], nested_sha1)) {
                  cerr << "Warning: failed to lookup nested catalog of parent catalog" << endl;
                  continue;
               }
               if (!catalog::register_nested(ci.id, parent_nested[j]) || 
                   !catalog::update_nested_sha1(ci.id, parent_nested[j], nested_sha1) ||
                   !catalog::unregister_nested(ci.parent_id, parent_nested[j]))
               {
                  cerr << "Warning: failed to relink nested catalog" << endl;
                  continue;
               }
            }
         }
         
         /* Update open catalogs */
         for (map<string, t_catalog_info>::iterator j = open_catalogs.begin(), jEnd = open_catalogs.end(); 
              j != jEnd; ++j) 
         {
            if ((j->second.parent_id == ci.parent_id) &&
                j->first.find((*i) + "/", 0) == 0)
            {
               j->second.parent_id = ci.id;
            }
         }
         open_catalogs.insert(make_pair(*i, ci));
      }
      clg_add.clear();
      
      
      
      /* Insert/compress Files */
      cout << "Step 4 - Building file list " 
           << "(" << reg_add.size() + reg_touch.size() << " entries): " << flush;
      count = 0;
      set<string>::const_iterator iZip = reg_add.empty() ? reg_touch.begin() : reg_add.begin();
      const set<string>::const_iterator iEndRegAdd = reg_add.end();
      const set<string>::const_iterator iEndRegTouch = reg_touch.end();
      vector<t_cas_file> file_list;
      while (iZip != iEndRegTouch) {
         const string clg_path = abs2clg_path(*iZip, dir_shadow);
         hash::t_md5 p_md5(catalog::mangled_path(get_parent_path(clg_path)));
         catalog::t_dirent d_parent;
         
         /* Find parent entry */
         if (catalog::lookup_unprotected(p_md5, d_parent)) {
            if (get_file_info(*iZip, &info)) {
               t_cas_file file;
               file.path = *iZip;
               file.md5_path = hash::t_md5(catalog::mangled_path(clg_path));
               file.md5_parent = p_md5;
               file.dirent = catalog::t_dirent(d_parent.catalog_id, get_file_name(*iZip), "", catalog::FILE,
                                               info.st_ino, info.st_mode, info.st_size, info.st_mtime, hash::t_sha1());
               file_list.push_back(file);
            } else {
               cerr << "Warning: could not stat " << *iZip << endl;
            }
         } else {
            cerr << "Warning: dangling file entry " << *iZip << endl;
         }
         
         if ((count % 1000) == 0) cout << "." << flush;
         ++count;
         if (++iZip == iEndRegAdd) iZip = reg_touch.begin();
         
         add_path_with_parent(get_parent_path(clg_path), prels);
      }
      reg_add.clear();
      reg_touch.clear();
      cout << endl;
      
      cout << "Step 5 - Compressing and calculating content hashes ";
/*

TODO: something wrong here!

#ifdef _OPENMP
      if (sync_threads == 0) {
#pragma omp parallel
         {
            if (omp_get_thread_num() == 0)
               sync_threads = 2*omp_get_num_threads();
         }
      }
      cout << "using " << sync_threads << " threads ";
#elif

*/
      sync_threads = 1;
//#endif
      cout << "(" << file_list.size() << " files): " << flush;
//#pragma omp parallel for num_threads(sync_threads)
      for (int i = 0; i < (int)file_list.size(); ++i) {
         hash::t_sha1 sha1;
         if (move_to_datastore(file_list[i].path, "", dir_data, sha1))
            file_list[i].dirent.checksum = sha1;
         
         if ((i % 1000) == 0) {
//#pragma omp critical
            cout << "." << flush;
         }
      }
      cout << endl;
      
      cout << "Step 6 - Updating file catalogs " 
           << "(" << file_list.size() << " files): " << flush;
      for (unsigned i = 0; i < file_list.size(); ++i) {
         if ((i % 1000) == 0) cout << "." << flush;
         if (file_list[i].dirent.checksum == hash::t_sha1())
            continue;

         /* Update catalog */
         catalog::t_dirent tmp;
         if (!catalog::lookup_unprotected(file_list[i].md5_path, tmp)) {
            if (!catalog::insert_unprotected(file_list[i].md5_path, file_list[i].md5_parent, file_list[i].dirent)) {
               cerr << "Warning: could not insert file entry " << file_list[i].path << endl;
            }
         } else {
            if (tmp.inode != file_list[i].dirent.inode) {
               cerr << "Warning: inodes differ for " << file_list[i].path << ", fixing" << endl;
               if (!catalog::unlink_unprotected(file_list[i].md5_path, tmp.catalog_id) || 
                   !catalog::insert_unprotected(file_list[i].md5_path, file_list[i].md5_parent, file_list[i].dirent)) 
               {
                  cerr << "Warning: could not insert file entry " << file_list[i].path << endl;
               } 
            }
            if (!catalog::update_inode(file_list[i].dirent.inode, file_list[i].dirent.mode, 
                                       file_list[i].dirent.size, file_list[i].dirent.mtime, 
                                       file_list[i].dirent.checksum)) 
            {
               cerr << "Warning: could not update file entry " << file_list[i].path << " for inode " << file_list[i].dirent.inode << endl;
            }
         }
      }
      file_list.clear();
      cout << endl;
      
      
      /* Pre-calculate direcotry listings */
      if (mucatalogs) {
         cout << "Step 7 - Updating pre-calculated directory listings "
              << "(" << prels.size() << " directories): " << flush;
         /* Sorted from child to parent directories */
         count = 0;
         for (set<string>::const_iterator i = --prels.end(), iBegin = prels.begin();; --i) {
            //cerr << "this is " << *i << endl;
            truncate((dir_data + "/txn/ls").c_str(), 0);
            if (!catalog::make_ls(*i, dir_data + "/txn/ls")) {
               cerr << "Warning: could not create ls file for " << *i << endl;
            } else {
               hash::t_sha1 sha1;
               if (move_to_datastore(dir_data + "/txn/ls", "L", dir_data, sha1)) {
                  hash::t_md5 md5(catalog::mangled_path(*i));
                  catalog::t_dirent d;
                  if (!catalog::lookup_unprotected(md5, d)) {
                     cerr << "Warning: failed to find " << *i << " in catalogs" << endl;
                  } else {
                     d.checksum = sha1;
                     if (!catalog::update_unprotected(md5, d)) {
                        cerr << "Warning: failed to store directory listing for " << *i << endl;
                     }
                     //cout << "LS: " << *i << " is " << sha1.to_string() << endl;
                  }
               }
            }
            
            if ((count % 1000) == 0)
               cout << "." << flush;
            count++;
            
            if (i == iBegin)
               break;
         }
         prels.clear();
         cout << endl;
      }
      
      
      cout << "Commit changes to catalogs..." << endl;
      for (int i = 0; i < catalog::get_num_catalogs(); ++i) 
         catalog::commit(i);

      
      /* Snapshot dirty catalogs, sorted from nested to main */
      for (map<string, t_catalog_info>::const_iterator i = open_catalogs.end(), iBegin = open_catalogs.begin();; --i)
      {
         if (i->second.dirty) {
            clg_snapshot(i->first, dir_shadow, dir_catalogs, dir_data, compat_catalog, keyfile, i->second);
            if (fbookkeeping.is_open()) {
               const string real_path = dir_catalogs + abs2clg_path(i->first, dir_shadow) + "/.cvmfscatalog";
               if (bookkeeping.find(real_path) == bookkeeping.end()) {
                  fbookkeeping << real_path << endl;
               }
            }
         }
         if (i == iBegin)
            break;
      }
      open_catalogs.clear();
      if (fbookkeeping.is_open())
         fbookkeeping.close();
   }
   
   catalog::fini();
   monitor::fini();
   
   return 0;
}
