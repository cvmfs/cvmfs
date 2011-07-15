/*
 *  \file cvmfs_clgcmp.cc
 *  This tool to compare the content of a catalog with
 *  the content of a directory hieve.  No checksum checking
 *  is done.
 */
 
#define _FILE_OFFSET_BITS 64
#include "config.h"
#include "util.h"
#include "catalog.h"
extern "C" {
   #include "compression.h"
}

#include <string>
#include <cstdio>
#include <iostream>
#include <vector>
#include <deque>
#include <set>
#include <map>
#include <cassert>
#include <algorithm>
#include <stdint.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/dir.h>

using namespace std;

static void usage() {
   cout << "This tool checks a catalog against a directory hieve." << endl;
   cout << "No checksum verification is done." << endl;
   cout << "Usage: cvmfs_clgcmp <catalog> <directory>" << endl;
}


struct t_fsent {
   string name;
   mode_t mode;
   ino64_t ino;
   time_t mtime;
   off_t size;
   t_fsent(const string n, const mode_t m, const ino64_t i, const time_t t, const off_t s) : 
      name(n), mode(m), ino(i), mtime(t), size(s) {}; 
   bool operator <(const t_fsent &other) const {
      return string(this->name) < string(other.name);
   }
};


static vector<t_fsent> lsfs(const string dir) {
   vector<t_fsent> result;
   DIR *dp = opendir(dir.c_str());
   if (dp) {
      struct dirent64 *d;
      while ((d = readdir64(dp)) != NULL) {
         const string name = d->d_name;
         if ((name == ".") || (name == "..") || (name == ".cvmfscatalog")) continue;
         struct stat64 info;
         if (lstat64((dir + "/" + d->d_name).c_str(), &info) == 0) {
            result.push_back(t_fsent(d->d_name, info.st_mode, info.st_ino, info.st_mtime, info.st_size));
         }
      }
      closedir(dp);
   }
   sort(result.begin(), result.end());
   return result;
}


int main(int argc, char **argv) {
   if (argc < 3) {
      usage();
      return 1;
   }
   
   const string cwd = string(getcwd(NULL, 0));
   
   if (!catalog::init(0, 0) || !catalog::attach(argv[1], "", true, false)) {
      cerr << "could not load catalog" << endl;
      return 1;
   }
   /* Compare .cvmfscatalog and .cvmfscatalog.working */
   hash::t_sha1 sha1_working;
   FILE *fworking = fopen(argv[1], "r");
   if (!fworking || compress_file_sha1_only(fworking, sha1_working.digest) != 0) {
      cout << "Warning: failed to calculate hash of .cvmfscatalog.working" << endl;
   } else {
      map<char, string> main_published;
      const string published_path = get_parent_path(argv[1]) + "/.cvmfspublished";
      if (!parse_keyval(published_path, main_published)) {
         cout << "Warning: failed to open .cvmfspublished" << endl;
      } else {
         if (main_published['C'] != sha1_working.to_string()) {
            cout << "Warning: catalog has uncommitted changes" << endl;
         }
      }
   }
   if (fworking) fclose(fworking);
   
   if (chdir(argv[2]) != 0) {
      cerr << "could not switch to directory to compare." << endl;
      return 1;
   } 
   struct stat64 info_root;
   if (stat64(".", &info_root) != 0) {
      cerr << "could not inspect directory." << endl;
      return 1;
   }
   
   catalog::t_dirent root;
   hash::t_md5 md5(catalog::mangled_path(""));
   if (!catalog::lookup(md5, root)) {
      cerr << "could not find root entry in catalog." << endl;
      return 1;
   }
   
   vector<string> tmp;
   if (!catalog::ls_nested(0, tmp)) {
      cerr << "Failed to find nested catalogs" << endl;
      return 1;
   }
   
   set<string> nested_catalogs;
   for (unsigned i = 0; i < tmp.size(); ++i) {
      nested_catalogs.insert(tmp[i]);
   }
   if (tmp.size() != nested_catalogs.size()) {
      cout << "Warning: duplicate nested catalogs registered" << endl;
   }
   
   const time_t last_modified = catalog::get_lastmodified(0);
   const string root_prefix = catalog::get_root_prefix();
   
   /* Check root node */
   if (info_root.st_ino != root.inode) {
      cout << "root inode differs, " << root.inode << " in catalog but " 
           << info_root.st_ino << " on file system" << endl;
   }
   if (!(root.flags & catalog::DIR))
      cout << "Warning: root node supposed to be directory" << endl;
   if (root.flags & catalog::DIR_NESTED)
      cout << "Warning: root node NOT supposed to be nested directory" << endl;
   if ((root_prefix != "") && !(root.flags & catalog::DIR_NESTED_ROOT))
      cout << "Warning: root node supposed to be nested directory" << endl;
   if ((root_prefix == "") && (root.flags & catalog::DIR_NESTED_ROOT))
      cout << "Warning: root node NOT supposed to be nested directory" << endl;
   
   deque<string> bfs_next; ///< contains directories to look for
   bfs_next.push_back("");
   
   /* concurrent bfs in catalog and directory tree */
   uint64_t no_clg = 1;
   uint64_t no_fs = 1;
   uint64_t no_nested = 0;
   while (!bfs_next.empty()) {
      const string inspect = bfs_next.front();
      bfs_next.pop_front();
      
      vector<catalog::t_dirent> dir_clg = 
         catalog::ls(hash::t_md5(catalog::mangled_path(inspect)));
      sort(dir_clg.begin(), dir_clg.end());
      vector<t_fsent> dir_fs = lsfs("." + inspect);
      
      /* compare sorted directory contents in catalog and in fs */
      vector<catalog::t_dirent>::const_iterator itr_clg = dir_clg.begin();
      vector<t_fsent>::const_iterator itr_fs = dir_fs.begin();
      do {
         while ((itr_clg != dir_clg.end()) && 
                ((itr_fs == dir_fs.end()) || (itr_clg->name < itr_fs->name))) 
         {
            if ((itr_clg->name != ".growfschecksum") && (itr_clg->name != ".growfsdir") &&
                (itr_clg->name != ".growfsdir.zgfs"))
            {
               cout << inspect << "/" << itr_clg->name << 
                       " in catalog but not in file system" << endl;
               ++no_clg;
            }
            ++itr_clg;
         }
         while ((itr_fs != dir_fs.end()) && 
                ((itr_clg == dir_clg.end()) || (itr_fs->name < itr_clg->name))) 
         {
            cout << inspect << "/" << itr_fs->name << 
                    " in file system but not in catalog" << endl;
            ++itr_fs;
            ++no_fs;
         }
         
         while (((itr_clg != dir_clg.end()) && (itr_fs != dir_fs.end()))) {
            const string n_clg = itr_clg->name;
            const string n_fs = itr_fs->name;
            if (n_clg != n_fs) break;
            
            if (S_ISLNK(itr_fs->mode)) {
               if (!(itr_clg->flags & catalog::FILE_LINK)) {
                  cout << inspect << "/" << n_clg << ": mode differs!" << endl;
               }
            } else if (S_ISREG(itr_fs->mode)) {
               if (!(itr_clg->flags & catalog::FILE)) {
                  cout << inspect << "/" << n_clg << ": mode differs!" << endl;
               }
            } else if (S_ISDIR(itr_fs->mode)) {
               if (!(itr_clg->flags & catalog::DIR)) {
                  cout << inspect << "/" << n_clg << ": mode differs!" << endl;
               } else {
                  if (!(itr_clg->flags & catalog::DIR_NESTED)) {
                     bfs_next.push_back(inspect + "/" + n_clg);
                  } else {
                     /* Check registered nested catalog */
                     no_nested++;
                     const string path = root_prefix + inspect + "/" + n_clg;
                     hash::t_sha1 sha1_nested;
                     if (!catalog::lookup_nested_unprotected(0, path, sha1_nested)) {
                        cout << "Warning: nested catalog at " << path << " is not registered" << endl;
                     } else {
                        map<char, string> published;
                        string nested_clg_path = get_parent_path(argv[1]) + inspect + "/" + n_clg + "/.cvmfspublished";
                        if (nested_clg_path[0] != '/')
                           nested_clg_path = cwd + "/" + nested_clg_path;
                        if (!parse_keyval(nested_clg_path, published)) {
                           cout << "Warning: failed to open .cvmfspublished of nested catalog " << nested_clg_path << endl;
                        } else {
                           if (published['C'] != sha1_nested.to_string()) {
                              cout << "Warning: registered catalog does not match published on at " << path << endl;
                           }
                        }
                     }
                  }
               }
            } else {
               cout << inspect << "/" << n_fs << ": unsupported mode (" << itr_fs->mode << ")!" << endl;
            }
            
            /* Check meta data */
            const string path = inspect + "/" + n_fs;
            if (itr_clg->inode != itr_fs->ino)
               cout << path << ": inode differs, " << itr_clg->inode << " in catalog but " 
                    << itr_fs->ino << " on file system" << endl;
            /* Time and size not for directories */
            if (!S_ISDIR(itr_fs->mode)) {
               if (itr_clg->mtime != itr_fs->mtime)
                  cout << path << ": last modified differs, " << itr_clg->mtime << " in catalog but " 
                       << itr_fs->mtime << " on file system" << endl;
               if (itr_clg->mtime > (last_modified+120)) /* five minutes drift ok */
                  cout << "Warning: last modified of " << path 
                       << " newer than catalog's last modified" << endl;
               if ((off_t)itr_clg->size != itr_fs->size)
                  cout << path << ": size differs, " << itr_clg->size << " in catalog but " 
                       << itr_fs->size << " on file system" << endl; 
            }
            
            /* Check chunk */
            if (itr_clg->checksum != hash::t_sha1()) {
               const string hash_str = itr_clg->checksum.to_string();
               string chunk_path = get_parent_path(argv[1]) + "/data/" +
                  hash_str.substr(0, 2) + "/" + hash_str.substr(2);
               if (chunk_path[0] != '/')
                  chunk_path = cwd + "/" + chunk_path;
               
               if (itr_clg->flags & catalog::DIR)
                  chunk_path += "L";
               
               if (!file_exists(chunk_path))
                  cout << "chunk " << hash_str << " of path " << path << " missing (looking for " << chunk_path << ")" << endl;
            }
            
            ++itr_clg;
            ++itr_fs;
            ++no_clg;
            ++no_fs;
         }
      } while ((itr_clg != dir_clg.end()) || (itr_fs != dir_fs.end()));
   }
   
   cout << "Compared " << no_clg << "/" << no_fs << " entries." << endl;
   if (no_clg != catalog::get_num_dirent(0)) {
      cout << "Warning: number of entries checked differs from catalog table ("
           << no_clg << "/" << catalog::get_num_dirent(0) << ")" << endl;
   }
   if (no_nested != nested_catalogs.size()) {
      cout << "Warning: number of registered nested catalogs differs ("
           << no_nested << "/" << nested_catalogs.size() << ")" << endl;
   }
   
   catalog::fini();
   
   return 0;
}
