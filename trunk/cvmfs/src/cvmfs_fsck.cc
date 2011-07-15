/*
 *  \file cvmfs_fsck.cc
 *  This tool checks a cvmfs2 cache directory for consistency.
 *  If necessary, the managed cache db is removed so that 
 *  it will be rebuilt on next mount.
 */
 
#define _FILE_OFFSET_BITS 64
#include "config.h"
#include "util.h"
#include "hash.h"
#include "atomic.h"

#include "compat.h"

extern "C" {
   #include "compression.h"
   #include "smalloc.h"
}

#include <iostream>
#include <string>
#include <cstring>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>

const int ERROR_OK = 0;
const int ERROR_FIXED = 1;
const int ERROR_REBOOT = 2;
const int ERROR_UNFIXED = 4;
const int ERROR_OPERATIONAL = 8;
const int ERROR_USAGE = 16;

using namespace std;

static void usage() {
   cout << "This tool checks a cvmfs2 cache directory for consistency." << endl;
   cout << "If necessary, the managed cache db is removed so that" << endl;
   cout << "it will be rebuilt on next mount." << endl << endl;
   cout << "Usage: cvmfs_fsck [-p] [-f] [-j #threads] <cache directory>" << endl;
   cout << "Options:" << endl;
   cout << "  -p try to fix automatically" << endl;
   cout << "  -f force rebuild of managed cache db on next mount" << endl;
   cout << "  -j number of concurrent integrity check worker threads" << endl;
}


string cache_dir;
atomic_int num_files;
atomic_int num_err_fixed;
atomic_int num_err_unfixed;
atomic_int num_err_operational;
atomic_int num_tmp_catalog;
pthread_mutex_t mutex_output = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_traverse = PTHREAD_MUTEX_INITIALIZER;
DIR *DIRP_current = 0;
int dir_current = 0;
string ndir_current = "00";

int nthreads = 1;
bool fix_errors = false;
bool force_rebuild = false;
bool modified_cache = false;
pthread_mutex_t mutex_force_rebuild = PTHREAD_MUTEX_INITIALIZER;



static bool next_file(string &rel_path, string &hash_name) {
   PortableDirent *d;
   
   pthread_mutex_lock(&mutex_traverse);
   if (!DIRP_current) {
      pthread_mutex_unlock(&mutex_traverse);
      return false;
   }

next_file_again:
   while ((d = portableReaddir(DIRP_current)) != NULL) {
      const string name = d->d_name;
      if ((name == ".") || (name == "..")) continue;
      
      if (d->d_type != DT_REG) {
         pthread_mutex_lock(&mutex_output);
         cout << "Warning: " << cache_dir << "/" << ndir_current << "/" << name 
              << " is not a regular file" << endl;
         pthread_mutex_unlock(&mutex_output);
         continue;
      }
      
      break;
   }
          
   if (!d) {
      closedir(DIRP_current);
      DIRP_current = NULL;
      dir_current++;
      if (dir_current < 256) {
         char hex[3];
         snprintf(hex, 3, "%02x", dir_current);
         ndir_current = string(hex, 2);
         
         if ((DIRP_current = opendir(hex)) == NULL) {
            cerr << "invalid cache directory, " << cache_dir << "/" << ndir_current 
                 << " does not exist" << endl;
            pthread_mutex_unlock(&mutex_traverse);
            exit(ERROR_UNFIXED);
         }
         goto next_file_again;
      }
   }
   pthread_mutex_unlock(&mutex_traverse);
          
   if (d) {
      const string name = d->d_name;
      rel_path = ndir_current + "/" + name;
      hash_name = ndir_current + name;
      return true;
   }
   
   return false;
}
          
          

static void *checker(void *data __attribute__((unused))) {
   string rel_path;
   string hash_name;
   
   while (next_file(rel_path, hash_name)) {
      const string path = cache_dir + "/" + rel_path;
      
      int n = atomic_xadd(&num_files, 1);
      if ((n % 1000) == 0) {
         pthread_mutex_lock(&mutex_output);
         cout << "." << flush;
         pthread_mutex_unlock(&mutex_output);
      }
         
      if (rel_path[rel_path.length()-1] == 'T') {
         cout << "Warning: temporary file catalog found " << path << endl;
         atomic_inc(&num_tmp_catalog);
         continue;
      }      
      
      FILE *fsrc = fopen(rel_path.c_str() , "r");
      if (!fsrc) {
         pthread_mutex_lock(&mutex_output);
         cout << "Error: cannot open " << path << endl;
         pthread_mutex_unlock(&mutex_output);
         atomic_inc(&num_err_operational);
         continue;
      }
            
      /* Compress every file and calculate SHA-1 of stream */
      hash::t_sha1 sha1;
      if (compress_file_sha1_only(fsrc, sha1.digest) != 0) {
         pthread_mutex_lock(&mutex_output);
         cout << "Error: could not compress " << path << endl;
         pthread_mutex_unlock(&mutex_output);
         atomic_inc(&num_err_operational);
      } else {
         if (sha1.to_string() != hash_name) {
            if (fix_errors) {
               if (unlink(rel_path.c_str()) == 0) {
                  pthread_mutex_lock(&mutex_output);
                  cout << "Fix: " << path << " is corrupted, file unlinked" << endl;
                  pthread_mutex_unlock(&mutex_output);
                  atomic_inc(&num_err_fixed);
                  
                  /* Changes made, we have to rebuild the managed cache db */
                  pthread_mutex_lock(&mutex_force_rebuild);
                  force_rebuild = modified_cache = true;
                  pthread_mutex_unlock(&mutex_force_rebuild);
               } else {
                  pthread_mutex_lock(&mutex_output);
                  cout << "Error: " << path << " is corrupted, could not unlink" << endl;
                  pthread_mutex_unlock(&mutex_output);
                  atomic_inc(&num_err_unfixed);
               }
            } else {
               pthread_mutex_lock(&mutex_output);
               cout << "Error: " << path << " has compressed checksum " << sha1.to_string() << 
                       ", delete this file from cache directory!" << endl;
               pthread_mutex_unlock(&mutex_output);
               atomic_inc(&num_err_unfixed);
            }
         }
      }
      fclose(fsrc);         
   }
   
   return NULL;
}

int main(int argc, char **argv) {
   char c;
   while ((c = getopt(argc, argv, "hpfj:")) != -1) {
      switch (c) {
         case 'h':
            usage();
            return ERROR_OK;
         case 'p':
            fix_errors = true;
            break;
         case 'f':
            force_rebuild = true;
            break;
         case 'j':
            nthreads = atoi(optarg);
            if (nthreads < 1) {
               cout << "There is at least one worker thread required" << endl;
               return ERROR_USAGE;
            }
            break;
         case '?':
         default:
            usage();
            return ERROR_USAGE;
      }
   }
     
   /* Switch to cache directory */
   if (optind >= argc) {
      usage();
      return ERROR_USAGE;
   }
   cache_dir = canonical_path(argv[optind]);
   if (chdir(cache_dir.c_str()) != 0) {
      cerr << "could not change to " << cache_dir << endl;
      return ERROR_OPERATIONAL;
   }
   
   /* Check if txn directory is empty */
   DIR *dirp_txn;
   if ((dirp_txn = opendir("txn")) == NULL) {
      cerr << "invalid cache directory, " << cache_dir << "/txn does not exist" << endl;
      return ERROR_OPERATIONAL;
   }
   PortableDirent *d;
   while ((d = portableReaddir(dirp_txn)) != NULL) {
      const string name = d->d_name;
      if ((name == ".") || (name == "..")) continue;
      
      cout << "Warning: temporary directory " << cache_dir << "/txn is not empty" << endl;
      cout << "If this repository is currently _not_ mounted, you can remove its contents" << endl;
      break;
   }
   closedir(dirp_txn);
   
   /* Run workers to recalculate checksums */
   if ((DIRP_current = opendir("00")) == NULL) {
      cerr << "invalid cache directory, " << cache_dir << "/" << "00 does not exist" << endl;
      return ERROR_UNFIXED;
   }
   
   atomic_init(&num_files);
   atomic_init(&num_err_fixed);
   atomic_init(&num_err_unfixed);
   atomic_init(&num_err_operational);
   atomic_init(&num_tmp_catalog);
   pthread_t *workers = (pthread_t *)smalloc(nthreads * sizeof(pthread_t));
   cout << "Verifying: " << flush;
   for (int i = 0; i < nthreads; ++i) {
      if (pthread_create(&workers[i], NULL, checker, NULL) != 0) {
         cout << "Fatal: could not create worker thread" << endl;
         return ERROR_OPERATIONAL;
      }
   }
   for (int i = 0; i < nthreads; ++i) {
      pthread_join(workers[i], NULL);
   }
   free(workers);
   cout << endl;
   cout << "Verified " << atomic_read(&num_files) << " files" << endl;
   
   if (atomic_read(&num_tmp_catalog) > 0) {
      cout << "Temorary file catalogs were found." << endl;
      cout << "If this is a currently mounted repository, this is probably all right." << endl;
      cout << "Otherwise, or if you see lots of them, it is probably not." << endl;
      cout << "In that case, please unmount and remove the temporary catalogs." << endl;
   }
   
   if (force_rebuild) {
      if (unlink("cvmfscatalog.cache") == 0) {
         cout << "Fix: managed cache db unlinked, will be rebuilt on next mount" << endl;
         atomic_inc(&num_err_fixed);
      } else {
         if (errno != ENOENT) {
            cout << "Error: could not unlink managed cache database (" << errno << ")" << endl;
            atomic_inc(&num_err_unfixed);
         }
      }
   }
   
   if (modified_cache) {
      cout << endl;
      cout << "WARNING: There might by corrupted files in the kernel buffers." << endl;
      cout << "Remount CernVM-FS or run 'echo 3 > /proc/sys/vm/drop_caches'" << endl;
      cout << endl;
   }
   
   int retval = 0;
   if (atomic_read(&num_err_fixed) > 0)
      retval |= ERROR_FIXED;
   if (atomic_read(&num_err_unfixed) > 0)
      retval |= ERROR_UNFIXED;
   if (atomic_read(&num_err_operational) > 0)
      retval |= ERROR_OPERATIONAL;
      
   return retval;
}
