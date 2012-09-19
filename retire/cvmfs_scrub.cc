/*
 *  \file cvmfs_fsck.cc
 *  This tool checks a cvmfs2 cache directory for consistency.
 *  If necessary, the managed cache db is removed so that
 *  it will be rebuilt on next mount.
 */

#define _FILE_OFFSET_BITS 64
#include "cvmfs_config.h"

#include <iostream>
#include <string>
#include <cstring>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>

#include "platform.h"
#include "util.h"
#include "hash.h"
#include "atomic.h"

using namespace std;

static void usage() {
   cout << "This tool checks a cvmfs2 server data directory for consistency." << endl;
   cout << "Usage: cvmfs_scrub <data directory>" << endl;
}


struct dir_data {
   DIR *dp; ///< opened directory pointer
   int dir_num; ///< between 0 and 255
};
typedef struct dir_data dir_data_t;

string data_dir;


int main(int argc, char **argv) {
   /* Switch to cache directory */
   if (argc < 2) {
      usage();
      return 2;
   }
   data_dir = canonical_path(argv[1]);
   if (chdir(data_dir.c_str()) != 0) {
      cerr << "could not change to " << data_dir << endl;
      return 1;
   }

   bool found_corruption = false;
   int num_files = 0;

   /* Walk through sub directories */
   cout << "Verifying: " << flush;
   for (int i = 0; i <= 0xff; ++i) {
      char hex[3];
      snprintf(hex, 3, "%02x", i);
      if (chdir(hex) != 0) {
         cerr << "Error: cannot open " << data_dir << "/" << hex << endl;
         continue;
      }

      DIR *dirp = opendir(".");
      if (!dirp) {
         cerr << "Error: cannot access " << data_dir << "/" << hex << endl;
         continue;
      }
      platform_dirent64 *d;
      while ((d = platform_readdir(dirp)) != NULL) {
         const string name = d->d_name;
         if ((name == ".") || (name == ".."))
            continue;

         if ((num_files % 1000) == 0)
            cout << "." << flush;
         num_files++;

         const string path = data_dir + "/" + string(hex, 2) + "/" + name;
         const string sha1_name = string(hex, 2) + name.substr(0, 38);

         hash::t_sha1 expected;
         expected.from_hash_str(sha1_name);

         hash::t_sha1 calculated;
        if (hash::sha1_file(d->d_name, calculated.digest) != 0) {
            cerr << "Error: failed to open " << path << endl;
            found_corruption = true;
            continue;
         }
         if (expected != calculated) {
            cerr << "Error: " << path << " corrupted" << endl;
            found_corruption = true;
         }
      }
      closedir(dirp);

      if (chdir("..") != 0) {
         cerr << "Error: cannot access " << data_dir << endl;
         return 3;
      }
   }
   cout << endl;

   if (found_corruption)
      return 5;

   return 0;
}
