
#include <iostream>
#include <string>
#include <vector>

#include "catalog.h"

using namespace std;

static void usage() {
   cout << "This tool does a recursive ls on a single catalog." << endl;
   cout << "Usage: cvmfs_lscat <catalog>" << endl;
}


static void recursive_ls(const hash::t_md5 dir, const string path) {
   vector<catalog::t_dirent> entries;
   
   entries = catalog::ls_unprotected(dir);
      
   for (unsigned i = 0; i < entries.size(); ++i) {
      const string full_path = path + "/" + entries[i].name;
      cout << full_path;
      if (entries[i].checksum != hash::t_sha1())
         cout << " " << entries[i].checksum.to_string();
      cout << endl;
      if (entries[i].flags & catalog::DIR)
      {
         recursive_ls(hash::t_md5(full_path), full_path);
      }
   }
}


int main(int argc, char **argv) {
   if (argc < 2) {
      usage();
      return 1;
   }
   
   if (!catalog::init(0, 0) || !catalog::attach(argv[1], "", true, false)) {
      cerr << "could not load catalog" << endl;
      return 1;
   }
   
   
   catalog::t_dirent result;
   hash::t_md5 root(catalog::mangled_path(""));
   if (!catalog::lookup(root, result)) {
      cerr << "could not find root entry in catalog." << endl;
      return 1;
   }
   
   recursive_ls(root, catalog::mangled_path(""));
   
   catalog::fini();

   return 0;
}
