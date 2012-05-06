/**
 * This file is part of the CernVM File System.
 */

#include "manifest.h"

#include <cstdio>

#include "catalog.h"
#include "util.h"

using namespace std;  // NOLINT

Manifest::Manifest(const hash::Any &catalog_hash, const string &root_path) {
  catalog_hash_ = catalog_hash;
  root_path_ = hash::Md5(hash::AsciiPtr(root_path));
  ttl_ = catalog::Catalog::kDefaultTTL;
  revision_ = 0;
}


/**
 * Writes the .cvmfspublished file.
 */
bool Manifest::Export(const std::string &path) const {
  FILE *fmanifest = fopen(path.c_str(), "w");
  if (!fmanifest)
    return false;
  
  string manifest =
    "C" + catalog_hash_.ToString() + "\n" +
    "R" + root_path_.ToString() + "\n" + 
    "L" + micro_catalog_hash_.ToString() + "\n" + 
    "D" + StringifyInt(ttl_) + "\n" + 
    "S" + StringifyInt(revision_) + "\n";
  
  if (fwrite(manifest.data(), 1, manifest.length(), fmanifest) != 
      manifest.length()) 
  {
    fclose(fmanifest);
    unlink(path.c_str());
    return false;
  }
  fclose(fmanifest);

  return true;
}
