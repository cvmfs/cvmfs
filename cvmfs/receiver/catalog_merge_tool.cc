/**
 * This file is part of the CernVM File System.
 */

#include "catalog_merge_tool.h"

#include "catalog.h"
#include "hash.h"
#include "logging.h"
#include "util/posix.h"

namespace {

//   const std::string compressed_catalog_path =
//       "/srv/cvmfs/" + repo_name + "/data/" + hash.MakePath();
//   const std::string decompressed_catalog_path = temp_dir + "/" + root_hash;
//
//   if (zlib::DecompressPath2Path(compressed_catalog_path,
//                                 decompressed_catalog_path)) {

catalog::SimpleCatalogManager* OpenCatalogManager(const std::string& repo_name,
                                                  const std::string& temp_dir,
                                                  const std::string& root_hash,
                                                  perf::Statistics* stats) {
  shash::Any hash(shash::MkFromSuffixedHexPtr(shash::HexPtr(root_hash)));
  catalog::SimpleCatalogManager* mgr = new catalog::SimpleCatalogManager(
      hash, repo_name, temp_dir, NULL, stats, true);
  mgr->Init();

  return mgr;
}

}  // namespace

namespace receiver {

CatalogMergeTool::CatalogMergeTool(const std::string& repo_name,
                                   const std::string& old_root_hash,
                                   const std::string& new_root_hash,
                                   const std::string& base_root_hash)
    : repo_name_("/srv/cvmfs/" + repo_name),
      old_root_hash_(old_root_hash),
      new_root_hash_(new_root_hash),
      base_root_hash_(base_root_hash) {}

CatalogMergeTool::~CatalogMergeTool() {}

bool CatalogMergeTool::Merge(shash::Any* /*resulting_root_hash*/) {
  // Create a temp directory
  const std::string temp_dir = CreateTempDir("/tmp/cvmfs_receiver_merge_tool");

  // Old catalog from release manager machine (before lease)
  old_catalog_mgr_ =
      OpenCatalogManager(repo_name_, temp_dir, old_root_hash_, &stats_old_);

  // New catalog from release manager machine (before lease)
  // new_catalog_ = OpenCatalog(repo_name_, temp_dir, new_root_hash_);

  // UniquePtr<catalog::Catalog> base_catalog(
  // OpenCatalog(repo_name_, temp_dir, base_root_hash_));

  if (!old_catalog_mgr_.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Could not open old catalog");
    return false;
  }

  // if (!new_catalog_.IsValid()) {
  //   LogCvmfs(kLogCvmfs, kLogStderr, "Could not open new catalog");
  //   return false;
  // }

  // if (!base_catalog.IsValid()) {
  //   LogCvmfs(kLogCvmfs, kLogStderr, "Could not open current catalog");
  //   return false;
  // }

  // DEBUG FILE
  debug_file_ = std::fopen("/home/radu/debug.log", "w");

  bool ret = MergeRec(PathString(""));

  std::fclose(debug_file_);

  return ret;
}

bool CatalogMergeTool::MergeRec(const PathString& path) {
  // Gather all the entries from the old catalog
  catalog::DirectoryEntryList old_listing;
  old_catalog_mgr_->Listing(path, &old_listing);

  bool ret = true;
  for (size_t i = 0; i < old_listing.size(); ++i) {
    const catalog::DirectoryEntry& entry = old_listing.at(i);
    std::fprintf(debug_file_, "%s/%s %s\n", path.c_str(), entry.name().c_str(),
                 entry.checksum().ToString(true).c_str());
    if (entry.IsDirectory()) {
      PathString subpath(path);
      subpath.Append("/", 1);
      subpath.Append(entry.name().GetChars(), entry.name().GetLength());
      ret &= MergeRec(subpath);
    }

    // // Gather all the entries from the new catalog
    // catalog::DirectoryEntryList new_listing;
    // new_catalog_->ListingPath(PathString(""), &new_listing, false);

    // std::fprintf(debug_file_, "\nNew entries:\n");
    // for (size_t i = 0; i < new_listing.size(); ++i) {
    //   const catalog::DirectoryEntry& entry = new_listing.at(i);
    //   std::fprintf(debug_file_, "%s %s\n", entry.name().c_str(),
    //                entry.checksum().ToString(true).c_str());
    // }
  }

  return ret;
}

}  // namespace receiver
