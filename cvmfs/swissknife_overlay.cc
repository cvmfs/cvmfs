/**
 * This file is part of the CernVM File System.
 *
 * Implementation of the overlay swissknife command that merges multiple
 * CVMFS subdirectory catalogs using overlay semantics (similar to OverlayFS)
 * and publishes the result as a repository subdirectory.
 */

#define __STDC_FORMAT_MACROS

#include "swissknife_overlay.h"

#include <inttypes.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <ctime>
#include <map>
#include <string>
#include <vector>

#include "catalog.h"
#include "catalog_mgr_rw.h"
#include "catalog_rw.h"
#include "catalog_sql.h"
#include "compression/compression.h"
#include "crypto/hash.h"
#include "directory_entry.h"
#include "manifest.h"
#include "network/download.h"
#include "network/sink_path.h"
#include "repository_tag.h"
#include "shortstring.h"
#include "statistics.h"
#include "upload.h"
#include "upload_spooler_definition.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"
#include "xattr.h"

using namespace std;  // NOLINT

namespace swissknife {

ParameterList CommandOverlay::GetParams() const {
  ParameterList r;
  // Publish workflow parameters (same convention as ingest/sync)
  r.push_back(Parameter::Mandatory('r', "upstream storage definition"));
  r.push_back(Parameter::Mandatory('w', "stratum 0 URL"));
  r.push_back(Parameter::Mandatory('t', "temporary directory"));
  r.push_back(Parameter::Mandatory('o', "manifest output path"));
  r.push_back(Parameter::Mandatory('b', "base hash of current root catalog"));
  r.push_back(Parameter::Mandatory('K', "public key path"));
  r.push_back(Parameter::Mandatory('N', "repository name"));

  // Overlay-specific parameters
  r.push_back(Parameter::Mandatory('l', "comma-separated layer paths "
               "(bottom-to-top order)"));
  r.push_back(Parameter::Mandatory('d', "destination subdirectory path in "
               "repository for the merged overlay"));
  r.push_back(Parameter::Optional('c', "cache directory for intermediate "
               "merge results"));
  r.push_back(Parameter::Optional('e', "hash algorithm (default: sha1)"));
  r.push_back(Parameter::Optional('Z', "compression algorithm "
               "(default: zlib)"));
  r.push_back(Parameter::Optional('@', "proxy URL"));
  r.push_back(Parameter::Switch('f', "force refresh (ignore cache)"));
  r.push_back(Parameter::Switch('L', "follow HTTP redirects"));
  return r;
}


string CommandOverlay::ComputeCacheKey(
    const vector<string> &layers) const {
  string combined;
  for (size_t i = 0; i < layers.size(); ++i) {
    if (i > 0) combined += "\n";
    combined += layers[i];
  }
  return shash::Sha256String(combined);
}


bool CommandOverlay::CheckCachedMerge(
    const string &cache_dir,
    const string &cache_key,
    map<string, OverlayEntry> *merged) {
  if (cache_dir.empty()) return false;

  const string cache_path = cache_dir + "/" + cache_key + ".db";
  if (!FileExists(cache_path)) return false;

  // Open the cached catalog database
  catalog::WritableCatalog *cached_catalog =
      catalog::WritableCatalog::AttachFreely(
          "", cache_path, shash::Any(shash::kSha1));
  if (cached_catalog == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to open cached catalog: %s", cache_path.c_str());
    return false;
  }

  // Read all entries from the cached catalog
  merged->clear();

  // Helper function to recursively read entries from a catalog path
  bool success = ReadCatalogEntries(cached_catalog, "", "", "", "", merged);

  delete cached_catalog;

  if (!success) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to read entries from cached catalog: %s",
             cache_path.c_str());
    return false;
  }

  LogCvmfs(kLogCvmfs, kLogStdout,
           "Cache hit for key %s (%zu entries)",
           cache_key.c_str(), merged->size());
  return true;
}


bool CommandOverlay::StoreMergeInCache(
    const string &cache_dir,
    const string &cache_key,
    catalog::WritableCatalogManager *catalog_mgr,
    const string &dest_path) const {
  if (cache_dir.empty()) return false;

  MkdirDeep(cache_dir, 0755);

  const string cache_path = cache_dir + "/" + cache_key + ".db";

  // The nested catalog at dest_path already contains the complete set of
  // merged entries (created by PublishMergedEntries).  Simply copy its
  // database file into the cache.
  // GetHostingCatalog calls MakeRelativePath internally which prepends '/'.
  // Strip any leading '/' to avoid a double leading slash.
  const string dest_path_rel = (!dest_path.empty() && dest_path[0] == '/')
      ? dest_path.substr(1) : dest_path;
  catalog::WritableCatalog *nested_catalog =
      catalog_mgr->GetHostingCatalog(dest_path_rel);
  if (nested_catalog == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to find nested catalog for cache: %s",
             dest_path.c_str());
    return false;
  }

  const string db_path = nested_catalog->database_path();
  if (!CopyPath2Path(db_path, cache_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to copy nested catalog to cache: %s -> %s",
             db_path.c_str(), cache_path.c_str());
    return false;
  }

  LogCvmfs(kLogCvmfs, kLogStdout, "Stored merge result in cache: %s",
           cache_key.c_str());
  return true;
}


bool CommandOverlay::IsWhiteoutFile(const string &name) {
  return HasPrefix(name, ".wh.", false) && !IsOpaqueMarker(name);
}


string CommandOverlay::GetWhiteoutTarget(const string &name) {
  // ".wh." is 4 characters
  if (name.length() <= 4) return "";
  return name.substr(4);
}


bool CommandOverlay::IsOpaqueMarker(const string &name) {
  return name == ".wh..wh..opq";
}


bool CommandOverlay::ReadCatalogEntries(
    catalog::Catalog *catalog,
    const string &catalog_root_path,
    const string &relative_prefix,
    const string &repo_base,
    const string &temp_dir,
    map<string, OverlayEntry> *entries) {
  // List entries at this path in the catalog
  catalog::DirectoryEntryList listing;
  const PathString ps_path(catalog_root_path.data(),
                           catalog_root_path.length());
  const bool has_entries = catalog->ListingPath(ps_path, &listing);

  if (!has_entries && catalog_root_path != catalog->mountpoint().ToString()) {
    // No entries at this path - it might be a file, not a directory
    return true;
  }

  for (size_t i = 0; i < listing.size(); ++i) {
    const catalog::DirectoryEntry &dirent = listing[i];
    const string name = dirent.name().ToString();
    const string child_catalog_path =
        catalog_root_path.empty() ? "/" + name : catalog_root_path + "/" + name;
    const string child_relative =
        relative_prefix.empty() ? name : relative_prefix + "/" + name;

    OverlayEntry oe;
    oe.entry = dirent;
    oe.path = child_relative;
    oe.parent = relative_prefix;
    oe.is_whiteout = IsWhiteoutFile(name);
    oe.is_opaque_dir = false;

    // Look up xattrs for this entry
    XattrList xattrs;
    const PathString ps_child(child_catalog_path.data(),
                              child_catalog_path.length());
    catalog->LookupXattrsPath(ps_child, &xattrs);
    oe.xattrs = xattrs;

    (*entries)[child_relative] = oe;

    if (dirent.IsDirectory()) {
      if (dirent.IsNestedCatalogMountpoint() && !repo_base.empty()) {
        // Load the nested catalog and recurse into it
        shash::Any nested_hash;
        uint64_t nested_size;
        if (!catalog->FindNested(ps_child, &nested_hash, &nested_size)) {
          LogCvmfs(kLogCvmfs, kLogStderr,
                   "Failed to find nested catalog hash for %s",
                   child_catalog_path.c_str());
          return false;
        }

        catalog::Catalog *nested = LoadCatalogForPath(
            repo_base, child_catalog_path, temp_dir, nested_hash);
        if (nested == NULL) {
          LogCvmfs(kLogCvmfs, kLogStderr,
                   "Failed to load nested catalog for %s",
                   child_catalog_path.c_str());
          return false;
        }

        // Check for opaque marker in the nested catalog root
        catalog::DirectoryEntryList sub_listing;
        nested->ListingPath(ps_child, &sub_listing);
        for (size_t j = 0; j < sub_listing.size(); ++j) {
          if (IsOpaqueMarker(sub_listing[j].name().ToString())) {
            (*entries)[child_relative].is_opaque_dir = true;
            break;
          }
        }

        if (!ReadCatalogEntries(nested, child_catalog_path,
                                child_relative, repo_base, temp_dir, entries)) {
          delete nested;
          return false;
        }
        delete nested;
      } else if (!dirent.IsNestedCatalogMountpoint()) {
        // Regular directory — check for opaque marker among children
        catalog::DirectoryEntryList sub_listing;
        catalog->ListingPath(ps_child, &sub_listing);
        for (size_t j = 0; j < sub_listing.size(); ++j) {
          if (IsOpaqueMarker(sub_listing[j].name().ToString())) {
            (*entries)[child_relative].is_opaque_dir = true;
            break;
          }
        }

        if (!ReadCatalogEntries(catalog, child_catalog_path,
                                child_relative, repo_base, temp_dir, entries)) {
          return false;
        }
      }
      // else: nested catalog mountpoint but no repo_base — skip (e.g. cache)
    }
  }

  return true;
}


void CommandOverlay::MergeLayer(
    const map<string, OverlayEntry> &layer_entries,
    map<string, OverlayEntry> *merged) const {
  // First pass: collect whiteouts and opaque directories
  vector<string> whiteout_targets;
  vector<string> opaque_dirs;

  for (map<string, OverlayEntry>::const_iterator it = layer_entries.begin();
       it != layer_entries.end(); ++it) {
    const OverlayEntry &oe = it->second;
    const string &path = it->first;

    if (oe.is_whiteout) {
      // Whiteout: mark the target for deletion from lower layers
      const string target_name = GetWhiteoutTarget(
          GetFileName(path));
      const string target_path =
          oe.parent.empty() ? target_name : oe.parent + "/" + target_name;
      whiteout_targets.push_back(target_path);
      continue;
    }

    if (IsOpaqueMarker(GetFileName(path))) {
      // Don't add the opaque marker itself to the merged output
      continue;
    }

    if (oe.is_opaque_dir) {
      opaque_dirs.push_back(path);
    }
  }

  // Apply opaque directory semantics: remove all entries from lower layers
  // that are under opaque directories
  for (size_t i = 0; i < opaque_dirs.size(); ++i) {
    const string &opaque_path = opaque_dirs[i];
    const string prefix = opaque_path + "/";

    // Remove children of this directory from merged (lower layer entries)
    vector<string> to_remove;
    for (map<string, OverlayEntry>::iterator it = merged->begin();
         it != merged->end(); ++it) {
      if (HasPrefix(it->first, prefix, false)) {
        to_remove.push_back(it->first);
      }
    }
    for (size_t j = 0; j < to_remove.size(); ++j) {
      merged->erase(to_remove[j]);
    }
  }

  // Apply whiteout semantics: remove targeted entries and their children
  for (size_t i = 0; i < whiteout_targets.size(); ++i) {
    const string &target = whiteout_targets[i];
    const string prefix = target + "/";

    // Remove the target entry itself
    merged->erase(target);

    // Remove all children of the target
    vector<string> to_remove;
    for (map<string, OverlayEntry>::iterator it = merged->begin();
         it != merged->end(); ++it) {
      if (HasPrefix(it->first, prefix, false)) {
        to_remove.push_back(it->first);
      }
    }
    for (size_t j = 0; j < to_remove.size(); ++j) {
      merged->erase(to_remove[j]);
    }
  }

  // Second pass: add/override entries from this layer
  for (map<string, OverlayEntry>::const_iterator it = layer_entries.begin();
       it != layer_entries.end(); ++it) {
    const OverlayEntry &oe = it->second;
    const string &path = it->first;

    // Skip whiteout files and opaque markers - they are control files
    if (oe.is_whiteout || IsOpaqueMarker(GetFileName(path))) {
      continue;
    }

    // Upper layer overrides lower layer for the same path
    (*merged)[path] = oe;
  }
}



bool CommandOverlay::PublishMergedEntries(
    catalog::WritableCatalogManager *catalog_mgr,
    const map<string, OverlayEntry> &merged,
    const string &dest_path) const {
  // dest_path starts with '/' for LookupPath, but AddDirectory/AddFile
  // expect parent_directory without leading '/' because MakeRelativePath
  // (called internally) prepends it.  Create a stripped copy for add calls.
  const string dest_path_rel = (!dest_path.empty() && dest_path[0] == '/')
      ? dest_path.substr(1) : dest_path;

  // Ensure the destination directory itself exists in the catalog.
  // Check if dest_path already exists; if not, create it.
  catalog::DirectoryEntry dest_dirent;
  if (!catalog_mgr->LookupPath(dest_path, catalog::kLookupDefault,
                                &dest_dirent)) {
    // Create the destination directory (and any missing parents)
    // Walk up to find the deepest existing ancestor
    vector<string> dirs_to_create;
    string check_path = dest_path;
    while (!check_path.empty() && check_path != "/") {
      catalog::DirectoryEntry check_dirent;
      if (catalog_mgr->LookupPath(check_path, catalog::kLookupDefault,
                                   &check_dirent)) {
        break;
      }
      dirs_to_create.push_back(check_path);
      check_path = GetParentPath(check_path);
    }

    // Create directories from outermost to innermost
    for (int i = static_cast<int>(dirs_to_create.size()) - 1; i >= 0; --i) {
      const string &dir = dirs_to_create[i];
      string parent = GetParentPath(dir);
      const string name = GetFileName(dir);

      // Strip leading '/' — AddDirectory calls MakeRelativePath which adds it
      if (!parent.empty() && parent[0] == '/') {
        parent = parent.substr(1);
      }

      catalog::DirectoryEntryBase new_dir;
      new_dir.name_.Assign(name.data(), name.length());
      new_dir.mode_ = S_IFDIR | 0755;
      new_dir.uid_ = 0;
      new_dir.gid_ = 0;
      new_dir.size_ = 4096;
      new_dir.mtime_ = time(NULL);
      new_dir.linkcount_ = 2;

      catalog_mgr->AddDirectory(new_dir, XattrList(), parent);
      LogCvmfs(kLogCvmfs, kLogDebug,
               "Created destination directory: %s", dir.c_str());
    }
  }

  // Add entries in sorted order. The map is sorted lexicographically,
  // so parent directories appear before their children.
  for (map<string, OverlayEntry>::const_iterator it = merged.begin();
       it != merged.end(); ++it) {
    const OverlayEntry &oe = it->second;

    // Build parent path without leading '/' for AddDirectory/AddFile
    // (MakeRelativePath inside those functions adds it back)
    const string parent_path = oe.parent.empty()
        ? dest_path_rel
        : dest_path_rel + "/" + oe.parent;

    if (oe.entry.IsDirectory()) {
      catalog_mgr->AddDirectory(oe.entry, oe.xattrs, parent_path);
    } else {
      catalog_mgr->AddFile(
          static_cast<const catalog::DirectoryEntryBase &>(oe.entry),
          oe.xattrs, parent_path);
    }
  }

  // Turn the destination directory into a nested catalog so that the overlay
  // content lives in its own catalog database file.  This also allows
  // StoreMergeInCache to simply copy the resulting DB instead of
  // re-inserting every entry.
  // CreateNestedCatalog calls MakeRelativePath internally which prepends '/'.
  // Pass the stripped version to avoid a double leading slash.
  catalog_mgr->CreateNestedCatalog(dest_path_rel);

  LogCvmfs(kLogCvmfs, kLogStdout,
           "Published %zu entries under %s (nested catalog)",
           merged.size(), dest_path.c_str());
  return true;
}


catalog::Catalog *CommandOverlay::LoadCatalogForPath(
    const string &repo_base,
    const string &subdirectory,
    const string &temp_dir,
    const shash::Any &root_hash) {
  // Fetch the root catalog from the repository
  const string hash_path = "data/" + root_hash.MakePath();
  string catalog_path;

  if (IsHttpUrl(repo_base)) {
    // Download and decompress from remote
    const string url = repo_base + "/" + hash_path;
    catalog_path = temp_dir + "/" + root_hash.ToString();

    cvmfs::PathSink pathsink(catalog_path);
    download::JobInfo download_job(&url, true, false, &root_hash, &pathsink);
    const download::Failures retval = download_manager()->Fetch(&download_job);
    if (retval != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to download catalog %s (%d)",
               root_hash.ToString().c_str(), retval);
      return NULL;
    }
  } else {
    // Local repository: decompress the catalog
    const string source_path = repo_base + "/" + hash_path;
    catalog_path = temp_dir + "/" + root_hash.ToString();

    if (!zlib::DecompressPath2Path(source_path, catalog_path)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Failed to decompress catalog %s from %s",
               root_hash.ToString().c_str(), source_path.c_str());
      return NULL;
    }
  }

  catalog::Catalog *catalog = catalog::Catalog::AttachFreely(
      subdirectory, catalog_path, root_hash);
  if (catalog == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to attach catalog for path %s",
             subdirectory.c_str());
    unlink(catalog_path.c_str());
    return NULL;
  }

  catalog->TakeDatabaseFileOwnership();
  return catalog;
}


catalog::Catalog *CommandOverlay::FindCatalogForLayer(
    const string &repo_base,
    const string &temp_dir,
    catalog::Catalog *catalog,
    const string &layer_path,
    vector<catalog::Catalog *> *loaded_catalogs) {
  // First try a direct lookup in the given catalog
  catalog::DirectoryEntry test_entry;
  const PathString ps_layer(layer_path.data(), layer_path.length());
  if (catalog->LookupPath(ps_layer, &test_entry)) {
    return catalog;
  }

  // The path was not found directly.  Walk the path components *below*
  // this catalog's mountpoint to find a nested catalog mountpoint that
  // is an ancestor of layer_path.
  const string mountpoint = catalog->mountpoint().ToString();

  // Verify layer_path starts with the mountpoint (or mountpoint is empty
  // for the root catalog)
  if (!mountpoint.empty() && layer_path.substr(0, mountpoint.length())
                                                            != mountpoint) {
    return NULL;
  }

  // Get the suffix of layer_path below the mountpoint
  const string suffix = mountpoint.empty() ? layer_path
                                           : layer_path.substr(
                                                 mountpoint.length());
  const vector<string> components = SplitString(suffix, '/');
  string prefix = mountpoint;
  for (size_t i = 0; i < components.size(); ++i) {
    if (components[i].empty()) continue;
    prefix += "/" + components[i];

    catalog::DirectoryEntry dir_entry;
    const PathString ps_prefix(prefix.data(), prefix.length());
    if (!catalog->LookupPath(ps_prefix, &dir_entry)) {
      break;
    }

    if (dir_entry.IsNestedCatalogMountpoint()) {
      shash::Any nested_hash;
      uint64_t nested_size;
      if (!catalog->FindNested(ps_prefix, &nested_hash, &nested_size)) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "Failed to find nested catalog hash for %s", prefix.c_str());
        return NULL;
      }

      catalog::Catalog *nested = LoadCatalogForPath(
          repo_base, prefix, temp_dir, nested_hash);
      if (nested == NULL) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "Failed to load nested catalog at %s", prefix.c_str());
        return NULL;
      }
      loaded_catalogs->push_back(nested);

      // Recurse: the layer path may be directly in this nested catalog
      // or in an even deeper nested catalog
      return FindCatalogForLayer(
          repo_base, temp_dir, nested, layer_path, loaded_catalogs);
    }
  }

  LogCvmfs(kLogCvmfs, kLogStderr, "Layer path not found: %s",
           layer_path.c_str());
  return NULL;
}


int CommandOverlay::Main(const ArgumentList &args) {
  // Parse publish workflow parameters
  const string spooler_definition_str = *args.find('r')->second;
  const string stratum0 = *args.find('w')->second;
  const string temp_dir = MakeCanonicalPath(*args.find('t')->second);
  const string manifest_path = *args.find('o')->second;
  const shash::Any base_hash =
      shash::MkFromHexPtr(shash::HexPtr(*args.find('b')->second),
                          shash::kSuffixCatalog);
  const string public_keys = *args.find('K')->second;
  const string repo_name = *args.find('N')->second;

  // Parse overlay-specific parameters
  const string layers_str = *args.find('l')->second;
  string dest_path = MakeCanonicalPath(*args.find('d')->second);
  // Ensure dest_path starts with exactly one '/'
  while (dest_path.length() > 1 && dest_path[0] == '/' && dest_path[1] == '/') {
    dest_path = dest_path.substr(1);
  }
  if (dest_path.empty() || dest_path[0] != '/') {
    dest_path = "/" + dest_path;
  }
  const string cache_dir =
      (args.count('c') > 0) ? *args.find('c')->second : "";
  const bool force_refresh = (args.count('f') > 0);

  shash::Algorithms hash_algorithm = shash::kSha1;
  if (args.find('e') != args.end()) {
    hash_algorithm = shash::ParseHashAlgorithm(*args.find('e')->second);
    if (hash_algorithm == shash::kAny) {
      PrintError("unknown hash algorithm");
      return 1;
    }
  }
  zlib::Algorithms compression_alg = zlib::kZlibDefault;
  if (args.find('Z') != args.end()) {
    compression_alg = zlib::ParseCompressionAlgorithm(
        *args.find('Z')->second);
  }

  // Parse comma-separated layer paths
  const vector<string> layers = SplitString(layers_str, ',');
  if (layers.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "No layers specified");
    return 1;
  }

  LogCvmfs(kLogCvmfs, kLogStdout, "Overlay merge of %zu layers into %s",
           layers.size(), dest_path.c_str());
  for (size_t i = 0; i < layers.size(); ++i) {
    LogCvmfs(kLogCvmfs, kLogStdout, "  Layer %zu: %s", i, layers[i].c_str());
  }

  // Set up spoolers (following the ingest pattern)
  perf::StatisticsTemplate publish_statistics("publish", this->statistics());

  const upload::SpoolerDefinition spooler_definition(
      spooler_definition_str, hash_algorithm, compression_alg,
      false /* generate_legacy_bulk_chunks */,
      false /* use_file_chunking */,
      0, 0, 0 /* chunk sizes: unused */,
      "" /* session_token_file */, "" /* key_file */);

  const upload::SpoolerDefinition spooler_definition_catalogs(
      spooler_definition.Dup2DefaultCompression());

  const UniquePtr<upload::Spooler> spooler_files(
      upload::Spooler::Construct(spooler_definition, &publish_statistics));
  if (!spooler_files.IsValid()) {
    PrintError("Failed to create file spooler");
    return 3;
  }
  const UniquePtr<upload::Spooler> spooler_catalogs(
      upload::Spooler::Construct(spooler_definition_catalogs,
                                 &publish_statistics));
  if (!spooler_catalogs.IsValid()) {
    PrintError("Failed to create catalog spooler");
    return 3;
  }

  // Initialize download manager and signature manager
  const bool follow_redirects = (args.count('L') > 0);
  const string proxy = (args.count('@') > 0) ? *args.find('@')->second : "";
  if (!InitDownloadManager(follow_redirects, proxy)) {
    PrintError("Failed to initialize download manager");
    return 3;
  }
  if (!InitSignatureManager(public_keys)) {
    PrintError("Failed to initialize signature manager");
    return 3;
  }

  // Fetch repository manifest
  const UniquePtr<manifest::Manifest> manifest(
      FetchRemoteManifest(stratum0, repo_name, base_hash));
  if (!manifest.IsValid()) {
    PrintError("Failed to load repository manifest");
    return 3;
  }

  const string old_root_hash = manifest->catalog_hash().ToString(true);
  LogCvmfs(kLogCvmfs, kLogStdout, "Root catalog hash: %s",
           old_root_hash.c_str());

  // Check merge cache first (unless force refresh)
  const string cache_key = ComputeCacheKey(layers);
  map<string, OverlayEntry> merged;
  bool cache_hit = false;

  if (!force_refresh && !cache_dir.empty()) {
    cache_hit = CheckCachedMerge(cache_dir, cache_key, &merged);
  }

  if (!cache_hit) {
    // Load root catalog for reading layer entries
    catalog::Catalog *root_catalog = LoadCatalogForPath(
        stratum0, "", temp_dir, manifest->catalog_hash());
    if (root_catalog == NULL) {
      PrintError("Failed to load root catalog");
      return 1;
    }

    // Process layers bottom-to-top
    for (size_t i = 0; i < layers.size(); ++i) {
      string layer_path = MakeCanonicalPath(layers[i]);
      // Ensure layer path starts with exactly one '/'
      while (layer_path.length() > 1
             && layer_path[0] == '/' && layer_path[1] == '/') {
        layer_path = layer_path.substr(1);
      }
      if (layer_path.empty() || layer_path[0] != '/') {
        layer_path = "/" + layer_path;
      }
      
      LogCvmfs(kLogCvmfs, kLogStdout, "Processing layer %zu: %s",
               i, layer_path.c_str());

      map<string, OverlayEntry> layer_entries;

      // Find the catalog that contains this layer path (may be nested)
      vector<catalog::Catalog *> loaded_catalogs;
      catalog::Catalog *layer_catalog = FindCatalogForLayer(
          stratum0, temp_dir, root_catalog, layer_path, &loaded_catalogs);
      if (layer_catalog == NULL) {
        for (size_t j = 0; j < loaded_catalogs.size(); ++j)
          delete loaded_catalogs[j];
        delete root_catalog;
        return 1;
      }

      catalog::DirectoryEntry subdir_entry;
      const PathString ps_layer_path(layer_path.data(), layer_path.length());
      if (!layer_catalog->LookupPath(ps_layer_path, &subdir_entry)) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "Unexpected: layer path not found after catalog resolution: %s",
                 layer_path.c_str());
        for (size_t j = 0; j < loaded_catalogs.size(); ++j)
          delete loaded_catalogs[j];
        delete root_catalog;
        return 1;
      }

      // Check if the layer path itself is a nested catalog mountpoint;
      // if so, load that catalog and read its entries.
      if (subdir_entry.IsNestedCatalogMountpoint()) {
        shash::Any nested_hash;
        uint64_t nested_size;
        if (!layer_catalog->FindNested(ps_layer_path, &nested_hash,
                                       &nested_size)) {
          LogCvmfs(kLogCvmfs, kLogStderr,
                   "Failed to find nested catalog for %s",
                   layer_path.c_str());
          for (size_t j = 0; j < loaded_catalogs.size(); ++j)
            delete loaded_catalogs[j];
          delete root_catalog;
          return 1;
        }

        catalog::Catalog *nested_catalog = LoadCatalogForPath(
            stratum0, layer_path, temp_dir, nested_hash);
        if (nested_catalog == NULL) {
          LogCvmfs(kLogCvmfs, kLogStderr,
                   "Failed to load nested catalog for %s",
                   layer_path.c_str());
          for (size_t j = 0; j < loaded_catalogs.size(); ++j)
            delete loaded_catalogs[j];
          delete root_catalog;
          return 1;
        }

        ReadCatalogEntries(nested_catalog, layer_path, "",
                           stratum0, temp_dir, &layer_entries);
        delete nested_catalog;
      } else {
        ReadCatalogEntries(layer_catalog, layer_path, "",
                           stratum0, temp_dir, &layer_entries);
      }

      // Clean up any intermediate catalogs loaded during hierarchy walk
      for (size_t j = 0; j < loaded_catalogs.size(); ++j)
        delete loaded_catalogs[j];

      LogCvmfs(kLogCvmfs, kLogStdout, "  Read %zu entries from layer %s",
               layer_entries.size(), layer_path.c_str());

      MergeLayer(layer_entries, &merged);

      LogCvmfs(kLogCvmfs, kLogStdout, "  Merged total: %zu entries",
               merged.size());
    }

    delete root_catalog;
  }

  // Set up WritableCatalogManager and publish merged entries
  LogCvmfs(kLogCvmfs, kLogStdout,
           "Publishing %zu merged entries under %s",
           merged.size(), dest_path.c_str());

  catalog::WritableCatalogManager catalog_manager(
      base_hash, stratum0, temp_dir,
      spooler_catalogs.weak_ref(), download_manager(),
      false /* enforce_limits */,
      0 /* nested_kcatalog_limit */,
      0 /* root_kcatalog_limit */,
      0 /* file_mbyte_limit */,
      statistics(),
      false /* is_balanceable */,
      0 /* max_weight */, 0 /* min_weight */);
  catalog_manager.Init();

  if (!PublishMergedEntries(&catalog_manager, merged, dest_path)) {
    PrintError("Failed to publish merged entries");
    return 5;
  }

  // Cache the nested catalog that PublishMergedEntries created at dest_path
  if (!cache_hit && !cache_dir.empty()) {
    StoreMergeInCache(cache_dir, cache_key, &catalog_manager, dest_path);
  }

  // Commit catalog changes and produce updated manifest
  catalog_manager.PrecalculateListings();
  if (!catalog_manager.Commit(false, 0, manifest.weak_ref())) {
    PrintError("Failed to commit catalog changes");
    return 5;
  }

  // Finalize spoolers
  LogCvmfs(kLogCvmfs, kLogStdout, "Waiting for uploads to finish...");
  spooler_files->WaitForUpload();
  spooler_catalogs->WaitForUpload();
  spooler_files->FinalizeSession(false);

  const string new_root_hash = manifest->catalog_hash().ToString(true);
  if (!spooler_catalogs->FinalizeSession(true, old_root_hash, new_root_hash,
                                         RepositoryTag())) {
    PrintError("Failed to finalize session");
    return 5;
  }

  // Export manifest
  if (!manifest->Export(manifest_path)) {
    PrintError("Failed to export manifest");
    return 6;
  }

  LogCvmfs(kLogCvmfs, kLogStdout,
           "Overlay published successfully to %s", dest_path.c_str());
  return 0;
}

}  // namespace swissknife
