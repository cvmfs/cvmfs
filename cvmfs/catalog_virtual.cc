/**
 * This file is part of the CernVM File System.
 */

#include "catalog_virtual.h"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <memory>

#include "catalog_mgr_rw.h"
#include "compression/compression.h"
#include "history.h"
#include "swissknife_history.h"
#include "swissknife_sync.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"
#include "xattr.h"

using namespace std;  // NOLINT

namespace catalog {

const char *VirtualCatalog::kVirtualPath = ".cvmfs";
const char *VirtualCatalog::kSnapshotDirectory = "snapshots";
const char *VirtualCatalog::kUserDirectory = "user";
const int VirtualCatalog::kActionNone = 0x00;
const int VirtualCatalog::kActionGenerateSnapshots = 0x01;
const int VirtualCatalog::kActionRemove = 0x02;
const int VirtualCatalog::kActionGeneratePrivateSnapshots = 0x04;


void VirtualCatalog::CreateBaseDirectory() {
  // Add /.cvmfs as a nested catalog
  DirectoryEntryBase entry_dir;
  entry_dir.name_ = NameString(string(kVirtualPath));
  entry_dir.mode_ = S_IFDIR | S_IRUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH
                    | S_IXOTH;
  entry_dir.uid_ = 0;
  entry_dir.gid_ = 0;
  entry_dir.size_ = 97;
  entry_dir.mtime_ = time(NULL);
  catalog_mgr_->AddDirectory(entry_dir, XattrList(), "");
  WritableCatalog *parent_catalog = catalog_mgr_->GetHostingCatalog(
      kVirtualPath);
  catalog_mgr_->CreateNestedCatalog(kVirtualPath);
  WritableCatalog *virtual_catalog = catalog_mgr_->GetHostingCatalog(
      kVirtualPath);
  assert(parent_catalog != virtual_catalog);

  // Set hidden flag in parent catalog
  DirectoryEntry entry_parent;
  bool retval = parent_catalog->LookupPath(
      PathString("/" + string(kVirtualPath)), &entry_parent);
  assert(retval);
  entry_parent.set_is_hidden(true);
  parent_catalog->UpdateEntry(entry_parent, "/" + string(kVirtualPath));

  // Set hidden flag in nested catalog
  DirectoryEntry entry_virtual;
  retval = virtual_catalog->LookupPath(PathString("/" + string(kVirtualPath)),
                                       &entry_virtual);
  assert(retval);
  entry_virtual.set_is_hidden(true);
  virtual_catalog->UpdateEntry(entry_virtual, "/" + string(kVirtualPath));
}


void VirtualCatalog::CreateNestedCatalogMarker() {
  DirectoryEntryBase entry_marker;
  // Note that another entity needs to ensure that the object of an empty
  // file is in the repository!  It is currently done by the sync_mediator.
  const shash::Algorithms algorithm = catalog_mgr_->spooler_
                                          ->GetHashAlgorithm();
  shash::Any file_hash(algorithm);
  void *empty_compressed;
  uint64_t sz_empty_compressed;
  const bool retval = zlib::CompressMem2Mem(NULL, 0, &empty_compressed,
                                            &sz_empty_compressed);
  assert(retval);
  shash::HashMem(static_cast<unsigned char *>(empty_compressed),
                 sz_empty_compressed, &file_hash);
  free(empty_compressed);
  entry_marker.name_ = NameString(".cvmfscatalog");
  entry_marker.mode_ = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
  entry_marker.checksum_ = file_hash;
  entry_marker.mtime_ = time(NULL);
  entry_marker.uid_ = 0;
  entry_marker.gid_ = 0;
  const XattrList xattrs;
  catalog_mgr_->AddFile(entry_marker, xattrs, kVirtualPath);
}


void VirtualCatalog::CreateSnapshotDirectory() {
  DirectoryEntryBase entry_dir;
  entry_dir.name_ = NameString(string(kSnapshotDirectory));
  entry_dir.mode_ = S_IFDIR | S_IRUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH
                    | S_IXOTH;
  entry_dir.uid_ = 0;
  entry_dir.gid_ = 0;
  entry_dir.size_ = 97;
  entry_dir.mtime_ = time(NULL);
  catalog_mgr_->AddDirectory(entry_dir, XattrList(), kVirtualPath);
}


void VirtualCatalog::CreateUserDirectory() {
  DirectoryEntryBase entry_dir;
  entry_dir.name_ = NameString(string(kUserDirectory));
  entry_dir.mode_ = S_IFDIR | S_IRUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH
                    | S_IXOTH;
  entry_dir.uid_ = 0;
  entry_dir.gid_ = 0;
  entry_dir.size_ = 97;
  entry_dir.mtime_ = time(NULL);
  catalog_mgr_->AddDirectory(entry_dir, XattrList(), kVirtualPath);
}


/**
 * Checks for the top-level /.cvmfs directory (as a nested catalog) and
 * the snapshots/ subdirectory, creating them if necessary.
 */
void VirtualCatalog::EnsurePresence() {
  DirectoryEntry e;
  const bool base_exists = catalog_mgr_->LookupPath(
      "/" + string(kVirtualPath), kLookupDefault, &e);
  if (!base_exists) {
    LogCvmfs(kLogCatalog, kLogDebug, "creating new virtual catalog");
    CreateBaseDirectory();
    CreateNestedCatalogMarker();
  }
  assert(catalog_mgr_->IsTransitionPoint(kVirtualPath));

  const string snapshots_path = "/" + string(kVirtualPath) + "/"
                                 + string(kSnapshotDirectory);
  if (!catalog_mgr_->LookupPath(snapshots_path, kLookupDefault, &e)) {
    CreateSnapshotDirectory();
  }
}


/**
 * Checks for the top-level /.cvmfs directory (as a nested catalog) and the
 * user/ subdirectory, creating them if necessary.  Does not create snapshots/.
 */
void VirtualCatalog::EnsureUserDirectoryPresence() {
  DirectoryEntry e;
  const bool base_exists = catalog_mgr_->LookupPath(
      "/" + string(kVirtualPath), kLookupDefault, &e);
  if (!base_exists) {
    LogCvmfs(kLogCatalog, kLogDebug,
             "creating virtual catalog base for user directory");
    CreateBaseDirectory();
    CreateNestedCatalogMarker();
  }
  assert(catalog_mgr_->IsTransitionPoint(kVirtualPath));

  const string user_path = "/" + string(kVirtualPath) + "/"
                            + string(kUserDirectory);
  if (!catalog_mgr_->LookupPath(user_path, kLookupDefault, &e)) {
    CreateUserDirectory();
  }
}


void VirtualCatalog::Generate(int actions) {
  if (actions & kActionGenerateSnapshots) {
    GenerateSnapshots();
  }
  if (actions & kActionGeneratePrivateSnapshots) {
    GeneratePrivateSnapshots();
  }
  if (actions & kActionRemove) {
    Remove();
  }
}


void VirtualCatalog::GenerateSnapshots() {
  LogCvmfs(kLogCvmfs, kLogStdout, "Creating virtual snapshots");
  EnsurePresence();

  vector<TagId> tags_history;
  vector<TagId> tags_catalog;
  GetSortedTagsFromHistory(&tags_history);
  GetSortedTagsFromCatalog(&tags_catalog);
  // Add artificial end markers to both lists
  string tag_name_end = "";
  if (!tags_history.empty())
    tag_name_end = std::max(tag_name_end, tags_history.rbegin()->name);
  if (!tags_catalog.empty())
    tag_name_end = std::max(tag_name_end, tags_catalog.rbegin()->name);
  tag_name_end += "X";
  tags_history.push_back(TagId(tag_name_end, shash::Any()));
  tags_catalog.push_back(TagId(tag_name_end, shash::Any()));

  // Walk through both sorted lists concurrently and determine change set
  unsigned i_history = 0, i_catalog = 0;
  const unsigned last_history = tags_history.size() - 1;
  const unsigned last_catalog = tags_catalog.size() - 1;
  while ((i_history < last_history) || (i_catalog < last_catalog)) {
    const TagId t_history = tags_history[i_history];
    const TagId t_catalog = tags_catalog[i_catalog];

    // Both the same, nothing to do
    if (t_history == t_catalog) {
      i_history++;
      i_catalog++;
      continue;
    }

    // Same tag name for different hash, re-insert
    if (t_history.name == t_catalog.name) {
      RemoveSnapshot(t_catalog);
      InsertSnapshot(t_history);
      i_history++;
      i_catalog++;
      continue;
    }

    // New tag that's missing
    if (t_history.name < t_catalog.name) {
      InsertSnapshot(t_history);
      i_history++;
      continue;
    }

    // A tag was removed but it is still present in the catalog
    assert(t_history.name > t_catalog.name);
    RemoveSnapshot(t_catalog);
    i_catalog++;
  }
}


bool VirtualCatalog::ParseActions(const string &action_desc, int *actions) {
  *actions = kActionNone;
  if (action_desc.empty())
    return true;

  vector<string> action_tokens = SplitString(action_desc, ',');
  for (unsigned i = 0; i < action_tokens.size(); ++i) {
    if (action_tokens[i] == "snapshots") {
      *actions |= kActionGenerateSnapshots;
    } else if (action_tokens[i] == "user") {
      *actions |= kActionGeneratePrivateSnapshots;
    } else if (action_tokens[i] == "remove") {
      *actions |= kActionRemove;
    } else {
      return false;
    }
  }
  return true;
}


void VirtualCatalog::GetSortedTagsFromHistory(vector<TagId> *tags) {
  const std::unique_ptr<history::History> history(
      assistant_.GetHistory(swissknife::Assistant::kOpenReadOnly));
  vector<history::History::Tag> tags_history;
  const bool retval = history->List(&tags_history);
  assert(retval);
  for (unsigned i = 0, l = tags_history.size(); i < l; ++i) {
    if ((tags_history[i].name == swissknife::CommandTag::kHeadTag)
        || (tags_history[i].name == swissknife::CommandTag::kPreviousHeadTag)) {
      continue;
    }
    tags->push_back(TagId(tags_history[i].name, tags_history[i].root_hash));
  }
  std::sort(tags->begin(), tags->end());
}


void VirtualCatalog::GetSortedTagsFromCatalog(vector<TagId> *tags) {
  const string snapshots_prefix = "/" + string(kVirtualPath) + "/"
                                   + string(kSnapshotDirectory) + "/";
  WritableCatalog *virtual_catalog = catalog_mgr_->GetHostingCatalog(
      kVirtualPath);
  assert(virtual_catalog != NULL);
  Catalog::NestedCatalogList nested_catalogs = virtual_catalog
                                                   ->ListNestedCatalogs();
  for (unsigned i = 0, l = nested_catalogs.size(); i < l; ++i) {
    const string mp = nested_catalogs[i].mountpoint.ToString();
    if (mp.substr(0, snapshots_prefix.size()) != snapshots_prefix)
      continue;
    tags->push_back(TagId(GetFileName(nested_catalogs[i].mountpoint).ToString(),
                          nested_catalogs[i].hash));
  }
  std::sort(tags->begin(), tags->end());
}


void VirtualCatalog::GetSortedHashesFromCatalog(vector<TagId> *hashes) {
  const string user_prefix = "/" + string(kVirtualPath) + "/"
                              + string(kUserDirectory) + "/";
  WritableCatalog *virtual_catalog = catalog_mgr_->GetHostingCatalog(
      kVirtualPath);
  assert(virtual_catalog != NULL);
  Catalog::NestedCatalogList nested_catalogs = virtual_catalog
                                                   ->ListNestedCatalogs();
  for (unsigned i = 0, l = nested_catalogs.size(); i < l; ++i) {
    const string mp = nested_catalogs[i].mountpoint.ToString();
    if (mp.substr(0, user_prefix.size()) != user_prefix)
      continue;
    hashes->push_back(
        TagId(GetFileName(nested_catalogs[i].mountpoint).ToString(),
              nested_catalogs[i].hash));
  }
  std::sort(hashes->begin(), hashes->end());
}


void VirtualCatalog::InsertSnapshot(TagId tag) {
  LogCvmfs(kLogCatalog, kLogDebug, "add snapshot %s (%s) to virtual catalog",
           tag.name.c_str(), tag.hash.ToString().c_str());
  const std::unique_ptr<Catalog> catalog(
      assistant_.GetCatalog(tag.hash, swissknife::Assistant::kOpenReadOnly));
  assert(catalog.get() != nullptr);
  assert(catalog->root_prefix().IsEmpty());
  DirectoryEntry entry_root;
  const bool retval = catalog->LookupPath(PathString(""), &entry_root);
  assert(retval);

  // Add directory entry
  DirectoryEntryBase entry_dir = entry_root;
  entry_dir.name_ = NameString(tag.name);
  catalog_mgr_->AddDirectory(
      entry_dir, XattrList(),
      string(kVirtualPath) + "/" + string(kSnapshotDirectory));

  // Set "bind mount" flag
  WritableCatalog *virtual_catalog = catalog_mgr_->GetHostingCatalog(
      kVirtualPath);
  assert(virtual_catalog != NULL);
  const string mountpoint = "/" + string(kVirtualPath) + "/"
                            + string(kSnapshotDirectory) + "/" + tag.name;
  DirectoryEntry entry_bind_mountpoint(entry_dir);
  entry_bind_mountpoint.set_is_bind_mountpoint(true);
  virtual_catalog->UpdateEntry(entry_bind_mountpoint, mountpoint);

  // Register nested catalog
  const uint64_t catalog_size = GetFileSize(catalog->database_path());
  assert(catalog_size > 0);
  virtual_catalog->InsertBindMountpoint(mountpoint, tag.hash, catalog_size);
}


void VirtualCatalog::InsertPrivateSnapshot(TagId tag) {
  LogCvmfs(kLogCatalog, kLogDebug,
           "add private snapshot %s to virtual catalog",
           tag.name.c_str());
  const UniquePtr<Catalog> catalog(
      assistant_.GetCatalog(tag.hash, swissknife::Assistant::kOpenReadOnly));
  assert(catalog.IsValid());
  assert(catalog->root_prefix().IsEmpty());
  DirectoryEntry entry_root;
  const bool retval = catalog->LookupPath(PathString(""), &entry_root);
  assert(retval);

  // Add directory entry named by the catalog hash string
  DirectoryEntryBase entry_dir = entry_root;
  entry_dir.name_ = NameString(tag.name);
  catalog_mgr_->AddDirectory(
      entry_dir, XattrList(),
      string(kVirtualPath) + "/" + string(kUserDirectory));

  // Mark the entry as a bind-mountpoint AND hidden (not shown in listings)
  WritableCatalog *virtual_catalog = catalog_mgr_->GetHostingCatalog(
      kVirtualPath);
  assert(virtual_catalog != NULL);
  const string mountpoint = "/" + string(kVirtualPath) + "/"
                            + string(kUserDirectory) + "/" + tag.name;
  DirectoryEntry entry_bind_mountpoint(entry_dir);
  entry_bind_mountpoint.set_is_bind_mountpoint(true);
  entry_bind_mountpoint.set_is_hidden(true);
  virtual_catalog->UpdateEntry(entry_bind_mountpoint, mountpoint);

  // Register nested catalog
  const uint64_t catalog_size = GetFileSize(catalog->database_path());
  assert(catalog_size > 0);
  virtual_catalog->InsertBindMountpoint(mountpoint, tag.hash, catalog_size);
}


void VirtualCatalog::RemovePrivateSnapshot(TagId tag) {
  LogCvmfs(kLogCatalog, kLogDebug,
           "remove private snapshot %s from virtual catalog",
           tag.name.c_str());
  const string entry_dir = string(kVirtualPath) + "/"
                           + string(kUserDirectory) + "/" + tag.name;
  catalog_mgr_->RemoveDirectory(entry_dir);

  WritableCatalog *virtual_catalog = catalog_mgr_->GetHostingCatalog(
      kVirtualPath);
  assert(virtual_catalog != NULL);
  virtual_catalog->RemoveBindMountpoint("/" + entry_dir);
}


void VirtualCatalog::GeneratePrivateSnapshots() {
  LogCvmfs(kLogCvmfs, kLogStdout, "Creating private virtual snapshots");
  EnsureUserDirectoryPresence();

  // Collect all unique catalog hashes from history (skip trunk tags)
  vector<TagId> tags_history;
  GetSortedTagsFromHistory(&tags_history);

  // Deduplicate: keep one TagId per unique hash (name = hash string)
  vector<TagId> hashes_history;
  shash::Any last_hash;
  for (unsigned i = 0, l = tags_history.size(); i < l; ++i) {
    if (tags_history[i].hash == last_hash)
      continue;
    last_hash = tags_history[i].hash;
    const string hash_name = tags_history[i].hash.ToStringWithSuffix();
    hashes_history.push_back(TagId(hash_name, tags_history[i].hash));
  }
  std::sort(hashes_history.begin(), hashes_history.end());

  // Read hashes already present in the virtual catalog
  vector<TagId> hashes_catalog;
  GetSortedHashesFromCatalog(&hashes_catalog);

  // Sentinel end-markers
  string end_marker = "";
  if (!hashes_history.empty())
    end_marker = std::max(end_marker, hashes_history.rbegin()->name);
  if (!hashes_catalog.empty())
    end_marker = std::max(end_marker, hashes_catalog.rbegin()->name);
  end_marker += "X";
  hashes_history.push_back(TagId(end_marker, shash::Any()));
  hashes_catalog.push_back(TagId(end_marker, shash::Any()));

  // Walk both sorted lists and sync
  unsigned i_hist = 0, i_cat = 0;
  const unsigned last_hist = hashes_history.size() - 1;
  const unsigned last_cat  = hashes_catalog.size() - 1;
  while ((i_hist < last_hist) || (i_cat < last_cat)) {
    const TagId t_hist = hashes_history[i_hist];
    const TagId t_cat  = hashes_catalog[i_cat];

    if (t_hist == t_cat) {
      i_hist++; i_cat++;
      continue;
    }
    if (t_hist.name == t_cat.name) {
      // Hash collision in name — should never happen (name IS the hash)
      RemovePrivateSnapshot(t_cat);
      InsertPrivateSnapshot(t_hist);
      i_hist++; i_cat++;
      continue;
    }
    if (t_hist.name < t_cat.name) {
      InsertPrivateSnapshot(t_hist);
      i_hist++;
      continue;
    }
    assert(t_hist.name > t_cat.name);
    RemovePrivateSnapshot(t_cat);
    i_cat++;
  }
}


void VirtualCatalog::Remove() {
  LogCvmfs(kLogCvmfs, kLogStdout, "Removing .cvmfs virtual catalog");

  // Safety check, make sure we don't remove the entire repository
  WritableCatalog *virtual_catalog = catalog_mgr_->GetHostingCatalog(
      kVirtualPath);
  assert(!virtual_catalog->IsRoot());
  DirectoryEntry entry_virtual;
  const bool retval = catalog_mgr_->LookupPath(
      PathString("/" + string(kVirtualPath)), kLookupDefault, &entry_virtual);
  assert(retval);
  assert(entry_virtual.IsHidden());

  RemoveRecursively(kVirtualPath);
  catalog_mgr_->RemoveNestedCatalog(kVirtualPath);
  catalog_mgr_->RemoveDirectory(kVirtualPath);
}


void VirtualCatalog::RemoveRecursively(const string &directory) {
  DirectoryEntryList listing;
  const bool retval = catalog_mgr_->Listing(PathString("/" + directory),
                                            &listing);
  assert(retval);
  for (unsigned i = 0; i < listing.size(); ++i) {
    const string this_path = directory + "/" + listing[i].name().ToString();
    if (listing[i].IsDirectory()) {
      if (!listing[i].IsBindMountpoint())
        RemoveRecursively(this_path);
      catalog_mgr_->RemoveDirectory(this_path);
    } else if (listing[i].IsRegular()) {
      assert(listing[i].name().ToString() == ".cvmfscatalog");
      catalog_mgr_->RemoveFile(this_path);
    } else {
      abort();
    }
  }
}


void VirtualCatalog::RemoveSnapshot(TagId tag) {
  LogCvmfs(kLogCatalog, kLogDebug,
           "remove snapshot %s (%s) from virtual catalog", tag.name.c_str(),
           tag.hash.ToString().c_str());
  const string tag_dir = string(kVirtualPath) + "/" + string(kSnapshotDirectory)
                         + "/" + tag.name;
  catalog_mgr_->RemoveDirectory(tag_dir);

  WritableCatalog *virtual_catalog = catalog_mgr_->GetHostingCatalog(
      kVirtualPath);
  assert(virtual_catalog != NULL);
  virtual_catalog->RemoveBindMountpoint("/" + tag_dir);
}


VirtualCatalog::VirtualCatalog(manifest::Manifest *m,
                               download::DownloadManager *d,
                               catalog::WritableCatalogManager *c,
                               SyncParameters *p)
    : catalog_mgr_(c), assistant_(d, m, p->stratum0, p->dir_temp) { }

}  // namespace catalog
