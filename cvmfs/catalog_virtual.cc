/**
 * This file is part of the CernVM File System.
 */

#include "catalog_virtual.h"

#include <algorithm>
#include <cassert>
#include <cstdlib>

#include "catalog_mgr_rw.h"
#include "compression/compression.h"
#include "history.h"
#include "swissknife_history.h"
#include "swissknife_sync.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"
#include "xattr.h"

using namespace std;  // NOLINT

namespace catalog {

const char *VirtualCatalog::kVirtualPath = ".cvmfs";
const char *VirtualCatalog::kSnapshotDirectory = "snapshots";
const int VirtualCatalog::kActionNone              = 0x00;
const int VirtualCatalog::kActionGenerateSnapshots = 0x01;
const int VirtualCatalog::kActionRemove            = 0x02;


void VirtualCatalog::CreateBaseDirectory() {
  // Add /.cvmfs as a nested catalog
  DirectoryEntryBase entry_dir;
  entry_dir.name_ = NameString(string(kVirtualPath));
  entry_dir.mode_ = S_IFDIR |
                    S_IRUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
  entry_dir.uid_ = 0;
  entry_dir.gid_ = 0;
  entry_dir.size_ = 97;
  entry_dir.mtime_ = time(NULL);
  catalog_mgr_->AddDirectory(entry_dir, XattrList(), "");
  WritableCatalog *parent_catalog =
    catalog_mgr_->GetHostingCatalog(kVirtualPath);
  catalog_mgr_->CreateNestedCatalog(kVirtualPath);
  WritableCatalog *virtual_catalog =
    catalog_mgr_->GetHostingCatalog(kVirtualPath);
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
  shash::Algorithms algorithm = catalog_mgr_->spooler_->GetHashAlgorithm();
  shash::Any file_hash(algorithm);
  void *empty_compressed;
  uint64_t sz_empty_compressed;
  bool retval = zlib::CompressMem2Mem(
    NULL, 0, &empty_compressed, &sz_empty_compressed);
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
  XattrList xattrs;
  catalog_mgr_->AddFile(entry_marker, xattrs, kVirtualPath);
}


void VirtualCatalog::CreateSnapshotDirectory() {
  DirectoryEntryBase entry_dir;
  entry_dir.name_ = NameString(string(kSnapshotDirectory));
  entry_dir.mode_ = S_IFDIR |
                    S_IRUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
  entry_dir.uid_ = 0;
  entry_dir.gid_ = 0;
  entry_dir.size_ = 97;
  entry_dir.mtime_ = time(NULL);
  catalog_mgr_->AddDirectory(entry_dir, XattrList(), kVirtualPath);
}


/**
 * Checks for the top-level /.cvmfs directory and creates it as a nested catalog
 * if necessary.
 */
void VirtualCatalog::EnsurePresence() {
  DirectoryEntry e;
  bool retval = catalog_mgr_->LookupPath("/" + string(kVirtualPath),
                                        kLookupDefault, &e);
  if (!retval) {
    LogCvmfs(kLogCatalog, kLogDebug, "creating new virtual catalog");
    CreateBaseDirectory();
    CreateNestedCatalogMarker();
    CreateSnapshotDirectory();
  }
  assert(catalog_mgr_->IsTransitionPoint(kVirtualPath));
}


void VirtualCatalog::Generate(int actions) {
  if (actions & kActionGenerateSnapshots) {
    GenerateSnapshots();
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
  unsigned last_history = tags_history.size() - 1;
  unsigned last_catalog = tags_catalog.size() - 1;
  while ((i_history < last_history) || (i_catalog < last_catalog)) {
    TagId t_history = tags_history[i_history];
    TagId t_catalog = tags_catalog[i_catalog];

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


bool VirtualCatalog::ParseActions(
  const string &action_desc,
  int *actions)
{
  *actions = kActionNone;
  if (action_desc.empty())
    return true;

  vector<string> action_tokens = SplitString(action_desc, ',');
  for (unsigned i = 0; i < action_tokens.size(); ++i) {
    if (action_tokens[i] == "snapshots") {
      *actions |= kActionGenerateSnapshots;
    } else if (action_tokens[i] == "remove") {
      *actions |= kActionRemove;
    } else {
      return false;
    }
  }
  return true;
}


void VirtualCatalog::GetSortedTagsFromHistory(vector<TagId> *tags) {
  UniquePtr<history::History> history(
    assistant_.GetHistory(swissknife::Assistant::kOpenReadOnly));
  vector<history::History::Tag> tags_history;
  bool retval = history->List(&tags_history);
  assert(retval);
  for (unsigned i = 0, l = tags_history.size(); i < l; ++i) {
    if ((tags_history[i].name == swissknife::CommandTag::kHeadTag) ||
        (tags_history[i].name == swissknife::CommandTag::kPreviousHeadTag))
    {
      continue;
    }
    tags->push_back(TagId(tags_history[i].name, tags_history[i].root_hash));
  }
  std::sort(tags->begin(), tags->end());
}


void VirtualCatalog::GetSortedTagsFromCatalog(vector<TagId> *tags) {
  WritableCatalog *virtual_catalog =
    catalog_mgr_->GetHostingCatalog(kVirtualPath);
  assert(virtual_catalog != NULL);
  Catalog::NestedCatalogList nested_catalogs =
    virtual_catalog->ListNestedCatalogs();
  for (unsigned i = 0, l = nested_catalogs.size(); i < l; ++i) {
    tags->push_back(TagId(GetFileName(nested_catalogs[i].mountpoint).ToString(),
                          nested_catalogs[i].hash));
  }
  std::sort(tags->begin(), tags->end());
}


void VirtualCatalog::InsertSnapshot(TagId tag) {
  LogCvmfs(kLogCatalog, kLogDebug, "add snapshot %s (%s) to virtual catalog",
           tag.name.c_str(), tag.hash.ToString().c_str());
  UniquePtr<Catalog> catalog(assistant_.GetCatalog(tag.hash,
                             swissknife::Assistant::kOpenReadOnly));
  assert(catalog.IsValid());
  assert(catalog->root_prefix().IsEmpty());
  DirectoryEntry entry_root;
  bool retval = catalog->LookupPath(PathString(""), &entry_root);
  assert(retval);

  // Add directory entry
  DirectoryEntryBase entry_dir = entry_root;
  entry_dir.name_ = NameString(tag.name);
  catalog_mgr_->AddDirectory(entry_dir, XattrList(),
    string(kVirtualPath) + "/" + string(kSnapshotDirectory));

  // Set "bind mount" flag
  WritableCatalog *virtual_catalog =
    catalog_mgr_->GetHostingCatalog(kVirtualPath);
  assert(virtual_catalog != NULL);
  string mountpoint =
    "/" + string(kVirtualPath) + "/" + string(kSnapshotDirectory) + "/" +
    tag.name;
  DirectoryEntry entry_bind_mountpoint(entry_dir);
  entry_bind_mountpoint.set_is_bind_mountpoint(true);
  virtual_catalog->UpdateEntry(entry_bind_mountpoint, mountpoint);

  // Register nested catalog
  uint64_t catalog_size = GetFileSize(catalog->database_path());
  assert(catalog_size > 0);
  virtual_catalog->InsertBindMountpoint(mountpoint, tag.hash, catalog_size);
}


void VirtualCatalog::Remove() {
  LogCvmfs(kLogCvmfs, kLogStdout, "Removing .cvmfs virtual catalog");

  // Safety check, make sure we don't remove the entire repository
  WritableCatalog *virtual_catalog =
    catalog_mgr_->GetHostingCatalog(kVirtualPath);
  assert(!virtual_catalog->IsRoot());
  DirectoryEntry entry_virtual;
  bool retval = catalog_mgr_->LookupPath(
    PathString("/" + string(kVirtualPath)), kLookupDefault, &entry_virtual);
  assert(retval);
  assert(entry_virtual.IsHidden());

  RemoveRecursively(kVirtualPath);
  catalog_mgr_->RemoveNestedCatalog(kVirtualPath);
  catalog_mgr_->RemoveDirectory(kVirtualPath);
}


void VirtualCatalog::RemoveRecursively(const string &directory) {
  DirectoryEntryList listing;
  bool retval = catalog_mgr_->Listing(PathString("/" + directory), &listing);
  assert(retval);
  for (unsigned i = 0; i < listing.size(); ++i) {
    string this_path = directory + "/" + listing[i].name().ToString();
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
           "remove snapshot %s (%s) from virtual catalog",
           tag.name.c_str(), tag.hash.ToString().c_str());
  string tag_dir =
    string(kVirtualPath) + "/" + string(kSnapshotDirectory) + "/" + tag.name;
  catalog_mgr_->RemoveDirectory(tag_dir);

  WritableCatalog *virtual_catalog =
    catalog_mgr_->GetHostingCatalog(kVirtualPath);
  assert(virtual_catalog != NULL);
  virtual_catalog->RemoveBindMountpoint("/" + tag_dir);
}


VirtualCatalog::VirtualCatalog(
  manifest::Manifest *m,
  download::DownloadManager *d,
  catalog::WritableCatalogManager *c,
  SyncParameters *p)
  : catalog_mgr_(c)
  , assistant_(d, m, p->stratum0, p->dir_temp)
{ }

}  // namespace catalog
