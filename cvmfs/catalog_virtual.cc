/**
 * This file is part of the CernVM File System.
 */
#include "cvmfs_config.h"
#include "catalog_virtual.h"

#include <algorithm>
#include <cassert>

#include "catalog_mgr_rw.h"
#include "compression.h"
#include "logging.h"
#include "history.h"
#include "swissknife_sync.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "xattr.h"

using namespace std;  // NOLINT

namespace catalog {

const string VirtualCatalog::kVirtualPath = ".cvmfs";
const string VirtualCatalog::kSnapshotDirectory = "snapshots";

namespace {

bool CmpTagName(const history::History::Tag &a, const history::History::Tag &b)
{
  return a.name < b.name;
}

bool CmpNestedCatalogPath(
  const Catalog::NestedCatalog &a,
  const Catalog::NestedCatalog &b)
{
  return a.path < b.path;
}

}  // anonymous namespace


void VirtualCatalog::CreateBaseDirectory() {
  // Add /.cvmfs as a nested catalog
  DirectoryEntryBase entry_dir;
  entry_dir.name_ = NameString(kVirtualPath);
  entry_dir.mode_ = S_IFDIR |
                    S_IRUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
  entry_dir.uid_ = 0;
  entry_dir.gid_ = 0;
  entry_dir.size_ = 97;
  entry_dir.mtime_ = time(NULL);
  catalog_mgr_->AddDirectory(entry_dir, "");
  WritableCatalog *parent_catalog =
    catalog_mgr_->GetHostingCatalog(kVirtualPath);
  catalog_mgr_->CreateNestedCatalog(kVirtualPath);
  WritableCatalog *virtual_catalog =
    catalog_mgr_->GetHostingCatalog(kVirtualPath);
  assert(parent_catalog != virtual_catalog);

  // Set hidden flag in parent catalog
  DirectoryEntry entry_parent;
  bool retval = parent_catalog->LookupPath(PathString("/" + kVirtualPath),
                                           &entry_parent);
  assert(retval);
  entry_parent.set_is_hidden(true);
  parent_catalog->UpdateEntry(entry_parent, "/" + kVirtualPath);

  // Set hidden flag in nested catalog
  DirectoryEntry entry_virtual;
  retval = virtual_catalog->LookupPath(PathString("/" + kVirtualPath),
                                       &entry_virtual);
  assert(retval);
  entry_virtual.set_is_hidden(true);
  virtual_catalog->UpdateEntry(entry_virtual, "/" + kVirtualPath);
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
  entry_dir.name_ = NameString(kSnapshotDirectory);
  entry_dir.mode_ = S_IFDIR |
                    S_IRUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
  entry_dir.uid_ = 0;
  entry_dir.gid_ = 0;
  entry_dir.size_ = 97;
  entry_dir.mtime_ = time(NULL);
  catalog_mgr_->AddDirectory(entry_dir, kVirtualPath);
}


/**
 * Checks for the top-level /.cvmfs directory and creates it as a nested catalog
 * if necessary.
 */
void VirtualCatalog::EnsurePresence() {
  DirectoryEntry e;
  bool retval = catalog_mgr_->LookupPath("/" + kVirtualPath, kLookupSole, &e);
  if (!retval) {
    LogCvmfs(kLogCatalog, kLogDebug, "creating new virtual catalog");
    CreateBaseDirectory();
    CreateNestedCatalogMarker();
    CreateSnapshotDirectory();
  }
  assert(catalog_mgr_->IsTransitionPoint(kVirtualPath));
}


void VirtualCatalog::GenerateSnapshots() {
  EnsurePresence();

  vector<TagId> tags_history;
  vector<TagId> tags_catalog;
  GetSortedTagsFromHistory(&tags_history);
  GetSortedTagsFromCatalog(&tags_catalog);
  // Add artifical end markers to both lists
  tags_history.push_back(TagId("", shash::Any()));
  tags_catalog.push_back(TagId("", shash::Any()));

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
    if ((t_history.name < t_catalog.name) || t_catalog.name.empty()) {
      InsertSnapshot(t_history);
      i_history++;
      continue;
    }

    // A tag was removed but it is still present in the catalog
    assert((t_history.name > t_catalog.name) || t_history.name.empty());
    RemoveSnapshot(t_catalog);
    i_catalog++;
  }
}


void VirtualCatalog::GetSortedTagsFromHistory(vector<TagId> *tags) {
  UniquePtr<history::History> history(
    assistant_.GetHistory(swissknife::Assistant::kOpenReadOnly));
  vector<history::History::Tag> tags_history;
  bool retval = history->List(&tags_history);
  assert(retval);
  sort(tags_history.begin(), tags_history.end(), CmpTagName);
  for (unsigned i = 0, l = tags_history.size(); i < l; ++i) {
    if ((tags_history[i].name == "trunk") ||
        (tags_history[i].name == "trunk-previous"))
    {
      continue;
    }
    tags->push_back(TagId(tags_history[i].name, tags_history[i].root_hash));
  }
}


void VirtualCatalog::GetSortedTagsFromCatalog(vector<TagId> *tags) {
  WritableCatalog *virtual_catalog =
    catalog_mgr_->GetHostingCatalog(kVirtualPath);
  assert(virtual_catalog != NULL);
  Catalog::NestedCatalogList nested_catalogs =
    virtual_catalog->ListNestedCatalogs();
  sort(nested_catalogs.begin(), nested_catalogs.end(), CmpNestedCatalogPath);
  for (unsigned i = 0, l = nested_catalogs.size(); i < l; ++i) {
    tags->push_back(TagId(GetFileName(nested_catalogs[i].path).ToString(),
                          nested_catalogs[i].hash));
  }
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
  catalog_mgr_->AddDirectory(entry_dir,
                             kVirtualPath + "/" + kSnapshotDirectory);

  // Set "bind mount" flag
  WritableCatalog *virtual_catalog =
    catalog_mgr_->GetHostingCatalog(kVirtualPath);
  assert(virtual_catalog != NULL);
  const string mountpoint =
    "/" + kVirtualPath + "/" + kSnapshotDirectory + "/" + tag.name;
  DirectoryEntry entry_bind_mountpoint(entry_dir);
  entry_bind_mountpoint.set_is_bind_mountpoint(true);
  virtual_catalog->UpdateEntry(entry_bind_mountpoint, mountpoint);

  // Register nested catalog
  uint64_t catalog_size = GetFileSize(catalog->database_path());
  assert(catalog_size > 0);
  virtual_catalog->InsertNestedCatalog(
    mountpoint, NULL, tag.hash, catalog_size);
}


void VirtualCatalog::RemoveSnapshot(TagId tag) {
  LogCvmfs(kLogCatalog, kLogDebug,
           "remove snapshot %s (%s) from virtual catalog",
           tag.name.c_str(), tag.hash.ToString().c_str());

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
