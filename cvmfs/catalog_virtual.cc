/**
 * This file is part of the CernVM File System.
 */
#include "cvmfs_config.h"
#include "catalog_virtual.h"

#include <cassert>

#include "catalog_mgr_rw.h"
#include "catalog_rw.h"
#include "compression.h"
#include "logging.h"
#include "hash.h"
#include "history.h"
#include "swissknife_sync.h"
#include "util/posix.h"
#include "xattr.h"

using namespace std;  // NOLINT

namespace catalog {

const string VirtualCatalog::kVirtualPath = ".cvmfs";


void VirtualCatalog::CreateCatalog() {
  DirectoryEntryBase entry_dir;
  entry_dir.name_ = NameString(kVirtualPath);
  entry_dir.mode_ = S_IFDIR |
                    S_IRUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
  entry_dir.uid_ = 0;
  entry_dir.gid_ = 0;
  entry_dir.size_ = 97;
  entry_dir.mtime_ = time(NULL);
  catalog_mgr_->AddDirectory(entry_dir, "");
  catalog_mgr_->CreateNestedCatalog(kVirtualPath);

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


void VirtualCatalog::EnsurePresence() {
  DirectoryEntry e;
  bool retval = catalog_mgr_->LookupPath("/" + kVirtualPath, kLookupSole, &e);
  if (!retval)
    CreateCatalog();
  assert(catalog_mgr_->IsTransitionPoint(kVirtualPath));
}


void VirtualCatalog::GenerateSnapshots() {
  EnsurePresence();
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
