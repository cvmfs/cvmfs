#include "inode.h"

#include "catalog_mgr.h"         // kLookupDefault
#include "catalog_mgr_client.h"  // GetRootInode(), LookupPath
#include "glue_buffer.h"         // glue::InodeEx
#include "lru_md.h"              // Lookup(ino,dirent)
#include "nfs_maps.h"            // nfs_maps()->GetPath()

bool cvmfs::GetDirentForInode(MountPoint *mountpoint, FileSystem *filesystem,
                       const fuse_ino_t ino, catalog::DirectoryEntry *dirent) {
  // Lookup inode in cache
  if (mountpoint->inode_cache()->Lookup(ino, dirent))
    return true;

  // Look in the catalogs in 2 steps: lookup inode->path, lookup path
  static const catalog::DirectoryEntry
      dirent_negative = catalog::DirectoryEntry(catalog::kDirentNegative);
  // Reset directory entry.  If the function returns false and dirent is no
  // the kDirentNegative, it was an I/O error
  *dirent = catalog::DirectoryEntry();

  catalog::ClientCatalogManager *catalog_mgr = mountpoint->catalog_mgr();

  if (filesystem->IsNfsSource()) {
    // NFS mode
    PathString path;
    const bool retval = filesystem->nfs_maps()->GetPath(ino, &path);
    if (!retval) {
      *dirent = dirent_negative;
      return false;
    }
    if (catalog_mgr->LookupPath(path, catalog::kLookupDefault, dirent)) {
      // Fix inodes
      dirent->set_inode(ino);
      mountpoint->inode_cache()->Insert(ino, *dirent);
      return true;
    }
    return false;  // Not found in catalog or catalog load error
  }

  // Non-NFS mode
  PathString path;
  if (ino == catalog_mgr->GetRootInode()) {
    const bool retval = catalog_mgr->LookupPath(
        PathString(), catalog::kLookupDefault, dirent);

    if (!AssertOrLog(retval, kLogCvmfs, kLogSyslogWarn | kLogDebug,
                     "GetDirentForInode: Race condition? Not found dirent %s",
                     dirent->name().c_str())) {
      return false;
    }

    dirent->set_inode(ino);
    mountpoint->inode_cache()->Insert(ino, *dirent);
    return true;
  }

  glue::InodeEx inode_ex(ino, glue::InodeEx::kUnknownType);
  const bool retval = mountpoint->inode_tracker()->FindPath(&inode_ex, &path);
  if (!retval) {
    // This may be a retired inode whose stat information is only available
    // in the page cache tracker because there is still an open file
    LogCvmfs(kLogCvmfs, kLogDebug,
             "GetDirentForInode inode lookup failure %" PRId64, ino);
    *dirent = dirent_negative;
    // Indicate that the inode was not found in the tracker rather than not
    // found in the catalog
    dirent->set_inode(ino);
    return false;
  }
  if (catalog_mgr->LookupPath(path, catalog::kLookupDefault, dirent)) {
    if (!inode_ex.IsCompatibleFileType(dirent->mode())) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "Warning: inode %" PRId64 " (%s) changed file type", ino,
               path.c_str());
      // TODO(jblomer): we detect this issue but let it continue unhandled.
      // Fix me.
    }

    // Fix inodes
    dirent->set_inode(ino);
    mountpoint->inode_cache()->Insert(ino, *dirent);
    return true;
  }

  // Can happen after reload of catalogs or on catalog load failure
  LogCvmfs(kLogCvmfs, kLogDebug, "GetDirentForInode path lookup failure");
  return false;
}

bool cvmfs::GetPathForInode(MountPoint *mountpoint, FileSystem *filesystem,
                     const fuse_ino_t ino, PathString *path) {
  // Check the path cache first
  if (mountpoint->path_cache()->Lookup(ino, path))
    return true;

  if (filesystem->IsNfsSource()) {
    // NFS mode, just a lookup
    LogCvmfs(kLogCvmfs, kLogDebug, "MISS %lu - lookup in NFS maps", ino);
    if (filesystem->nfs_maps()->GetPath(ino, path)) {
      mountpoint->path_cache()->Insert(ino, *path);
      return true;
    }
    return false;
  }

  if (ino == mountpoint->catalog_mgr()->GetRootInode())
    return true;

  LogCvmfs(kLogCvmfs, kLogDebug, "MISS %lu - looking in inode tracker", ino);
  glue::InodeEx inode_ex(ino, glue::InodeEx::kUnknownType);
  const bool retval = mountpoint->inode_tracker()->FindPath(&inode_ex, path);

  if (!AssertOrLog(retval, kLogCvmfs, kLogSyslogWarn | kLogDebug,
                   "GetPathForInode: Race condition? "
                   "Inode not found in inode tracker at path %s",
                   path->c_str())) {
    return false;
  }


  mountpoint->path_cache()->Insert(ino, *path);
  return true;
}

