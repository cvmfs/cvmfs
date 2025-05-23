/**
 * This file is part of the CernVM File System
 */

#include "sync_item_dummy.h"

#include <cassert>
#include <ctime>

namespace publish {

catalog::DirectoryEntryBase SyncItemDummyDir::CreateBasicCatalogDirent(
    bool /* enable_mtime_ns */) const {
  catalog::DirectoryEntryBase dirent;

  dirent.inode_ = catalog::DirectoryEntry::kInvalidInode;

  dirent.linkcount_ = 1;

  dirent.mode_ = kPermision;

  dirent.uid_ = scratch_stat_.stat.st_uid;
  dirent.gid_ = scratch_stat_.stat.st_gid;
  dirent.size_ = 4096;
  dirent.mtime_ = time(NULL);
  dirent.checksum_ = this->GetContentHash();
  dirent.is_external_file_ = this->IsExternalData();
  dirent.compression_algorithm_ = this->GetCompressionAlgorithm();

  dirent.name_.Assign(this->filename().data(), this->filename().length());

  assert(dirent.IsDirectory());

  return dirent;
}


SyncItemType SyncItemDummyDir::GetScratchFiletype() const { return kItemDir; }

}  // namespace publish
