/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_SYNC_ITEM_DUMMY_H_
#define CVMFS_SYNC_ITEM_DUMMY_H_

#include <ctime>
#include <string>

#include "ingestion/ingestion_source.h"
#include "sync_item.h"
#include "sync_union_tarball.h"

namespace publish {

class SyncItemDummyCatalog : public SyncItem {
  friend class SyncUnionTarball;

 protected:
  SyncItemDummyCatalog(const std::string &relative_parent_path,
                       const SyncUnion *union_engine)
      : SyncItem(relative_parent_path, ".cvmfscatalog", union_engine,
                 kItemFile) { }

 public:
  bool IsType(const SyncItemType expected_type) const {
    return expected_type == kItemFile;
  }

  catalog::DirectoryEntryBase CreateBasicCatalogDirent(
      bool /* enable_mtime_ns */) const {
    catalog::DirectoryEntryBase dirent;
    std::string name(".cvmfscatalog");
    dirent.inode_ = catalog::DirectoryEntry::kInvalidInode;
    dirent.linkcount_ = 1;
    dirent.mode_ = S_IFREG | S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;
    dirent.uid_ = getuid();
    dirent.gid_ = getgid();
    dirent.size_ = 0;
    dirent.mtime_ = time(NULL);
    dirent.checksum_ = this->GetContentHash();
    dirent.is_external_file_ = false;
    dirent.compression_algorithm_ = this->GetCompressionAlgorithm();

    dirent.name_.Assign(name.data(), name.length());

    return dirent;
  }

  IngestionSource *CreateIngestionSource() const {
    return new StringIngestionSource("", GetUnionPath());
  }

  void StatScratch(const bool /* refresh */) const { return; }

  SyncItemType GetScratchFiletype() const { return kItemFile; }

  void MakePlaceholderDirectory() const { }
};

/**
 * Represents an empty regular file that is materialized on the fly, without a
 * backing entry in the tarball.  It is used by the tarball engine to stand in
 * for a hardlink whose target is not part of the archive (e.g. a cross-layer
 * hardlink in an OCI image layer): instead of cloning a non-existent source we
 * create an empty file with the link's own ownership and permissions.  The
 * content (and therefore the content hash) is the empty string, pushed through
 * the normal ingestion pipeline so that the empty object is uploaded and
 * compressed/hashed consistently with the spooler configuration.
 */
class SyncItemDummyFile : public SyncItem {
  friend class SyncUnionTarball;

 protected:
  SyncItemDummyFile(const std::string &relative_parent_path,
                    const std::string &filename, const SyncUnion *union_engine,
                    const unsigned int mode, const uid_t uid, const gid_t gid,
                    const time_t mtime)
      : SyncItem(relative_parent_path, filename, union_engine, kItemFile)
      , mode_(mode)
      , uid_(uid)
      , gid_(gid)
      , mtime_(mtime) { }

 public:
  bool IsType(const SyncItemType expected_type) const {
    return expected_type == kItemFile;
  }

  catalog::DirectoryEntryBase CreateBasicCatalogDirent(
      bool /* enable_mtime_ns */) const {
    catalog::DirectoryEntryBase dirent;
    dirent.inode_ = catalog::DirectoryEntry::kInvalidInode;
    dirent.linkcount_ = 1;
    // Force a regular file type, keeping only the permission bits of the
    // original hardlink entry.
    dirent.mode_ = (mode_ & 07777) | S_IFREG;
    dirent.uid_ = uid_;
    dirent.gid_ = gid_;
    dirent.size_ = 0;
    dirent.mtime_ = mtime_;
    dirent.checksum_ = this->GetContentHash();
    dirent.is_external_file_ = false;
    dirent.compression_algorithm_ = this->GetCompressionAlgorithm();

    dirent.name_.Assign(this->filename().data(), this->filename().length());

    return dirent;
  }

  IngestionSource *CreateIngestionSource() const {
    return new StringIngestionSource("", GetUnionPath());
  }

  void StatScratch(const bool /* refresh */) const { return; }

  SyncItemType GetScratchFiletype() const { return kItemFile; }

  void MakePlaceholderDirectory() const { }

 private:
  const unsigned int mode_;
  const uid_t uid_;
  const gid_t gid_;
  const time_t mtime_;
};

/*
 * This class represents dummy directories that we know are going to be there
 * but we still haven't found yet. This is possible in the extraction of
 * tarball, where the files are not extracted in order (root to leaves) but in a
 * random fashion.
 */
class SyncItemDummyDir : public SyncItemNative {
  friend class SyncUnionTarball;

 public:
  virtual catalog::DirectoryEntryBase CreateBasicCatalogDirent(
      bool enable_mtime_ns) const;
  SyncItemType GetScratchFiletype() const;
  virtual void MakePlaceholderDirectory() const { rdonly_type_ = kItemDir; }

 protected:
  SyncItemDummyDir(const std::string &relative_parent_path,
                   const std::string &filename, const SyncUnion *union_engine,
                   const SyncItemType entry_type)
      : SyncItemNative(relative_parent_path, filename, union_engine,
                       entry_type) {
    assert(kItemDir == entry_type);

    scratch_stat_.obtained = true;
    scratch_stat_.stat.st_mode = kPermision;
    scratch_stat_.stat.st_nlink = 1;
    scratch_stat_.stat.st_uid = getuid();
    scratch_stat_.stat.st_gid = getgid();
  }
  SyncItemDummyDir(const std::string &relative_parent_path,
                   const std::string &filename, const SyncUnion *union_engine,
                   const SyncItemType entry_type, uid_t uid, gid_t gid)
      : SyncItemNative(relative_parent_path, filename, union_engine,
                       entry_type) {
    assert(kItemDir == entry_type);

    scratch_stat_.obtained = true;
    scratch_stat_.stat.st_mode = kPermision;
    scratch_stat_.stat.st_nlink = 1;
    scratch_stat_.stat.st_uid = uid;
    scratch_stat_.stat.st_gid = gid;
  }

 private:
  static const mode_t kPermision = S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR
                                   | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
};

}  // namespace publish

#endif  // CVMFS_SYNC_ITEM_DUMMY_H_
