/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_SYNC_ITEM_H_
#define CVMFS_SYNC_ITEM_H_

#include <sys/types.h>

#if !defined(__APPLE__)
#include <sys/sysmacros.h>
#endif  // __APPLE__

#include <cstring>
#include <map>
#include <string>

#include "archive.h"
#include "directory_entry.h"
#include "file_chunk.h"
#include "hash.h"
#include "ingestion/ingestion_source.h"
#include "platform.h"
#include "util/shared_ptr.h"

class IngestionSource;

namespace publish {

enum SyncItemType {
  kItemDir,
  kItemFile,
  kItemSymlink,
  kItemCharacterDevice,
  kItemBlockDevice,
  kItemFifo,
  kItemSocket,
  kItemNew,
  kItemMarker,
  kItemUnknown,
  kItemTarfile
};

class SyncUnion;
/**
 * Every directory entry emitted by the FileSystemTraversal is wrapped in a
 * SyncItem structure by the factory method SyncUnion::CreateSyncItem().
 *
 * Since we are dealing with a union file system setup, this class represents
 * potentially three concrete files:
 *   - <read-only path>/<filename>                 | cf. rdonly_stat_
 *   - <scratch (read-write) branch>/<filename>    | cf. scratch_stat_
 *   - <union volume path>/<filename>              | cf. union_stat_
 *
 * This class caches stat calls to the underlying files in different branches of
 * the union file system and hides some interpretation details.
 */
class SyncItem {
  // only SyncUnion can create SyncItems (see SyncUnion::CreateSyncItem)
  friend class SyncUnion;
  friend class SyncUnionTarball;

 public:
  SyncItem();
  virtual ~SyncItem();

  inline bool IsDirectory()       const { return IsType(kItemDir);             }
  inline bool WasDirectory()      const { return WasType(kItemDir);            }
  inline bool IsRegularFile()     const { return IsType(kItemFile);            }
  inline bool WasRegularFile()    const { return WasType(kItemFile);           }
  inline bool IsSymlink()         const { return IsType(kItemSymlink);         }
  inline bool WasSymlink()        const { return WasType(kItemSymlink);        }
  inline bool IsNew()             const { return WasType(kItemNew);            }
  inline bool IsCharacterDevice() const { return IsType(kItemCharacterDevice); }
  inline bool IsBlockDevice()     const { return IsType(kItemBlockDevice);     }
  inline bool IsFifo()            const { return IsType(kItemFifo);            }
  inline bool IsSocket()          const { return IsType(kItemSocket);          }
  inline bool IsGraftMarker()     const { return IsType(kItemMarker);          }
  inline bool IsExternalData()    const { return external_data_;               }

  inline bool IsWhiteout()        const { return whiteout_;                    }
  inline bool IsCatalogMarker()   const { return filename_ == ".cvmfscatalog"; }
  inline bool IsOpaqueDirectory() const { return IsDirectory() && opaque_;     }

  inline bool IsSpecialFile()     const {
    return IsCharacterDevice() || IsBlockDevice() || IsFifo() || IsSocket();
  }
  inline bool WasSpecialFile()    const {
    return WasType(kItemCharacterDevice) ||
           WasType(kItemBlockDevice) ||
           WasType(kItemFifo) ||
           WasType(kItemSocket);
  }

  inline unsigned int GetRdevMajor()     const {
    assert(IsSpecialFile());
    StatUnion(true); return major(union_stat_.stat.st_rdev);
  }

  inline unsigned int GetRdevMinor()     const {
    assert(IsSpecialFile());
    StatUnion(true); return minor(union_stat_.stat.st_rdev);
  }

  bool HasCatalogMarker()         const { return has_catalog_marker_;          }
  bool HasGraftMarker()           const { return graft_marker_present_;        }
  bool IsValidGraft()             const { return valid_graft_;                 }
  bool IsChunkedGraft()           const { return graft_chunklist_;             }

  inline const FileChunkList* GetGraftChunks() const {return graft_chunklist_;}
  inline shash::Any GetContentHash() const { return content_hash_; }
  inline void SetContentHash(const shash::Any &hash) { content_hash_ = hash; }
  inline bool HasContentHash() const { return !content_hash_.IsNull(); }
  void SetExternalData(bool val) {external_data_ = val;}

  inline zlib::Algorithms GetCompressionAlgorithm() const {
    return compression_algorithm_;
  }
  inline void SetCompressionAlgorithm(const zlib::Algorithms &alg) {
    compression_algorithm_ = alg;
  }

  /**
   * Generates a DirectoryEntry that can be directly stored into a catalog db.
   * Note: this sets the inode fields to kInvalidInode as well as the link
   *       count to 1 if MaskHardlink() has been called before (cf. OverlayFS)
   * @return  a DirectoryEntry structure to be written into a catalog
   */
  virtual catalog::DirectoryEntryBase CreateBasicCatalogDirent() const;

  inline std::string GetRelativePath() const {
    return (relative_parent_path_.empty()) ?
      filename_                            :
      relative_parent_path_ + (filename_.empty() ? "" : ("/" + filename_));
  }

  std::string GetRdOnlyPath() const;
  std::string GetUnionPath() const;
  std::string GetScratchPath() const;

  void MarkAsWhiteout(const std::string &actual_filename);
  void MarkAsOpaqueDirectory();

  /**
   * Union file systems (i.e. OverlayFS) might not properly support hardlinks,
   * forcing us to ignore them during publishing. A 'masked hardlink' will be
   * treated as a normal file (linkcount == 1). Hence, any created hardlinks
   * will be broken up into individual files with differing inodes.
   */
  inline void MaskHardlink() { masked_hardlink_ = true; }
  inline bool HasHardlinks() const {
    return !masked_hardlink_ && GetUnionLinkcount() > 1;
  }

  unsigned int GetRdOnlyLinkcount() const;
  uint64_t GetRdOnlyInode() const;
  unsigned int GetUnionLinkcount() const;
  uint64_t GetUnionInode() const;

  inline std::string filename() const { return filename_; }
  inline std::string relative_parent_path() const {
    return relative_parent_path_;
  }

  virtual IngestionSource *GetIngestionSource() const;
  virtual void AlreadyCreatedDir() const { rdonly_type_ = kItemDir; }

  bool operator==(const SyncItem &other) const {
    return ((relative_parent_path_ == other.relative_parent_path_) &&
            (filename_ == other.filename_));
  }

 protected:
  inline platform_stat64 GetUnionStat() const {
    StatUnion();
    return union_stat_.stat;
  }

  SyncItemType GetRdOnlyFiletype() const;
  virtual SyncItemType GetScratchFiletype() const;

  /**
   * Checks if the SyncItem _is_ the given file type (file, dir, symlink, ...)
   * in the union file system volume. Hence: After the publish operation, the
   * file will be this type in CVMFS.
   * @param expected_type  the file type to be checked against
   * @return               true if file type matches the expected type
   */
  inline bool IsType(const SyncItemType expected_type) const {
    if (filename_.substr(0, 12) == ".cvmfsgraft-") {
      scratch_type_ = kItemMarker;
    } else if (scratch_type_ == kItemUnknown || scratch_type_ == kItemTarfile) {
      scratch_type_ = GetScratchFiletype();
    }
    return scratch_type_ == expected_type;
  }

  /**
   * Checks if the SyncItem _was_ the given file type (file, dir, symlink, ...)
   * in CVMFS (or the lower layer of the union file system). Hence: Before the
   * current transaction the file _was_ this type in CVMFS.
   * @param expected_type  the file type to be checked against
   * @return               true if file type was the expected type in CVMFS
   */
  inline bool WasType(const SyncItemType expected_type) const {
    if (rdonly_type_ == kItemUnknown) {
      rdonly_type_ = GetRdOnlyFiletype();
    }
    return rdonly_type_ == expected_type;
  }

  const SyncUnion *union_engine_; /**< this SyncUnion created this object */

  mutable SyncItemType scratch_type_;
  mutable SyncItemType rdonly_type_;

  /**
   * create a new SyncItem
   * Note: SyncItems cannot be created by any using code. SyncUnion will take
   *       care of their creating through a factory method to make sure they
   *       are initialised correctly (whiteout, hardlink handling, ...)
   *
   * @param dirPath the RELATIVE path to the file
   * @param filename the name of the file ;-)
   * @param entryType well...
   */
  SyncItem(const std::string &relative_parent_path, const std::string &filename,
           const SyncUnion *union_engine, const SyncItemType entry_type);
  void SetCatalogMarker() { has_catalog_marker_ = true; }

  platform_stat64 GetStatFromTar() const;
  /**
   * Structure to cache stat calls to the different file locations.
   */
  struct EntryStat {
    EntryStat() : obtained(false), error_code(0) {
      memset(&stat, 0, sizeof(stat));
    }

    inline SyncItemType GetSyncItemType() const {
      assert(obtained);
      if (S_ISREG(stat.st_mode)) return kItemFile;
      if (S_ISLNK(stat.st_mode)) return kItemSymlink;
      if (S_ISDIR(stat.st_mode)) return kItemDir;
      if (S_ISFIFO(stat.st_mode)) return kItemFifo;
      if (S_ISSOCK(stat.st_mode)) return kItemSocket;
      if (S_ISCHR(stat.st_mode)) return kItemCharacterDevice;
      if (S_ISBLK(stat.st_mode)) return kItemBlockDevice;
      return kItemUnknown;
    }

    bool obtained;   /**< false at the beginning, true after first stat call */
    int error_code;  /**< errno value of the stat call */
    platform_stat64 stat;
  };

  mutable EntryStat rdonly_stat_;
  mutable EntryStat union_stat_;
  mutable EntryStat scratch_stat_;

  bool has_catalog_marker_; /**< directory containing .cvmfscatalog */

 private:
  SyncItemType GetGenericFiletype(const EntryStat &stat) const;
  SyncItemType GetScratchTypeFromArchiveEntry() const;

  void CheckMarkerFiles();

  void CheckCatalogMarker();

  std::string GetGraftMarkerPath() const;
  void CheckGraft();

  bool whiteout_;                     /**< SyncUnion marked this as whiteout  */
  bool opaque_;                       /**< SyncUnion marked this as opaque dir*/
  bool masked_hardlink_;              /**< SyncUnion masked out the linkcount */
  bool valid_graft_;                  /**< checksum and size in graft marker */
  bool graft_marker_present_;         /**< .cvmfsgraft-$filename exists */

  bool external_data_;
  std::string relative_parent_path_;
  std::string filename_;

  /**
   * Chunklist from graft. Not initialized by default to save memory.
   */
  FileChunkList *graft_chunklist_;
  ssize_t graft_size_;

  // The hash of regular file's content
  shash::Any content_hash_;

  // The compression algorithm for the file
  zlib::Algorithms compression_algorithm_;

  // Lazy evaluation and caching of results of file stats
  inline void StatRdOnly(const bool refresh = false) const {
    StatGeneric(GetRdOnlyPath(), &rdonly_stat_, refresh);
  }
  inline void StatUnion(const bool refresh = false) const {
    StatGeneric(GetUnionPath(), &union_stat_, refresh);
  }
  virtual void StatScratch(const bool refresh = false) const {
    StatGeneric(GetScratchPath(), &scratch_stat_, refresh);
  }
  static void StatGeneric(const std::string  &path,
                          EntryStat          *info,
                          const bool          refresh);
};

typedef std::map<std::string, SharedPtr<SyncItem> > SyncItemList;

}  // namespace publish

#endif  // CVMFS_SYNC_ITEM_H_
