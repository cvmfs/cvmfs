/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MAGIC_XATTR_H_
#define CVMFS_MAGIC_XATTR_H_

#include <map>
#include <string>

#include "catalog_counters.h"
#include "directory_entry.h"
#include "shortstring.h"
#include "util/exception.h"
#include "util/string.h"

class MountPoint;

enum MagicXattrFlavor {
  kXattrBase = 0,
  kXattrWithHash,
  kXattrRegular,
  kXattrSymlink,
  kXattrAuthz
};

/**
 * This is a base class for magic extended attribute. Concrete extended
 * attributes inherit from this class. It should be generally used only
 * in cooperation with MagicXattrManager.
 * It contains an access mutex. Users should use Lock() and Release()
 * before and after usage (Lock() is called implicitly in MagicXattrManager).
 *
 * To read out the attribute value, do:
 * 0. Get an instance through MagicXattrManager::Get()
 * 1. Call PrepareValueFenced() inside FuseRemounter::fence()
 * 2. Call GetValue() to get the actual value (can be called outside the fence)
 */
class BaseMagicXattr {
  friend class MagicXattrManager;

 public:
  BaseMagicXattr() {
    int retval = pthread_mutex_init(&access_mutex_, NULL);
    assert(retval == 0);
  }
  /**
  * This function is used to obtain the necessary information while
  * inside FuseRemounter::fence(), which should prevent data races.
  */
  virtual bool PrepareValueFenced() { return true; }
  /**
   * This function needs to be called after PrepareValueFenced(),
   * which prepares the necessary data.
   * It does the computationaly intensive part, which should not
   * be done inside the FuseRemounter::fence(), and returns the
   * value.
   */
  virtual std::string GetValue() = 0;
  virtual MagicXattrFlavor GetXattrFlavor() { return kXattrBase; }

  void Lock(PathString path, catalog::DirectoryEntry *dirent) {
    int retval = pthread_mutex_lock(&access_mutex_);
    assert(retval == 0);
    path_ = path;
    dirent_ = dirent;
  }
  void Release() {
    int retval = pthread_mutex_unlock(&access_mutex_);
    assert(retval == 0);
  }

  virtual ~BaseMagicXattr() {}

 protected:
  MountPoint *mount_point_;
  PathString path_;
  catalog::DirectoryEntry *dirent_;

  pthread_mutex_t access_mutex_;
};

/**
 * This wrapper ensures that the attribute instance "ptr_" is
 * released after the user finishes using it (on wrapper destruction).
 */
class MagicXattrRAIIWrapper: public SingleCopy {
 public:
  inline MagicXattrRAIIWrapper() : ptr_(NULL) { }

  inline explicit MagicXattrRAIIWrapper(
    BaseMagicXattr *ptr,
    PathString path,
    catalog::DirectoryEntry *d)
    : ptr_(ptr)
  {
    if (ptr_ != NULL) ptr_->Lock(path, d);
  }
  /// Wraps around a BaseMagicXattr* tha is already locked (or NULL)
  inline explicit MagicXattrRAIIWrapper(BaseMagicXattr *ptr) : ptr_(ptr) { }

  inline ~MagicXattrRAIIWrapper() { if (ptr_ != NULL) ptr_->Release(); }

  inline BaseMagicXattr* operator->() const { return ptr_; }
  inline bool IsNull() const { return ptr_ == NULL; }
  inline BaseMagicXattr* Move() { BaseMagicXattr* ret = ptr_; ptr_ = NULL; return ret; }

 protected:
  BaseMagicXattr *ptr_;
};

class WithHashMagicXattr : public BaseMagicXattr {
  virtual MagicXattrFlavor GetXattrFlavor() { return kXattrWithHash; }
};

class RegularMagicXattr : public BaseMagicXattr {
  virtual MagicXattrFlavor GetXattrFlavor() { return kXattrRegular; }
};

class SymlinkMagicXattr : public BaseMagicXattr {
  virtual MagicXattrFlavor GetXattrFlavor() { return kXattrSymlink; }
};

/**
 * This class is acting as a user entry point for magic extended attributes.
 * It instantiates all defined attributes in the constructor.
 * Users can:
 * 1. Register additional attributes
 * 2. Get a string containing zero-byte delimited list of attribute names
 *    (used in "cvmfs.cc")
 * 3. Get an attribute by name. Specifically, get a RAII wrapper around
 *    a singleton attribute instance. This means that the attribute instance
 *    registered with the manager does not get cloned or copied inside Get().
 *    Instead, member variables are set and the original instance is returned.
 *    A mutex prevents from race conditions in case of concurrent access.
 */
class MagicXattrManager : public SingleCopy {
 public:
  MagicXattrManager(MountPoint *mountpoint, bool hide_magic_xattrs);
  /// The returned BaseMagicXattr* is supposed to be wrapped by a
  /// MagicXattrRAIIWrapper
  BaseMagicXattr* GetLocked(const std::string &name, PathString path,
                            catalog::DirectoryEntry *d);
  std::string GetListString(catalog::DirectoryEntry *dirent);
  void Register(const std::string &name, BaseMagicXattr *magic_xattr);

  bool hide_magic_xattrs() { return hide_magic_xattrs_; }

 protected:
  std::map<std::string, BaseMagicXattr *> xattr_list_;
  MountPoint *mount_point_;
  bool hide_magic_xattrs_;
};

class AuthzMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
  virtual MagicXattrFlavor GetXattrFlavor();
};

class CatalogCountersMagicXattr : public BaseMagicXattr {
  std::string subcatalog_path_;
  catalog::Counters counters_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class ChunkListMagicXattr : public RegularMagicXattr {
  std::string chunk_list_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class ChunksMagicXattr : public RegularMagicXattr {
  uint64_t n_chunks_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class CompressionMagicXattr : public RegularMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class DirectIoMagicXattr : public RegularMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class ExternalFileMagicXattr : public RegularMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class ExternalHostMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class ExternalTimeoutMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class FqrnMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class HashMagicXattr : public WithHashMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class HostMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class HostListMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class LHashMagicXattr : public WithHashMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class NCleanup24MagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class NClgMagicXattr : public BaseMagicXattr {
  int n_catalogs_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class NDirOpenMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class NDownloadMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class NIOErrMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class NOpenMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class HitrateMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class ProxyMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class PubkeysMagicXattr : public BaseMagicXattr {
  std::string pubkeys_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class RawlinkMagicXattr : public SymlinkMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class RepoCountersMagicXattr : public BaseMagicXattr {
  catalog::Counters counters_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class RepoMetainfoMagicXattr : public BaseMagicXattr {
  static uint64_t kMaxMetainfoLength;

  shash::Any metainfo_hash_;
  std::string error_reason_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class RevisionMagicXattr : public BaseMagicXattr {
  uint64_t revision_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class RootHashMagicXattr : public BaseMagicXattr {
  shash::Any root_hash_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class RxMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class SpeedMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class TagMagicXattr : public BaseMagicXattr {
  std::string tag_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class TimeoutMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class TimeoutDirectMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class UsedFdMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class UsedDirPMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

class VersionMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

#endif  // CVMFS_MAGIC_XATTR_H_
