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

class BaseMagicXattr {
  friend class MagicXattrManager;

 public:
  BaseMagicXattr() {
    int retval = pthread_mutex_init(&access_mutex_, NULL);
    assert(retval == 0);
  }

  virtual bool PrepareValueFenced() { return true; }
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

class MagicXattrRAIIWrapper {
 public:
  inline MagicXattrRAIIWrapper() : ref_(NULL) { }
  inline explicit MagicXattrRAIIWrapper(BaseMagicXattr *ref) : ref_(ref) { }
  inline ~MagicXattrRAIIWrapper() { if (ref_ != NULL) ref_->Release(); }
  inline BaseMagicXattr* operator->() const { return ref_; }
  inline bool IsNull() const { return ref_ == NULL; }
 
 protected:
  BaseMagicXattr *ref_;
};

class WithHashMagicXattr : public BaseMagicXattr {
  virtual MagicXattrFlavor GetXattrFlavor() { return kXattrWithHash; }
};

class RegularMagicXattr : public BaseMagicXattr {
  virtual MagicXattrFlavor GetXattrFlavor() { return kXattrRegular; }
};

class MagicXattrManager {
 public:
  explicit MagicXattrManager(MountPoint *mountpoint, bool hide_magic_xattrs);
  MagicXattrRAIIWrapper Get(const std::string &name, PathString path,
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
  std::string n_io_err_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class NOpenMagicXattr : public BaseMagicXattr {
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

class RawlinkMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
  virtual MagicXattrFlavor GetXattrFlavor();
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
  std::string n_used_fd_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class UsedDirPMagicXattr : public BaseMagicXattr {
  std::string n_used_dirp_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class VersionMagicXattr : public BaseMagicXattr {
  virtual std::string GetValue();
};

#endif  // CVMFS_MAGIC_XATTR_H_
