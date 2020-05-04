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
#include "util/string.h"

class MountPoint;

class BaseMagicXattr {
  friend class MagicXattrManager;

 public:
  virtual bool PrepareValueFenced() { return false; }
  virtual std::string GetValue() { return ""; }

  virtual ~BaseMagicXattr() {}
 protected:
  BaseMagicXattr() {}

  MountPoint *mount_point_;
  PathString path_;
  catalog::DirectoryEntry *dirent_;
};

class MagicXattrManager {
 public:
  explicit MagicXattrManager(MountPoint *mountpoint);
  BaseMagicXattr *Get(const std::string &name, PathString path,
                      catalog::DirectoryEntry *d);
  std::string GetListString(catalog::DirectoryEntry *dirent);
  void Register(const std::string &name, BaseMagicXattr *magic_xattr);

 protected:
  std::map<std::string, BaseMagicXattr *> base_xattrs_;
  std::map<std::string, BaseMagicXattr *> withhash_xattrs_;
  std::map<std::string, BaseMagicXattr *> regular_xattrs_;
  std::map<std::string, BaseMagicXattr *> symlink_xattrs_;
  std::map<std::string, BaseMagicXattr *> authz_xattrs_;
  MountPoint *mount_point_;
};

class AuthZMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class CatalogCountersMagicXattr : public BaseMagicXattr {
  std::string subcatalog_path_;
  catalog::Counters counters_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class ChunkListMagicXattr : public BaseMagicXattr {
  std::string chunk_list_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class ChunksMagicXattr : public BaseMagicXattr {
  uint64_t n_chunks_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class CompressionMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class ExternalFileMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class ExternalHostMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class ExternalTimeoutMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class FqrnMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class HashMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class HostMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class HostListMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class LHashMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class NCleanup24MagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class NClgMagicXattr : public BaseMagicXattr {
  int n_catalogs_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class NDirOpenMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class NDownloadMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class NIOErrMagicXattr : public BaseMagicXattr {
  std::string n_io_err_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class NOpenMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class ProxyMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
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
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class SpeedMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class TagMagicXattr : public BaseMagicXattr {
  std::string tag_;

  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class TimeoutMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

class TimeoutDirectMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
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
  virtual bool PrepareValueFenced();
  virtual std::string GetValue();
};

#endif  // CVMFS_MAGIC_XATTR_H_
