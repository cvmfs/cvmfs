/**
 * This file is part of the CernVM File System.
 *
 * Exports a local directory tree read-only.  Used to test the NFS server
 * against kernel clients independently of the cvmfs client.
 */

#ifndef CVMFS_NFS4_POSIX_BACKEND_H_
#define CVMFS_NFS4_POSIX_BACKEND_H_

#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "nfs4/backend.h"

namespace nfs4 {

/**
 * Inode numbers are the ones of the underlying file system.  The inode to path
 * map is filled by lookups and listings and lives in memory only, so file
 * handles become stale when the process restarts.
 */
class PosixBackend : public Backend {
 public:
  /** Returns NULL if root is not a directory */
  static PosixBackend *Create(const std::string &root);
  /**
   * Slows down the first Read of every file by delay_ms, like a download
   * into a cache, to test slow backends
   */
  void set_read_delay_ms(unsigned delay_ms) { read_delay_ms_ = delay_ms; }

  virtual uint64_t RootInode() { return root_ino_; }
  virtual uint64_t FsId() { return fsid_; }
  /** Modification time of the export root; touching it recalls delegations */
  virtual uint64_t Generation();

  virtual int GetAttr(uint64_t ino, Attr *attr);
  virtual int Lookup(uint64_t parent, const std::string &name, Attr *attr);
  virtual int LookupParent(uint64_t ino, Attr *attr);
  virtual int ReadLink(uint64_t ino, std::string *target);
  virtual int ReadDir(uint64_t ino, std::vector<DirEntry> *entries);
  virtual int Read(uint64_t ino, uint64_t offset, uint32_t size,
                   std::string *data);
  virtual int StatFs(FsStat *stat);
  virtual int ListXattr(uint64_t ino, std::vector<std::string> *names);
  virtual int GetXattr(uint64_t ino, const std::string &name,
                       const Credentials &creds, std::string *value);

 private:
  explicit PosixBackend(const std::string &root)
      : root_(root), root_ino_(0), fsid_(0), read_delay_ms_(0) { }
  bool PathOf(uint64_t ino, std::string *path);
  int StatPath(const std::string &path, Attr *attr);

  std::string root_;
  uint64_t root_ino_;
  uint64_t fsid_;
  unsigned read_delay_ms_;
  std::mutex lock_;
  std::map<uint64_t, std::string> paths_;
  std::set<uint64_t> fetched_;
};

}  // namespace nfs4

#endif  // CVMFS_NFS4_POSIX_BACKEND_H_
