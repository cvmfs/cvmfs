/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NFS4_FUSE_BACKEND_H_
#define CVMFS_NFS4_FUSE_BACKEND_H_

#include <stdint.h>
#include <time.h>

#include <list>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "duplex_fuse.h"
#include "nfs4/backend.h"

namespace nfs4 {

/**
 * Serves a file system implemented as fuse_lowlevel_ops (i.e. cvmfs).
 *
 * The cvmfs client must run in NFS export mode (CVMFS_NFS_SOURCE), so that
 * inodes are persistent and do not depend on lookup/forget reference counts:
 * NFS has no forget.
 */
class FuseBackend : public Backend {
 public:
  FuseBackend(const struct fuse_lowlevel_ops *ops, uint64_t fsid);
  virtual ~FuseBackend();

  virtual uint64_t RootInode() { return FUSE_ROOT_ID; }
  virtual uint64_t FsId() { return fsid_; }
  virtual uint64_t Generation();

  virtual int GetAttr(uint64_t ino, Attr *attr);
  virtual int Lookup(uint64_t parent, const std::string &name, Attr *attr);
  virtual int LookupParent(uint64_t ino, Attr *attr);
  virtual int ReadLink(uint64_t ino, std::string *target);
  virtual int ReadDir(uint64_t ino, std::vector<DirEntry> *entries);
  virtual int Read(uint64_t ino, uint64_t offset, uint32_t size,
                   std::string *data);
  virtual void Forget(uint64_t ino);
  virtual int StatFs(FsStat *stat);
  virtual int ListXattr(uint64_t ino, std::vector<std::string> *names);
  virtual int GetXattr(uint64_t ino, const std::string &name,
                       const Credentials &creds, std::string *value);

  /** Releases all cached open files, e.g. before unmount */
  void ReleaseAll();
  /**
   * Releases cached open files unused for max_idle seconds.  Open files pin
   * their objects in the cvmfs cache, so they must not be kept indefinitely.
   */
  void ReleaseIdle(unsigned max_idle);

  /** Upper bound of cached open files, if the descriptor limit allows */
  static const unsigned kMaxOpenFiles = 256;

 private:
  /**
   * NFS has no open/close for reads, and opening a cvmfs file means fetching
   * it, so file handles are cached per inode.  Entries in use by a READ are
   * pinned and never evicted.
   */
  struct OpenFile {
    OpenFile() : generation(0), pins(0), last_used(0) { }
    struct fuse_file_info fi;
    uint64_t generation;
    unsigned pins;
    time_t last_used;
    std::list<uint64_t>::iterator lru_pos;
  };

  int AcquireFile(uint64_t ino, struct fuse_file_info *fi);
  void ReleaseFile(uint64_t ino);
  /** Called with lock_ held; returns entries to close outside the lock */
  void EvictLocked(std::vector<std::pair<uint64_t, struct fuse_file_info> >
                       *to_close);
  void CloseFiles(const std::vector<std::pair<uint64_t, struct fuse_file_info> >
                      &files);
  int Xattr(uint64_t ino, const std::string *name, const Credentials &creds,
            std::string *value);

  const struct fuse_lowlevel_ops *ops_;
  uint64_t fsid_;
  /** Derived from RLIMIT_NOFILE: cvmfs needs descriptors for itself, too */
  unsigned max_open_files_;

  std::mutex lock_;
  std::map<uint64_t, OpenFile> open_files_;
  std::list<uint64_t> lru_;

  std::mutex generation_lock_;
  uint64_t generation_;
  time_t generation_checked_;
};

}  // namespace nfs4

#endif  // CVMFS_NFS4_FUSE_BACKEND_H_
