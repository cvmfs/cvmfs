/**
 * This file is part of the CernVM File System.
 *
 * Read-only, inode-based file system interface served by the NFS server.
 */

#ifndef CVMFS_NFS4_BACKEND_H_
#define CVMFS_NFS4_BACKEND_H_

#include <stdint.h>
#include <sys/types.h>

#include <string>
#include <vector>

namespace nfs4 {

struct Timespec {
  Timespec() : sec(0), nsec(0) { }
  int64_t sec;
  uint32_t nsec;
};

struct Attr {
  Attr() : ino(0), mode(0), nlink(1), uid(0), gid(0), size(0), rdev(0) { }

  uint64_t ino;
  /** Includes the file type bits (S_IFDIR, ...) */
  mode_t mode;
  uint32_t nlink;
  uint32_t uid;
  uint32_t gid;
  uint64_t size;
  uint64_t rdev;
  Timespec atime;
  Timespec mtime;
  Timespec ctime;
};

struct DirEntry {
  DirEntry() : ino(0) { }
  DirEntry(const std::string &n, uint64_t i) : name(n), ino(i) { }
  std::string name;
  uint64_t ino;
};

struct FsStat {
  FsStat()
      : bytes_total(0)
      , bytes_free(0)
      , bytes_avail(0)
      , files_total(0)
      , files_free(0)
      , files_avail(0) { }
  uint64_t bytes_total;
  uint64_t bytes_free;
  uint64_t bytes_avail;
  uint64_t files_total;
  uint64_t files_free;
  uint64_t files_avail;
};

struct Credentials {
  Credentials() : uid(65534), gid(65534) { }
  uint32_t uid;
  uint32_t gid;
  std::vector<uint32_t> gids;
};

/**
 * Methods return 0 or a positive errno.  They are called concurrently from the
 * server's worker threads.  Inode numbers must stay valid for the lifetime of
 * the backend and should survive restarts, because they end up in persistent
 * NFS file handles.
 */
class Backend {
 public:
  virtual ~Backend() { }

  virtual uint64_t RootInode() = 0;
  /**
   * Identifies the exported file system; part of every file handle so that
   * handles of a different export are rejected.
   */
  virtual uint64_t FsId() = 0;
  /**
   * Changes whenever the exported tree may have changed (for cvmfs: the root
   * catalog revision).  Used as READDIR cookie verifier and as part of the
   * change attribute.
   */
  virtual uint64_t Generation() = 0;

  virtual int GetAttr(uint64_t ino, Attr *attr) = 0;
  virtual int Lookup(uint64_t parent, const std::string &name, Attr *attr) = 0;
  virtual int LookupParent(uint64_t ino, Attr *attr) = 0;
  virtual int ReadLink(uint64_t ino, std::string *target) = 0;
  virtual int ReadDir(uint64_t ino, std::vector<DirEntry> *entries) = 0;
  /**
   * Fills up to size bytes at offset.  A short read means end of file.
   */
  virtual int Read(uint64_t ino, uint64_t offset, uint32_t size,
                   std::string *data) = 0;
  /** Hint that a client stopped using the file; may release cached state. */
  virtual void Forget(uint64_t /* ino */) { }
  virtual int StatFs(FsStat *stat) = 0;

  virtual int ListXattr(uint64_t ino, std::vector<std::string> *names) = 0;
  virtual int GetXattr(uint64_t ino, const std::string &name,
                       const Credentials &creds, std::string *value) = 0;
};

}  // namespace nfs4

#endif  // CVMFS_NFS4_BACKEND_H_
