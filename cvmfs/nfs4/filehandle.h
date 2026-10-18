/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NFS4_FILEHANDLE_H_
#define CVMFS_NFS4_FILEHANDLE_H_

#include <stdint.h>

#include <string>

namespace nfs4 {

/**
 * Fixed-size, path-free NFS file handle:
 *
 *   magic "CVN4" | version | kind | 2 reserved | fsid (8) | inode (8) |
 *   aux (4) | FNV-1a checksum (4)
 *
 * The handle carries no boot time, so it remains valid across server restarts
 * as long as the backend's inode numbers are stable.  For named attributes,
 * aux holds a hash of the attribute name, which keeps the handle size fixed.
 */
struct FileHandle {
  enum Kind {
    kObject = 1,
    kAttrDir = 2,
    kNamedAttr = 3,
  };

  static const unsigned kSize = 32;

  FileHandle() : kind(kObject), fsid(0), ino(0), aux(0) { }
  FileHandle(Kind k, uint64_t f, uint64_t i, uint32_t a = 0)
      : kind(k), fsid(f), ino(i), aux(a) { }

  std::string Encode() const;
  /** Returns false for anything that is not a well-formed handle. */
  static bool Decode(const std::string &raw, FileHandle *fh);

  bool operator==(const FileHandle &other) const {
    return kind == other.kind && fsid == other.fsid && ino == other.ino
           && aux == other.aux;
  }

  Kind kind;
  uint64_t fsid;
  uint64_t ino;
  uint32_t aux;
};

/** Named attribute hash stored in FileHandle::aux */
uint32_t HashName(const std::string &name);

}  // namespace nfs4

#endif  // CVMFS_NFS4_FILEHANDLE_H_
