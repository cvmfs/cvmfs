/**
 * This file is part of the CernVM File System.
 *
 * This is the internal implementation of libcvmfs, not to be exposed to the
 * code using the library.
 *
 * Defines LibGlobals and LibContext classes that wrap around a FileSystem
 * and a MountPoint.  The libcvmfs.cc implementation of the libcvmfs interface
 * uses LibGlobals for library initialization and deinitialization.  It uses
 * LibContext objects for all the file system heavy lifting.
 */

#ifndef CVMFS_LIBCVMFS_INT_H_
#define CVMFS_LIBCVMFS_INT_H_

#include <syslog.h>
#include <time.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "backoff.h"
#include "catalog_mgr.h"
#include "file_chunk.h"
#include "interrupt.h"
#include "loader.h"
#include "lru.h"
#include "mountpoint.h"
#include "options.h"


class CacheManager;
namespace catalog {
class ClientCatalogManager;
}
namespace signature {
class SignatureManager;
}
namespace download {
class DownloadManager;
}
namespace perf {
class Statistics;
}

namespace cvmfs {
extern pid_t pid_;
extern std::string *repository_name_;
extern bool foreground_;
class Fetcher;
}  // namespace cvmfs

struct cvmfs_stat_t;


/**
 * A singleton managing the cvmfs resources for all attached repositories.  A
 * thin wrapper around the FileSystem object that does most of the heavy work.
 */
class LibGlobals : SingleCopy {
 public:
  static loader::Failures Initialize(OptionsManager *options_mgr);
  static void CleanupInstance();
  static LibGlobals *GetInstance();

  FileSystem *file_system() { return file_system_; }
  void set_options_mgr(OptionsManager *value) { options_mgr_ = value; }

 private:
  LibGlobals();
  ~LibGlobals();
  static LibGlobals *instance_;

  /**
   * Only non-NULL if cvmfs_init is used for initialization.  In this case, the
   * options manager needs to be cleaned up by cvmfs_fini.
   */
  OptionsManager *options_mgr_;
  FileSystem *file_system_;
};


/**
 * Encapsulates a single attached repository.  It uses a MountPoint object for
 * creating the state of all the necessary manager objects.  On top of the
 * managers it implements file system operations (read, list, ...).
 */
class LibContext : SingleCopy {
 public:
  static LibContext *Create(const std::string &fqrn,
                            OptionsManager *options_mgr);
  ~LibContext();

  void EnableMultiThreaded();
  int GetAttr(const char *c_path, struct stat *info);
  int Readlink(const char *path, char *buf, size_t size);
  int ListDirectory(const char *path,
                    char ***buf,
                    size_t *listlen,
                    size_t *buflen,
                    bool self_reference);
  int ListDirectoryStat(const char *c_path,
                        cvmfs_stat_t **buf,
                        size_t *listlen,
                        size_t *buflen);

  int Open(const char *c_path);
  int64_t Pread(int fd, void *buf, uint64_t size, uint64_t off);
  int Close(int fd);

  int GetExtAttr(const char *c_path, struct cvmfs_attr *info);
  int GetNestedCatalogAttr(const char *c_path, struct cvmfs_nc_attr *nc_attr);
  int ListNestedCatalogs(const char *path, char ***buf, size_t *buflen);

  int Remount();
  uint64_t GetRevision();

  MountPoint *mount_point() { return mount_point_; }
  void set_options_mgr(OptionsManager *value) { options_mgr_ = value; }

 private:
  /**
   * File descriptors of chunked files have bit 30 set.
   */
  static const int kFdChunked = 1 << 30;
  /**
   * use static method Create() for construction
   */
  LibContext();
  FileSystem *file_system() { return LibGlobals::GetInstance()->file_system(); }

  void AppendStringToList(char const *str,
                          char ***buf,
                          size_t *listlen,
                          size_t *buflen);
  void AppendStatToList(const cvmfs_stat_t st,
                        cvmfs_stat_t **buf,
                        size_t *listlen,
                        size_t *buflen);
  bool GetDirentForPath(const PathString &path,
                        catalog::DirectoryEntry *dirent);
  void CvmfsAttrFromDirent(const catalog::DirectoryEntry dirent,
                           struct cvmfs_attr *attr);

  /**
   * Only non-NULL if cvmfs_attache_repo is used for initialization.  In this
   * case, the options manager needs to be cleaned up by cvmfs_fini.
   */
  OptionsManager *options_mgr_;
  MountPoint *mount_point_;

  /**
   * Used to prevent construction/destruction of an InterruptCue object in every
   * file system operation.
   */
  InterruptCue default_interrupt_cue_;
};

#endif  // CVMFS_LIBCVMFS_INT_H_
