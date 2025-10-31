/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MAGIC_XATTR_H_
#define CVMFS_MAGIC_XATTR_H_

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "backoff.h"
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
  kXattrExternal,
  kXattrSymlink,
  kXattrAuthz
};

enum MagicXattrMode {
  kXattrMachineMode = 0,
  kXattrHumanMode
};

class MagicXattrManager;  // needed for BaseMagicXattr
/**
 * This is a base class for magic extended attribute. Every extended
 * attributes must inherit from this class. It should be generally used only
 * in cooperation with MagicXattrManager.
 * It contains an access mutex. Users should use Lock() and Release()
 * before and after usage (Lock() is called implicitly in MagicXattrManager).
 *
 * To read out the attribute value, do:
 * 0. Get an instance through MagicXattrManager::Get()
 * 1. Call PrepareValueFenced() inside FuseRemounter::fence()
 * 2. Call GetValue(int32_t requested_page, const MagicXattrMode mode);
 *    It returns a <bool, string> pair where the bool is set to true if the
 *    request was successful and the string contains the actual value.
 *    GetValue() can be called outside the fence.
 *    This will internally call FinalizeValue() to finalize the value
 *    preparation outside the fuse fence.
 *
 * Implementation notes:
 * - Each MagicXattr must implement it's own FinalizeValue() function which
 *   has to write its value into result_pages_.
 * - result_pages_ is a vector of strings. Each string is <= kMaxCharsPerPage
 * - BaseMagicXattr::GetValue() takes care of clearing result_pages_ before each
 *   new call of FinalizeValue(), and takes care of adding a header for the
 *   human readable version
 * - In case an invalid request happens, GetValue() <bool> is set to false.
 *   "ENOENT" will be returned, while in human-readable mode a more verbose
 *   error message will be returned
 */
class BaseMagicXattr {
  friend class MagicXattrManager;
  FRIEND_TEST(T_MagicXattr, ProtectedXattr);
  FRIEND_TEST(T_MagicXattr, TestFqrn);
  FRIEND_TEST(T_MagicXattr, TestLogBuffer);

 public:
  BaseMagicXattr() : is_protected_(false) {
    const int retval = pthread_mutex_init(&access_mutex_, NULL);
    assert(retval == 0);
  }

  /**
   * Mark a Xattr protected so that only certain users with the correct gid
   * can access it.
   */
  void MarkProtected() { is_protected_ = true; }


  // TODO(hereThereBeDragons) from C++11 should be marked final
  /**
   * Access right check before normal fence
   */
  bool PrepareValueFencedProtected(gid_t gid);

  /**
   * This function needs to be called after PrepareValueFenced(),
   * which prepares the necessary data and header for kXattrHumanMode.
   * It does the computationaly intensive part, which should not
   * be done inside the FuseRemounter::fence(), and returns the
   * value.
   *
   * Internally it calls FinalizeValue() which each MagicXAttr has to implement
   * to set the value of result_pages_
   *
   * @params
   *  - requested_page:
   *      >= 0: requested paged of the attribute
   *      -1: get info about xattr: e.g. the number of pages available
   *  - mode: either machine-readable or human-readable
   *
   * @returns
   *  std::pair<bool, std::string>
   *   bool = false if in machine-readable mode an invalid request is performed
   *          (human-readable mode always succeeds and gives a verbose message)
   *          otherwise true
   *   std::string = the actual value of the attribute or info element
   *                 ( or error message in human-readable mode)
   *
   */
  std::pair<bool, std::string> GetValue(int32_t requested_page,
                                        const MagicXattrMode mode);

  virtual MagicXattrFlavor GetXattrFlavor() { return kXattrBase; }

  void Lock(PathString path, catalog::DirectoryEntry *dirent) {
    const int retval = pthread_mutex_lock(&access_mutex_);
    assert(retval == 0);
    path_ = path;
    dirent_ = dirent;
  }
  void Release() {
    const int retval = pthread_mutex_unlock(&access_mutex_);
    assert(retval == 0);
  }

  virtual ~BaseMagicXattr() { }

  // how many chars per page (with some leeway). system maximum would be 64k
  static const uint32_t kMaxCharsPerPage = 40000;

 protected:
  /**
   * This function is used to obtain the necessary information while
   * inside FuseRemounter::fence(), which should prevent data races.
   */
  virtual bool PrepareValueFenced() { return true; }
  virtual void FinalizeValue() { }

  std::string HeaderMultipageHuman(uint32_t requested_page);

  MagicXattrManager *xattr_mgr_;
  PathString path_;
  catalog::DirectoryEntry *dirent_;

  pthread_mutex_t access_mutex_;
  bool is_protected_;
  std::vector<std::string> result_pages_;
};

/**
 * This wrapper ensures that the attribute instance "ptr_" is
 * released after the user finishes using it (on wrapper destruction).
 */
class MagicXattrRAIIWrapper : public SingleCopy {
 public:
  inline MagicXattrRAIIWrapper() : ptr_(NULL) { }

  inline explicit MagicXattrRAIIWrapper(BaseMagicXattr *ptr,
                                        PathString path,
                                        catalog::DirectoryEntry *d)
      : ptr_(ptr) {
    if (ptr_ != NULL)
      ptr_->Lock(path, d);
  }
  /// Wraps around a BaseMagicXattr* that is already locked (or NULL)
  inline explicit MagicXattrRAIIWrapper(BaseMagicXattr *ptr) : ptr_(ptr) { }

  inline ~MagicXattrRAIIWrapper() {
    if (ptr_ != NULL)
      ptr_->Release();
  }

  inline BaseMagicXattr *operator->() const { return ptr_; }
  inline bool IsNull() const { return ptr_ == NULL; }
  inline BaseMagicXattr *Move() {
    BaseMagicXattr *ret = ptr_;
    ptr_ = NULL;
    return ret;
  }

 protected:
  BaseMagicXattr *ptr_;
};

class WithHashMagicXattr : public BaseMagicXattr {
  virtual MagicXattrFlavor GetXattrFlavor() { return kXattrWithHash; }
};

class RegularMagicXattr : public BaseMagicXattr {
  virtual MagicXattrFlavor GetXattrFlavor() { return kXattrRegular; }
};

class ExternalMagicXattr : public BaseMagicXattr {
  virtual MagicXattrFlavor GetXattrFlavor() { return kXattrExternal; }
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
  enum EVisibility {
    kVisibilityAlways,
    kVisibilityNever,
    kVisibilityRootOnly
  };

  MagicXattrManager(MountPoint *mountpoint, EVisibility visibility,
                    const std::set<std::string> &protected_xattrs,
                    const std::set<gid_t> &privileged_xattr_gids);
  /// The returned BaseMagicXattr* is supposed to be wrapped by a
  /// MagicXattrRAIIWrapper
  BaseMagicXattr *GetLocked(const std::string &name, PathString path,
                            catalog::DirectoryEntry *d);
  std::string GetListString(catalog::DirectoryEntry *dirent);
  /**
   * Registers a new extended attribute.
   * Will fail if called after Freeze().
   */
  void Register(const std::string &name, BaseMagicXattr *magic_xattr);

  /**
   * Freezes the current setup of MagicXattrManager.
   * No new extended attributes can be added.
   * Only after freezing MagicXattrManager can registered attributes be
   * accessed.
   */
  void Freeze() {
    is_frozen_ = true;
    SanityCheckProtectedXattrs();
  }
  bool IsPrivilegedGid(gid_t gid);


  EVisibility visibility() { return visibility_; }
  std::set<gid_t> privileged_xattr_gids() { return privileged_xattr_gids_; }
  MountPoint *mount_point() { return mount_point_; }
  bool is_frozen() const { return is_frozen_; }

 protected:
  std::map<std::string, BaseMagicXattr *> xattr_list_;
  MountPoint *mount_point_;
  EVisibility visibility_;

  // privileged_xattr_gids_ contains the (fuse) gids that
  // can access xattrs that are part of protected_xattrs_
  const std::set<std::string> protected_xattrs_;
  const std::set<gid_t> privileged_xattr_gids_;

 private:
  bool is_frozen_;

  void SanityCheckProtectedXattrs();
};

class AuthzMagicXattr : public BaseMagicXattr {
  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();

  virtual MagicXattrFlavor GetXattrFlavor();
};

class CatalogCountersMagicXattr : public BaseMagicXattr {
  std::string subcatalog_path_;
  shash::Any hash_;
  catalog::Counters counters_;

  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class ChunkListMagicXattr : public RegularMagicXattr {
  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();

 private:
  std::vector<std::string> chunk_list_;
};

class ChunksMagicXattr : public RegularMagicXattr {
  uint64_t n_chunks_;

  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class CompressionMagicXattr : public RegularMagicXattr {
  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class DirectIoMagicXattr : public RegularMagicXattr {
  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class ExternalFileMagicXattr : public RegularMagicXattr {
  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class ExternalHostMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class ExternalTimeoutMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class FqrnMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class HashMagicXattr : public WithHashMagicXattr {
  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class HostMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class HostListMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class LHashMagicXattr : public WithHashMagicXattr {
  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class LogBufferXattr : public BaseMagicXattr {
 public:
  LogBufferXattr();

 private:
  const unsigned int kMaxLogLine = 4096;  // longer log lines are trimmed
  // Generating the log buffer report involves 64 string copies. To mitigate
  // memory fragmentation and performance loss, throttle the use of this
  // attribute a little.
  BackoffThrottle throttle_;

  virtual void FinalizeValue();
};

class NCleanup24MagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class NClgMagicXattr : public BaseMagicXattr {
  int n_catalogs_;

  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class NDirOpenMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class NDownloadMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class NIOErrMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class NOpenMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class HitrateMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class ProxyMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class ProxyListMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class ProxyListExternalMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class PubkeysMagicXattr : public BaseMagicXattr {
  FRIEND_TEST(T_MagicXattr, MultiPageMachineModeXattr);
  FRIEND_TEST(T_MagicXattr, MultiPageHumanModeXattr);

  std::vector<std::string> pubkeys_;

  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class RawlinkMagicXattr : public SymlinkMagicXattr {
  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class RepoCountersMagicXattr : public BaseMagicXattr {
  catalog::Counters counters_;

  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class RepoMetainfoMagicXattr : public BaseMagicXattr {
  static uint64_t kMaxMetainfoLength;

  shash::Any metainfo_hash_;
  std::string error_reason_;

  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class RevisionMagicXattr : public BaseMagicXattr {
  uint64_t revision_;

  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class RootHashMagicXattr : public BaseMagicXattr {
  shash::Any root_hash_;

  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class RxMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class SpeedMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class TagMagicXattr : public BaseMagicXattr {
  std::string tag_;

  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class TimeoutMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class TimeoutDirectMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class TimestampLastIOErrMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class UsedFdMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class UsedDirPMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class VersionMagicXattr : public BaseMagicXattr {
  virtual void FinalizeValue();
};

class ExternalURLMagicXattr : public ExternalMagicXattr {
  virtual bool PrepareValueFenced();
  virtual void FinalizeValue();
};

class CleanupUnusedFirstMagicXattr : public ExternalMagicXattr {
  virtual void FinalizeValue();
};

class ListOpenHashesMagicXattr : public ExternalMagicXattr {
  virtual void FinalizeValue();
};
#endif  // CVMFS_MAGIC_XATTR_H_

