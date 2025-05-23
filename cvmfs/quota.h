/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_QUOTA_H_
#define CVMFS_QUOTA_H_

#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

#include <map>
#include <string>
#include <vector>

#include "crypto/hash.h"
#include "util/single_copy.h"

/**
 * The QuotaManager keeps track of the cache contents.  It is informed by the
 * cache manager about opens and inserts.  The cache manager picks a quota
 * manager that fits to the backend storage (e.g. POSIX, key-value store).  File
 * catalogs are "pinned" in the quota manager.  Since they remain loaded
 * (virtual file descriptor stays open), it does not make sense to remove them.
 * Regular files might get pinned occasionally as well, for instance for the
 * CernVM "core files".
 *
 * Multiple cache managers can share a single quota manager instance, as it is
 * done for the local shared hard disk cache.
 *
 * Sometimes it is necessary that a quota manager instance gives feedback to its
 * users.  This is where back channels are used.  Users can register a back
 * channel, which gets informed for instance if the number of pinned catalogs
 * get large and should be released.
 */
class QuotaManager : SingleCopy {
 public:
  /**
   * Quota manager protocol revision.
   * Revision 1:
   *  - backchannel command 'R': release pinned files if possible
   * Revision 2:
   *  - add kCleanupRate command
   */
  static const uint32_t kProtocolRevision;

  enum Capabilities {
    kCapIntrospectSize = 0,
    kCapIntrospectCleanupRate,
    kCapList,
    kCapShrink,
    kCapListeners,
  };

  QuotaManager();
  virtual ~QuotaManager();
  virtual bool HasCapability(Capabilities capability) = 0;

  virtual void Insert(const shash::Any &hash, const uint64_t size,
                      const std::string &description) = 0;
  virtual void InsertVolatile(const shash::Any &hash, const uint64_t size,
                              const std::string &description) = 0;
  virtual bool Pin(const shash::Any &hash, const uint64_t size,
                   const std::string &description, const bool is_catalog) = 0;
  virtual void Unpin(const shash::Any &hash) = 0;
  virtual void Touch(const shash::Any &hash) = 0;
  virtual void Remove(const shash::Any &file) = 0;
  virtual bool Cleanup(const uint64_t leave_size) = 0;

  virtual std::vector<std::string> List() = 0;
  virtual std::vector<std::string> ListPinned() = 0;
  virtual std::vector<std::string> ListCatalogs() = 0;
  virtual std::vector<std::string> ListVolatile() = 0;
  virtual uint64_t GetMaxFileSize() = 0;
  virtual uint64_t GetCapacity() = 0;
  virtual uint64_t GetSize() = 0;
  virtual uint64_t GetSizePinned() = 0;
  virtual bool SetLimit(uint64_t limit) = 0;
  virtual uint64_t GetCleanupRate(uint64_t period_s) = 0;

  virtual void Spawn() = 0;
  virtual pid_t GetPid() = 0;
  virtual uint32_t GetProtocolRevision() = 0;

  virtual void RegisterBackChannel(int back_channel[2],
                                   const std::string &channel_id) = 0;
  virtual void UnregisterBackChannel(int back_channel[2],
                                     const std::string &channel_id) = 0;
  void BroadcastBackchannels(const std::string &message);

 protected:
  /**
   * Hashes over the channel identifier mapped to writing ends of pipes.
   */
  std::map<shash::Md5, int> back_channels_;
  pthread_mutex_t *lock_back_channels_;
  void LockBackChannels() {
    int retval = pthread_mutex_lock(lock_back_channels_);
    assert(retval == 0);
  }
  void UnlockBackChannels() {
    int retval = pthread_mutex_unlock(lock_back_channels_);
    assert(retval == 0);
  }

  /**
   * Protocol of the running cache manager instance.  Needs to be figured out
   * and set during initialization of concrete instances.
   */
  uint32_t protocol_revision_;
};


/**
 * No quota management.
 */
class NoopQuotaManager : public QuotaManager {
 public:
  virtual ~NoopQuotaManager() { }
  virtual bool HasCapability(Capabilities capability) { return false; }

  virtual void Insert(const shash::Any &hash, const uint64_t size,
                      const std::string &description) { }
  virtual void InsertVolatile(const shash::Any &hash, const uint64_t size,
                              const std::string &description) { }
  virtual bool Pin(const shash::Any &hash, const uint64_t size,
                   const std::string &description, const bool is_catalog) {
    return true;
  }
  virtual void Unpin(const shash::Any &hash) { }
  virtual void Touch(const shash::Any &hash) { }
  virtual void Remove(const shash::Any &file) { }
  virtual bool Cleanup(const uint64_t leave_size) { return false; }

  virtual void RegisterBackChannel(int back_channel[2],
                                   const std::string &channel_id) { }
  virtual void UnregisterBackChannel(int back_channel[2],
                                     const std::string &channel_id) { }

  virtual std::vector<std::string> List() { return std::vector<std::string>(); }
  virtual std::vector<std::string> ListPinned() {
    return std::vector<std::string>();
  }
  virtual std::vector<std::string> ListCatalogs() {
    return std::vector<std::string>();
  }
  virtual std::vector<std::string> ListVolatile() {
    return std::vector<std::string>();
  }
  virtual uint64_t GetMaxFileSize() { return uint64_t(-1); }
  virtual uint64_t GetCapacity() { return uint64_t(-1); }
  virtual uint64_t GetSize() { return 0; }
  virtual uint64_t GetSizePinned() { return 0; }
  virtual bool SetLimit(uint64_t) { return false; }
  virtual uint64_t GetCleanupRate(uint64_t period_s) { return 0; }

  virtual void Spawn() { }
  virtual pid_t GetPid() { return getpid(); }
  virtual uint32_t GetProtocolRevision() { return 0; }
};

#endif  // CVMFS_QUOTA_H_
