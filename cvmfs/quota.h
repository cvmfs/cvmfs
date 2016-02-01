/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_QUOTA_H_
#define CVMFS_QUOTA_H_

#include <pthread.h>
#include <stdint.h>
#include <sys/types.h>
#include <unistd.h>

#include <map>
#include <string>
#include <vector>

#include "duplex_sqlite3.h"
#include "hash.h"
#include "statistics.h"
#include "util.h"

namespace perf {
class Recorder;
}

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

  QuotaManager();
  virtual ~QuotaManager();
  virtual bool IsEnforcing() = 0;

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
  virtual bool IsEnforcing() { return false; }

  virtual void Insert(const shash::Any &hash, const uint64_t size,
                      const std::string &description) { }
  virtual void InsertVolatile(const shash::Any &hash, const uint64_t size,
                              const std::string &description) { }
  virtual bool Pin(const shash::Any &hash, const uint64_t size,
                   const std::string &description, const bool is_catalog)
  {
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
  virtual uint64_t GetCleanupRate(uint64_t period_s) { return 0; }

  virtual void Spawn() { }
  virtual pid_t GetPid() { return getpid(); }
  virtual uint32_t GetProtocolRevision() { return 0; }
};


/**
 * Works with the PosixCacheManager.  Uses an SQlite database for cache contents
 * tracking.  Tracking is asynchronously.
 *
 * TODO(jblomer): split into client, server, and protocol classes.
 */
class PosixQuotaManager : public QuotaManager {
  FRIEND_TEST(T_QuotaManager, BindReturnPipe);
  FRIEND_TEST(T_QuotaManager, Cleanup);
  FRIEND_TEST(T_QuotaManager, Contains);
  FRIEND_TEST(T_QuotaManager, InitDatabase);
  FRIEND_TEST(T_QuotaManager, MakeReturnPipe);

 public:
  static PosixQuotaManager *Create(const std::string &cache_dir,
    const uint64_t limit, const uint64_t cleanup_threshold,
    const bool rebuild_database);
  static PosixQuotaManager *CreateShared(const std::string &exe_path,
    const std::string &cache_dir,
    const uint64_t limit, const uint64_t cleanup_threshold);
  static int MainCacheManager(int argc, char **argv);

  virtual ~PosixQuotaManager();
  virtual bool IsEnforcing() { return true; }

  virtual void Insert(const shash::Any &hash, const uint64_t size,
                      const std::string &description);
  virtual void InsertVolatile(const shash::Any &hash, const uint64_t size,
                              const std::string &description);
  virtual bool Pin(const shash::Any &hash, const uint64_t size,
                   const std::string &description, const bool is_catalog);
  virtual void Unpin(const shash::Any &hash);
  virtual void Touch(const shash::Any &hash);
  virtual void Remove(const shash::Any &file);
  virtual bool Cleanup(const uint64_t leave_size);

  virtual void RegisterBackChannel(int back_channel[2],
                                   const std::string &channel_id);
  virtual void UnregisterBackChannel(int back_channel[2],
                                     const std::string &channel_id);

  virtual std::vector<std::string> List();
  virtual std::vector<std::string> ListPinned();
  virtual std::vector<std::string> ListCatalogs();
  virtual std::vector<std::string> ListVolatile();
  virtual uint64_t GetMaxFileSize();
  virtual uint64_t GetCapacity();
  virtual uint64_t GetSize();
  virtual uint64_t GetSizePinned();
  virtual uint64_t GetCleanupRate(uint64_t period_s);

  virtual void Spawn();
  virtual pid_t GetPid();
  virtual uint32_t GetProtocolRevision();

 private:
  /**
   * Loaded catalogs are pinned in the LRU and have to be treated differently.
   */
  enum FileTypes {
    kFileRegular = 0,
    kFileCatalog,
  };

  /**
   * List of RPCs that can be sent to the cache manager.
   */
  enum CommandType {
    kTouch = 0,
    kInsert,
    kReserve,
    kPin,
    kUnpin,
    kRemove,
    kCleanup,
    kList,
    kListPinned,
    kListCatalogs,
    kStatus,
    kLimits,
    kPid,
    kPinRegular,
    kRegisterBackChannel,
    kUnregisterBackChannel,
    kGetProtocolRevision,
    kInsertVolatile,
    // as of protocol revision 2
    kListVolatile,
    kCleanupRate,
  };

  /**
   * That could be done in more elegant way.  However, we might have a situation
   * with old cache manager serving new clients (or vice versa) and we don't
   * want to change the memory layout of LruCommand.
   */
  struct LruCommand {
    CommandType command_type;
    uint64_t size;    /**< Careful! Last 3 bits store hash algorithm */
    int return_pipe;  /**< For cleanup, listing, and reservations */
    unsigned char digest[shash::kMaxDigestSize];
    /**
     * Maximum 512-sizeof(LruCommand) in order to guarantee atomic pipe
     * operations.
     */
    uint16_t desc_length;

    LruCommand()
      : command_type(static_cast<CommandType>(0))
      , size(0)
      , return_pipe(-1)
      , desc_length(0)
    {
      memset(digest, 0, shash::kMaxDigestSize);
    }

    void SetSize(const uint64_t new_size) {
      uint64_t mask = 7;
      mask = ~(mask << (64-3));
      size = (new_size & mask) | size;
    }

    uint64_t GetSize() const {
      uint64_t mask = 7;
      mask = ~(mask << (64-3));
      return size & mask;
    }

    void StoreHash(const shash::Any &hash) {
      memcpy(digest, hash.digest, hash.GetDigestSize());
      // Exclude MD5
      uint64_t algo_flags = hash.algorithm - 1;
      algo_flags = algo_flags << (64-3);
      size |= algo_flags;
    }

    shash::Any RetrieveHash() const {
      uint64_t algo_flags = size >> (64-3);
      shash::Any result(static_cast<shash::Algorithms>(algo_flags+1));
      memcpy(result.digest, digest, result.GetDigestSize());
      return result;
    }
  };

  /**
   * Maximum page cache per thread (Bytes).
   */
  static const unsigned kSqliteMemPerThread = 2*1024*1024;

  /**
   * Collect a number of insert and touch operations before processing them
   * as sqlite commands.
   */
  static const unsigned kCommandBufferSize = 32;

  /**
   * Make sure that the amount of data transferred through the RPC pipe is
   * within the OS's guarantees for atomiticity.
   */
  static const unsigned kMaxDescription = 512-sizeof(LruCommand);

  /**
   * Alarm when more than 75% of the cache fraction allowed for pinned files
   * (50%) is filled with pinned files
   */
  static const unsigned kHighPinWatermark = 75;

  /**
   * The last bit in the sequence number indicates if an entry is volatile.
   * Such sequence numbers are negative and they are preferred during cleanup.
   * Volatile entries are used for instance for ALICE conditions data.
   */
  static const uint64_t kVolatileFlag = 1ULL << 63;

  bool InitDatabase(const bool rebuild_database);
  bool RebuildDatabase();
  void CloseDatabase();
  bool Contains(const std::string &hash_str);
  bool DoCleanup(const uint64_t leave_size);

  void MakeReturnPipe(int pipe[2]);
  int BindReturnPipe(int pipe_wronly);
  void UnbindReturnPipe(int pipe_wronly);
  void UnlinkReturnPipe(int pipe_wronly);
  void CloseReturnPipe(int pipe[2]);
  void CleanupPipes();

  void CheckHighPinWatermark();
  void ProcessCommandBunch(const unsigned num,
                           const LruCommand *commands,
                           const char *descriptions);
  static void *MainCommandServer(void *data);

  void DoInsert(const shash::Any &hash, const uint64_t size,
                const std::string &description, const CommandType command_type);
  std::vector<std::string> DoList(const CommandType list_command);
  void GetSharedStatus(uint64_t *gauge, uint64_t *pinned);
  void GetLimits(uint64_t *limit, uint64_t *cleanup_threshold);

  PosixQuotaManager(const uint64_t limit, const uint64_t cleanup_threshold,
                    const std::string &cache_dir);

  /**
   * Indicates if the cache manager is a shared process or a thread within the
   * same process (exclusive cache manager)
   */
  bool shared_;

 /**
  * True once the program switches into multi-threaded mode or the quota manager
  * process has been forked resp.
  */
  bool spawned_;

  /**
   * Soft limit in bytes, start cleanup when reached.
   */
  uint64_t limit_;

  /**
   * Cleanup until cleanup_threshold_ are left in the cache.
   */
  uint64_t cleanup_threshold_;

  /**
   * Current size of cache.
   */
  uint64_t gauge_;

  /**
   * Size of pinned files in bytes (usually file catalogs).
   */
  uint64_t pinned_;

  /**
   * Current access sequence number.  Gets increased on every access/insert
   * operation.
   */
  uint64_t seq_;

  /**
   * Should match the directory given to the cache manager.
   */
  std::string cache_dir_;

  /**
   * Pinned content hashes and their size.
   */
  std::map<shash::Any, uint64_t> pinned_chunks_;

  /**
   * Used to send RPCs to the quota manager thread or process.
   */
  int pipe_lru_[2];

  /**
   * In exclusive mode, controls the quota manager thread.
   */
  pthread_t thread_lru_;

  /**
   * Ensures exclusive cache database access through POSIX file lock.
   */
  int fd_lock_cachedb_;

  /**
   * If this is true, the unlink operations that correspond to a cleanup run
   * will be performed in a detached, asynchronous process.
   */
  bool async_delete_;

  /**
   * Keeps track of the number of cleanups over time.  Use by
   * `cvmfs_talk cleanup rate`
   */
  perf::MultiRecorder cleanup_recorder_;

  sqlite3 *database_;
  sqlite3_stmt *stmt_touch_;
  sqlite3_stmt *stmt_unpin_;
  sqlite3_stmt *stmt_block_;
  sqlite3_stmt *stmt_unblock_;
  sqlite3_stmt *stmt_new_;
  sqlite3_stmt *stmt_lru_;
  sqlite3_stmt *stmt_size_;
  sqlite3_stmt *stmt_rm_;
  sqlite3_stmt *stmt_list_;
  sqlite3_stmt *stmt_list_pinned_;  /**< Loaded catalogs are pinned. */
  sqlite3_stmt *stmt_list_catalogs_;
  sqlite3_stmt *stmt_list_volatile_;

  /**
   * Used in the destructor to steer closing of the database and so on.
   */
  bool initialized_;
};  // class PosixQuotaManager

#endif  // CVMFS_QUOTA_H_
