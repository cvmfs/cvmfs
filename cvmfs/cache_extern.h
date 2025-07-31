/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_CACHE_EXTERN_H_
#define CVMFS_CACHE_EXTERN_H_

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

#include <cassert>
#include <string>
#include <vector>

#include "cache.h"
#include "cache_transport.h"
#include "crypto/hash.h"
#include "fd_table.h"
#include "duplex_testing.h"
#include "quota.h"
#include "util/atomic.h"
#include "util/concurrency.h"
#include "util/single_copy.h"


class ExternalCacheManager : public CacheManager {
  FRIEND_TEST(T_ExternalCacheManager, TransactionAbort);
  friend class ExternalQuotaManager;

 public:
  static const unsigned kPbProtocolVersion = 1;
  /**
   * Used for race-free startup of an external cache plugin.
   */
  class PluginHandle {
    friend class ExternalCacheManager;

   public:
    PluginHandle() : fd_connection_(-1) { }
    bool IsValid() const { return fd_connection_ >= 0; }
    int fd_connection() const { return fd_connection_; }
    std::string error_msg() const { return error_msg_; }

   private:
    /**
     * The connected file descriptor to pass to Create()
     */
    int fd_connection_;

    std::string error_msg_;
  };

  static PluginHandle *CreatePlugin(const std::string &locator,
                                    const std::vector<std::string> &cmd_line);

  static ExternalCacheManager *Create(int fd_connection,
                                      unsigned max_open_fds,
                                      const std::string &ident);
  virtual ~ExternalCacheManager();

  virtual CacheManagerIds id() { return kExternalCacheManager; }
  virtual std::string Describe();
  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr);

  virtual int Open(const LabeledObject &object);
  virtual int64_t GetSize(int fd);
  virtual int Close(int fd);
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset);
  virtual int Dup(int fd);
  virtual int Readahead(int fd);

#ifdef __APPLE__
  virtual uint32_t SizeOfTxn() { return sizeof(Transaction); }
#else
  virtual uint32_t SizeOfTxn() {
    return sizeof(Transaction) + max_object_size_;
  }
#endif
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn);
  virtual void CtrlTxn(const Label &label, const int flags, void *txn);
  virtual int64_t Write(const void *buf, uint64_t size, void *txn);
  virtual int Reset(void *txn);
  virtual int AbortTxn(void *txn);
  virtual int OpenFromTxn(void *txn);
  virtual int CommitTxn(void *txn);

  virtual manifest::Breadcrumb LoadBreadcrumb(const std::string &fqrn);
  virtual bool StoreBreadcrumb(const manifest::Manifest &manifest);

  virtual void Spawn();

  int64_t session_id() const { return session_id_; }
  uint32_t max_object_size() const { return max_object_size_; }
  uint64_t capabilities() const { return capabilities_; }
  pid_t pid_plugin() const { return pid_plugin_; }

 protected:
  virtual void *DoSaveState();
  virtual int DoRestoreState(void *data);
  virtual bool DoFreeState(void *data);

 private:
  /**
   * The null hash (hashed output is all null bytes) serves as a marker for an
   * invalid handle
   */
  static const shash::Any kInvalidHandle;
  /**
   * Objects cannot be larger than 512 kB.  Keeps transaction memory consumption
   * under control.
   */
  static const unsigned kMaxSupportedObjectSize = 512 * 1024;
  /**
   * Statistically, at least half of our objects should not be further chunked.
   */
  static const unsigned kMinSupportedObjectSize = 4 * 1024;

  struct Transaction {
    explicit Transaction(const shash::Any &id)
        : buffer(reinterpret_cast<unsigned char *>(this) + sizeof(Transaction))
        , buf_pos(0)
        , size(0)
        , expected_size(kSizeUnknown)
        , label()
        , open_fds(0)
        , flushed(false)
        , committed(false)
        , label_modified(false)
        , transaction_id(0)
        , id(id) { }

    /**
     * Allocated size is max_object_size_, allocated by the caller at the end
     * of the transaction (Linux only).
     */
    unsigned char *buffer;
    unsigned buf_pos;
    uint64_t size;
    uint64_t expected_size;
    Label label;
    int open_fds;
    bool flushed;
    bool committed;
    bool label_modified;
    uint64_t transaction_id;
    shash::Any id;
  };  // class Transaction

  struct ReadOnlyHandle {
    ReadOnlyHandle() : id(kInvalidHandle) { }
    explicit ReadOnlyHandle(const shash::Any &h) : id(h) { }
    bool operator==(const ReadOnlyHandle &other) const {
      return this->id == other.id;
    }
    bool operator!=(const ReadOnlyHandle &other) const {
      return this->id != other.id;
    }
    shash::Any id;
  };  // class ReadOnlyHandle

  class RpcJob {
   public:
    explicit RpcJob(cvmfs::MsgRefcountReq *msg)
        : req_id_(msg->req_id())
        , part_nr_(0)
        , msg_req_(msg)
        , frame_send_(msg) { }
    explicit RpcJob(cvmfs::MsgObjectInfoReq *msg)
        : req_id_(msg->req_id())
        , part_nr_(0)
        , msg_req_(msg)
        , frame_send_(msg) { }
    explicit RpcJob(cvmfs::MsgReadReq *msg)
        : req_id_(msg->req_id())
        , part_nr_(0)
        , msg_req_(msg)
        , frame_send_(msg) { }
    explicit RpcJob(cvmfs::MsgStoreReq *msg)
        : req_id_(msg->req_id())
        , part_nr_(msg->part_nr())
        , msg_req_(msg)
        , frame_send_(msg) { }
    explicit RpcJob(cvmfs::MsgStoreAbortReq *msg)
        : req_id_(msg->req_id())
        , part_nr_(0)
        , msg_req_(msg)
        , frame_send_(msg) { }
    explicit RpcJob(cvmfs::MsgInfoReq *msg)
        : req_id_(msg->req_id())
        , part_nr_(0)
        , msg_req_(msg)
        , frame_send_(msg) { }
    explicit RpcJob(cvmfs::MsgShrinkReq *msg)
        : req_id_(msg->req_id())
        , part_nr_(0)
        , msg_req_(msg)
        , frame_send_(msg) { }
    explicit RpcJob(cvmfs::MsgListReq *msg)
        : req_id_(msg->req_id())
        , part_nr_(0)
        , msg_req_(msg)
        , frame_send_(msg) { }
    explicit RpcJob(cvmfs::MsgBreadcrumbLoadReq *msg)
        : req_id_(msg->req_id())
        , part_nr_(0)
        , msg_req_(msg)
        , frame_send_(msg) { }
    explicit RpcJob(cvmfs::MsgBreadcrumbStoreReq *msg)
        : req_id_(msg->req_id())
        , part_nr_(0)
        , msg_req_(msg)
        , frame_send_(msg) { }

    void set_attachment_send(void *data, unsigned size) {
      frame_send_.set_attachment(data, size);
    }

    void set_attachment_recv(void *data, unsigned size) {
      frame_recv_.set_attachment(data, size);
    }

    google::protobuf::MessageLite *msg_req() { return msg_req_; }
    // Type checking has been already performed
    cvmfs::MsgRefcountReply *msg_refcount_reply() {
      cvmfs::MsgRefcountReply *m = reinterpret_cast<cvmfs::MsgRefcountReply *>(
          frame_recv_.GetMsgTyped());
      assert(m->req_id() == req_id_);
      return m;
    }
    cvmfs::MsgObjectInfoReply *msg_object_info_reply() {
      cvmfs::MsgObjectInfoReply
          *m = reinterpret_cast<cvmfs::MsgObjectInfoReply *>(
              frame_recv_.GetMsgTyped());
      assert(m->req_id() == req_id_);
      return m;
    }
    cvmfs::MsgReadReply *msg_read_reply() {
      cvmfs::MsgReadReply *m = reinterpret_cast<cvmfs::MsgReadReply *>(
          frame_recv_.GetMsgTyped());
      assert(m->req_id() == req_id_);
      return m;
    }
    cvmfs::MsgStoreReply *msg_store_reply() {
      cvmfs::MsgStoreReply *m = reinterpret_cast<cvmfs::MsgStoreReply *>(
          frame_recv_.GetMsgTyped());
      assert(m->req_id() == req_id_);
      assert(m->part_nr() == part_nr_);
      return m;
    }
    cvmfs::MsgInfoReply *msg_info_reply() {
      cvmfs::MsgInfoReply *m = reinterpret_cast<cvmfs::MsgInfoReply *>(
          frame_recv_.GetMsgTyped());
      assert(m->req_id() == req_id_);
      return m;
    }
    cvmfs::MsgShrinkReply *msg_shrink_reply() {
      cvmfs::MsgShrinkReply *m = reinterpret_cast<cvmfs::MsgShrinkReply *>(
          frame_recv_.GetMsgTyped());
      assert(m->req_id() == req_id_);
      return m;
    }
    cvmfs::MsgListReply *msg_list_reply() {
      cvmfs::MsgListReply *m = reinterpret_cast<cvmfs::MsgListReply *>(
          frame_recv_.GetMsgTyped());
      assert(m->req_id() == req_id_);
      return m;
    }
    cvmfs::MsgBreadcrumbReply *msg_breadcrumb_reply() {
      cvmfs::MsgBreadcrumbReply
          *m = reinterpret_cast<cvmfs::MsgBreadcrumbReply *>(
              frame_recv_.GetMsgTyped());
      assert(m->req_id() == req_id_);
      return m;
    }

    CacheTransport::Frame *frame_send() { return &frame_send_; }
    CacheTransport::Frame *frame_recv() { return &frame_recv_; }
    uint64_t req_id() const { return req_id_; }
    uint64_t part_nr() const { return part_nr_; }

   private:
    uint64_t req_id_;
    uint64_t part_nr_;
    google::protobuf::MessageLite *msg_req_;
    CacheTransport::Frame frame_send_;
    CacheTransport::Frame frame_recv_;
  };  // class RpcJob

  struct RpcInFlight {
    RpcInFlight() : rpc_job(NULL), signal(NULL) { }
    RpcInFlight(RpcJob *r, Signal *s) : rpc_job(r), signal(s) { }

    RpcJob *rpc_job;
    Signal *signal;
  };

  static void *MainRead(void *data);
  static int ConnectLocator(const std::string &locator, bool print_error);
  static bool SpawnPlugin(const std::vector<std::string> &cmd_line);

  explicit ExternalCacheManager(int fd_connection, unsigned max_open_fds);
  int64_t NextRequestId() { return atomic_xadd64(&next_request_id_, 1); }
  void CallRemotely(RpcJob *rpc_job);
  int ChangeRefcount(const shash::Any &id, int change_by);
  int DoOpen(const shash::Any &id);
  shash::Any GetHandle(int fd);
  int Flush(bool do_commit, Transaction *transaction);

  pid_t pid_plugin_;
  FdTable<ReadOnlyHandle> fd_table_;
  CacheTransport transport_;
  int64_t session_id_;
  uint32_t max_object_size_;
  bool spawned_;
  bool terminated_;
  pthread_rwlock_t rwlock_fd_table_;
  atomic_int64 next_request_id_;

  /**
   * Serialize concurrent write access to the session fd
   */
  pthread_mutex_t lock_send_fd_;
  std::vector<RpcInFlight> inflight_rpcs_;
  pthread_mutex_t lock_inflight_rpcs_;
  pthread_t thread_read_;
  uint64_t capabilities_;
};  // class ExternalCacheManager


class ExternalQuotaManager : public QuotaManager {
 public:
  static ExternalQuotaManager *Create(ExternalCacheManager *cache_mgr);
  virtual bool HasCapability(Capabilities capability);

  virtual void Insert(const shash::Any &hash, const uint64_t size,
                      const std::string &description) { }

  virtual void InsertVolatile(const shash::Any &hash, const uint64_t size,
                              const std::string &description) { }

  virtual bool Pin(const shash::Any &hash, const uint64_t size,
                   const std::string &description, const bool is_catalog) {
    return is_catalog;
  }

  virtual void Unpin(const shash::Any &hash) { }
  virtual void Touch(const shash::Any &hash) { }
  virtual void Remove(const shash::Any &file) { }
  virtual bool Cleanup(const uint64_t leave_size);

  virtual void RegisterBackChannel(int back_channel[2],
                                   const std::string &channel_id);
  virtual void UnregisterBackChannel(int back_channel[2],
                                     const std::string &channel_id);

  virtual std::vector<std::string> List();
  virtual std::vector<std::string> ListPinned();
  virtual std::vector<std::string> ListCatalogs();
  virtual std::vector<std::string> ListVolatile();
  virtual uint64_t GetMaxFileSize() { return uint64_t(-1); }
  virtual uint64_t GetCapacity();
  virtual uint64_t GetSize();
  virtual uint64_t GetSizePinned();
  virtual bool SetLimit(uint64_t limit) { return false; }  // NOLINT
  virtual uint64_t GetCleanupRate(uint64_t period_s);

  virtual void Spawn() { }
  virtual pid_t GetPid() { return cache_mgr_->pid_plugin(); }
  virtual uint32_t GetProtocolRevision() { return 0; }

 private:
  struct QuotaInfo {
    QuotaInfo() : size(0), used(0), pinned(0), no_shrink(0) { }
    uint64_t size;
    uint64_t used;
    uint64_t pinned;
    uint64_t no_shrink;
  };

  explicit ExternalQuotaManager(ExternalCacheManager *cache_mgr)
      : cache_mgr_(cache_mgr) { }
  int GetInfo(QuotaInfo *quota_info);
  bool DoListing(cvmfs::EnumObjectType type,
                 std::vector<cvmfs::MsgListRecord> *result);

  ExternalCacheManager *cache_mgr_;
};

#endif  // CVMFS_CACHE_EXTERN_H_
