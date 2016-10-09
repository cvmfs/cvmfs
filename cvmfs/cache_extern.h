/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_CACHE_EXTERN_H_
#define CVMFS_CACHE_EXTERN_H_

#include <pthread.h>
#include <stdint.h>

#include <cassert>

#include "atomic.h"
#include "cache.h"
#include "cache_transport.h"
#include "fd_table.h"
#include "hash.h"

class ExternalCacheManager : public CacheManager {
 public:
  static const unsigned kPbProtocolVersion = 1;

  static ExternalCacheManager *Create(int fd_connection, unsigned max_open_fds);
  virtual ~ExternalCacheManager();

  virtual CacheManagerIds id() { return kExternalCacheManager; }
  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr);

  virtual int Open(const BlessedObject &object);
  virtual int64_t GetSize(int fd);
  virtual int Close(int fd);
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset);
  virtual int Dup(int fd);
  virtual int Readahead(int fd);

  virtual uint16_t SizeOfTxn();
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn);
  virtual void CtrlTxn(const ObjectInfo &object_info,
                       const int flags,
                       void *txn);
  virtual int64_t Write(const void *buf, uint64_t sz, void *txn);
  virtual int Reset(void *txn);
  virtual int AbortTxn(void *txn);
  virtual int OpenFromTxn(void *txn);
  virtual int CommitTxn(void *txn);

  int64_t session_id() const { return session_id_; }

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

  struct Transaction {
    Transaction(const shash::Any &id)
      : buffer(reinterpret_cast<unsigned char *>(this) + sizeof(this))
      , buf_pos(0)
      , size(0)
      , expected_size(kSizeUnknown)
      , object_info(kTypeRegular, "")
      , id(id)
    { }

    /**
     * Allocated size is max_object_size_, allocated by the caller at the end
     * of the transaction.
     */
    unsigned char *buffer;
    unsigned buf_pos;
    uint64_t size;
    uint64_t expected_size;
    ObjectInfo object_info;
    shash::Any id;
  };

  struct ReadOnlyHandle {
    ReadOnlyHandle() : id(kInvalidHandle) { }
    explicit ReadOnlyHandle(const shash::Any &h) : id(h) { }
    bool operator ==(const ReadOnlyHandle &other) const {
      return this->id == other.id;
    }
    bool operator !=(const ReadOnlyHandle &other) const {
      return this->id != other.id;
    }
    shash::Any id;
  };

  class RpcJob {
   public:
    explicit RpcJob(cvmfs::MsgRefcountReq *msg)
      : req_id_(msg->req_id()), msg_req_(msg), frame_send_(msg) { }
    explicit RpcJob(cvmfs::MsgObjectInfoReq *msg)
      : req_id_(msg->req_id()), msg_req_(msg), frame_send_(msg) { }
    explicit RpcJob(cvmfs::MsgReadReq *msg)
      : req_id_(msg->req_id()), msg_req_(msg), frame_send_(msg) { }

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
      cvmfs::MsgObjectInfoReply *m =
        reinterpret_cast<cvmfs::MsgObjectInfoReply *>(
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

    CacheTransport::Frame *frame_send() { return &frame_send_; }
    CacheTransport::Frame *frame_recv() { return &frame_recv_; }

   private:
    uint64_t req_id_;
    google::protobuf::MessageLite *msg_req_;
    CacheTransport::Frame frame_send_;
    CacheTransport::Frame frame_recv_;
  };

  explicit ExternalCacheManager(int fd_connection, unsigned max_open_fds);
  int64_t NextRequestId() { return atomic_xadd64(&next_request_id_, 1); }
  void CallRemotely(RpcJob *rpc_job);
  int ChangeRefcount(const shash::Any &id, int change_by);
  int DoOpen(const shash::Any &id);
  shash::Any GetHandle(int fd);

  FdTable<ReadOnlyHandle> fd_table_;
  CacheTransport transport_;
  int64_t session_id_;
  uint32_t max_object_size_;
  bool spawned_;
  pthread_rwlock_t rwlock_fd_table_;
  atomic_int64 next_request_id_;
};  // class ExternalCacheManager

#endif  // CVMFS_CACHE_EXTERN_H_
