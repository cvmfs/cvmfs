/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_CACHE_EXTERN_H_
#define CVMFS_CACHE_EXTERN_H_

#include <pthread.h>

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

  explicit ExternalCacheManager(int fd_connection, unsigned max_open_fds);
  int64_t NextRequestId() { return atomic_xadd64(&next_request_id_, 1); }
  void CallRemotely(google::protobuf::MessageLite *msg_req,
                    google::protobuf::MessageLite *msg_reply);
  int ChangeRefcount(const shash::Any &id, int change_by);
  int DoOpen(const shash::Any &id);
  shash::Any GetHandle(int fd);

  FdTable<ReadOnlyHandle> fd_table_;
  CacheTransport transport_;
  int64_t session_id_;
  bool spawned_;
  pthread_rwlock_t rwlock_fd_table_;
  atomic_int64 next_request_id_;
};  // class ExternalCacheManager

#endif  // CVMFS_CACHE_EXTERN_H_
