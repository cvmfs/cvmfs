/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_CACHE_PLUGIN_CHANNEL_H_
#define CVMFS_CACHE_PLUGIN_CHANNEL_H_

#include <pthread.h>
#include <stdint.h>

#include <string>

#include "atomic.h"
#include "cache.pb.h"
#include "cache_transport.h"
#include "hash.h"
#include "murmur.h"
#include "smallhash.h"
#include "util/single_copy.h"

class CachePlugin : SingleCopy {
 public:
  static const unsigned kPbProtocolVersion = 1;
  static const uint64_t kSizeUnknown;

  struct ObjectInfo {
    ObjectInfo() : size(kSizeUnknown), object_type(cvmfs::OBJECT_REGULAR) { }
    uint64_t size;
    cvmfs::EnumObjectType object_type;
    std::string description;
  };

  bool Listen();
  ~CachePlugin();
  void ProcessRequests();

  unsigned max_object_size() const { return max_object_size_; }

 protected:
  explicit CachePlugin(const std::string &socket_path);

  virtual cvmfs::EnumStatus ChangeRefcount(const shash::Any &id,
                                           int32_t change_by) = 0;
  virtual cvmfs::EnumStatus GetObjectInfo(const shash::Any &id,
                                          ObjectInfo *info) = 0;
  virtual cvmfs::EnumStatus Pread(const shash::Any &id,
                                  uint64_t offset,
                                  unsigned *size,
                                  unsigned char *buffer) = 0;
  virtual cvmfs::EnumStatus StartTxn(const shash::Any &id,
                                     const uint64_t txn_id,
                                     const ObjectInfo &info) = 0;
  virtual cvmfs::EnumStatus WriteTxn(const uint64_t txn_id,
                                     unsigned char *buffer,
                                     unsigned size) = 0;
  virtual cvmfs::EnumStatus AbortTxn(const uint64_t txn_id) = 0;
  virtual cvmfs::EnumStatus CommitTxn(const uint64_t txn_id) = 0;

 private:
  static const unsigned kDefaultMaxObjectSize = 256 * 1024;  // 256kB

  struct UniqueRequest {
    UniqueRequest() : session_id(-1), req_id(-1) { }
    UniqueRequest(int64_t s, int64_t r) : session_id(s), req_id(r) { }
    bool operator ==(const UniqueRequest &other) const {
      return (this->session_id == other.session_id) &&
             (this->req_id == other.req_id);
    }
    bool operator !=(const UniqueRequest &other) const {
      return !(*this == other);
    }

    int64_t session_id;
    int64_t req_id;
  };

  static void *MainProcessRequests(void *data);

  uint64_t NextSessionId() {
    return atomic_xadd64(&next_session_id_, 1);
  }
  uint64_t NextTxnId() {
    return atomic_xadd64(&next_txn_id_, 1);
  }
  static inline uint32_t HashUniqueRequest(const UniqueRequest &req) {
    return MurmurHash2(&req, sizeof(req), 0x07387a4f);
  }

  bool HandleRequest(int fd_con);
  void HandleHandshake(CacheTransport *transport);
  void HandleRefcount(cvmfs::MsgRefcountReq *msg_req,
                      CacheTransport *transport);
  void HandleObjectInfo(cvmfs::MsgObjectInfoReq *msg_req,
                        CacheTransport *transport);
  void HandleRead(cvmfs::MsgReadReq *msg_req,
                     CacheTransport *transport);
  void HandleStore(cvmfs::MsgStoreReq *msg_req,
                   CacheTransport::Frame *frame,
                   CacheTransport *transport);
  void HandleStoreAbort(cvmfs::MsgStoreAbortReq *msg_req,
                        CacheTransport *transport);

  std::string socket_path_;
  int fd_socket_;
  bool running_;
  unsigned max_object_size_;
  std::string name_;
  atomic_int64 next_session_id_;
  atomic_int64 next_txn_id_;
  SmallHashDynamic<UniqueRequest, uint64_t> txn_ids_;
  pthread_t thread_io_;
  int pipe_ctrl_[2];
};  // class CachePlugin

#endif  // CVMFS_CACHE_PLUGIN_CHANNEL_H_
