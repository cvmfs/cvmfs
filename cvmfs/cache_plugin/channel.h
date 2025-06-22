/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_CACHE_PLUGIN_CHANNEL_H_
#define CVMFS_CACHE_PLUGIN_CHANNEL_H_

#include <pthread.h>
#include <stdint.h>

#include <cassert>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "cache.pb.h"
#include "cache_transport.h"
#include "crypto/hash.h"
#include "manifest.h"
#include "smallhash.h"
#include "util/atomic.h"
#include "util/murmur.hxx"
#include "util/single_copy.h"

/**
 * A SessionCtx stores the session information related to the current cache
 * plugin callback in thread-local-storage.  Singleton.
 *
 * TODO(jblomer): merge code with ClientCtx
 */
class SessionCtx : SingleCopy {
 public:
  struct ThreadLocalStorage {
    ThreadLocalStorage(uint64_t id, char *reponame, char *client_instance)
        : id(id)
        , reponame(reponame)
        , client_instance(client_instance)
        , is_set(true) { }

    uint64_t id;
    char *reponame;
    char *client_instance;
    bool is_set;  ///< either not yet set or deliberately unset
  };

  static SessionCtx *GetInstance();
  static void CleanupInstance();
  ~SessionCtx();

  void Set(uint64_t id, char *reponame, char *client_instance);
  void Unset();
  void Get(uint64_t *id, char **reponame, char **client_instance);
  bool IsSet();

 private:
  static SessionCtx *instance_;
  static void TlsDestructor(void *data);

  SessionCtx();

  pthread_key_t thread_local_storage_;
  pthread_mutex_t *lock_tls_blocks_;
  std::vector<ThreadLocalStorage *> tls_blocks_;
};


class CachePlugin {
 public:
  static const unsigned kPbProtocolVersion = 1;
  static const uint64_t kSizeUnknown;

  struct ObjectInfo {
    ObjectInfo()
        : id()
        , size(kSizeUnknown)
        , object_type(cvmfs::OBJECT_REGULAR)
        , pinned(false) { }
    shash::Any id;
    uint64_t size;
    cvmfs::EnumObjectType object_type;
    bool pinned;
    std::string description;
  };

  struct Info {
    Info() : size_bytes(0), used_bytes(0), pinned_bytes(0), no_shrink(-1) { }
    uint64_t size_bytes;
    uint64_t used_bytes;
    uint64_t pinned_bytes;
    int64_t no_shrink;
  };

  bool Listen(const std::string &locator);
  virtual ~CachePlugin();
  void ProcessRequests(unsigned num_workers);
  bool IsRunning();
  void Terminate();
  void WaitFor();
  void AskToDetach();

  unsigned max_object_size() const { return max_object_size_; }
  uint64_t capabilities() const { return capabilities_; }

 protected:
  explicit CachePlugin(uint64_t capabilities);

  virtual cvmfs::EnumStatus ChangeRefcount(const shash::Any &id,
                                           int32_t change_by) = 0;
  virtual cvmfs::EnumStatus GetObjectInfo(const shash::Any &id,
                                          ObjectInfo *info) = 0;
  virtual cvmfs::EnumStatus Pread(const shash::Any &id,
                                  uint64_t offset,
                                  uint32_t *size,
                                  unsigned char *buffer) = 0;
  virtual cvmfs::EnumStatus StartTxn(const shash::Any &id,
                                     const uint64_t txn_id,
                                     const ObjectInfo &info) = 0;
  virtual cvmfs::EnumStatus WriteTxn(const uint64_t txn_id,
                                     unsigned char *buffer,
                                     uint32_t size) = 0;
  virtual cvmfs::EnumStatus AbortTxn(const uint64_t txn_id) = 0;
  virtual cvmfs::EnumStatus CommitTxn(const uint64_t txn_id) = 0;

  virtual cvmfs::EnumStatus GetInfo(Info *info) = 0;
  virtual cvmfs::EnumStatus Shrink(uint64_t shrink_to,
                                   uint64_t *used_bytes) = 0;
  virtual cvmfs::EnumStatus ListingBegin(uint64_t lst_id,
                                         cvmfs::EnumObjectType type) = 0;
  virtual cvmfs::EnumStatus ListingNext(int64_t lst_id, ObjectInfo *item) = 0;
  virtual cvmfs::EnumStatus ListingEnd(int64_t lst_id) = 0;

  virtual cvmfs::EnumStatus LoadBreadcrumb(
      const std::string &fqrn, manifest::Breadcrumb *breadcrumb) = 0;
  virtual cvmfs::EnumStatus StoreBreadcrumb(
      const std::string &fqrn, const manifest::Breadcrumb &breadcrumb) = 0;

 private:
  static const unsigned kDefaultMaxObjectSize = 256 * 1024;  // 256kB
  static const unsigned kListingSize = 4 * 1024 * 1024;      // 4MB
  static const char kSignalTerminate = 'q';
  static const char kSignalDetach = 'd';

  struct UniqueRequest {
    UniqueRequest() : session_id(-1), req_id(-1) { }
    UniqueRequest(int64_t s, int64_t r) : session_id(s), req_id(r) { }
    bool operator==(const UniqueRequest &other) const {
      return (this->session_id == other.session_id)
             && (this->req_id == other.req_id);
    }
    bool operator!=(const UniqueRequest &other) const {
      return !(*this == other);
    }

    int64_t session_id;
    int64_t req_id;
  };

  /**
   * The char pointers are prepared on Handshake and removed when the session
   * closes.  They are created to be consumed by the cvmcache_get_session() API.
   */
  struct SessionInfo {
    SessionInfo() : id(0), reponame(NULL), client_instance(NULL) { }
    SessionInfo(uint64_t id, const std::string &name);

    uint64_t id;
    std::string name;
    char *reponame;
    char *client_instance;
  };

  /**
   * RAII form of the SessionCtx.  On construction, automatically sets the
   * session context if the session id is found.  On destruction, unsets the
   * session information.
   */
  class SessionCtxGuard {
   public:
    SessionCtxGuard(uint64_t session_id, CachePlugin *plugin) {
      char *reponame = NULL;
      char *client_instance = NULL;
      const std::map<uint64_t, SessionInfo>::const_iterator
          iter = plugin->sessions_.find(session_id);
      if (iter != plugin->sessions_.end()) {
        reponame = iter->second.reponame;
        client_instance = iter->second.client_instance;
      }
      SessionCtx *session_ctx = SessionCtx::GetInstance();
      assert(session_ctx);
      session_ctx->Set(session_id, reponame, client_instance);
    }

    ~SessionCtxGuard() {
      SessionCtx *session_ctx = SessionCtx::GetInstance();
      assert(session_ctx);
      session_ctx->Unset();
    }
  };

  static void *MainProcessRequests(void *data);

  inline uint64_t NextSessionId() {
    return atomic_xadd64(&next_session_id_, 1);
  }
  inline uint64_t NextTxnId() { return atomic_xadd64(&next_txn_id_, 1); }
  inline uint64_t NextLstId() { return atomic_xadd64(&next_lst_id_, 1); }
  static inline uint32_t HashUniqueRequest(const UniqueRequest &req) {
    return MurmurHash2(&req, sizeof(req), 0x07387a4f);
  }

  bool HandleRequest(int fd_con);
  void HandleHandshake(cvmfs::MsgHandshake *msg_req, CacheTransport *transport);
  void HandleRefcount(cvmfs::MsgRefcountReq *msg_req,
                      CacheTransport *transport);
  void HandleObjectInfo(cvmfs::MsgObjectInfoReq *msg_req,
                        CacheTransport *transport);
  void HandleRead(cvmfs::MsgReadReq *msg_req, CacheTransport *transport);
  void HandleStore(cvmfs::MsgStoreReq *msg_req,
                   CacheTransport::Frame *frame,
                   CacheTransport *transport);
  void HandleStoreAbort(cvmfs::MsgStoreAbortReq *msg_req,
                        CacheTransport *transport);
  void HandleInfo(cvmfs::MsgInfoReq *msg_req, CacheTransport *transport);
  void HandleShrink(cvmfs::MsgShrinkReq *msg_req, CacheTransport *transport);
  void HandleList(cvmfs::MsgListReq *msg_req, CacheTransport *transport);
  void HandleBreadcrumbStore(cvmfs::MsgBreadcrumbStoreReq *msg_req,
                             CacheTransport *transport);
  void HandleBreadcrumbLoad(cvmfs::MsgBreadcrumbLoadReq *msg_req,
                            CacheTransport *transport);
  void HandleIoctl(cvmfs::MsgIoctl *msg_req);
  void SendDetachRequests();

  void NotifySupervisor(char signal);

  void LogSessionError(uint64_t session_id,
                       cvmfs::EnumStatus status,
                       const std::string &msg);
  void LogSessionInfo(uint64_t session_id, const std::string &msg);

  bool is_local_;
  uint64_t capabilities_;
  int fd_socket_;
  int fd_socket_lock_;
  atomic_int32 running_;
  unsigned num_workers_;
  unsigned max_object_size_;
  /**
   * Number of clients undergoing a reload, i.e. they promise to come back
   * and open a new connection soon.
   */
  uint64_t num_inlimbo_clients_;
  std::string name_;
  atomic_int64 next_session_id_;
  atomic_int64 next_txn_id_;
  atomic_int64 next_lst_id_;
  SmallHashDynamic<UniqueRequest, uint64_t> txn_ids_;
  std::set<int> connections_;
  std::map<uint64_t, SessionInfo> sessions_;
  pthread_t thread_io_;
  int pipe_ctrl_[2];
};  // class CachePlugin

#endif  // CVMFS_CACHE_PLUGIN_CHANNEL_H_
