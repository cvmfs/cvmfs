/**
 * This file is part of the CernVM File System.
 *
 * Read-only NFSv4.0 server (RFC 7530) on top of an inode-based Backend.
 *
 * The export is read-only and never grants delegations, share-deny conflicts
 * or locks, so open state cannot conflict between clients.  This lets the
 * server keep all state in memory, never revoke leases, and grant reclaims
 * after a restart without a grace period.
 */

#ifndef CVMFS_NFS4_NFS4_SERVER_H_
#define CVMFS_NFS4_NFS4_SERVER_H_

#include <stdint.h>

#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "nfs4/backend.h"
#include "nfs4/callback.h"
#include "nfs4/filehandle.h"
#include "nfs4/nfs4_const.h"
#include "nfs4/rpc.h"
#include "nfs4/xdr.h"

namespace nfs4 {

class Nfs4Server : public RpcProgram {
 public:
  struct Options {
    Options()
        : lease_time(90)
        , max_read(1u << 20)
        , max_compound_ops(128)
        , max_stateids(65536)
        , delay_after_ms(0)
        , executor_threads(32)
        , delegations(false) { }
    uint32_t lease_time;
    uint32_t max_read;
    uint32_t max_compound_ops;
    uint32_t max_stateids;
    /**
     * If non-zero, a COMPOUND that takes longer is answered with
     * NFS4ERR_DELAY and finishes in the background; the client retries.
     * Keeps soft mounts from timing out while the backend blocks, e.g. on a
     * large download.
     */
    uint32_t delay_after_ms;
    /** Threads running COMPOUNDs when delay_after_ms is set */
    unsigned executor_threads;
    /**
     * Grant read delegations to clients with a working callback path, so
     * that they can cache without revalidating.  All delegations are
     * recalled when the backend's generation changes.
     */
    bool delegations;
  };

  Nfs4Server(Backend *backend, const Options &options);
  virtual ~Nfs4Server();

  virtual uint32_t program() const { return kNfsProgram; }
  virtual uint32_t min_version() const { return kNfsVersion4; }
  virtual uint32_t max_version() const { return kNfsVersion4; }
  virtual AcceptStat Dispatch(const RpcCall &call, XdrReader *args,
                              XdrWriter *res);

  /** Number of open stateids, exposed for tests */
  size_t NumOpenStates();
  /** Number of outstanding delegations, exposed for tests */
  size_t NumDelegations();

 private:
  struct Compound {
    Compound() : call(NULL), has_fh(false), has_saved(false) { }
    const RpcCall *call;
    bool has_fh;
    FileHandle fh;
    bool has_saved;
    FileHandle saved;
  };

  /** A resolved file handle: backend attributes plus what kind of object */
  struct Object {
    FileHandle fh;
    Attr attr;
    std::string xattr_name;  // set for named attributes
  };

  struct Stateid {
    Stateid() : seqid(0) { }
    uint32_t seqid;
    std::string other;  // kStateidOtherSize bytes
  };

  struct Client {
    Client() : confirmed(false), callback_ok(false) { }
    std::string name;
    std::string verifier;
    std::string confirm;
    bool confirmed;
    std::shared_ptr<CallbackClient> callback;
    /** Set once CB_NULL succeeded; only then delegations are granted */
    bool callback_ok;
  };

  struct Delegation {
    Delegation() : clientid(0), seqid(1), recalled(0) { }
    uint64_t clientid;
    FileHandle fh;
    uint32_t seqid;
    /** When the recall was sent, 0 if not recalled */
    time_t recalled;
  };

  typedef std::pair<uint64_t, std::string> OwnerKey;

  /** Open-owner sequencing state, including the reply for replays */
  struct OpenOwner {
    OpenOwner() : seqid(0), last_op(0), last_status(0) { }
    uint32_t seqid;
    uint32_t last_op;
    uint32_t last_status;
    std::string last_body;
  };

  struct OpenState {
    OpenState() : seqid(0), access(0) { }
    uint32_t seqid;
    OwnerKey owner;
    FileHandle fh;
    uint32_t access;
  };

  enum SeqidCheck {
    kSeqidProcess,
    kSeqidReplay,
    kSeqidBad
  };

  /** A COMPOUND handed to the executor threads */
  struct Task {
    Task() : done(false) { }
    RpcCall call;
    std::string args;
    XdrWriter res;
    bool done;
    /** Signals done; guarded by executor_lock_ */
    std::condition_variable done_cond;
  };
  typedef std::shared_ptr<Task> TaskPtr;

  void RunWithDeadline(const RpcCall &call, XdrReader *args, XdrWriter *res);
  /** Probes callback paths and recalls delegations, see Options::delegations */
  void MonitorLoop();
  void StartMonitor();
  uint32_t OpDelegreturn(XdrReader *args);
  /** Called with state_lock_ held; writes the open_delegation4 result */
  void PutDelegation(uint64_t clientid, const FileHandle &fh, XdrWriter *w);
  void ExecutorLoop();

  uint32_t ProcessCompound(const RpcCall &call, XdrReader *args,
                           XdrWriter *res);
  uint32_t ProcessOp(uint32_t op, XdrReader *args, Compound *c,
                     XdrWriter *body);

  uint32_t OpAccess(XdrReader *args, Compound *c, XdrWriter *body);
  uint32_t OpClose(XdrReader *args, Compound *c, XdrWriter *body);
  uint32_t OpGetattr(XdrReader *args, Compound *c, XdrWriter *body);
  uint32_t OpGetfh(Compound *c, XdrWriter *body);
  uint32_t OpLookup(XdrReader *args, Compound *c);
  uint32_t OpLookupp(Compound *c);
  uint32_t OpVerify(XdrReader *args, Compound *c, bool negate);
  uint32_t OpOpen(XdrReader *args, Compound *c, XdrWriter *body);
  uint32_t OpOpenattr(XdrReader *args, Compound *c);
  uint32_t OpOpenConfirm(XdrReader *args, Compound *c, XdrWriter *body);
  uint32_t OpOpenDowngrade(XdrReader *args, Compound *c, XdrWriter *body);
  uint32_t OpPutfh(XdrReader *args, Compound *c);
  uint32_t OpRead(XdrReader *args, Compound *c, XdrWriter *body);
  uint32_t OpReaddir(XdrReader *args, Compound *c, XdrWriter *body);
  uint32_t OpReadlink(Compound *c, XdrWriter *body);
  uint32_t OpRenew(XdrReader *args);
  uint32_t OpSecinfo(XdrReader *args, Compound *c, XdrWriter *body);
  uint32_t OpSetclientid(XdrReader *args, XdrWriter *body);
  uint32_t OpSetclientidConfirm(XdrReader *args);

  uint32_t Resolve(const FileHandle &fh, const Credentials &creds, Object *obj);
  uint32_t ResolveXattrName(uint64_t ino, uint32_t hash, std::string *name);
  uint32_t LookupChild(const Object &dir, const std::string &name,
                       const Credentials &creds, Object *child);
  uint32_t ListChildren(const Object &dir, std::vector<DirEntry> *entries);

  bool EncodeFattr(const Object &obj, const uint32_t *request,
                   unsigned request_len, const Credentials &creds,
                   XdrWriter *out);
  static bool DecodeBitmap(XdrReader *r, std::vector<uint32_t> *bitmap);
  uint64_t FileId(const Object &obj) const;

  // State management, all called with state_lock_ held
  uint32_t CheckClient(uint64_t clientid);
  SeqidCheck CheckSeqid(const OwnerKey &owner, uint32_t seqid, uint32_t op,
                        bool create_owner, uint32_t *status, std::string *body);
  void RecordSeqid(const OwnerKey &owner, uint32_t seqid, uint32_t op,
                   uint32_t status, const std::string &body);
  uint32_t FindOpenState(const Stateid &stateid, bool exact_seqid,
                         std::map<std::string, OpenState>::iterator *it);
  void DropClient(uint64_t clientid);
  std::string NewStateidOther();
  /** For READ; accepts the anonymous and read-bypass special stateids */
  uint32_t CheckReadStateid(const Stateid &stateid, const FileHandle &fh);

  static void PutStateid(const Stateid &stateid, XdrWriter *w);
  static bool GetStateid(XdrReader *r, Stateid *stateid);

  Backend *backend_;
  Options options_;
  uint64_t fsid_;
  /** Seconds since epoch at startup; prefixes client ids and stateids */
  uint32_t boot_;

  std::mutex state_lock_;
  uint32_t next_clientid_;
  uint64_t next_stateid_;
  std::map<uint64_t, Client> clients_;
  std::map<OwnerKey, OpenOwner> owners_;
  std::map<std::string, OpenState> open_states_;
  std::map<std::string, Delegation> delegations_;
  std::vector<uint64_t> pending_probes_;
  uint64_t last_generation_;
  std::thread monitor_;
  bool monitor_stopping_;
  std::condition_variable monitor_cond_;

  // Executor threads start lazily, so that a server constructed before the
  // process daemonizes (forks) does not lose them
  std::mutex executor_lock_;
  /** Wakes executor threads; waiters use Task::done_cond */
  std::condition_variable executor_cond_;
  std::deque<TaskPtr> tasks_;
  std::vector<std::thread> executors_;
  bool executors_stopping_;
};

/** Maps a backend errno to an NFSv4 status */
uint32_t ErrnoToStatus(int err);

}  // namespace nfs4

#endif  // CVMFS_NFS4_NFS4_SERVER_H_
