/**
 * This file is part of the CernVM File System.
 *
 * ONC RPC v2 over TCP (RFC 5531) for the built-in NFS server.
 */

#ifndef CVMFS_NFS4_RPC_H_
#define CVMFS_NFS4_RPC_H_

#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "nfs4/backend.h"
#include "nfs4/nfs4_const.h"
#include "nfs4/xdr.h"

namespace nfs4 {

struct RpcCall {
  RpcCall()
      : xid(0)
      , program(0)
      , version(0)
      , procedure(0)
      , cred_flavor(kAuthNone)
      , args(NULL)
      , args_size(0) { }

  uint32_t xid;
  uint32_t program;
  uint32_t version;
  uint32_t procedure;
  uint32_t cred_flavor;
  /** Only meaningful for AUTH_SYS; nobody otherwise */
  Credentials creds;
  const unsigned char *args;
  size_t args_size;
};

class RpcProgram {
 public:
  virtual ~RpcProgram() { }
  virtual uint32_t program() const = 0;
  virtual uint32_t min_version() const = 0;
  virtual uint32_t max_version() const = 0;
  /**
   * Decodes the arguments and appends the procedure's results to res.  On
   * anything but kAcceptSuccess, res is discarded.
   */
  virtual AcceptStat Dispatch(const RpcCall &call, XdrReader *args,
                              XdrWriter *res) = 0;
};

/**
 * Turns one RPC record (without record marking) into a reply record.  Returns
 * false if the record cannot be answered (not a call, or too garbled to
 * extract the xid).
 */
bool HandleRpcRecord(RpcProgram *program, const std::string &record,
                     std::string *reply);


/**
 * Threaded TCP server.  One reader thread per connection collects records;
 * a pool of workers runs the procedures, so a call that blocks in the backend
 * (e.g. a download) does not stall other calls on the same connection.
 */
class RpcServer {
 public:
  struct Options {
    Options() : workers(16), allow_remote(false), max_record(4u << 20) { }
    unsigned workers;
    /** Accept connections from non-loopback peers (testing only) */
    bool allow_remote;
    uint32_t max_record;
  };

  RpcServer(RpcProgram *program, const Options &options);
  ~RpcServer();

  /** Binds and listens; port 0 picks an ephemeral port. */
  bool Listen(const std::string &address, uint16_t port);
  /** Serves on an already listening socket, e.g. inherited from a parent */
  bool Adopt(int listen_fd);
  uint16_t port() const { return port_; }
  int listen_fd() const { return listen_fd_; }
  /** Only effective before Start() */
  void SetWorkers(unsigned workers) { options_.workers = workers; }
  /** Can be restarted after Stop(); the listening socket stays open */
  void Start();
  /** Joins all threads and closes client connections */
  void Stop();

 private:
  struct Connection {
    explicit Connection(int f) : fd(f) { }
    ~Connection();
    int fd;
    std::mutex write_lock;
    std::mutex inflight_lock;
    std::set<uint32_t> inflight;
  };
  typedef std::shared_ptr<Connection> ConnectionPtr;

  struct Job {
    ConnectionPtr connection;
    uint32_t xid;
    std::string record;
  };

  void AcceptLoop();
  void ReadLoop(ConnectionPtr connection);
  void WorkerLoop();
  void Send(const ConnectionPtr &connection, const std::string &reply);

  RpcProgram *program_;
  Options options_;
  int listen_fd_;
  uint16_t port_;
  std::atomic<bool> stopping_;

  std::thread accept_thread_;
  std::vector<std::thread> workers_;

  std::mutex connections_lock_;
  std::vector<std::weak_ptr<Connection> > connections_;
  // Readers are detached; Stop() waits for this to drop to zero
  unsigned active_readers_;
  std::condition_variable readers_cond_;

  std::mutex queue_lock_;
  std::condition_variable queue_cond_;
  std::deque<Job> queue_;
};

}  // namespace nfs4

#endif  // CVMFS_NFS4_RPC_H_
