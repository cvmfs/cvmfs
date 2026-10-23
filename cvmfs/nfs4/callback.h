/**
 * This file is part of the CernVM File System.
 *
 * Client side of the NFSv4.0 callback program (RFC 7530 section 16.2), used
 * to recall delegations.
 */

#ifndef CVMFS_NFS4_CALLBACK_H_
#define CVMFS_NFS4_CALLBACK_H_

#include <stdint.h>

#include <mutex>
#include <string>

namespace nfs4 {

class CallbackClient {
 public:
  /**
   * netid and address as sent by the client in SETCLIENTID, e.g. "tcp" and
   * "127.0.0.1.3.28" (universal address: host, then port high and low byte).
   */
  CallbackClient(uint32_t program, uint32_t ident, const std::string &netid,
                 const std::string &address);
  ~CallbackClient();

  /** Whether the address could be parsed; only IPv4 over TCP is supported */
  bool valid() const { return port_ != 0; }

  /** CB_NULL, i.e. checks that the callback path works */
  bool Null(unsigned timeout_ms);
  /**
   * CB_RECALL of a delegation.  Returns the NFS status of the client's reply,
   * or -1 if the client did not answer.
   */
  int Recall(uint32_t stateid_seqid, const std::string &stateid_other,
             const std::string &fh, unsigned timeout_ms);

 private:
  bool Call(uint32_t procedure, const std::string &args, std::string *result,
            unsigned timeout_ms);
  bool Connect(unsigned timeout_ms);
  void Disconnect();

  uint32_t program_;
  uint32_t ident_;
  std::string host_;
  uint16_t port_;
  std::mutex lock_;
  int fd_;
  uint32_t next_xid_;
};

}  // namespace nfs4

#endif  // CVMFS_NFS4_CALLBACK_H_
