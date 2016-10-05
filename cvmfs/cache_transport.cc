/**
 * This file is part of the CernVM File System.
 */
#include "cvmfs_config.h"
#include "cache_transport.h"

#include <alloca.h>
#include <errno.h>

#include <cassert>
#include <cstdlib>

#include "cache.pb.h"
#include "hash.h"
#include "logging.h"
#include "smalloc.h"
#include "util/posix.h"

using namespace std;  // NOLINT

CacheTransport::CacheTransport(int fd_connection)
  : fd_connection_(fd_connection)
{
  assert(fd_connection_ >= 0);
}


void CacheTransport::FillMsgHash(
  const shash::Any &hash,
  cvmfs::MsgHash *msg_hash)
{
  switch (hash.algorithm) {
    case shash::kSha1:
      msg_hash->set_algorithm(cvmfs::HASH_SHA1);
      break;
    case shash::kRmd160:
      msg_hash->set_algorithm(cvmfs::HASH_RIPEMD160);
      break;
    case shash::kShake128:
      msg_hash->set_algorithm(cvmfs::HASH_SHAKE128);
      break;
    default:
      abort();
  }
  msg_hash->set_digest(hash.digest, shash::kDigestSizes[hash.algorithm]);
}


bool CacheTransport::ParseMsgHash(
  const cvmfs::MsgHash &msg_hash,
  shash::Any *hash)
{
  switch (msg_hash.algorithm()) {
    case cvmfs::HASH_SHA1:
      hash->algorithm = shash::kSha1;
      break;
    case cvmfs::HASH_RIPEMD160:
      hash->algorithm = shash::kRmd160;
      break;
    case cvmfs::HASH_SHAKE128:
      hash->algorithm = shash::kShake128;
      break;
    default:
      return false;
  }
  const unsigned digest_size = shash::kDigestSizes[hash->algorithm];
  if (msg_hash.digest().length() != digest_size)
    return false;
  memcpy(hash->digest, msg_hash.digest().data(), digest_size);
  return true;
}


bool CacheTransport::RecvHeader(uint32_t *size) {
  unsigned char header[4];
  ssize_t nbytes = SafeRead(fd_connection_, header, sizeof(header));
  if (nbytes != sizeof(header))
    return false;
  if (header[0] != kWireProtocolVersion)
    return false;
  *size = header[1] + (header[2] << 8) + (header[3] << 16);
  return (*size > 0) && (*size <= kMaxMsgSize);
}


bool CacheTransport::RecvMsg(google::protobuf::MessageLite *msg) {
  uint32_t size;
  bool retval = RecvHeader(&size);
  if (!retval)
    return false;

  void *buffer;
  if (size <= kMaxStackAlloc)
    buffer = alloca(size);
  else
    buffer = smalloc(size);
  retval = RecvRawMsg(buffer, size);
  if (!retval) {
    if (size > kMaxStackAlloc) { free(buffer); }
    return false;
  }
  retval = msg->ParseFromArray(buffer, size);
  if (size > kMaxStackAlloc) { free(buffer); }
  return retval;
}


bool CacheTransport::RecvRawMsg(void *buffer, uint32_t size) {
  assert(size > 0);
  ssize_t nbytes = SafeRead(fd_connection_, buffer, size);
  return (nbytes == size);
}


void CacheTransport::SendData(
  void *data,
  uint32_t size,
  void *attachment,
  uint32_t att_size)
{
  assert(size <= kMaxMsgSize);
  LogCvmfs(kLogCache, kLogDebug,
           "sending message of size %u to external cache plugin", size);

  unsigned char header[4];
  header[0] = kWireProtocolVersion;
  header[1] = (size & 0x000000FF);
  header[2] = (size & 0x0000FF00) >> 8;
  header[3] = (size & 0x00FF0000) >> 16;

  struct iovec iov[3];
  iov[0].iov_base = header;
  iov[0].iov_len = sizeof(header);
  iov[1].iov_base = data;
  iov[1].iov_len = size;
  iov[2].iov_base = attachment;
  iov[2].iov_len = att_size;
  bool retval = SafeWriteV(fd_connection_, iov, (att_size == 0) ? 2 : 3);

  if (!retval) {
    LogCvmfs(kLogCache, kLogSyslogErr | kLogDebug,
             "failed to write to external cache plugin (%d), aborting", errno);
    abort();
  }
}


void CacheTransport::SendMsg(google::protobuf::MessageLite *msg_typed) {
  if (msg_typed->GetTypeName() == "cvmfs.MsgHandshake") {
    cvmfs::MsgClientCall msg;
    msg.set_allocated_msg_handshake(
      reinterpret_cast<cvmfs::MsgHandshake *>(msg_typed));
    SendRawMsg(&msg);
    msg.release_msg_handshake();
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgHandshakeAck") {
    cvmfs::MsgServerCall msg;
    msg.set_allocated_msg_handshake_ack(
      reinterpret_cast<cvmfs::MsgHandshakeAck *>(msg_typed));
    SendRawMsg(&msg);
    msg.release_msg_handshake_ack();
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgQuit") {
    cvmfs::MsgClientCall msg;
    msg.set_allocated_msg_quit(
      reinterpret_cast<cvmfs::MsgQuit *>(msg_typed));
    SendRawMsg(&msg);
    msg.release_msg_quit();
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgRefcountReq") {
    cvmfs::MsgClientCall msg;
    msg.set_allocated_msg_refcount_req(
      reinterpret_cast<cvmfs::MsgRefcountReq *>(msg_typed));
    SendRawMsg(&msg);
    msg.release_msg_refcount_req();
  } else if (msg_typed->GetTypeName() == "cvmfs.MsgRefcountReply") {
    cvmfs::MsgServerCall msg;
    msg.set_allocated_msg_refcount_reply(
      reinterpret_cast<cvmfs::MsgRefcountReply *>(msg_typed));
    SendRawMsg(&msg);
    msg.release_msg_refcount_reply();
  } else {
    abort();
  }
}


void CacheTransport::SendRawMsg(google::protobuf::MessageLite *msg) {
  int32_t size = msg->ByteSize();
  assert(size >= 0);
  void *buffer = alloca(size);
  bool retval = msg->SerializeToArray(buffer, size);
  assert(retval);
  SendData(buffer, size);
}
