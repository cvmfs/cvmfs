/**
 * This file is part of the CernVM File System.
 */
#include "cvmfs_config.h"
#include "cache_transport.h"

#include <alloca.h>
#include <errno.h>

#include <cassert>
#include <cstdlib>

#include "google/protobuf/message_lite.h"
#include "logging.h"
#include "smalloc.h"
#include "util/posix.h"

using namespace std;  // NOLINT

CacheTransport::CacheTransport(int fd_connection)
  : fd_connection_(fd_connection)
{
  assert(fd_connection_ >= 0);
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


void CacheTransport::SendData(void *data, uint32_t size) {
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
  bool retval = SafeWriteV(fd_connection_, iov, 2);

  if (!retval) {
    LogCvmfs(kLogCache, kLogSyslogErr | kLogDebug,
             "failed to write to external cache plugin (%d), aborting", errno);
    abort();
  }
}


void CacheTransport::SendMsg(google::protobuf::MessageLite *msg) {
  int32_t size = msg->ByteSize();
  assert(size >= 0);
  void *buffer = alloca(size);
  bool retval = msg->SerializeToArray(buffer, size);
  assert(retval);
  SendData(buffer, size);
}
