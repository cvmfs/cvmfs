/**
 * This file is part of the CernVM File System.
 */

#include "cache_transport.h"

#include <alloca.h>
#include <errno.h>
#include <sys/socket.h>

#include <cassert>
#include <cstdlib>
#include <cstring>

#include "crypto/hash.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/smalloc.h"

// TODO(jblomer): Check for possible starvation of plugin by dying clients
// (blocking read).  Probably only relevant for TCP sockets.

using namespace std;  // NOLINT

const char *CacheTransport::kEnvReadyNotifyFd =
  "__CVMFS_CACHE_EXTERNAL_PIPE_READY__";

/**
 * Called on the sender side to wrap a message into a MsgRpc message for wire
 * transfer.
 */
cvmfs::MsgRpc *CacheTransport::Frame::GetMsgRpc() {
  assert(msg_typed_ != NULL);
  if (!is_wrapped_)
    WrapMsg();
  return &msg_rpc_;
}


/**
 * Called on the receiving end of an RPC to extract the actual message from the
 * MsgRpc.
 */
google::protobuf::MessageLite *CacheTransport::Frame::GetMsgTyped() {
  assert(msg_rpc_.IsInitialized());
  if (msg_typed_ == NULL)
    UnwrapMsg();
  return msg_typed_;
}


CacheTransport::Frame::Frame()
  : owns_msg_typed_(false)
  , msg_typed_(NULL)
  , attachment_(NULL)
  , att_size_(0)
  , is_wrapped_(false)
  , is_msg_out_of_band_(false)
{ }


CacheTransport::Frame::Frame(google::protobuf::MessageLite *m)
  : owns_msg_typed_(false)
  , msg_typed_(m)
  , attachment_(NULL)
  , att_size_(0)
  , is_wrapped_(false)
  , is_msg_out_of_band_(false)
{ }


CacheTransport::Frame::~Frame() {
  Reset(0);
}


bool CacheTransport::Frame::IsMsgOutOfBand() {
  assert(msg_rpc_.IsInitialized());
  if (msg_typed_ == NULL)
    UnwrapMsg();
  return is_msg_out_of_band_;
}


void CacheTransport::Frame::MergeFrom(const Frame &other) {
  msg_rpc_.CheckTypeAndMergeFrom(other.msg_rpc_);
  owns_msg_typed_ = true;
  if (other.att_size_ > 0) {
    assert(att_size_ >= other.att_size_);
    memcpy(attachment_, other.attachment_, other.att_size_);
    att_size_ = other.att_size_;
  }
}


bool CacheTransport::Frame::ParseMsgRpc(void *buffer, uint32_t size) {
  bool retval = msg_rpc_.ParseFromArray(buffer, size);
  if (!retval)
    return false;

  // Cleanup typed message when Frame leaves scope
  owns_msg_typed_ = true;
  return true;
}


void CacheTransport::Frame::Release() {
  if (owns_msg_typed_)
    return;

  msg_rpc_.release_msg_refcount_req();
  msg_rpc_.release_msg_refcount_reply();
  msg_rpc_.release_msg_read_req();
  msg_rpc_.release_msg_read_reply();
  msg_rpc_.release_msg_object_info_req();
  msg_rpc_.release_msg_object_info_reply();
  msg_rpc_.release_msg_store_req();
  msg_rpc_.release_msg_store_abort_req();
  msg_rpc_.release_msg_store_reply();
  msg_rpc_.release_msg_handshake();
  msg_rpc_.release_msg_handshake_ack();
  msg_rpc_.release_msg_quit();
  msg_rpc_.release_msg_ioctl();
  msg_rpc_.release_msg_info_req();
  msg_rpc_.release_msg_info_reply();
  msg_rpc_.release_msg_shrink_req();
  msg_rpc_.release_msg_shrink_reply();
  msg_rpc_.release_msg_list_req();
  msg_rpc_.release_msg_list_reply();
  msg_rpc_.release_msg_detach();
  msg_rpc_.release_msg_breadcrumb_store_req();
  msg_rpc_.release_msg_breadcrumb_load_req();
  msg_rpc_.release_msg_breadcrumb_reply();
}


void CacheTransport::Frame::Reset(uint32_t original_att_size) {
  msg_typed_ = NULL;
  att_size_ = original_att_size;
  is_wrapped_ = false;
  is_msg_out_of_band_ = false;
  Release();
  msg_rpc_.Clear();
  owns_msg_typed_ = false;
}


void CacheTransport::Frame::WrapMsg() {
  if (msg_typed_->GetTypeName() == "cvmfs.MsgHandshake") {
    msg_rpc_.set_allocated_msg_handshake(
      reinterpret_cast<cvmfs::MsgHandshake *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgHandshakeAck") {
    msg_rpc_.set_allocated_msg_handshake_ack(
      reinterpret_cast<cvmfs::MsgHandshakeAck *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgQuit") {
    msg_rpc_.set_allocated_msg_quit(
      reinterpret_cast<cvmfs::MsgQuit *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgIoctl") {
    msg_rpc_.set_allocated_msg_ioctl(
      reinterpret_cast<cvmfs::MsgIoctl *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgRefcountReq") {
    msg_rpc_.set_allocated_msg_refcount_req(
      reinterpret_cast<cvmfs::MsgRefcountReq *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgRefcountReply") {
    msg_rpc_.set_allocated_msg_refcount_reply(
      reinterpret_cast<cvmfs::MsgRefcountReply *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgObjectInfoReq") {
    msg_rpc_.set_allocated_msg_object_info_req(
      reinterpret_cast<cvmfs::MsgObjectInfoReq *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgObjectInfoReply") {
    msg_rpc_.set_allocated_msg_object_info_reply(
      reinterpret_cast<cvmfs::MsgObjectInfoReply *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgReadReq") {
    msg_rpc_.set_allocated_msg_read_req(
      reinterpret_cast<cvmfs::MsgReadReq *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgReadReply") {
    msg_rpc_.set_allocated_msg_read_reply(
      reinterpret_cast<cvmfs::MsgReadReply *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgStoreReq") {
    msg_rpc_.set_allocated_msg_store_req(
      reinterpret_cast<cvmfs::MsgStoreReq *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgStoreAbortReq") {
    msg_rpc_.set_allocated_msg_store_abort_req(
      reinterpret_cast<cvmfs::MsgStoreAbortReq *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgStoreReply") {
    msg_rpc_.set_allocated_msg_store_reply(
      reinterpret_cast<cvmfs::MsgStoreReply *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgInfoReq") {
    msg_rpc_.set_allocated_msg_info_req(
      reinterpret_cast<cvmfs::MsgInfoReq *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgInfoReply") {
    msg_rpc_.set_allocated_msg_info_reply(
      reinterpret_cast<cvmfs::MsgInfoReply *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgShrinkReq") {
    msg_rpc_.set_allocated_msg_shrink_req(
      reinterpret_cast<cvmfs::MsgShrinkReq *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgShrinkReply") {
    msg_rpc_.set_allocated_msg_shrink_reply(
      reinterpret_cast<cvmfs::MsgShrinkReply *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgListReq") {
    msg_rpc_.set_allocated_msg_list_req(
      reinterpret_cast<cvmfs::MsgListReq *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgListReply") {
    msg_rpc_.set_allocated_msg_list_reply(
      reinterpret_cast<cvmfs::MsgListReply *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgBreadcrumbStoreReq") {
    msg_rpc_.set_allocated_msg_breadcrumb_store_req(
      reinterpret_cast<cvmfs::MsgBreadcrumbStoreReq *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgBreadcrumbLoadReq") {
    msg_rpc_.set_allocated_msg_breadcrumb_load_req(
      reinterpret_cast<cvmfs::MsgBreadcrumbLoadReq *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgBreadcrumbReply") {
    msg_rpc_.set_allocated_msg_breadcrumb_reply(
      reinterpret_cast<cvmfs::MsgBreadcrumbReply *>(msg_typed_));
  } else if (msg_typed_->GetTypeName() == "cvmfs.MsgDetach") {
    msg_rpc_.set_allocated_msg_detach(
      reinterpret_cast<cvmfs::MsgDetach *>(msg_typed_));
    is_msg_out_of_band_ = true;
  } else {
    // Unexpected message type, should never happen
    PANIC(NULL);
  }
  is_wrapped_ = true;
}


void CacheTransport::Frame::UnwrapMsg() {
  if (msg_rpc_.has_msg_handshake()) {
    msg_typed_ = msg_rpc_.mutable_msg_handshake();
  } else if (msg_rpc_.has_msg_handshake_ack()) {
    msg_typed_ = msg_rpc_.mutable_msg_handshake_ack();
  } else if (msg_rpc_.has_msg_quit()) {
    msg_typed_ = msg_rpc_.mutable_msg_quit();
  } else if (msg_rpc_.has_msg_ioctl()) {
    msg_typed_ = msg_rpc_.mutable_msg_ioctl();
  } else if (msg_rpc_.has_msg_refcount_req()) {
    msg_typed_ = msg_rpc_.mutable_msg_refcount_req();
  } else if (msg_rpc_.has_msg_refcount_reply()) {
    msg_typed_ = msg_rpc_.mutable_msg_refcount_reply();
  } else if (msg_rpc_.has_msg_object_info_req()) {
    msg_typed_ = msg_rpc_.mutable_msg_object_info_req();
  } else if (msg_rpc_.has_msg_object_info_reply()) {
    msg_typed_ = msg_rpc_.mutable_msg_object_info_reply();
  } else if (msg_rpc_.has_msg_read_req()) {
    msg_typed_ = msg_rpc_.mutable_msg_read_req();
  } else if (msg_rpc_.has_msg_read_reply()) {
    msg_typed_ = msg_rpc_.mutable_msg_read_reply();
  } else if (msg_rpc_.has_msg_store_req()) {
    msg_typed_ = msg_rpc_.mutable_msg_store_req();
  } else if (msg_rpc_.has_msg_store_abort_req()) {
    msg_typed_ = msg_rpc_.mutable_msg_store_abort_req();
  } else if (msg_rpc_.has_msg_store_reply()) {
    msg_typed_ = msg_rpc_.mutable_msg_store_reply();
  } else if (msg_rpc_.has_msg_info_req()) {
    msg_typed_ = msg_rpc_.mutable_msg_info_req();
  } else if (msg_rpc_.has_msg_info_reply()) {
    msg_typed_ = msg_rpc_.mutable_msg_info_reply();
  } else if (msg_rpc_.has_msg_shrink_req()) {
    msg_typed_ = msg_rpc_.mutable_msg_shrink_req();
  } else if (msg_rpc_.has_msg_shrink_reply()) {
    msg_typed_ = msg_rpc_.mutable_msg_shrink_reply();
  } else if (msg_rpc_.has_msg_list_req()) {
    msg_typed_ = msg_rpc_.mutable_msg_list_req();
  } else if (msg_rpc_.has_msg_list_reply()) {
    msg_typed_ = msg_rpc_.mutable_msg_list_reply();
  } else if (msg_rpc_.has_msg_breadcrumb_store_req()) {
    msg_typed_ = msg_rpc_.mutable_msg_breadcrumb_store_req();
  } else if (msg_rpc_.has_msg_breadcrumb_load_req()) {
    msg_typed_ = msg_rpc_.mutable_msg_breadcrumb_load_req();
  } else if (msg_rpc_.has_msg_breadcrumb_reply()) {
    msg_typed_ = msg_rpc_.mutable_msg_breadcrumb_reply();
  } else if (msg_rpc_.has_msg_detach()) {
    msg_typed_ = msg_rpc_.mutable_msg_detach();
    is_msg_out_of_band_ = true;
  } else {
    // Unexpected message type, should never happen
  PANIC(NULL);
  }
}


//------------------------------------------------------------------------------


CacheTransport::CacheTransport(int fd_connection)
  : fd_connection_(fd_connection)
  , flags_(0)
{
  assert(fd_connection_ >= 0);
}


CacheTransport::CacheTransport(int fd_connection, uint32_t flags)
  : fd_connection_(fd_connection)
  , flags_(flags)
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
      PANIC(NULL);
  }
  msg_hash->set_digest(hash.digest, shash::kDigestSizes[hash.algorithm]);
}


void CacheTransport::FillObjectType(
  int object_flags, cvmfs::EnumObjectType *wire_type)
{
  *wire_type = cvmfs::OBJECT_REGULAR;
  if (object_flags & CacheManager::kLabelCatalog)
    *wire_type = cvmfs::OBJECT_CATALOG;
  if (object_flags & CacheManager::kLabelVolatile)
    *wire_type = cvmfs::OBJECT_VOLATILE;
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


bool CacheTransport::ParseObjectType(
  cvmfs::EnumObjectType wire_type, int *object_flags)
{
  *object_flags = 0;
  switch (wire_type) {
    case cvmfs::OBJECT_REGULAR:
      return true;
    case cvmfs::OBJECT_CATALOG:
      *object_flags |= CacheManager::kLabelCatalog;
      return true;
    case cvmfs::OBJECT_VOLATILE:
      *object_flags |= CacheManager::kLabelVolatile;
      return true;
    default:
      return false;
  }
}


bool CacheTransport::RecvFrame(CacheTransport::Frame *frame) {
  uint32_t size;
  bool has_attachment;
  bool retval = RecvHeader(&size, &has_attachment);
  if (!retval)
    return false;

  void *buffer;
  if (size <= kMaxStackAlloc)
    buffer = alloca(size);
  else
    buffer = smalloc(size);
  ssize_t nbytes = SafeRead(fd_connection_, buffer, size);
  if ((nbytes < 0) || (static_cast<uint32_t>(nbytes) != size)) {
    if (size > kMaxStackAlloc) { free(buffer); }
    return false;
  }

  uint32_t msg_size = size;
  if (has_attachment) {
    if (size < 2) {
      // kMaxStackAlloc is > 2 (of course!) but we'll leave the condition here
      // for consistency.
      if (size > kMaxStackAlloc) { free(buffer); }
      return false;
    }
    msg_size = (*reinterpret_cast<unsigned char *>(buffer)) +
               ((*(reinterpret_cast<unsigned char *>(buffer) + 1)) << 8);
    if ((msg_size + kInnerHeaderSize) > size) {
      if (size > kMaxStackAlloc) { free(buffer); }
      return false;
    }
  }

  void *ptr_msg = has_attachment
    ? (reinterpret_cast<char *>(buffer) + kInnerHeaderSize)
    : buffer;
  retval = frame->ParseMsgRpc(ptr_msg, msg_size);
  if (!retval) {
    if (size > kMaxStackAlloc) { free(buffer); }
    return false;
  }

  if (has_attachment) {
    uint32_t attachment_size = size - (msg_size + kInnerHeaderSize);
    if (frame->att_size() < attachment_size) {
      if (size > kMaxStackAlloc) { free(buffer); }
      return false;
    }
    void *ptr_attachment =
      reinterpret_cast<char *>(buffer) + kInnerHeaderSize + msg_size;
    memcpy(frame->attachment(), ptr_attachment, attachment_size);
    frame->set_att_size(attachment_size);
  } else {
    frame->set_att_size(0);
  }
  if (size > kMaxStackAlloc) { free(buffer); }
  return true;
}


bool CacheTransport::RecvHeader(uint32_t *size, bool *has_attachment) {
  unsigned char header[kHeaderSize];
  ssize_t nbytes = SafeRead(fd_connection_, header, kHeaderSize);
  if ((nbytes < 0) || (static_cast<unsigned>(nbytes) != kHeaderSize))
    return false;
  if ((header[0] & (~kFlagHasAttachment)) != kWireProtocolVersion)
    return false;
  *has_attachment = header[0] & kFlagHasAttachment;
  *size = header[1] + (header[2] << 8) + (header[3] << 16);
  return (*size > 0) && (*size <= kMaxMsgSize);
}


void CacheTransport::SendData(
  void *message,
  uint32_t msg_size,
  void *attachment,
  uint32_t att_size)
{
  uint32_t total_size =
    msg_size + att_size + ((att_size > 0) ? kInnerHeaderSize : 0);

  assert(total_size > 0);
  assert(total_size <= kMaxMsgSize);
  LogCvmfs(kLogCache, kLogDebug,
           "sending message of size %u to cache transport", total_size);

  unsigned char header[kHeaderSize];
  header[0] = kWireProtocolVersion | ((att_size == 0) ? 0 : kFlagHasAttachment);
  header[1] = (total_size & 0x000000FF);
  header[2] = (total_size & 0x0000FF00) >> 8;
  header[3] = (total_size & 0x00FF0000) >> 16;
  // Only transferred if an attachment is present.  Otherwise the overall size
  // is also the size of the protobuf message.
  unsigned char inner_header[kInnerHeaderSize];

  struct iovec iov[4];
  iov[0].iov_base = header;
  iov[0].iov_len = kHeaderSize;

  if (att_size > 0) {
    inner_header[0] = (msg_size & 0x000000FF);
    inner_header[1] = (msg_size & 0x0000FF00) >> 8;
    iov[1].iov_base = inner_header;
    iov[1].iov_len = kInnerHeaderSize;
    iov[2].iov_base = message;
    iov[2].iov_len = msg_size;
    iov[3].iov_base = attachment;
    iov[3].iov_len = att_size;
  } else {
    iov[1].iov_base = message;
    iov[1].iov_len = msg_size;
  }
  if (flags_ & kFlagSendNonBlocking) {
    SendNonBlocking(iov, (att_size == 0) ? 2 : 4);
    return;
  }
  bool retval = SafeWriteV(fd_connection_, iov, (att_size == 0) ? 2 : 4);

  if (!retval && !(flags_ & kFlagSendIgnoreFailure)) {
    PANIC(kLogSyslogErr | kLogDebug,
          "failed to write to external cache transport (%d), aborting", errno);
  }
}

void CacheTransport::SendNonBlocking(struct iovec *iov, unsigned iovcnt) {
  assert(iovcnt > 0);
  unsigned total_size = 0;
  for (unsigned i = 0; i < iovcnt; ++i)
    total_size += iov[i].iov_len;
  unsigned char *buffer = reinterpret_cast<unsigned char *>(alloca(total_size));

  unsigned pos = 0;
  for (unsigned i = 0; i < iovcnt; ++i) {
    memcpy(buffer + pos, iov[i].iov_base, iov[i].iov_len);
    pos += iov[i].iov_len;
  }

  int retval = send(fd_connection_, buffer, total_size, MSG_DONTWAIT);
  if (retval < 0) {
    assert(errno != EMSGSIZE);
    if (!(flags_ & kFlagSendIgnoreFailure)) {
      PANIC(kLogSyslogErr | kLogDebug,
            "failed to write to external cache transport (%d), aborting",
            errno);
    }
  }
}


void CacheTransport::SendFrame(CacheTransport::Frame *frame) {
  cvmfs::MsgRpc *msg_rpc = frame->GetMsgRpc();
  int32_t size = msg_rpc->ByteSize();
  assert(size > 0);
#ifdef __APPLE__
  void *buffer = smalloc(size);
#else
  void *buffer = alloca(size);
#endif
  bool retval = msg_rpc->SerializeToArray(buffer, size);
  assert(retval);
  SendData(buffer, size, frame->attachment(), frame->att_size());
#ifdef __APPLE__
  free(buffer);
#endif
}
