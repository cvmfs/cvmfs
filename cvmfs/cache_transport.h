/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_CACHE_TRANSPORT_H_
#define CVMFS_CACHE_TRANSPORT_H_

#include <stdint.h>

#include <cstdlib>

#include "cache.h"
#include "cache.pb.h"
#include "util/single_copy.h"

namespace shash {
struct Any;
}

inline const char *CacheTransportCode2Ascii(const cvmfs::EnumStatus code) {
  switch (code) {
    case cvmfs::STATUS_UNKNOWN:
      return "unknown cache protocol error";
    case cvmfs::STATUS_OK:
      return "OK";
    case cvmfs::STATUS_NOSUPPORT:
      return "operation not implemented by cache plugin";
    case cvmfs::STATUS_FORBIDDEN:
      return "cache plugin denied the operation";
    case cvmfs::STATUS_NOSPACE:
      return "no space in cache";
    case cvmfs::STATUS_NOENTRY:
      return "object not found in cache";
    case cvmfs::STATUS_MALFORMED:
      return "malformed cache protocol message";
    case cvmfs::STATUS_IOERR:
      return "I/O error";
    case cvmfs::STATUS_CORRUPTED:
      return "corrupted data detected";
    case cvmfs::STATUS_TIMEOUT:
      return "multipart request timed out";
    case cvmfs::STATUS_BADCOUNT:
      return "invalid attempt to set negative reference count";
    case cvmfs::STATUS_OUTOFBOUNDS:
      return "out of bounds";
    case cvmfs::STATUS_PARTIAL:
      return "cache could not be cleaned up to the given limit";
    default:
      return "unexpected cache protocol error";
  }
}

/**
 * Sending and receiving with a file descriptor. Does _not_ take the ownership
 * of the file descriptor.
 */
class CacheTransport {
 public:
  /**
   * When this environment variable is set, the plugin will notify a cvmfs
   * client once it is ready to accept connections.
   */
  static const char *kEnvReadyNotifyFd;  // __CVMFS_CACHE_EXTERNAL_PIPE_READY__
  static const char kReadyNotification = 'C';
  static const char kFailureNotification = 'F';
  /**
   * Version of the wire protocol.  The effective protocol version is negotiated
   * through the handshake.
   */
  static const unsigned char kWireProtocolVersion = 0x01;
  /**
   * This is or-ed to the version number if the message has an attachment.  In
   * this case, the 2 bytes after the header specify the size of the protobuf
   * message alone.
   */
  static const unsigned char kFlagHasAttachment = 0x80;
  /**
   * Maximum size of the protobuf message _and_ the attachment, should it exist.
   */
  static const uint32_t kMaxMsgSize = (2 << 24) - 1;  // 24MB (3 bytes)
  /**
   * The first byte has the wire protocol version, optionally or-ed with the
   * "has attachment" flag.  The other three bytes encode the overall message
   * size in little-endian.
   */
  static const unsigned kHeaderSize = 4;
  /**
   * The "inner header" are two byte following the header.  The two bytes encode
   * in little-endian the size of the protobuf message alone, if there is an
   * attachment.  The inner header is only present if there is an attachment.
   */
  static const unsigned kInnerHeaderSize = 2;

  static const uint32_t kFlagSendIgnoreFailure = 0x01;
  static const uint32_t kFlagSendNonBlocking = 0x02;


  /**
   * A single unit of data transfer contains a "typed" Msg... protobuf message
   * inside a MsgRpc message.  Optionally, there can be an "attachment", which
   * is a byte stream following the protobuf message.  The typed message and the
   * attachment buffer are (stack)-allocated by users of CacheTransport.  The
   * Frame subclass takes care of wrapping and unwrapping into/from MsgRpc
   * message.
   */
  class Frame : SingleCopy {
   public:
    Frame();
    explicit Frame(google::protobuf::MessageLite *m);
    ~Frame();
    void MergeFrom(const Frame &other);
    void Reset(uint32_t original_att_size);

    void *attachment() const { return attachment_; }
    uint32_t att_size() const { return att_size_; }
    void set_att_size(uint32_t size) { att_size_ = size; }
    void set_attachment(void *attachment, uint32_t att_size) {
      attachment_ = attachment;
      att_size_ = att_size;
    }

    bool ParseMsgRpc(void *buffer, uint32_t size);
    cvmfs::MsgRpc *GetMsgRpc();
    google::protobuf::MessageLite *GetMsgTyped();
    // The Detach message does not follow the command-reply pattern
    bool IsMsgOutOfBand();

   private:
    void WrapMsg();
    void UnwrapMsg();
    void Release();

    cvmfs::MsgRpc msg_rpc_;
    bool owns_msg_typed_;
    /**
     * Can either point to a user-provided message (sender side) or to a message
     * inside msg_rpc_ (receiving end)
     */
    google::protobuf::MessageLite *msg_typed_;
    void *attachment_;
    uint32_t att_size_;
    bool is_wrapped_;
    bool is_msg_out_of_band_;
  };  // class CacheTransport::Frame


  explicit CacheTransport(int fd_connection);
  CacheTransport(int fd_connection, uint32_t flags);
  ~CacheTransport() { }

  void SendFrame(Frame *frame);
  bool RecvFrame(Frame *frame);

  void FillMsgHash(const shash::Any &hash, cvmfs::MsgHash *msg_hash);
  bool ParseMsgHash(const cvmfs::MsgHash &msg_hash, shash::Any *hash);
  void FillObjectType(int object_flags, cvmfs::EnumObjectType *wire_type);
  bool ParseObjectType(cvmfs::EnumObjectType wire_type, int *object_flags);

  int fd_connection() const { return fd_connection_; }

 private:
  static const unsigned kMaxStackAlloc = 256 * 1024;  // 256 kB

  void SendData(void *message,
                uint32_t msg_size,
                void *attachment = NULL,
                uint32_t att_size = 0);
  void SendNonBlocking(struct iovec *iov, unsigned iovcnt);
  bool RecvHeader(uint32_t *size, bool *has_attachment);

  int fd_connection_;
  uint32_t flags_;
};  // class CacheTransport

#endif  // CVMFS_CACHE_TRANSPORT_H_
