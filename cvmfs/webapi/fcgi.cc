/*
 * This file is part of the CernVM File System
 */

#include "cvmfs_config.h"
#include "fcgi.h"

#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>

using namespace std;  // NOLINT


const unsigned FastCgi::kMaxContentLength = 64 * 1024 - 1;
const char *FastCgi::kValueMaxConns = "FCGI_MAX_CONNS";
const char *FastCgi::kValueMaxReqs = "FCGI_MAX_REQS";
const char *FastCgi::kValueMpxConns = "FCGI_MPXS_CONNS";


/**
 * The HTTP client has closed the connection prematurely.
 */
void FastCgi::AbortRequest() {
  if (request_id_ == -1)
    return;
  ReplyEndRequest(fd_transport_, request_id_, 1, kStatusReqComplete);
  if (!keep_connection_) {
    CloseConnection();
  } else {
    request_id_ = -1;
    keep_connection_ = false;
  }
}


/**
 * Checks if a network connection was received from the allowed set of IP
 * addresses (see section 3.2 of fcgi specification).
 */
bool FastCgi::CheckValidSource() {
  char *environ_web_server_addrs = getenv("FCGI_WEB_SERVER_ADDRS");
  if (environ_web_server_addrs == NULL)
    return true;

  // TODO(jblomer): actually perform the check.  For the time being, this
  // class supports UNIX sockets only
  return false;
}


void FastCgi::CloseConnection() {
  if (fd_transport_ != -1) {
    close(fd_transport_);
    fd_transport_ = -1;
    request_id_ = -1;
    keep_connection_ = false;
  }
}


/**
 * Like CGI applications, exit_code != 0 indicates a failed request.
 */
void FastCgi::EndRequest(uint32_t exit_code) {
  if (request_id_ == -1)
    return;

  ReplyEndRequest(fd_transport_, request_id_, exit_code, kStatusReqComplete);
  if (!keep_connection_) {
    CloseConnection();
  } else {
    request_id_ = -1;
    keep_connection_ = false;
  }
}


/**
 * Returns whether the process is running in FastCGI context or as a normal
 * process.
 */
bool FastCgi::IsFcgi() {
  union {
    struct sockaddr_in in;
    struct sockaddr_un un;
  } sa;
  socklen_t len = sizeof(sa);

  if ((getpeername(kCgiListnsockFileno, (struct sockaddr *)&sa, &len) != 0) &&
      (errno == ENOTCONN))
  {
    return true;
  }
  return false;
}


FastCgi::FastCgi()
    : fd_transport_(-1)
    , request_id_(-1)
    , keep_connection_(false)
{ }


/**
 * Protocol processing as far as the application is not involved.  What is
 * received for stdin is pointed to in buf and length.
 */
FastCgi::Event FastCgi::NextEvent(unsigned char **buf, unsigned *length) {
  *buf = NULL;
  *length = 0;

  while (true) {
    if (fd_transport_ == -1) {
      // TODO(jblomer): deal properly with interrupts
      fd_transport_ = accept(kCgiListnsockFileno, NULL, NULL);
      if ((fd_transport_ < 0) || !CheckValidSource())
        return kEventExit;
    }

    Header header;
    if (!ReadHeader(fd_transport_, &header)) {
      CloseConnection();
      continue;
    }

    uint16_t role;
    bool keep_connection;
    switch (header.type) {
      case kTypeValues:
        assert(header.request_id == 0);
        // TODO(jblomer): implement reply
        break;

      case kTypeBegin:
        if (!ReadBeginBody(fd_transport_, &role, &keep_connection)) {
          CloseConnection();
          continue;
        }
        if (request_id_ != -1) {
          ReplyEndRequest(fd_transport_, header.request_id, 0,
                          kStatusCantMpxConn);
          continue;
        }
        if (role != kRoleResponder) {
          ReplyEndRequest(fd_transport_, header.request_id, 0,
                          kStatusUnknownRole);
          if (!keep_connection) CloseConnection();
          continue;
        }
        keep_connection_ = keep_connection;
        request_id_ = header.request_id;
        params_.clear();
        break;

      case kTypeAbort:
        if (request_id_ != header.request_id) {
          CloseConnection();
          continue;
        }
        return kEventAbortReq;
        break;

      case kTypeParams:
        if (request_id_ != header.request_id) {
          CloseConnection();
          continue;
        }
        if (!ReadParams(header)) {
          CloseConnection();
          continue;
        }
        break;

      case kTypeStdin:
        if (request_id_ != header.request_id) {
          CloseConnection();
          continue;
        }
        if (!ReadContent(header.content_length, header.padding_length)) {
          CloseConnection();
          continue;
        }
        *buf = content_buf_;
        *length = header.content_length;
        return kEventStdin;
        break;

      case kTypeData:
        // Only used in Filter role
        CloseConnection();
        continue;

      default:
        // Unexpected type
        if (header.request_id == 0) {
          ReplyUnknownType(fd_transport_, header.type);
        }
        continue;
    }  // switch (header.type)
  }  // accept loop
}


bool FastCgi::ParseKvPair(
  const char *data,
  unsigned len,
  unsigned *nparsed,
  std::string *key,
  std::string *value)
{
  if (len == 0) return false;
  *nparsed = 0;
  uint32_t key_length;
  if ((data[0] >> 7) == 1) {
    if (len < 4) return false;
    const unsigned char *key_length_bx =
      reinterpret_cast<const unsigned char *>(data);
    key_length = static_cast<uint32_t>(key_length_bx[0] & 0x7f) << 24;
    key_length += static_cast<uint32_t>(key_length_bx[1]) << 16;
    key_length += static_cast<uint32_t>(key_length_bx[2]) << 8;
    key_length += static_cast<uint32_t>(key_length_bx[3]);
    *nparsed += 4;
  } else {
    key_length = data[0];
    *nparsed += 1;
  }

  if (*nparsed >= len) return false;
  uint32_t value_length;
  if ((data[*nparsed] >> 7) == 1) {
    if (len < *nparsed + 4) return false;
    const unsigned char *value_length_bx =
      &(reinterpret_cast<const unsigned char *>(data)[*nparsed]);
    value_length = static_cast<uint32_t>(value_length_bx[0] & 0x7f) << 24;
    value_length += static_cast<uint32_t>(value_length_bx[1]) << 16;
    value_length += static_cast<uint32_t>(value_length_bx[2]) << 8;
    value_length += static_cast<uint32_t>(value_length_bx[3]);
    *nparsed += 4;
  } else {
    value_length = data[*nparsed];
    *nparsed += 1;
  }

  if (len < *nparsed + key_length + value_length) return false;
  *key = string(data + *nparsed, key_length);
  *nparsed += key_length;
  *value = string(data + *nparsed, value_length);
  *nparsed += value_length;
  return true;
}


bool FastCgi::ReadBeginBody(
  int fd_transport,
  uint16_t *role,
  bool *keep_connection)
{
  BeginRequestBody body;
  int nbytes = read(fd_transport, &body, sizeof(body));
  if (nbytes != sizeof(body))
    return false;

  *role = MkUint16(body.role_b1, body.role_b0);
  *keep_connection = body.flags & kKeepConn;
  return true;
}


bool FastCgi::ReadContent(uint16_t content_length, unsigned char padding_length)
{
  uint32_t nbytes = static_cast<uint32_t>(content_length) +
                    static_cast<uint32_t>(padding_length);
  if (nbytes > 0) {
    int received = read(fd_transport_, content_buf_, nbytes);
    return ((received >= 0) && (static_cast<unsigned>(received) == nbytes));
  }
  return true;
}


bool FastCgi::ReadHeader(int fd_transport, Header *header) {
  RawHeader raw_header;
  int nbytes = read(fd_transport, &raw_header, sizeof(raw_header));
  if (nbytes != sizeof(raw_header))
    return false;
  if (raw_header.version != kVersion1)
    return false;

  header->type = raw_header.type;
  header->request_id =
    MkUint16(raw_header.request_id_b1, raw_header.request_id_b0);
  header->content_length =
    MkUint16(raw_header.content_length_b1, raw_header.content_length_b0);
  header->padding_length = raw_header.padding_length;
  return true;
}


bool FastCgi::ReadParams(const Header &first_header) {
  uint16_t content_length = first_header.content_length;
  if (!ReadContent(content_length, first_header.padding_length))
  {
    return false;
  }
  string data;
  while (content_length > 0) {
    data += string(reinterpret_cast<char *>(content_buf_), content_length);

    Header header;
    if (!ReadHeader(fd_transport_, &header))
      return false;
    if ((header.type != kTypeParams) || (header.request_id != request_id_))
      return false;
    content_length = header.content_length;
    if (!ReadContent(content_length, header.padding_length))
      return false;
  }

  const unsigned len = data.length();
  unsigned pos = 0;
  while (pos < len) {
    unsigned nparsed;
    string key;
    string value;
    if (!ParseKvPair(data.data() + pos, len - pos, &nparsed, &key, &value))
      return false;
    pos += nparsed;
    params_[key] = value;
  }
  return true;
}


void FastCgi::ReplyEndRequest(
  int fd_transport,
  uint16_t req_id,
  uint32_t exit_code,
  unsigned char status)
{
  struct {
    RawHeader raw_header;
    EndRequestBody body;
  } reply;
  reply.raw_header.type = kTypeEnd;
  FlattenUint16(req_id,
                &reply.raw_header.request_id_b1,
                &reply.raw_header.request_id_b0);
  reply.body.protocol_status = status;
  reply.body.app_status_b3 =
    static_cast<unsigned char>((exit_code >> 24) & 0xff);
  reply.body.app_status_b2 =
    static_cast<unsigned char>((exit_code >> 16) & 0xff);
  reply.body.app_status_b1 =
    static_cast<unsigned char>((exit_code >> 8) & 0xff);
  reply.body.app_status_b0 = static_cast<unsigned char>(exit_code & 0xff);
  write(fd_transport, &reply, sizeof(reply));
}


bool FastCgi::ReplyStream(
  unsigned char type,
  const unsigned char *data,
  unsigned length)
{
  assert((type == kTypeStdout) || (type == kTypeStderr));

  RawHeader raw_header;
  raw_header.type = type;
  FlattenUint16(request_id_,
                &raw_header.request_id_b1, &raw_header.request_id_b0);
  if (length == 0) {
    return (write(fd_transport_, &raw_header, sizeof(raw_header)) ==
            sizeof(raw_header));
  }

  unsigned written = 0;
  while (written < length) {
    unsigned nbytes = std::min(length - written, kMaxContentLength);
    FlattenUint16(nbytes,
                  &raw_header.content_length_b1,
                  &raw_header.content_length_b0);
    int retval = write(fd_transport_, &raw_header, sizeof(raw_header));
    if (retval != sizeof(raw_header))
      return false;
    retval = write(fd_transport_, data + written, nbytes);
    if ((retval < 0) || (static_cast<unsigned>(retval) != nbytes))
      return false;
    written += nbytes;
  }
  return true;
}


void FastCgi::ReplyUnknownType(int fd_transport, unsigned char received_type) {
  struct {
    RawHeader raw_header;
    UnknownTypeBody body;
  } reply;
  reply.raw_header.type = kTypeUnknown;
  reply.body.type = received_type;
  write(fd_transport, &reply, sizeof(reply));
}


/**
 * The HTTP body.  Indicate the end of the stream with finish.
 */
bool FastCgi::SendData(const string &data, bool finish) {
  if (request_id_ == -1)
    return false;

  if (!ReplyStream(kTypeStdout, reinterpret_cast<const unsigned char *>(
                   data.data()), data.length()))
  {
    return false;
  }
  if (finish)
    return ReplyStream(kTypeStdout, NULL, 0);
  return true;
}


/**
 * What would normally be sent to stderr. Indicate the end of the stream with
 * finish.
 */
bool FastCgi::SendError(const string &data, bool finish) {
  if (request_id_ == -1)
    return false;

  if (!ReplyStream(kTypeStderr, reinterpret_cast<const unsigned char *>(
                   data.data()), data.length()))
  {
    return false;
  }
  if (finish)
    return ReplyStream(kTypeStderr, NULL, 0);
  return true;
}
