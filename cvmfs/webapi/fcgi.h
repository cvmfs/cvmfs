/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_WEBAPI_FCGI_H_
#define CVMFS_WEBAPI_FCGI_H_

#include <netinet/in.h>
#include <stdint.h>

#include <cstring>
#include <map>
#include <string>

// TODO(jblomer): deal with termination signals

/**
 * Implements a simple FastCGI responder role.  This responder cannot multi-
 * plex connections.  See http://www.fastcgi.com/devkit/doc/fcgi-spec.html
 * It is supposed to be used in the following way.  Don't forget to sanitize
 * everything you get out from fcgi.
 *
 * unsigned char *buf;
 * unsigned length;
 * uint64_t id = 0;
 * FastCgi::Event event;
 * while ((event = fcgi.NextEvent(&buf, &length, &id)) != FastCgi::kEventExit) {
 *   switch (event) {
 *     case FastCgi::kEventAbortReq:
 *       fcgi.AbortRequest();
 *       break;
 *     case FastCgi::kEventStdin:
 *       // Use id to detect if this new input is part of the same request than
 *       // the previous one.
 *       // Process buf; if length == 0, the input stream is finished
 *       // For POST: compare CONTENT-LENGTH with actual stream length
 *       fcgi.GetParam("PARAMETER", ...) ...
 *       fcgi.SendData("...", false);
 *       fcgi.SendData("...", true);
 *       fcgi.SendError("...", true);
 *       fcgi.EndRequest(0);  // or failure status code
 *       break;
 *     default:
 *       abort();
 *   }
 * }
 */
class FastCgi {
 public:
  enum Event {
    kEventExit = 0,
    kEventTransportError,
    kEventAbortReq,
    kEventStdin,
  };

  FastCgi();
  ~FastCgi();
  bool IsFcgi();
  void AbortRequest();
  void EndRequest(uint32_t exit_code);
  bool SendData(const std::string &data, bool finish);
  bool SendError(const std::string &data, bool finish);
  void ReturnBadRequest(const std::string &reason);
  Event NextEvent(unsigned char **buf, unsigned *length, uint64_t *id);

  bool GetParam(const std::string &key, std::string *value);
  std::string DumpParams();

  bool MkTcpSocket(const std::string &ip4_address, uint16_t port);

 private:
  static const int kCgiListnsockFileno = 0;

  static const unsigned kMaxContentLength;

  /**
   * Value for version component of Header.
   */
  static const int kVersion1 = 1;

  /**
   * Value for requestId component of Header.
   */
  static const int kNullRequestId = 0;

  /**
   * Mask for flags component of BeginRequestBody.
   */
  static const int kKeepConn = 1;

  /**
   * Values the server might query from the app.
   */
  static const char *kValueMaxConns;
  static const char *kValueMaxReqs;
  static const char *kValueMpxConns;

  /**
   * Values for type component of Header.
   */
  enum RecordType {
    kTypeBegin = 1,           // Application record, WS --> App
    kTypeAbort = 2,           // Application record, WS --> App
    kTypeEnd = 3,             // Application record, App --> WS
    kTypeParams = 4,          // Application record, WS --> App
    kTypeStdin = 5,           // Application stream record, WS --> App
    kTypeStdout = 6,          // Application stream record, App --> WS
    kTypeStderr = 7,          // Application stream record, App --> WS
    kTypeData = 8,            // Application stream record, WS --> App
    kTypeValues = 9,          // Management record, WS --> App
    kTypeValuesResult = 10,   // Management record, App --> WS
    kTypeUnknown = 11,        // Management record, App --> WS
  };

  /**
   * Values for role component of BeginRequestBody.
   */
  enum Role {
    kRoleResponder = 1,
    kRoleAuthorizer = 2,  // unavailable in this implementation
    kRoleFilter = 3,  // unavailable in this implementation
  };

  /**
   * Values for protocolStatus component of EndRequestBody.
   */
  enum ProtocolStatus {
    kStatusReqComplete = 0,
    kStatusCantMpxConn = 1,
    kStatusOverloaded = 2,  // unused in this implementation
    kStatusUnknownRole = 3,
  };

  struct RawHeader {
    RawHeader()
      : version(kVersion1), type(0), request_id_b1(0), request_id_b0(0)
      , content_length_b1(0), content_length_b0(0), padding_length(0)
      , reserved(0)
    { }
    unsigned char version;
    unsigned char type;
    unsigned char request_id_b1;
    unsigned char request_id_b0;
    unsigned char content_length_b1;
    unsigned char content_length_b0;
    unsigned char padding_length;
    unsigned char reserved;
  };

  struct Header {
    unsigned char type;
    unsigned char padding_length;
    uint16_t request_id;
    uint16_t content_length;
  };

  struct BeginRequestBody {
    unsigned char role_b1;
    unsigned char role_b0;
    unsigned char flags;
    unsigned char reserved[5];
  };

  struct EndRequestBody {
    EndRequestBody()
      : app_status_b3(0), app_status_b2(0), app_status_b1(0), app_status_b0(0)
      , protocol_status(0)
    {
      memset(reserved, 0, 3);
    }
    unsigned char app_status_b3;
    unsigned char app_status_b2;
    unsigned char app_status_b1;
    unsigned char app_status_b0;
    unsigned char protocol_status;
    unsigned char reserved[3];
  };

  struct UnknownTypeBody {
    UnknownTypeBody() : type(0) {
      memset(reserved, 0, 7);
    }
    unsigned char type;
    unsigned char reserved[7];
  };

  bool CheckValidSource(const struct sockaddr_in &addr_in);
  void CloseConnection();

  bool ReadHeader(int fd_transport, Header *header);
  bool ReadContent(uint16_t content_length, unsigned char padding_length);
  bool ReadBeginBody(int fd_transport, uint16_t *role, bool *keep_connection);
  bool ReadParams(const Header &first_header);

  bool ParseKvPair(const char *data, unsigned len,
                   unsigned *nparsed, std::string *key, std::string *value);

  void ReplyUnknownType(int fd_transport, unsigned char received_type);
  void ReplyEndRequest(int fd_transport, uint16_t req_id,
                       uint32_t exit_code, unsigned char status);
  bool ReplyStream(unsigned char type,
                   const unsigned char *data, unsigned length);

  bool ProcessValues(const Header &request_header);

  inline uint16_t MkUint16(const unsigned char b1, const unsigned char b0) {
    return (static_cast<uint16_t>(b1) << 8) + static_cast<uint16_t>(b0);
  }

  inline void FlattenUint16(uint16_t val, unsigned char *b1, unsigned char *b0)
  {
    *b1 = static_cast<unsigned char>((val >> 8) & 0xff);
    *b0 = static_cast<unsigned char>(val & 0xff);
  }

  unsigned AddShortKv(const std::string &key, const std::string &value,
                      unsigned buf_size, unsigned char *buf);

  /**
   * kCgiListnsockFileno for fcgi processes spawned by the server, otherwise
   * filled by MkTcpSocket().
   */
  int fd_sock_;
  bool is_tcp_socket_;

  unsigned char content_buf_[64 * 1024 + 255];
  int fd_transport_;

  /**
   * Returned by NextEvent().  Has a different value for every request.  Never
   * zero, so the application can initialize its id state to zero and detect
   * if a stdin event is for the same request or a new one.
   */
  uint64_t global_request_id_;

  int request_id_;
  bool keep_connection_;
  std::map<std::string, std::string> params_;
};

#endif  // CVMFS_WEBAPI_FCGI_H_
