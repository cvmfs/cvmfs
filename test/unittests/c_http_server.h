/**
 * This file is part of the CernVM File System.
 */

#ifndef TEST_UNITTESTS_C_HTTP_SERVER_H_
#define TEST_UNITTESTS_C_HTTP_SERVER_H_

#include <pthread.h>

#include <cassert>
#include <string>
#include <utility>
#include <vector>

#include "util/atomic.h"
#include "util/string.h"

typedef std::vector<std::pair<std::string, std::string> > HTTPHeaderList;

struct HTTPRequest {
  HTTPRequest() { content_length = 0; }

  std::string ToString() const {
    std::string result;
    result += method + " " + path + " " + protocol + "\r\n";
    HTTPHeaderList::const_iterator it = headers.begin();
    HTTPHeaderList::const_iterator itend = headers.end();
    for (; it != itend; ++it) {
      result += it->first + ": " + it->second + "\r\n";
    }
    result += "\r\n";
    result += body;
    return result;
  }

  std::string method;
  std::string path;
  std::string protocol;
  uint64_t content_length;
  HTTPHeaderList headers;
  std::string body;
};

struct HTTPResponse {
  HTTPResponse() {
    protocol = "HTTP/1.1";
    code = 200;
    reason = "OK";
    raw = false;
  }

  std::string ToString() const {
    if (raw)
      return body;
    std::string result;
    result += protocol + " " + StringifyInt(code) + " " + reason + "\r\n";
    HTTPHeaderList::const_iterator it = headers.begin();
    HTTPHeaderList::const_iterator itend = headers.end();
    for (; it != itend; ++it) {
      result += it->first + ": " + it->second + "\r\n";
    }
    if (body.length())
      result += "Content-Length: " + StringifyUint(body.length()) + "\r\n";
    result += "\r\n";
    result += body;
    return result;
  }

  void AddHeader(std::string key, std::string value) {
    headers.push_back(std::pair<std::string, std::string>(key, value));
  }

  std::string protocol;
  int code;
  std::string reason;
  HTTPHeaderList headers;
  std::string body;

  // If true, the whole request is stored in body and other variables
  // will not be used in the reply
  bool raw;
};

// Class for parsing HTTP request. Create one instance per one request.
// Works in a state-machine-like manner. Function Parse() parses characters
// one by one, changing the state of the parser and gradually filling the
// HTTPResponse object when needed.
class HTTPRequestParser {
 public:
  HTTPRequestParser();
  // Parses one character of the HTTP request.
  // Call this function repeatedly, providing single characters
  // in a sequential order.
  // Returns true if parsing is finished,
  // false if more characters need to be parsed.
  bool Parse(char c);
  // Returns complete HTTPResponse object. Can be called
  // only after parsing has been finished.
  const HTTPRequest &GetParsedRequest() const;

 protected:
  void PushHeaderField();
  void FillContentLength();

  enum States {
    kBegin = 0,
    kMethod,
    kPath,
    kProtocol,
    kHeaderKey,
    kSpacePreHeaderVal,
    kHeaderVal,
    kCRFirst,
    kCRLFFirst,
    kCRSecond,
    kCRLFSecond,
    kLFFirst,
    kLFSecond,
    kBody,
    kFinished
  };

  int state_;
  std::string buffer_;
  std::string headerKeyBuffer_;
  uint64_t body_bytes_read_;
  HTTPRequest request_;
};


// A class providing HTTP server running on localhost
class MockHTTPServer {
 public:
  explicit MockHTTPServer(int port);
  ~MockHTTPServer();
  // Start function spawns the main thread. A custom response callback needs
  // to be set by SetResponseCallback before starting the HTTP server.
  bool Start();
  bool Stop();
  // Set a callback function which implements specific behavior of the server.
  // The function needs to accept an HTTPRequest object and a pointer to
  // custom persistent data, which can be used by the function to remember
  // the state. The function needs to return an HTTPResponse object, which
  // is then used by the server to respond to given HTTP request.
  bool SetResponseCallback(HTTPResponse (*callback_func)(const HTTPRequest &,
                                                         void *),
                           void *data = NULL);

 protected:
  // Main function of the server.
  // Handles the entire communication in one event loop.
  static void *Main(void *data);

  atomic_int32 running_;
  atomic_int32 server_thread_ready_;
  int server_port_;

  void *callback_data_;
  HTTPResponse (*callback_func_)(const HTTPRequest &, void *);

  pthread_t server_thread_;
};

class MockFileServer {
 public:
  explicit MockFileServer(int port, std::string root_dir);
  ~MockFileServer();
  int num_processed_requests() { return num_processed_requests_; }

 protected:
  static HTTPResponse FileServerHandler(const HTTPRequest &req, void *data);

  std::string root_dir_;
  int port_;
  MockHTTPServer *server_;
  int num_processed_requests_;
};

class MockProxyServer {
 public:
  explicit MockProxyServer(int port);
  ~MockProxyServer();
  int num_processed_requests() { return num_processed_requests_; }

 protected:
  static size_t ProxyServerWriteCallback(char *ptr, size_t size, size_t nmemb,
                                         void *userdata);
  static HTTPResponse ProxyServerHandler(const HTTPRequest &req, void *data);

  int port_;
  MockHTTPServer *server_;
  int num_processed_requests_;
};

class MockRedirectServer {
 public:
  explicit MockRedirectServer(int port, std::string redirect_destination);
  ~MockRedirectServer();
  int num_processed_requests() { return num_processed_requests_; }

 protected:
  static HTTPResponse RedirectServerHandler(const HTTPRequest &req, void *data);

  int port_;
  std::string redirect_destination_;
  MockHTTPServer *server_;
  int num_processed_requests_;
};

class MockGateway {
 public:
  explicit MockGateway(int port);
  ~MockGateway();

  HTTPResponse next_response_;

 private:
  static HTTPResponse GatewayHandler(const HTTPRequest &req, void *data);

  MockHTTPServer *server_;
};

#endif  // TEST_UNITTESTS_C_HTTP_SERVER_H_
