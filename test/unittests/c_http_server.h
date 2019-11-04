/**
 * This file is part of the CernVM File System.
 */

#ifndef TEST_UNITTESTS_C_HTTP_SERVER_H_
#define TEST_UNITTESTS_C_HTTP_SERVER_H_


#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "atomic.h"
#include "duplex_curl.h"
#include "util/posix.h"
#include "util/string.h"

typedef std::vector< std::pair<std::string, std::string> > HTTPHeaderList;

struct HTTPRequest {
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
    if (raw) return body;
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
  bool SetResponseCallback(
    HTTPResponse (*callback_func)(const HTTPRequest &, void*),
    void *data = NULL);

 protected:
  // Main function of the server.
  // Handles the entire communication in one event loop.
  static void *Main(void *data);

  atomic_int32 running_;
  atomic_int32 server_thread_ready_;
  int server_port_;

  void *callback_data_ = NULL;
  HTTPResponse (*callback_func_)(const HTTPRequest &, void*) = NULL;

  pthread_t server_thread_;
};

class MockFileServer {
 public:
  explicit MockFileServer(int port, std::string root_dir) {
    port_ = port;
    root_dir_ = root_dir;
    server_ = new MockHTTPServer(port);
    server_->SetResponseCallback(FileServerHandler, this);
    assert(server_->Start());
  }
  ~MockFileServer() {
    delete server_;
  }

 protected:
  static HTTPResponse FileServerHandler(const HTTPRequest &req, void *data) {
    MockFileServer *file_server = static_cast<MockFileServer *>(data);
    HTTPResponse response;
    if (req.method == "GET") {
      bool host_header = false;
      HeaderList::const_iterator it = req.headers.begin();
      HeaderList::const_iterator itend = req.headers.end();
      for (; it != itend; ++it) {
        if (it->first == "Host") {
          host_header = true;
          break;
        }
      }
      std::string local_path = file_server->root_dir_;
      if (host_header) {
        local_path += "/" + req.path;
      } else {
        local_path += "/" + req.path.substr(req.path.find("/") + 1);
      }
      if (FileExists(local_path)) {
        int fd = open(local_path.c_str(), O_RDONLY);
        assert(fd >= 0);
        SafeReadToString(fd, &response.body);
        close(fd);
      } else {
        response.code = 404;
        response.reason = "Not Found";
      }
    }
    return response;
  }
  std::string root_dir_;
  int port_;
  MockHTTPServer *server_;
};

class MockProxyServer {
 public:
  explicit MockProxyServer(int port) {
    port_ = port;
    server_ = new MockHTTPServer(port);
    server_->SetResponseCallback(ProxyServerHandler, this);
    assert(server_->Start());
  }
  ~MockProxyServer() {
    delete server_;
  }
 
 protected:
  static size_t ProxyServerWriteCallback(char *ptr, size_t size, size_t nmemb,
                                         void* userdata) {
    std::string *response = static_cast<std::string *>(userdata);
    (*response) += std::string(ptr, size*nmemb);
    return size*nmemb;
  }

  static HTTPResponse ProxyServerHandler(const HTTPRequest &req, void *data) {
    // MockProxyServer *proxy_server = static_cast<MockProxyServer *>(data);    
    HTTPResponse response;
    CURL* handle = curl_easy_init();
    curl_easy_setopt(handle, CURLOPT_HEADER, 1);
    curl_easy_setopt(handle, CURLOPT_URL, req.path.c_str());
    curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, req.method.c_str());

    std::string destination_response;
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, ProxyServerWriteCallback);
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, &destination_response);

    struct curl_slist *header_list = NULL;
    HeaderList::const_iterator it = req.headers.begin();
    HeaderList::const_iterator itend = req.headers.end();
    for (; it != itend; ++it) {
      std::string header = it->first + ": " + it->second;
      header_list = curl_slist_append(header_list, header.c_str());
    }
    curl_easy_setopt(handle, CURLOPT_HTTPHEADER, header_list);

    if (req.body.length()) {
      curl_easy_setopt(handle, CURLOPT_POSTFIELDS, req.body.c_str());
      curl_easy_setopt(handle, CURLOPT_POSTFIELDSIZE, req.body.length());
    }
    curl_easy_perform(handle);
    curl_easy_cleanup(handle);
    curl_slist_free_all(header_list);
    response.raw = true;
    response.body = destination_response;
    return response;
  }

  int port_;
  MockHTTPServer *server_;
};

class MockRedirectServer {
 public:
  explicit MockRedirectServer(int port, std::string redirect_destination) {
    port_ = port;
    server_ = new MockHTTPServer(port);
    redirect_destination_ = redirect_destination;
    server_->SetResponseCallback(RedirectServerHandler, this);
    assert(server_->Start());
  }
  ~MockRedirectServer() {
    delete server_;
  }

 protected:
  static HTTPResponse RedirectServerHandler(const HTTPRequest &req,
                                            void *data) {
    MockRedirectServer *redirect_server = static_cast<MockRedirectServer *>(data);    
    HTTPResponse response;
    response.code = 301;
    response.reason = "Moved Permanently";
    response.AddHeader("Location",
                       redirect_server->redirect_destination_ + req.path);
    return response;
  }

  int port_;
  std::string redirect_destination_;
  MockHTTPServer *server_;
};

#endif  // TEST_UNITTESTS_C_HTTP_SERVER_H_
