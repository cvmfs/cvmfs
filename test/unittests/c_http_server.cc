/**
 * This file is part of the CernVM File System.
 */

#include "c_http_server.h"

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

#include "duplex_curl.h"
#include "util/posix.h"

HTTPRequestParser::HTTPRequestParser() {
  state_ = kBegin;
  body_bytes_read_ = 0;
}

bool HTTPRequestParser::Parse(char c) {
  switch (state_) {
    case kBegin:
      buffer_ += c;
      state_ = kMethod;
      break;
    case kMethod:
      if (c == ' ') {
        request_.method = buffer_;
        buffer_.clear();
        state_ = kPath;
      } else {
        buffer_ += c;
      }
      break;
    case kPath:
      if (c == ' ') {
        request_.path = buffer_;
        buffer_.clear();
        state_ = kProtocol;
      } else {
        buffer_ += c;
      }
      break;
    case kProtocol:
      if (c == '\r') {
        request_.protocol = buffer_;
        buffer_.clear();
        state_ = kCRFirst;
      } else if (c == '\n') {
        request_.protocol = buffer_;
        buffer_.clear();
        state_ = kLFFirst;
      } else {
        buffer_ += c;
      }
      break;
    case kHeaderKey:
      if (c == ':') {
        state_ = kSpacePreHeaderVal;
      } else {
        headerKeyBuffer_ += c;
      }
      break;
    case kSpacePreHeaderVal:
      assert(c == ' ');
      state_ = kHeaderVal;
      break;
    case kHeaderVal:
      if (c == '\n') {
        PushHeaderField();
        state_ = kLFFirst;
      } else if (c == '\r') {
        PushHeaderField();
        state_ = kCRFirst;
      } else {
        buffer_ += c;
      }
      break;
    case kCRFirst:
      assert(c == '\n');
      state_ = kCRLFFirst;
      break;
    case kCRLFFirst:
      assert(c != '\n');
      if (c == '\r') {
        state_ = kCRSecond;
      } else {
        headerKeyBuffer_ += c;
        state_ = kHeaderKey;
      }
      break;
    case kLFFirst:
      assert(c != '\r');
      if (c == '\n') {
        FillContentLength();
        if (request_.content_length) {
          state_ = kBody;
        } else {
          state_ = kFinished;
          return true;
        }
      } else {
        headerKeyBuffer_ += c;
        state_ = kHeaderKey;
      }
      break;
    case kCRSecond:
      assert(c == '\n');
      FillContentLength();
      if (request_.content_length) {
        state_ = kBody;
      } else {
        state_ = kFinished;
        return true;
      }
      break;
    case kBody:
      if (body_bytes_read_ < request_.content_length) {
        buffer_ += c;
        body_bytes_read_++;
      }
      if (body_bytes_read_ == request_.content_length) {
        request_.body = buffer_;
        state_ = kFinished;
        return true;
      }
      break;
    case kFinished:
      assert(false);  // Parse() should not be called after parsing is finished
      break;
  }
  return false;
}

const HTTPRequest &HTTPRequestParser::GetParsedRequest() const {
  assert(state_ == kFinished);
  return request_;
}

void HTTPRequestParser::PushHeaderField() {
  request_.headers.push_back(
      std::pair<std::string, std::string>(headerKeyBuffer_, buffer_));
  headerKeyBuffer_.clear();
  buffer_.clear();
}

void HTTPRequestParser::FillContentLength() {
  HTTPHeaderList::iterator it = request_.headers.begin();
  HTTPHeaderList::iterator itend = request_.headers.end();
  for (; it != itend; ++it) {
    if (it->first == "Content-Length") {
      request_.content_length = String2Uint64(it->second);
      return;
    }
  }
  request_.content_length = 0;
}


//------------------------------------------------------------------------------


MockHTTPServer::MockHTTPServer(int port) {
  atomic_init32(&running_);
  atomic_init32(&server_thread_ready_);
  server_port_ = port;
  callback_func_ = NULL;
  callback_data_ = NULL;
}

MockHTTPServer::~MockHTTPServer() {
  if (atomic_read32(&running_)) {
    Stop();
  }
}

bool MockHTTPServer::Start() {
  if (atomic_read32(&running_))
    return false;
  atomic_write32(&running_, 1);
  pthread_create(&server_thread_, NULL, Main, this);
  // wait for server thread to open the socket
  while (!atomic_read32(&server_thread_ready_)) {
  }
  return true;
}


bool MockHTTPServer::Stop() {
  if (!atomic_read32(&running_))
    return false;
  atomic_write32(&running_, 0);
  pthread_join(server_thread_, NULL);
  return true;
}

bool MockHTTPServer::SetResponseCallback(
    HTTPResponse (*callback_func)(const HTTPRequest &, void *), void *data) {
  if (atomic_read32(&running_))
    return false;
  callback_func_ = callback_func;
  callback_data_ = data;
  return true;
}

void *MockHTTPServer::Main(void *data) {
  MockHTTPServer *server = static_cast<MockHTTPServer *>(data);
  const int kReadBufferSize = 1000;
  int listen_sockfd = -1, accept_sockfd = -1;
  socklen_t clilen;
  struct sockaddr_in cli_addr;
  char buffer[kReadBufferSize];
  int retval = 0;

  // Listen incoming connections
  listen_sockfd = MakeTcpEndpoint("", server->server_port_);
  assert(listen_sockfd >= 0);
  listen(listen_sockfd, 5);

  clilen = sizeof(cli_addr);

  struct timeval select_timeout;
  select_timeout.tv_sec = 0;
  select_timeout.tv_usec = 2000;  // 2 ms
  fd_set rfds;
  atomic_inc32(&(server->server_thread_ready_));
  while (atomic_read32(&(server->running_))) {
    // Wait for traffic
    FD_ZERO(&rfds);
    FD_SET(listen_sockfd, &rfds);
    retval = select(listen_sockfd + 1, &rfds, NULL, NULL, &select_timeout);
    assert(retval >= 0);
    if (retval == 0)  // Timeout
      continue;

    accept_sockfd = accept(
        listen_sockfd, (struct sockaddr *)&cli_addr, &clilen);
    bzero(buffer, kReadBufferSize);
    int bytes_read = 0;
    HTTPRequestParser parser;
    bool finished = false;
    while (!finished
           && (bytes_read = read(accept_sockfd, buffer, kReadBufferSize))) {
      for (int i = 0; i < bytes_read; ++i) {
        if (parser.Parse(buffer[i])) {
          finished = true;
          break;
        }
      }
    }
    const HTTPRequest &req = parser.GetParsedRequest();
    assert(server->callback_func_ != NULL);
    HTTPResponse response = server->callback_func_(req, server->callback_data_);
    // Mandatory since the connection is closed after replying
    response.AddHeader("Connection", "close");
    std::string reply = response.ToString();
    int bytes_written = write(accept_sockfd, reply.c_str(), reply.length());
    assert(bytes_written >= 0);
    assert((uint64_t)bytes_written == reply.length());
    close(accept_sockfd);
  }
  close(listen_sockfd);
  return NULL;
}


//------------------------------------------------------------------------------


MockFileServer::MockFileServer(int port, std::string root_dir) {
  port_ = port;
  root_dir_ = root_dir;
  num_processed_requests_ = 0;
  server_ = new MockHTTPServer(port);
  server_->SetResponseCallback(FileServerHandler, this);
  assert(server_->Start());
}

MockFileServer::~MockFileServer() { delete server_; }

HTTPResponse MockFileServer::FileServerHandler(const HTTPRequest &req,
                                               void *data) {
  MockFileServer *file_server = static_cast<MockFileServer *>(data);
  HTTPResponse response;
  if (req.method == "GET") {
    bool host_header = false;
    HTTPHeaderList::const_iterator it = req.headers.begin();
    HTTPHeaderList::const_iterator itend = req.headers.end();
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
  ++file_server->num_processed_requests_;
  return response;
}


//------------------------------------------------------------------------------


MockProxyServer::MockProxyServer(int port) {
  port_ = port;
  num_processed_requests_ = 0;
  server_ = new MockHTTPServer(port);
  server_->SetResponseCallback(ProxyServerHandler, this);
  assert(server_->Start());
}

MockProxyServer::~MockProxyServer() { delete server_; }

size_t MockProxyServer::ProxyServerWriteCallback(char *ptr, size_t size,
                                                 size_t nmemb, void *userdata) {
  std::string *response = static_cast<std::string *>(userdata);
  (*response) += std::string(ptr, size * nmemb);
  return size * nmemb;
}

HTTPResponse MockProxyServer::ProxyServerHandler(const HTTPRequest &req,
                                                 void *data) {
  MockProxyServer *proxy_server = static_cast<MockProxyServer *>(data);
  HTTPResponse response;
  CURL *handle = curl_easy_init();
  curl_easy_setopt(handle, CURLOPT_HEADER, 1);
  curl_easy_setopt(handle, CURLOPT_URL, req.path.c_str());
  curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, req.method.c_str());

  std::string destination_response;
  curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, ProxyServerWriteCallback);
  curl_easy_setopt(handle, CURLOPT_WRITEDATA, &destination_response);

  struct curl_slist *header_list = NULL;
  HTTPHeaderList::const_iterator it = req.headers.begin();
  HTTPHeaderList::const_iterator itend = req.headers.end();
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
  ++proxy_server->num_processed_requests_;
  return response;
}


//------------------------------------------------------------------------------


MockRedirectServer::MockRedirectServer(int port,
                                       std::string redirect_destination) {
  port_ = port;
  server_ = new MockHTTPServer(port);
  redirect_destination_ = redirect_destination;
  num_processed_requests_ = 0;
  server_->SetResponseCallback(RedirectServerHandler, this);
  assert(server_->Start());
}

MockRedirectServer::~MockRedirectServer() { delete server_; }

HTTPResponse MockRedirectServer::RedirectServerHandler(const HTTPRequest &req,
                                                       void *data) {
  MockRedirectServer *redirect_server = static_cast<MockRedirectServer *>(data);
  HTTPResponse response;
  response.code = 301;
  response.reason = "Moved Permanently";
  response.AddHeader("Location",
                     redirect_server->redirect_destination_ + req.path);
  ++redirect_server->num_processed_requests_;
  return response;
}


//------------------------------------------------------------------------------


MockGateway::MockGateway(int port) {
  server_ = new MockHTTPServer(port);
  server_->SetResponseCallback(GatewayHandler, this);
  assert(server_->Start());
}

MockGateway::~MockGateway() { delete server_; }

HTTPResponse MockGateway::GatewayHandler(const HTTPRequest &req, void *data) {
  MockGateway *gateway = static_cast<MockGateway *>(data);
  return gateway->next_response_;
}
