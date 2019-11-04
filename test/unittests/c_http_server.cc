/**
 * This file is part of the CernVM File System.
 */

#include "c_http_server.h"

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
  HeaderList::iterator it = request_.headers.begin();
  HeaderList::iterator itend = request_.headers.end();
  for (; it != itend; ++it) {
    if (it->first == "Content-Length") {
      request_.content_length = String2Uint64(it->second);
      return;
    }
  }
  request_.content_length = 0;
}

MockHTTPServer::MockHTTPServer(int port) {
  atomic_init32(&running_);
  atomic_init32(&server_thread_ready_);
  server_port_ = port;
}

MockHTTPServer::~MockHTTPServer() {
  if (atomic_read32(&running_)) {
    Stop();
  }
}

bool MockHTTPServer::Start() {
  if (atomic_read32(&running_)) return false;
  atomic_write32(&running_, 1);
  pthread_create(&server_thread_, NULL, Main, this);
  // wait for server thread to open the socket
  while (!atomic_read32(&server_thread_ready_)) {}
  return true;
}


bool MockHTTPServer::Stop() {
  if (!atomic_read32(&running_)) return false;
  atomic_write32(&running_, 0);
  pthread_join(server_thread_, NULL);
  return true;
}

bool MockHTTPServer::SetResponseCallback(
  HTTPResponse (*callback_func)(const HTTPRequest &, void*),
  void *data) {
  if (atomic_read32(&running_)) return false;
  callback_func_ = callback_func;
  callback_data_ = data;
  return true;
}

void *MockHTTPServer::Main(void *data) {
  MockHTTPServer *server = static_cast<MockHTTPServer *>(data);
  const int kReadBufferSize = 1000;
  int listen_sockfd = -1, accept_sockfd = -1;
  socklen_t clilen;
  struct sockaddr_in serv_addr, cli_addr;
  char buffer[kReadBufferSize];
  int retval = 0;

  // Listen incoming connections
  listen_sockfd = socket(AF_INET, SOCK_STREAM, 0);
  assert(listen_sockfd >= 0);
  bzero(reinterpret_cast<char *>(&serv_addr), sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(server->server_port_);
  int on = 1;
  retval = setsockopt(listen_sockfd, SOL_SOCKET,
                      SO_REUSEADDR, &on, sizeof(on));
  assert(retval == 0);
  retval = bind(listen_sockfd,
                (struct sockaddr *) &serv_addr,
                sizeof(serv_addr));
  assert(retval >= 0);
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
    retval = select(listen_sockfd+1, &rfds, NULL, NULL, &select_timeout);
    assert(retval >= 0);
    if (retval == 0)  // Timeout
      continue;

    accept_sockfd = accept(listen_sockfd,
                            (struct sockaddr *) &cli_addr,
                            &clilen);
    bzero(buffer, kReadBufferSize);
    int bytes_read = 0;
    HTTPRequestParser parser;
    bool finished = false;
    while (!finished &&
           (bytes_read = read(accept_sockfd, buffer, kReadBufferSize))) {
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
    assert((uint64_t) bytes_written == reply.length());
    close(accept_sockfd);
  }
  close(listen_sockfd);
  return NULL;
}
