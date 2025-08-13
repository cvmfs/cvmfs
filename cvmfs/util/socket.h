#ifndef __LOCAL_UNIX_SOCKET_H_
#define __LOCAL_UNIX_SOCKET_H_

#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <string>
#include <type_traits>

enum class ProcessType {
  Client,
  Server
};

template<size_t BufferSize, ProcessType PT,
         bool (*TerminationCriterion)(const std::string &),
         std::string &(*ResultPolisher)(std::string &)>
class LocalUnixSocket {
 public:
  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Server, int>::type = 0>
  explicit LocalUnixSocket(const char *name)
      : socket_{socket(AF_UNIX, SOCK_STREAM, 0)}
      , addr_(LocalUnixSocketAddress(name))
      , name_{name} {
    if (socket_ == -1) {
      perror("socket");
      exit(EXIT_FAILURE);
    }
    int res = bind(socket_, (const struct sockaddr *)&addr_.get(),
                   sizeof(struct sockaddr_un));
    if (res == -1) {
      perror("bind");
      exit(EXIT_FAILURE);
    }

    res = listen(socket_, 20);
    if (res == -1) {
      perror("listen");
      exit(EXIT_FAILURE);
    }
  }

  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Client, int>::type = 0>
  explicit LocalUnixSocket(const char *name)
      : socket_{socket(AF_UNIX, SOCK_STREAM, 0)}
      , addr_(LocalUnixSocketAddress(name))
      , name_{name} {
    if (socket_ == -1) {
      perror("socket");
      exit(EXIT_FAILURE);
    }
  }

  ~LocalUnixSocket() {
    close(socket_);
    if constexpr (PT == ProcessType::Server) {
      close(data_);
    }
    unlink(name_.c_str());
  }

  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Server, int>::type = 0>
  LocalUnixSocket &accept() {
    data_ = ::accept(socket_, NULL, NULL);
    if (data_ == -1) {
      perror("listen");
      exit(EXIT_FAILURE);
    }
    return *this;
  }

  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Client, int>::type = 0>
  LocalUnixSocket &connect() {
    int res = ::connect(socket_, (const struct sockaddr *)&addr_.get(),
                        sizeof(struct sockaddr_un));
    if (res == -1) {
      perror("connect");
      exit(EXIT_FAILURE);
    }
    return *this;
  }

  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Server, int>::type = 0>
  std::string read() {
    return read_from_socket(data_);
  }
  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Client, int>::type = 0>
  std::string read() {
    return read_from_socket(socket_);
  }

  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Server, int>::type = 0>
  LocalUnixSocket &write(const std::string &data) {
    return write_to_socket(data_, data);
  }
  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Client, int>::type = 0>
  LocalUnixSocket &write(const std::string &data) {
    return write_to_socket(socket_, data);
  }

 private:
  class LocalUnixSocketAddress {
   public:
    explicit LocalUnixSocketAddress(const char *name) {
      memset(&addr_, 0, sizeof(addr_));
      addr_.sun_family = AF_UNIX;
      strncpy(addr_.sun_path, name, sizeof(addr_.sun_path) - 1);
    }

    struct sockaddr_un &get() { return addr_; }
    const struct sockaddr_un &get() const { return addr_; }

   private:
    struct sockaddr_un addr_;
  };

  std::string read_from_socket(int socket) {
    std::string result;
    static char buffer[BufferSize + 1];
    buffer[BufferSize - 1] = '\0';

    do {
      int res = ::read(socket, buffer, BufferSize);
      if (res == -1) {
        perror("read");
        exit(EXIT_FAILURE);
      }
      buffer[res] = '\0';
      result += buffer;
    } while (!TerminationCriterion(result));
    return ResultPolisher(result);
  }

  LocalUnixSocket &write_to_socket(int socket, const std::string &data) {
    int res = ::write(socket, data.c_str(), data.size());
    if (res == -1) {
      perror("write");
      exit(EXIT_FAILURE);
    }
    return *this;
  }

  LocalUnixSocketAddress addr_;
  std::string name_;
  int socket_ = -1;
  int data_ = -1;
};

#endif  // __LOCAL_UNIX_SOCKET_H_

