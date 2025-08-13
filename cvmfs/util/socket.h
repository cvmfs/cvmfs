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
#include <vector>

enum class ProcessType {
  Client,
  Server
};


template<ProcessType PT>
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
      for (auto socket : data_v_) {
        close(socket);
      }
    }
    unlink(name_.c_str());
  }

  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Server, int>::type = 0>
  LocalUnixSocket &accept() {
    int res = ::accept(socket_, NULL, NULL);
    if (res == -1) {
      perror("listen");
      exit(EXIT_FAILURE);
    }
    data_v_.emplace_back(res);
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

  template<typename ContiguousType, ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Server, int>::type = 0>
  std::vector<ContiguousType> read(size_t elements = 1,
                                   size_t socket_number = 0) const {
    return read_from_socket<ContiguousType>(elements, data_v_[socket_number]);
  }
  template<typename ContiguousType, ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Client, int>::type = 0>
  std::vector<ContiguousType> read(size_t elements = 1) const {
    return read_from_socket<ContiguousType>(elements, socket_);
  }

  template<typename ContiguousType, ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Server, int>::type = 0>
  const LocalUnixSocket &write(const ContiguousType &data,
                               size_t socket_number = 0) const {
    return write_to_socket<ContiguousType>(data_v_[socket_number], data);
  }
  template<typename ContiguousType, ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Client, int>::type = 0>
  const LocalUnixSocket &write(const ContiguousType &data) const {
    return write_to_socket<ContiguousType>(socket_, data);
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

  template<typename ContiguousType>
  std::vector<ContiguousType> read_from_socket(size_t elements,
                                               int socket) const {
    std::vector<ContiguousType> result;
    ContiguousType buffer;
    for (int i = 0; i < elements; ++i) {
      int res = ::read(socket, &buffer, sizeof(ContiguousType));
      if (res == -1) {
        perror("read");
        exit(EXIT_FAILURE);
      }
      result.emplace_back(buffer);
    }
    return result;
  }

  template<typename ContiguousType>
  const LocalUnixSocket &write_to_socket(int socket,
                                         const ContiguousType &data) const {
    int res = ::write(socket, &data, sizeof(ContiguousType));
    if (res == -1) {
      perror("write");
      exit(EXIT_FAILURE);
    }
    return *this;
  }

  LocalUnixSocketAddress addr_;
  std::string name_;
  int socket_ = -1;
  std::vector<int> data_v_;
};

#endif  // __LOCAL_UNIX_SOCKET_H_

