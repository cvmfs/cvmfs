#ifndef __LOCAL_UNIX_SOCKET_H_
#define __LOCAL_UNIX_SOCKET_H_

#include <asm/termbits.h>  // FIONREAD: examine if there are data in socket
#include <crypto/hash.h>
#include <errno.h>
#include <quota.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/ioctl.h>  // ioctl: examine if there are data in socket
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

#include "util/logging.h"

enum class ProcessType {
  Client,
  Server
};

namespace util {
enum class Command {
  SendHashes,
  RecvHashes
};
};  // namespace util

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
      LogCvmfs(kLogCvmfs, kLogDebug, "creating socket %s failed (%d)",
               name_.c_str(), errno);
      exit(EXIT_FAILURE);
    }
    int res = bind(socket_, (const struct sockaddr *)&addr_.get(),
                   sizeof(struct sockaddr_un));
    if (res == -1) {
      LogCvmfs(kLogCvmfs, kLogDebug, "binding to socket %s failed (%d)",
               name_.c_str(), errno);
      exit(EXIT_FAILURE);
    }

    res = listen(socket_, 20);
    if (res == -1) {
      LogCvmfs(kLogCvmfs, kLogDebug, "listening to socket %s failed (%d)",
               name_.c_str(), errno);
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
      LogCvmfs(kLogCvmfs, kLogDebug, "creating socket %s failed (%d)",
               name_.c_str(), errno);
      exit(EXIT_FAILURE);
    }
  }

  // Disable any copy or moving
  LocalUnixSocket(const LocalUnixSocket &) = delete;
  LocalUnixSocket &operator=(const LocalUnixSocket &) = delete;
  LocalUnixSocket(LocalUnixSocket &&) = delete;
  LocalUnixSocket &operator=(LocalUnixSocket &&) = delete;

  virtual ~LocalUnixSocket() {
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
      LogCvmfs(kLogCvmfs, kLogDebug,
               "accepting connection with socket %s failed (%d)", name_.c_str(),
               errno);
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
      LogCvmfs(kLogCvmfs, kLogDebug,
               "connecting to server with socket %s failed (%d)", name_.c_str(),
               errno);
      exit(EXIT_FAILURE);
    }
    return *this;
  }

  /*
   * try_read() will only attempt to read when there are SOME data inside the
   * socket. More precisely, when multiple data are asked, it will read when
   * there is at least one instance available. After that the call to read will
   * be blocking.
   */
  template<typename ContiguousType, ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Server, int>::type = 0>
  std::vector<ContiguousType> try_read(size_t elements = 1,
                                       size_t socket_number = 0) const {
    return try_read_from_socket<ContiguousType>(elements,
                                                data_v_[socket_number]);
  }
  template<typename ContiguousType, ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Client, int>::type = 0>
  std::vector<ContiguousType> try_read(size_t elements = 1) const {
    return try_read_from_socket<ContiguousType>(elements, socket_);
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

  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Server, int>::type = 0>
  [[__nodiscard__]] std::size_t nclients() const {
    return data_v_.size();
  }

 protected:
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
  std::vector<ContiguousType> try_read_from_socket(size_t elements,
                                                   int socket) const {
    std::vector<ContiguousType> result{};

    int bytes = 0;
    if (ioctl(socket, FIONREAD, &bytes) == -1 or bytes <= 0
        or static_cast<std::size_t>(bytes) / sizeof(ContiguousType) == 0) {
      /*
       * If there are not enough data for one ContiguousType word
       *  RETURN empty
       */
      return result;
    }

    return read_from_socket<ContiguousType>(elements, socket);
  }

  template<typename ContiguousType>
  std::vector<ContiguousType> read_from_socket(size_t elements,
                                               int socket) const {
    std::vector<ContiguousType> result;
    ContiguousType buffer;
    for (int i = 0; i < elements; ++i) {
      int res = ::read(socket, &buffer, sizeof(ContiguousType));
      if (res == -1) {
        LogCvmfs(kLogCvmfs, kLogDebug, "reading from socket %s failed (%d)",
                 name_.c_str(), errno);
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
      LogCvmfs(kLogCvmfs, kLogDebug, "writing to socket %s failed (%d)",
               name_.c_str(), errno);
      exit(EXIT_FAILURE);
    }
    return *this;
  }

  LocalUnixSocketAddress addr_;
  std::string name_;
  int socket_ = -1;
  std::vector<int> data_v_;
};

class CacheManagerSocket : public LocalUnixSocket<ProcessType::Client> {
 public:
  CacheManagerSocket(const char *socket_name)
      : LocalUnixSocket<ProcessType::Client>(socket_name) { }

  void send_hashes(size_t socket_number = 0) { }
};

class QuotaManagerSocket : public LocalUnixSocket<ProcessType::Server> {
 public:
  QuotaManagerSocket(const char *socket_name)
      : LocalUnixSocket<ProcessType::Server>::LocalUnixSocket(socket_name) { }

  /*
   * TODO(christge) Points to consider:
   * 1. One a more mature version LocalUnixSocket should have methods send() for
   * the Client and try_recv() for the server. The real bottleneck here is that
   * we don't have std::optional here yet, so we can't say if a client isn't
   * responsive or doesn't have any hashes to send.
   * 2. Maybe collect<ContiguousType> should be a method of a
   * LocalUnixSocket<ProcessType::Server> and the override here should be a
   * specialized: collect<shash::Any>
   */
  std::set<shash::Any> collect_hashes() { return collect<shash::Any>(); }

  template<typename ContiguousType>
  std::set<ContiguousType> collect() {
    std::set<ContiguousType> result{};
    size_t nclients = data_v_.size();

    // ask every CM socket for the hashes
    for (size_t i = 0; i < nclients; ++i) {
      write(util::Command::SendHashes, i);
    }

    auto still_missing = [] [[__nodiscard__]] (const std::vector<bool> vec) {
      bool res = false;
      for (const auto &elem : vec) {
        if (!elem) {
          res = true;
          break;
        }
      }
      return res;
    };

    auto try_collect = [this, &result](int socket_number) -> bool {
      auto res = try_read<util::Command>(1, socket_number);
      if (res.size()) {
        if (res[0] == util::Command::RecvHashes) {
          auto ndata = read<size_t>(1, socket_number);
          auto data = read<ContiguousType>(ndata[0], socket_number);
          for (auto elem : data) {
            result.insert(elem);
          }
          return true;
        }
      }
      return false;
    };

    // TODO(gchr): this won't abort until the last CM communicates its hashes,
    // which is not what we want. After a couple of rounds, the QM should
    // proceed with the cleanup with what's available
    std::vector<bool> collected(nclients, false);

    while (still_missing(collected)) {
      for (size_t i = 0; i < nclients; ++i) {
        if (not collected[i]) {
          collected[i] = try_collect(i);
        }
      }
    }
    return result;
  }
};

#endif  // __LOCAL_UNIX_SOCKET_H_

