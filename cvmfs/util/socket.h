#ifndef __LOCAL_UNIX_SOCKET_H_
#define __LOCAL_UNIX_SOCKET_H_

#include <asm/termbits.h>  // FIONREAD: examine if there are data in socket
#include <errno.h>
#include <fcntl.h>      // fcntl: examine if there are pending connect requests
#include <sys/ioctl.h>  // ioctl: examine if there are data in socket
#include <sys/socket.h>
#include <sys/un.h>  // sizeof
#include <unistd.h>  // unlink, sleep

#include <set>
#include <string>
#include <vector>

#include "crypto/hash.h"   // shash::Any
#include "smallhash.h"     // SmallHashDynamic
#include "util/logging.h"  // LogCvmfs
#include "util/posix.h"    // MakeSocket

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


/*
 *  Design decisions
 *  1. This container will only work iff the socket name is within the OS limits
 *  2. A Linux socket construction, binding, listening, connection
 * request/acceptance are considered core socket functionalities. If any of
 * those fails, the container will become invalid. It is entirely up to the user
 * to handle failure. read/write are not considered core functionalities and the
 * user will find out about their failure via their side effects
 */
template<ProcessType PT>
class LocalUnixSocket {
 public:
  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Server, int>::type = 0>
  explicit LocalUnixSocket(const char *name, const int mode = 0777,
                           const bool auto_listen = true)
      : socket_{socket(AF_UNIX, SOCK_STREAM, 0)}
      , addr_(LocalUnixSocketAddress(name))
      , name_{name} {
    std::string path{name};
    if (socket_ == -1) {
      LogCvmfs(kLogCvmfs, kLogDebug, "creating socket %s failed (%d)",
               name_.c_str(), errno);
      is_valid_ = false;
      return;
    }
#ifndef __APPLE__
    // fchmod on a socket is not allowed under Mac OS X
    // using default 0770 here
    if (fchmod(socket_, mode) != 0) {
      is_valid_ = false;
      return;
    }
#endif
    int res = bind(socket_,
                   reinterpret_cast<const struct sockaddr *>(&addr_.get()),
                   sizeof(struct sockaddr_un));
    if (res == -1) {
      if ((errno == EADDRINUSE) && (unlink(path.c_str()) == 0)) {
        res = bind(socket_,
                   reinterpret_cast<const struct sockaddr *>(&addr_.get()),
                   sizeof(struct sockaddr_un));
        if (res == -1) {
          LogCvmfs(kLogCvmfs, kLogDebug, "binding to socket %s failed (%d)",
                   name_.c_str(), errno);
          is_valid_ = false;
          return;
        }

      } else {
        LogCvmfs(kLogCvmfs, kLogDebug, "binding to socket %s failed (%d)",
                 name_.c_str(), errno);
        is_valid_ = false;
        return;
      }
    }
    if (auto_listen) {
      res = listen(socket_, 20);
      if (res == -1) {
        LogCvmfs(kLogCvmfs, kLogDebug, "listening to socket %s failed (%d)",
                 name_.c_str(), errno);
        is_valid_ = false;
        return;
      }
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
      is_valid_ = false;
      return;
    }
  }

  // Disable any copy or moving
  LocalUnixSocket(const LocalUnixSocket &) = delete;
  LocalUnixSocket &operator=(const LocalUnixSocket &) = delete;
  LocalUnixSocket(LocalUnixSocket &&) = delete;
  LocalUnixSocket &operator=(LocalUnixSocket &&) = delete;

  virtual ~LocalUnixSocket() {
    if constexpr (PT == ProcessType::Server) {
      for (auto socket : data_v_) {
        if (socket != -1) {
          close(socket);
        }
      }
      unlink(name_.c_str());
    }
    if (socket_ != -1) {
      close(socket_);
    }
  }

  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Server, int>::type = 0>
  bool accept() {
    int res = ::accept(socket_, NULL, NULL);
    return handle_accept(res);
  }

  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Server, int>::type = 0>
  bool try_accept() {
    int socket_flags = fcntl(socket_, F_GETFL, 0);
    if (socket_flags == -1) {
      return false;
    }

    if (!(socket_flags & O_NONBLOCK)) {
      if (fcntl(socket_, F_SETFL, socket_flags | O_NONBLOCK) == -1) {
        return false;
      }
    }

    int res = ::accept(socket_, NULL, NULL);
    if (res == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {  // no pending connection
        return false;
      }
    }
    return handle_accept(res);
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
      is_valid_ = false;
      return (*this);
    }
    return *this;
  }

  /*
   * try_read() will only attempt to read when there are SOME data inside the
   * socket. More precisely, when multiple data are asked, it will read when
   * there is at least one instance available. After that the call to read
   * will be blocking.
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

  explicit operator bool() const { return is_valid_; }

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


  template<ProcessType X = PT,
           typename std::enable_if<X == ProcessType::Server, int>::type = 0>
  bool handle_accept(int val) {
    if (val == -1) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "accepting connection with socket %s failed (%d)", name_.c_str(),
               errno);
      is_valid_ = false;
      return (val != -1);
    }
    data_v_.emplace_back(val);
    return (val != -1);
  }

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
    if (not(elements > 0)) {
      return result;
    }
    ContiguousType buffer;
    for (size_t i = 0; i < elements; ++i) {
      int res = ::read(socket, &buffer, sizeof(ContiguousType));
      if (res == -1) {
        LogCvmfs(kLogCvmfs, kLogDebug, "reading from socket %s failed (%d)",
                 name_.c_str(), errno);
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
    }
    return *this;
  }

  int socket_ = -1;
  LocalUnixSocketAddress addr_;
  std::string name_;
  std::vector<int> data_v_;

 private:
  bool is_valid_ = true;
};

class CacheManagerSocket : public LocalUnixSocket<ProcessType::Client> {
 public:
  CacheManagerSocket(const char *socket_name)
      : LocalUnixSocket<ProcessType::Client>(socket_name) { }

  size_t send_hashes(const SmallHashDynamic<shash::Any, int> &hash_map) {
    const size_t &msize = hash_map.size();
    write(util::Command::RecvHashes);
    write(msize);
    for (size_t i = 0; i < hash_map.capacity(); ++i) {
      shash::Any hash, empty = hash_map.empty_key_, *keys = hash_map.keys_;
      if ((hash = keys[i]) != empty) {
        write(hash);
      }
    }
    return msize;
  }

  size_t send_hashes(const SmallHashDynamic<shash::Any, int> *hash_map_ptr) {
    return send_hashes(*hash_map_ptr);
  }

 private:
  friend int MakeSocket(const std::string &path, const int mode);
  /*
   * Release the handled socket only if there are no active client connections
   */
  int release() {
    if (not data_v_.empty()) {
      return -1;
    }
    int res = socket_;
    socket_ = -1;
    return res;
  }
};

class QuotaManagerSocket : public LocalUnixSocket<ProcessType::Server> {
 public:
  QuotaManagerSocket(const char *socket_name)
      : LocalUnixSocket<ProcessType::Server>::LocalUnixSocket(socket_name) { }

  /*
   * TODO(christge) Points to consider:
   * 1. On a more mature version LocalUnixSocket should have methods send()
   * for the Client and try_recv() for the server. The real bottleneck in this
   * version is that we don't have std::optional in our current C++ version,
   * so we can't say if a client isn't responsive or doesn't have any hashes
   * to send (bc both case would return an empty std::set of hashes).
   * 2. Maybe collect<ContiguousType> should be a method of a
   * LocalUnixSocket<ProcessType::Server> and the override here should be a
   * specialized: collect<shash::Any>
   */
  std::set<shash::Any> collect_hashes() { return collect<shash::Any>(); }

  /*
   * collect() works on a best effort basis; QuotaManagerSocket will attempt
   * up to <number_of_attempts> times to collect data from each socket and
   * then will return what is available so far.
   */
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

    std::vector<bool> collected(nclients, false);
    size_t attempt = 0;
    while (still_missing(collected) and attempt <= number_of_attempts) {
      sleep(static_cast<int>(pow(2, attempt) / 100));
      for (size_t i = 0; i < nclients; ++i) {
        if (not collected[i]) {
          collected[i] = try_collect(i);
        }
      }
      ++attempt;
    }
    return result;
  }

 private:
  static constexpr size_t number_of_attempts = 8;
};

#endif  // __LOCAL_UNIX_SOCKET_H_

