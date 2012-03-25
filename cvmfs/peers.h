/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PEERS_H_
#define CVMFS_PEERS_H_

#include <stdint.h>
#include <pthread.h>

#include <cstring>
#include <cstdlib>

#include <string>
#include <vector>

#include "util.h"
#include "logging.h"
#include "smalloc.h"

namespace peers {

const unsigned kMaxMessage = 512;
const unsigned kPingInterval = 5;  // Seconds

enum IPVersion {
  kIPv4 = 0,
  kIPv6,
};

enum MessageType {
  kMsgMoin = 0,
  kMsgCiao,
  kMsgPing,
  kMsgPong,
};


struct Address {
  IPVersion version;
  union {
    uint32_t ip4_address;
    uint32_t ip6_address[4];  // Not yet implemented
  };
  uint16_t port;

  Address() : version(kIPv4) { }
  explicit Address(const IPVersion v) : version(v) { }
  Address(const uint32_t a, const uint16_t p) :
    version(kIPv4), ip4_address(a), port(p) { }

  bool operator ==(const Address &other) const {
    return (this->ip4_address == other.ip4_address) &&
           (this->port == other.port);
  }

  bool operator !=(const Address &other) const {
    return (this->ip4_address != other.ip4_address) ||
           (this->port != other.port);
  }

  bool operator <(const Address &other) const {
    if ((this->ip4_address < other.ip4_address) ||
        ((this->ip4_address == other.ip4_address) &&
         (this->port < other.port)))
    {
      return true;
    }
    return false;
  }

  std::string ToString() const {
    return std::string(StringifyIpv4(ip4_address) + ":" + StringifyInt(port));
  }
};


class Message {
 public:
  virtual ~Message() { };
  void Pack(uint16_t *size, unsigned char buffer[kMaxMessage]) const {
    DoPack(size, buffer);
  };
  void Unpack(unsigned char *buffer, const uint16_t size) {
    DoUnpack(buffer, size);
  }

 protected:
  virtual void DoPack(uint16_t *size, unsigned char buffer[kMaxMessage])
    const = 0;
  virtual void DoUnpack(unsigned char *buffer, const uint16_t size) = 0;
};

class MessageMoin : public Message {
 public:
  MessageMoin(unsigned char *buffer, uint16_t size) { Unpack(buffer, size); }
  MessageMoin(const uint16_t port) : port_(port) { }
  inline uint16_t port() const { return port_; }
 protected:
  void DoPack(uint16_t *size, unsigned char buffer[kMaxMessage]) const {
    *size = 1 + sizeof(port_);
    buffer[0] = kMsgMoin;
    buffer++;
    memcpy(buffer, &port_, sizeof(port_));
  }
  void DoUnpack(unsigned char *buffer, const uint16_t size) {
    buffer++;
    memcpy(&port_, buffer, sizeof(port_));
  }
 private:
  uint16_t port_;
};

class MessagePing : public Message {
 public:
  MessagePing(unsigned char *buffer, uint16_t size) { Unpack(buffer, size); }
  MessagePing(const uint16_t port) : port_(port) { }
  inline uint16_t port() const { return port_; }
 protected:
  void DoPack(uint16_t *size, unsigned char buffer[kMaxMessage]) const {
    *size = 1 + sizeof(port_);
    buffer[0] = kMsgPing;
    buffer++;
    memcpy(buffer, &port_, sizeof(port_));
  }
  void DoUnpack(unsigned char *buffer, const uint16_t size) {
    buffer++;
    memcpy(&port_, buffer, sizeof(port_));
  }
 private:
  uint16_t port_;
};

class MessagePong : public Message {
 public:
  MessagePong(unsigned char *buffer, uint16_t size) { Unpack(buffer, size); }
  MessagePong(const uint16_t port) : port_(port) { }
  inline uint16_t port() const { return port_; }
 protected:
  void DoPack(uint16_t *size, unsigned char buffer[kMaxMessage]) const {
    *size = 1 + sizeof(port_);
    buffer[0] = kMsgPong;
    buffer++;
    memcpy(buffer, &port_, sizeof(port_));
  }
  void DoUnpack(unsigned char *buffer, const uint16_t size) {
    buffer++;
    memcpy(&port_, buffer, sizeof(port_));
  }
 private:
  uint16_t port_;
};

class MessageCiao : public Message {
 public:
  MessageCiao(unsigned char *buffer, uint16_t size) { Unpack(buffer, size); }
  MessageCiao(const Address &address) : address_(address) { }
  inline Address address() const { return address_; }
 protected:
  virtual void DoPack(uint16_t *size, unsigned char buffer[kMaxMessage]) const {
    *size = 1 + sizeof(address_);
    buffer[0] = kMsgCiao;
    buffer++;
    memcpy(buffer, &address_, sizeof(address_));
  }
  virtual void DoUnpack(unsigned char *buffer, const uint16_t size) {
    buffer++;
    memcpy(&address_, buffer, sizeof(address_));
  }
 private:
  Address address_;
};


class Peers {
 public:
  explicit Peers(const Address &me) {
    lock_ =
      reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
    int retval = pthread_mutex_init(lock_, NULL);
    assert(retval == 0);
    addresses_.push_back(me);
    index_me_ = 0;
  }

  ~Peers() { free(lock_); }

  bool Insert(const Address &peer) {
    pthread_mutex_lock(lock_);
    std::vector<Address>::iterator elem_addr;
    bool found = Find(peer, elem_addr);
    if (found) {
      LogCvmfs(kLogPeers, kLogDebug, "peer %s already in list",
               peer.ToString().c_str());
      pthread_mutex_unlock(lock_);
      return true;
    }

    const int position = std::distance(addresses_.begin(), elem_addr);
    addresses_.insert(elem_addr, peer);
    if (position <= index_me_)
      ++index_me_;
    LogCvmfs(kLogPeers, kLogDebug, "inserted %s at position %d",
             peer.ToString().c_str(), position);
    pthread_mutex_unlock(lock_);
    return false;
  }

  void GetWatchees(Address *watchee1, Address *watchee2) {
    pthread_mutex_lock(lock_);
    *watchee1 = addresses_[random()%addresses_.size()];
    *watchee2 = addresses_[random()%addresses_.size()];
    pthread_mutex_unlock(lock_);
  }

  void Erase(const Address &peer) {
    pthread_mutex_lock(lock_);
    std::vector<Address>::iterator elem_addr;
    if (Find(peer, elem_addr)) {
      const int position = std::distance(addresses_.begin(), elem_addr);
      if (position == index_me_) {
        LogCvmfs(kLogPeers, kLogDebug | kLogSyslog,
                 "won't delete myself from peer list!");
        pthread_mutex_unlock(lock_);
        return;
      }

      if (position < index_me_)
        --index_me_;
      addresses_.erase(elem_addr);
      LogCvmfs(kLogPeers, kLogDebug, "erased %s at position %d",
               peer.ToString().c_str(), position);
    } else {
      LogCvmfs(kLogPeers, kLogDebug, "attempt to erase non-existing peer %s",
            peer.ToString().c_str());
    }
    pthread_mutex_unlock(lock_);
  }

  std::string Print() const {
    std::string result;
    pthread_mutex_lock(lock_);
    for (unsigned i = 0; i < addresses_.size(); ++i) {
      result += StringifyInt(i) + " -- " + addresses_[i].ToString() + "\n";
    }
    pthread_mutex_unlock(lock_);
    return result;
  }

 private:
  pthread_mutex_t *lock_;
  std::vector<Address> addresses_;
  int index_me_;
  /**
   * Iterator to smallest element greater or equal to peer.
   * \return true if element exists
   */
  bool Find(const Address &peer, std::vector<Address>::iterator &itr) {
    const int N = addresses_.size();
    int low = 0;
    int high = N;
    while (low < high) {
      int mid = low + ((high - low) / 2);
      if (addresses_[mid] < peer)
        low = mid + 1;
      else
        high = mid;
    }
    itr = (high == N) ? addresses_.end() : addresses_.begin() + high;
    return (low < N) && (*itr == peer);
  }
};



bool Init(const std::string &cachedir, const std::string &exe_path,
          const std::string &interface);
void Fini();
int MainPeerServer(int argc, char **argv);

}  // namespace peers

#endif  // CVMFS_PEERS_H_
