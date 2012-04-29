/**
 * This file is part of the CernVM File System.
 *
 * The peer server is a daemon shared among all currently mounted file system
 * instances (repositories).  It is automatically created by the first instance
 * and automatically shuts itself down when the last instance is unmounted.
 *
 * The peer server maintains a list of available cvmfs peers.  Neighbor cvmfs
 * instances are discovered by IP multicast.  Availability of cvmfs peers
 * is supervised by watchdogs.
 *
 * Cvmfs instances ask the peer server via sockets for a responsible peer
 * given a certain hash.  The peer server implements a distributed hash table.
 *
 * This module contains both server and client code.
 */

#include "peers.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <netdb.h>

#include <string>
#include <vector>

#include "platform.h"
#include "util.h"
#include "logging.h"
#include "atomic.h"
#include "smalloc.h"
#include "cvmfs.h"

using namespace std;  // NOLINT

namespace peers {

// Server variables
std::string *cachedir_ = NULL;
atomic_int32 num_connections_;
pthread_attr_t pthread_connection_attr_;
Address *address_self_;
Address watchee1_;
Address watchee2_;
pthread_mutex_t lock_watchees_ = PTHREAD_MUTEX_INITIALIZER;
std::string *interface_;
struct sockaddr_in mcast_addr_;
int udp_send_;
int unicast_receive_;
int mcast_receive_;
pthread_t thread_receive_unicast_;
pthread_t thread_receive_multicast_;
pthread_t thread_watchdog_;
Peers *peers_;

// Client variables
int socket_fd_ = -1;


/**
 * Connects to a running peer server.  Creates a peer server, if necessary.
 */
bool Init(const string &cachedir, const string &exe_path,
          const string &interface)
{
  cachedir_ = new string(cachedir);

  // Create lock file
  const int fd_lockfile = LockFile(*cachedir_ + "/lock_peers");
  if (fd_lockfile < 0) {
    LogCvmfs(kLogPeers, kLogDebug, "could not open lock file %s (%d)",
             (*cachedir_ + "/lock_peers").c_str(), errno);
    return false;
  }

  // Try to connect to socket
  LogCvmfs(kLogPeers, kLogDebug, "trying to connect to existing socket");
  socket_fd_ = ConnectSocket(*cachedir_ + "/peers");
  if (socket_fd_ != -1) {
    char buf = '\0';
    read(socket_fd_, &buf, 1);
    if (buf == 'C') {
      LogCvmfs(kLogPeers, kLogDebug, "connected to existing socket");
      UnlockFile(fd_lockfile);
      return true;
    }
  }

  // Opening new socket for peer server (to be created)
  int socket_pair[2];
  int retval = socketpair(AF_UNIX, SOCK_STREAM, 0, socket_pair);
  assert(retval == 0);
  socket_fd_ = socket_pair[0];
  int pipe_boot[2];
  MakePipe(pipe_boot);

  // Create new peer server
  vector<string> command_line;
  command_line.push_back(exe_path);
  command_line.push_back("__peersrv__");
  command_line.push_back(*cachedir_);
  command_line.push_back(StringifyInt(pipe_boot[1]));
  command_line.push_back(StringifyInt(socket_pair[1]));
  command_line.push_back(StringifyInt(cvmfs::foreground_));
  command_line.push_back(GetLogDebugFile());
  
  vector<int> preserve_filedes;
  preserve_filedes.push_back(0);
  preserve_filedes.push_back(1);
  preserve_filedes.push_back(2);
  preserve_filedes.push_back(pipe_boot[1]);
  preserve_filedes.push_back(socket_pair[1]);
  
  retval = ManagedExec(command_line, preserve_filedes);
  close(socket_pair[1]);
  
  if (!retval) {
    UnlockFile(fd_lockfile);
    ClosePipe(pipe_boot);
    LogCvmfs(kLogPeers, kLogDebug, "failed to start peer server");
    return false;
  }

  // Wait for peer server to be ready
  close(pipe_boot[1]);
  char buf;
  if (read(pipe_boot[0], &buf, 1) != 1) {
    UnlockFile(fd_lockfile);
    close(pipe_boot[0]);
    LogCvmfs(kLogPeers, kLogDebug, "peer server did not start");
    return false;
  }
  close(pipe_boot[0]);

  UnlockFile(fd_lockfile);
  return true;
}


void Fini() {
  delete cachedir_;
  cachedir_ = NULL;

  LogCvmfs(kLogPeers, kLogDebug, "disconnecting from peer server");
  if (socket_fd_ >= 0) {
    close(socket_fd_);
  }
}


static bool InitGossip() {
  int retval = 0;
  struct sockaddr_in self_addr;
  memset(&self_addr, 0, sizeof(self_addr));
  self_addr.sin_family = AF_INET;

  // Figure out own address
  char hostname[HOST_NAME_MAX+1];
  retval = gethostname(hostname, HOST_NAME_MAX+1);
  assert(retval == 0);
  if (*interface_ == "") {
    struct addrinfo hints;
    struct addrinfo *result0;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_flags = AI_PASSIVE;
    retval = getaddrinfo(hostname, NULL, &hints, &result0);
    if (retval != 0)
      return false;
    self_addr.sin_addr = ((struct sockaddr_in *)(result0->ai_addr))->sin_addr;
    freeaddrinfo(result0);
  } else {
    if (!inet_aton(interface_->c_str(), &self_addr.sin_addr))
      return false;
  }
  address_self_ = new Address(kIPv4);
  address_self_->ip4_address = self_addr.sin_addr.s_addr;
  LogCvmfs(kLogPeers, kLogDebug, "bind %s to address %s", hostname,
           StringifyIpv4(address_self_->ip4_address).c_str());

  // Sender socket
  udp_send_ = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
  assert(udp_send_ >= 0);
  int on = 1;
  int ttl = 1;
  retval |= setsockopt(udp_send_, IPPROTO_IP, IP_MULTICAST_LOOP, &on,
                       sizeof(on));  // Enable loop-back
  retval |= setsockopt(udp_send_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl,
                       sizeof(ttl));  // Local subnet only
  retval |= setsockopt(udp_send_, IPPROTO_IP, IP_MULTICAST_IF,
                       &self_addr.sin_addr.s_addr,
                       sizeof(self_addr.sin_addr.s_addr));
  if (retval != 0) {
    LogCvmfs(kLogPeers, kLogDebug, "failed to set multicast options (%d)",
             errno);
    return false;
  }
  retval = bind(udp_send_, (struct sockaddr *)&self_addr, sizeof(self_addr));
  if (retval != 0)
    return false;

  // Init receiver sockets
  unicast_receive_ = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
  assert(unicast_receive_ >= 0);
  mcast_receive_ = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
  assert(mcast_receive_ >= 0);
  retval = setsockopt(mcast_receive_, SOL_SOCKET, SO_REUSEADDR, &on,
                      sizeof(on));
  if (retval != 0)
    return false;

  // Multicast address
  memset(&mcast_addr_, 0, sizeof(mcast_addr_));
  mcast_addr_.sin_family = AF_INET;
  mcast_addr_.sin_addr.s_addr = inet_addr("225.0.0.13");
  mcast_addr_.sin_port = htons(5001);

  // Set up multicast receiver
  retval = bind(mcast_receive_, (struct sockaddr *) &mcast_addr_,
                sizeof(mcast_addr_));
  if (retval != 0)
    return false;

  struct ip_mreq mreq;
  mreq.imr_multiaddr.s_addr = mcast_addr_.sin_addr.s_addr;
  mreq.imr_interface.s_addr = self_addr.sin_addr.s_addr;
  retval = setsockopt(mcast_receive_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq,
                      sizeof(mreq));
  if (retval != 0)
    return false;

  // Probe for ports
  bool bound = false;
  uint16_t port_base = 5001;
  uint16_t port_offset = 0;
  for (int retries = 0; (retries < 500) && !bound; retries++) {
    self_addr.sin_port = htons(port_base + port_offset);

    // Set up unicast receiver
    retval = bind(unicast_receive_, (struct sockaddr *) &self_addr,
                  sizeof(self_addr));
    if (retval != 0) {
      LogCvmfs(kLogPeers, kLogDebug, "binding unicast recv socket returned %d",
               errno);
      if (errno == EADDRINUSE) {
        port_offset++;
        continue;
      }
      return false;
    }
    bound = true;
  }
  if (!bound) {
    LogCvmfs(kLogPeers, kLogDebug, "failed to bind receiver sockets");
    return false;
  }
  address_self_->port = port_base + port_offset;
  LogCvmfs(kLogPeers, kLogDebug, "using UDP port %d", address_self_->port);

  return true;
}


static void SendTo(const Address &address, const Message &message) {
  struct sockaddr_in sock_addr;
  memset(&sock_addr, 0, sizeof(sock_addr));
  sock_addr.sin_family = AF_INET;
  sock_addr.sin_addr.s_addr = address.ip4_address;
  sock_addr.sin_port = htons(address.port);

  uint16_t message_size;
  unsigned char dgram[kMaxMessage];
  message.Pack(&message_size, dgram);

  sendto(udp_send_, dgram, message_size, 0,
         (struct sockaddr *) &sock_addr, sizeof(sock_addr));
}


static void SendMulticast(const Message &message) {
  uint16_t message_size;
  unsigned char dgram[kMaxMessage];
  message.Pack(&message_size, dgram);

  sendto(udp_send_, dgram, message_size, 0,
         (struct sockaddr *) &mcast_addr_, sizeof(mcast_addr_));
}


static void *MainUnicast(void *data __attribute__((unused))) {
  LogCvmfs(kLogPeers, kLogDebug, "starting udp unicast listener");
  unsigned char message_buffer[kMaxMessage];
  int num_bytes;
  struct sockaddr_in addr_sender;
  socklen_t addr_sender_len = sizeof(addr_sender);
  while (1) {
    num_bytes = recvfrom(unicast_receive_, message_buffer,
                         sizeof(message_buffer), 0,
                         (struct sockaddr *)&addr_sender, &addr_sender_len);
    if (num_bytes <= 0)
      break;

    char message_type = message_buffer[0];
    switch (message_type) {
      case kMsgPing: {
        MessagePing ping(message_buffer, num_bytes);
        Address remote_address(addr_sender.sin_addr.s_addr,
                               ping.port());
        LogCvmfs(kLogPeers, kLogDebug, "received ping from %s",
                 remote_address.ToString().c_str());
        peers_->Insert(remote_address);
        SendTo(remote_address, MessagePong(address_self_->port));
        break;
      }
      case kMsgPong: {
        MessagePing pong(message_buffer, num_bytes);
        Address remote_address(addr_sender.sin_addr.s_addr,
                               pong.port());
        LogCvmfs(kLogPeers, kLogDebug, "received pong from %s",
                 remote_address.ToString().c_str());
        pthread_mutex_lock(&lock_watchees_);
        if (remote_address == watchee1_)
          watchee1_.ip4_address = 0;
        if (remote_address == watchee2_)
          watchee2_.ip4_address = 0;
        pthread_mutex_unlock(&lock_watchees_);
        break;
      }
      default:
        LogCvmfs(kLogPeers, kLogDebug,
                 "unknown unicast message type from %s (%d)",
                 StringifyIpv4(addr_sender.sin_addr.s_addr).c_str(),
                 message_type);
    }
  }

  LogCvmfs(kLogPeers, kLogDebug, "stopping udp unicast listener");
  return NULL;
}


static void *MainMulticast(void *data __attribute__((unused))) {
  LogCvmfs(kLogPeers, kLogDebug, "starting udp multicast listener");
  unsigned char message_buffer[kMaxMessage];
  int num_bytes;
  struct sockaddr_in addr_sender;
  socklen_t addr_sender_len = sizeof(addr_sender);
  while (1) {
    num_bytes = recvfrom(mcast_receive_, message_buffer,
                         sizeof(message_buffer), 0,
                         (struct sockaddr *)&addr_sender, &addr_sender_len);
    if (num_bytes <= 0)
      break;

    char message_type = message_buffer[0];
    switch (message_type) {
      case kMsgMoin: {
        MessageMoin moin(message_buffer, num_bytes);
        Address remote_address(addr_sender.sin_addr.s_addr,
                               moin.port());
        LogCvmfs(kLogPeers, kLogDebug, "received moin from %s",
                 remote_address.ToString().c_str());
        bool known = peers_->Insert(remote_address);
        if (!known)
          SendTo(remote_address, MessagePing(address_self_->port));
        break;
      }
      case kMsgCiao: {
        MessageCiao ciao(message_buffer, num_bytes);
        LogCvmfs(kLogPeers, kLogDebug, "received moin from %s for %s",
                 StringifyIpv4(addr_sender.sin_addr.s_addr).c_str(),
                 ciao.address().ToString().c_str());
        peers_->Erase(ciao.address());
        if (ciao.address() == *address_self_)
          SendMulticast(MessageMoin(address_self_->port));
        break;
      }
      default:
        LogCvmfs(kLogPeers, kLogDebug,
                 "unknown multicast message type from %s (%d)",
                 StringifyIpv4(addr_sender.sin_addr.s_addr).c_str(),
                 message_type);
    }
  }

  LogCvmfs(kLogPeers, kLogDebug, "stopping udp multicast listener");
  return NULL;
}


static void *MainWatchdog(void *data __attribute__((unused))) {
  LogCvmfs(kLogPeers, kLogDebug, "starting watchdog");
  pthread_mutex_lock(&lock_watchees_);
  watchee1_.ip4_address = watchee2_.ip4_address = 0;
  pthread_mutex_unlock(&lock_watchees_);

  MessagePing ping(address_self_->port);
  while (1) {
    sleep(kPingInterval);
    pthread_mutex_lock(&lock_watchees_);
    if (watchee1_.ip4_address != 0) {
      LogCvmfs(kLogPeers, kLogDebug, "lost peer %s",
               watchee1_.ToString().c_str());
      SendMulticast(MessageCiao(watchee1_));
    }
    if (watchee2_.ip4_address != 0) {
      LogCvmfs(kLogPeers, kLogDebug, "lost peer %s",
               watchee2_.ToString().c_str());
      SendMulticast(MessageCiao(watchee2_));
    }
    peers_->GetWatchees(&watchee1_, &watchee2_);
    SendTo(watchee1_, ping);
    SendTo(watchee2_, ping);
    pthread_mutex_unlock(&lock_watchees_);
  }

  LogCvmfs(kLogPeers, kLogDebug, "stopping watchdog");
  return NULL;
}


/**
 * A connection to a mounted repository, receives hashes and returns the
 * repsonsible peer (redis) address.
 */
static void *MainPeerConnection(void *data) {
  int connection_fd = *(reinterpret_cast<int *>(data));
  free(data);
  LogCvmfs(kLogPeers, kLogDebug, "starting new peer connection on %d",
           connection_fd);

  char buf;
  read(connection_fd, &buf, 1);

  LogCvmfs(kLogPeers, kLogDebug, "shutting down peer connection %d",
           connection_fd);
  close(connection_fd);

  // Clean up after last instance
  int active_connections = atomic_xadd32(&num_connections_, -1);
  if (active_connections == 1) {
    LogCvmfs(kLogPeers, kLogDebug, "last connection, stopping peer server");
    SendMulticast(MessageCiao(*address_self_));
    exit(0);
  }

  return NULL;
}


/**
 * Accepts connection requests from new file system instances (new mounted
 * repositories).  The spawning instance is dealt with differenlty: here the
 * socket is a pre-created anonymous socketpair.
 */
int MainPeerServer(int argc, char **argv) {
  LogCvmfs(kLogPeers, kLogDebug, "starting peer server");
  int retval;

  // Process command line arguments
  cachedir_ = new string(argv[2]);
  int pipe_boot = String2Int64(argv[3]);
  int inital_socket = String2Int64(argv[4]);
  int foreground = String2Int64(argv[5]);
  const string logfile = argv[6];
  if (logfile != "")
    SetLogDebugFile(logfile + ".peersrv");
  interface_ = new string(argv[7]);

  retval = pthread_attr_init(&pthread_connection_attr_);
  assert(retval == 0);
  retval = pthread_attr_setdetachstate(&pthread_connection_attr_,
                                       PTHREAD_CREATE_DETACHED);
  assert(retval == 0);
  pthread_t pthread_connection;

  // Initialize socket
  int socket_fd = MakeSocket(*cachedir_ + "/peers", 0600);
  if (socket_fd == -1) {
    LogCvmfs(kLogPeers, kLogDebug, "failed to create peer socket (%d)", errno);
    return 1;
  }
  if (listen(socket_fd, 128) != 0) {
    LogCvmfs(kLogPeers, kLogDebug, "failed to listen at peer socket (%d)",
             errno);
    return 1;
  }
  LogCvmfs(kLogPeers, kLogDebug, "listening on %s",
           (*cachedir_ + "/peers").c_str());

  // Network initialization
  if (!InitGossip())
    return 1;
  retval = pthread_create(&thread_receive_unicast_, NULL, MainUnicast, NULL);
  retval |= pthread_create(&thread_receive_multicast_, NULL, MainMulticast,
                           NULL);
  retval |= pthread_create(&thread_watchdog_, NULL, MainWatchdog, NULL);
  assert(retval == 0);
  peers_ = new Peers(*address_self_);
  SendMulticast(MessageMoin(address_self_->port));

  if (!foreground)
    Daemonize();
  char buf = 'C';
  WritePipe(pipe_boot, &buf, 1);
  close(pipe_boot);

  atomic_init32(&num_connections_);
  atomic_inc32(&num_connections_);
  int *connection_fd_ptr =reinterpret_cast<int *>(smalloc(sizeof(int)));
  *connection_fd_ptr = inital_socket;
  retval = pthread_create(&pthread_connection, &pthread_connection_attr_,
                          MainPeerConnection, connection_fd_ptr);
  assert(retval == 0);

  struct sockaddr_un remote;
  socklen_t socket_size = sizeof(remote);
  int connection_fd = -1;
  while (true) {
    connection_fd = accept(socket_fd, (struct sockaddr *)&remote, &socket_size);
    int active_connections = atomic_xadd32(&num_connections_, 1);
    if (active_connections == 0) {
      close(connection_fd);
      continue;  // Will be cleaned up by last thread
    }
    char buf = 'C';
    retval = write(connection_fd, &buf, 1);
    if (retval == 1) {
      connection_fd_ptr =reinterpret_cast<int *>(smalloc(sizeof(int)));
      *connection_fd_ptr = connection_fd;
      retval = pthread_create(&pthread_connection, &pthread_connection_attr_,
                              MainPeerConnection, connection_fd_ptr);
      assert(retval == 0);
    }
  }

  return 0;
}

}  // namespace peers
