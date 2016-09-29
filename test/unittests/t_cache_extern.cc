/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <pthread.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <cassert>
#include <cstring>
#include <string>

#include "cache.pb.h"
#include "cache_extern.h"
#include "cache_transport.h"
#include "util/posix.h"

using namespace std;  // NOLINT

/**
 * Receiving end of the cache manager.
 */
class TestCachePlugin {
 public:
  explicit TestCachePlugin(const string &socket_path) {
    fd_connection_ = -1;
    fd_socket_ = MakeSocket(socket_path, 0600);
    if (fd_socket_ >= 0) {
      int retval = listen(fd_socket_, 1);
      assert(retval == 0);
      retval = pthread_create(&thread_recv_, NULL, MainServer, this);
      assert(retval == 0);
    }
  }

  ~TestCachePlugin() {
    if (fd_socket_ >= 0) {
      shutdown(fd_socket_, SHUT_RDWR);
      close(fd_socket_);
      pthread_join(thread_recv_, NULL);
    }
  }

  static void HandleConnection(int fd) {
    CacheTransport transport(fd);
    while (true) {
      cvmfs::MsgClientCall msg_client_call;
      cvmfs::MsgServerCall msg_server_call;
      bool retval = transport.RecvMsg(&msg_client_call);
      if (!retval)
        abort();

      if (msg_client_call.has_msg_handshake()) {
        cvmfs::MsgHandshakeAck msg_ack;
        msg_ack.set_status(cvmfs::STATUS_OK);
        msg_ack.set_name("unit test cache manager");
        msg_ack.set_protocol_version(ExternalCacheManager::kPbProtocolVersion);
        msg_ack.set_session_id(42);
        msg_ack.set_max_object_size(1024 * 1024);  // 1MB
        msg_server_call.set_allocated_msg_handshake_ack(&msg_ack);
        transport.SendMsg(&msg_server_call);
        msg_server_call.release_msg_handshake_ack();
      } else if (msg_client_call.has_msg_quit()) {
        break;
      } else {
        abort();
      }
    }
  }

  static void *MainServer(void *data) {
    TestCachePlugin *cache_plugin = reinterpret_cast<TestCachePlugin *>(data);

    struct sockaddr_un remote;
    socklen_t socket_size = sizeof(remote);
    while (true) {
      if (cache_plugin->fd_connection_ >= 0) {
        shutdown(cache_plugin->fd_connection_, SHUT_RDWR);
        close(cache_plugin->fd_connection_);
        cache_plugin->fd_connection_ = -1;
      }
      cache_plugin->fd_connection_ = accept(cache_plugin->fd_socket_,
                                            (struct sockaddr *)&remote,
                                            &socket_size);
      if (cache_plugin->fd_connection_ < 0)
        return NULL;

      HandleConnection(cache_plugin->fd_connection_);
    }
    return NULL;
  }

  int fd_connection_;
  int fd_socket_;
  pthread_t thread_recv_;
};


class T_ExternalCacheManager : public ::testing::Test {
 protected:
  virtual void SetUp() {
    socket_path_ = "cvmfs_cache_plugin.socket";
    test_plugin_ = new TestCachePlugin(socket_path_);
    ASSERT_GE(test_plugin_->fd_socket_, 0);

    int fd_client = ConnectSocket(socket_path_);
    ASSERT_GE(fd_client, 0);
    cache_mgr_ = ExternalCacheManager::Create(fd_client);
    ASSERT_TRUE(cache_mgr_ != NULL);
  }

  virtual void TearDown() {
    delete cache_mgr_;
    unlink(socket_path_.c_str());
    delete test_plugin_;
  }

  string socket_path_;
  TestCachePlugin *test_plugin_;
  ExternalCacheManager *cache_mgr_;
};



TEST_F(T_ExternalCacheManager, Basics) {
}
