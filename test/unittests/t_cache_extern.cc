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
#include "hash.h"
#include "util/posix.h"

using namespace std;  // NOLINT

/**
 * Receiving end of the cache manager.
 */
class MockCachePlugin {
 public:
  explicit MockCachePlugin(const string &socket_path) {
    fd_connection_ = -1;
    fd_socket_ = MakeSocket(socket_path, 0600);
    if (fd_socket_ >= 0) {
      int retval = listen(fd_socket_, 1);
      assert(retval == 0);
      retval = pthread_create(&thread_recv_, NULL, MainServer, this);
      assert(retval == 0);
    }
    known_object.algorithm = shash::kSha1;
    known_object_content = "Hello, World";
    shash::HashString(known_object_content, &known_object);
    known_object_refcnt = 0;
    next_status = -1;
  }

  ~MockCachePlugin() {
    if (fd_socket_ >= 0) {
      shutdown(fd_socket_, SHUT_RDWR);
      close(fd_socket_);
      pthread_join(thread_recv_, NULL);
    }
  }

  static void HandleConnection(MockCachePlugin *cache_plugin) {
    CacheTransport transport(cache_plugin->fd_connection_);
    while (true) {
      cvmfs::MsgClientCall msg_client_call;
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
        transport.SendMsg(&msg_ack);
      } else if (msg_client_call.has_msg_quit()) {
        break;
      } else if (msg_client_call.has_msg_refcount_req()) {
        cvmfs::MsgRefcountReq msg_req = msg_client_call.msg_refcount_req();
        cvmfs::MsgRefcountReply msg_reply;
        msg_reply.set_req_id(msg_req.req_id());
        shash::Any object_id;
        bool retval = transport.ParseMsgHash(msg_req.object_id(), &object_id);
        if (!retval) {
          msg_reply.set_status(cvmfs::STATUS_MALFORMED);
        } else if (object_id != cache_plugin->known_object) {
          msg_reply.set_status(cvmfs::STATUS_NOENTRY);
        } else {
          if ((cache_plugin->known_object_refcnt + msg_req.change_by()) < 0) {
            msg_reply.set_status(cvmfs::STATUS_BADCOUNT);
          } else {
            cache_plugin->known_object_refcnt += msg_req.change_by();
            msg_reply.set_status(cvmfs::STATUS_OK);
          }
        }
        if (cache_plugin->next_status >= 0) {
          msg_reply.set_status(static_cast<cvmfs::EnumStatus>(
                               cache_plugin->next_status));
        }
        transport.SendMsg(&msg_reply);
      } else if (msg_client_call.has_msg_object_info_req()) {
        cvmfs::MsgObjectInfoReq msg_req = msg_client_call.msg_object_info_req();
        cvmfs::MsgObjectInfoReply msg_reply;
        msg_reply.set_req_id(msg_req.req_id());
        shash::Any object_id;
        bool retval = transport.ParseMsgHash(msg_req.object_id(), &object_id);
        if (!retval) {
          msg_reply.set_status(cvmfs::STATUS_MALFORMED);
        } else if (object_id != cache_plugin->known_object) {
          msg_reply.set_status(cvmfs::STATUS_NOENTRY);
        } else {
          msg_reply.set_status(cvmfs::STATUS_OK);
          msg_reply.set_object_type(cvmfs::OBJECT_REGULAR);
          msg_reply.set_nparts(1);
          msg_reply.set_size(cache_plugin->known_object_content.length());
        }
        if (cache_plugin->next_status >= 0) {
          msg_reply.set_status(static_cast<cvmfs::EnumStatus>(
                               cache_plugin->next_status));
        }
        transport.SendMsg(&msg_reply);
      } else {
        abort();
      }
    }
  }

  static void *MainServer(void *data) {
    MockCachePlugin *cache_plugin = reinterpret_cast<MockCachePlugin *>(data);

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

      HandleConnection(cache_plugin);
    }
    return NULL;
  }

  int fd_connection_;
  int fd_socket_;
  pthread_t thread_recv_;
  string known_object_content;
  shash::Any known_object;
  int known_object_refcnt;
  int next_status;
};


class T_ExternalCacheManager : public ::testing::Test {
 protected:
  virtual void SetUp() {
    socket_path_ = "cvmfs_cache_plugin.socket";
    mock_plugin_ = new MockCachePlugin(socket_path_);
    ASSERT_GE(mock_plugin_->fd_socket_, 0);

    int fd_client = ConnectSocket(socket_path_);
    ASSERT_GE(fd_client, 0);
    cache_mgr_ = ExternalCacheManager::Create(fd_client, nfiles);
    ASSERT_TRUE(cache_mgr_ != NULL);
  }

  virtual void TearDown() {
    delete cache_mgr_;
    unlink(socket_path_.c_str());
    delete mock_plugin_;
  }

  static const int nfiles;
  string socket_path_;
  MockCachePlugin *mock_plugin_;
  ExternalCacheManager *cache_mgr_;
};

const int T_ExternalCacheManager::nfiles = 128;



TEST_F(T_ExternalCacheManager, Connection) {
  EXPECT_EQ(42, cache_mgr_->session_id());
}


TEST_F(T_ExternalCacheManager, OpenClose) {
  EXPECT_EQ(-EBADF, cache_mgr_->Close(0));
  shash::Any rnd_id(shash::kSha1);
  rnd_id.Randomize();
  EXPECT_EQ(-ENOENT, cache_mgr_->Open(CacheManager::Bless(rnd_id)));

  int fds[nfiles];
  for (unsigned i = 0; i < nfiles; ++i) {
    fds[i] = cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object));
    EXPECT_GE(fds[i], 0);
  }
  EXPECT_EQ(nfiles, mock_plugin_->known_object_refcnt);
  EXPECT_EQ(-ENFILE,
            cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object)));
  for (unsigned i = 0; i < nfiles; ++i) {
    EXPECT_EQ(0, cache_mgr_->Close(fds[i]));
  }
  EXPECT_EQ(0, mock_plugin_->known_object_refcnt);

  mock_plugin_->next_status = cvmfs::STATUS_MALFORMED;
  EXPECT_EQ(-EINVAL,
            cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object)));
  mock_plugin_->next_status = -1;
}


TEST_F(T_ExternalCacheManager, GetSize) {
  EXPECT_EQ(-EBADF, cache_mgr_->GetSize(0));
  int fd = cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(static_cast<int64_t>(mock_plugin_->known_object_content.length()),
            cache_mgr_->GetSize(fd));

  mock_plugin_->next_status = cvmfs::STATUS_MALFORMED;
  EXPECT_EQ(-EINVAL, cache_mgr_->GetSize(fd));
  mock_plugin_->next_status = -1;

  EXPECT_EQ(0, cache_mgr_->Close(fd));
}
