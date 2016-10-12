/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <pthread.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <string>
#include <vector>

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
  static const unsigned kObjectSize = 256 * 1024;

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
    char buffer[kObjectSize];
    while (true) {
      CacheTransport::Frame frame_recv;
      frame_recv.set_attachment(buffer, kObjectSize);
      bool retval = transport.RecvFrame(&frame_recv);
      if (!retval)
        abort();
      google::protobuf::MessageLite *msg_typed = frame_recv.GetMsgTyped();

      if (msg_typed->GetTypeName() == "cvmfs.MsgHandshake") {
        cvmfs::MsgHandshakeAck msg_ack;
        msg_ack.set_status(cvmfs::STATUS_OK);
        msg_ack.set_name("unit test cache manager");
        msg_ack.set_protocol_version(ExternalCacheManager::kPbProtocolVersion);
        msg_ack.set_session_id(42);
        msg_ack.set_max_object_size(kObjectSize);  // 256kB
        CacheTransport::Frame frame_send(&msg_ack);
        transport.SendFrame(&frame_send);
      } else if (msg_typed->GetTypeName() == "cvmfs.MsgQuit") {
        break;
      } else if (msg_typed->GetTypeName() == "cvmfs.MsgRefcountReq") {
        cvmfs::MsgRefcountReq *msg_req =
          reinterpret_cast<cvmfs::MsgRefcountReq *>(msg_typed);
        cvmfs::MsgRefcountReply msg_reply;
        msg_reply.set_req_id(msg_req->req_id());
        shash::Any object_id;
        bool retval = transport.ParseMsgHash(msg_req->object_id(), &object_id);
        if (!retval) {
          msg_reply.set_status(cvmfs::STATUS_MALFORMED);
        } else if (object_id != cache_plugin->known_object) {
          if (object_id == cache_plugin->new_object)
            msg_reply.set_status(cvmfs::STATUS_OK);
          else
            msg_reply.set_status(cvmfs::STATUS_NOENTRY);
        } else {
          if ((cache_plugin->known_object_refcnt + msg_req->change_by()) < 0) {
            msg_reply.set_status(cvmfs::STATUS_BADCOUNT);
          } else {
            cache_plugin->known_object_refcnt += msg_req->change_by();
            msg_reply.set_status(cvmfs::STATUS_OK);
          }
        }
        if (cache_plugin->next_status >= 0) {
          msg_reply.set_status(static_cast<cvmfs::EnumStatus>(
                               cache_plugin->next_status));
        }
        CacheTransport::Frame frame_send(&msg_reply);
        transport.SendFrame(&frame_send);
      } else if (msg_typed->GetTypeName() == "cvmfs.MsgObjectInfoReq") {
        cvmfs::MsgObjectInfoReq *msg_req =
          reinterpret_cast<cvmfs::MsgObjectInfoReq *>(msg_typed);
        cvmfs::MsgObjectInfoReply msg_reply;
        msg_reply.set_req_id(msg_req->req_id());
        shash::Any object_id;
        bool retval = transport.ParseMsgHash(msg_req->object_id(), &object_id);
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
        CacheTransport::Frame frame_send(&msg_reply);
        transport.SendFrame(&frame_send);
      } else if (msg_typed->GetTypeName() == "cvmfs.MsgReadReq") {
        cvmfs::MsgReadReq *msg_req =
          reinterpret_cast<cvmfs::MsgReadReq *>(msg_typed);
        cvmfs::MsgReadReply msg_reply;
        CacheTransport::Frame frame_send(&msg_reply);
        msg_reply.set_req_id(msg_req->req_id());
        shash::Any object_id;
        bool retval = transport.ParseMsgHash(msg_req->object_id(), &object_id);
        if (!retval) {
          msg_reply.set_status(cvmfs::STATUS_MALFORMED);
        } else if (object_id != cache_plugin->known_object) {
          msg_reply.set_status(cvmfs::STATUS_NOENTRY);
        } else {
          const char *data = cache_plugin->known_object_content.data();
          uint32_t data_size = cache_plugin->known_object_content.length();
          if (msg_req->offset() >= data_size) {
            msg_reply.set_status(cvmfs::STATUS_OUTOFBOUNDS);
          } else {
            frame_send.set_attachment(
              const_cast<char *>(data) + msg_req->offset(),
              std::min(msg_req->size(),
                       static_cast<uint32_t>(data_size - msg_req->offset()))
            );
            msg_reply.set_status(cvmfs::STATUS_OK);
          }
        }
        if (cache_plugin->next_status >= 0) {
          msg_reply.set_status(static_cast<cvmfs::EnumStatus>(
                               cache_plugin->next_status));
        }
        transport.SendFrame(&frame_send);
      } else if (msg_typed->GetTypeName() == "cvmfs.MsgStoreReq") {
        cvmfs::MsgStoreReq *msg_req =
          reinterpret_cast<cvmfs::MsgStoreReq *>(msg_typed);
        cvmfs::MsgStoreReply msg_reply;
        CacheTransport::Frame frame_send(&msg_reply);
        msg_reply.set_req_id(msg_req->req_id());
        msg_reply.set_part_nr(msg_req->part_nr());
        bool retval = transport.ParseMsgHash(msg_req->object_id(),
                                             &(cache_plugin->new_object));
        if (!retval) {
          msg_reply.set_status(cvmfs::STATUS_MALFORMED);
        } else {
          string data(reinterpret_cast<char *>(frame_recv.attachment()),
                      frame_recv.att_size());
          cache_plugin->chunks.push_back(data);
          msg_reply.set_status(cvmfs::STATUS_OK);
        }
        transport.SendFrame(&frame_send);
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
  shash::Any new_object;
  vector<string> chunks;
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


TEST_F(T_ExternalCacheManager, Dup) {
  EXPECT_EQ(-EBADF, cache_mgr_->Dup(0));
  int fds[nfiles];
  fds[0] = cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object));
  EXPECT_GE(fds[0], 0);
  for (unsigned i = 1; i < nfiles; ++i) {
    fds[i] = cache_mgr_->Dup(fds[0]);
    EXPECT_GE(fds[i], 0);
  }
  EXPECT_EQ(nfiles, mock_plugin_->known_object_refcnt);
  EXPECT_EQ(-ENFILE, cache_mgr_->Dup(fds[0]));
  for (unsigned i = 0; i < nfiles; ++i) {
    EXPECT_EQ(0, cache_mgr_->Close(fds[i]));
  }
  EXPECT_EQ(0, mock_plugin_->known_object_refcnt);
}


TEST_F(T_ExternalCacheManager, Pread) {
  unsigned buf_size = 64;
  char buffer[64];
  EXPECT_EQ(-EBADF, cache_mgr_->Pread(0, buffer, buf_size, 0));

  int fd = cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(-EINVAL, cache_mgr_->Pread(fd, buffer, 0, 64));
  int64_t len = cache_mgr_->Pread(fd, buffer, 64, 0);
  EXPECT_EQ(static_cast<int>(mock_plugin_->known_object_content.length()), len);
  EXPECT_EQ(mock_plugin_->known_object_content, string(buffer, len));
  EXPECT_EQ(1, cache_mgr_->Pread(fd, buffer, 1, len-1));
  EXPECT_EQ(mock_plugin_->known_object_content[len-1], buffer[0]);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}


TEST_F(T_ExternalCacheManager, Readahead) {
  EXPECT_EQ(-EBADF, cache_mgr_->Readahead(0));
  int fd = cache_mgr_->Open(CacheManager::Bless(mock_plugin_->known_object));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Readahead(fd));
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}


TEST_F(T_ExternalCacheManager, Transaction) {
  shash::Any id(shash::kSha1);
  string content = "foo";
  HashString(content, &id);
  unsigned char *data = const_cast<unsigned char *>(
    reinterpret_cast<const unsigned char *>(content.data()));
  EXPECT_TRUE(
    cache_mgr_->CommitFromMem(id, data, content.length(), "test"));

  // test 0-byte
  // test large
  // test large and multiple of max object size
}
