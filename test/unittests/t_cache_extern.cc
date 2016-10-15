/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <alloca.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>

#include "cache.pb.h"
#include "cache_extern.h"
#include "cache_transport.h"
#include "hash.h"
#include "smalloc.h"
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
    latest_part = 0;
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

  void HandleHandshake(CacheTransport *transport) {
    cvmfs::MsgHandshakeAck msg_ack;
    msg_ack.set_status(cvmfs::STATUS_OK);
    msg_ack.set_name("unit test cache manager");
    msg_ack.set_protocol_version(ExternalCacheManager::kPbProtocolVersion);
    msg_ack.set_session_id(42);
    msg_ack.set_max_object_size(kObjectSize);  // 256kB
    CacheTransport::Frame frame_send(&msg_ack);
    transport->SendFrame(&frame_send);
  }

  void HandleRefcountRequest(
    cvmfs::MsgRefcountReq *msg_req,
    CacheTransport *transport)
  {
    cvmfs::MsgRefcountReply msg_reply;
    msg_reply.set_req_id(msg_req->req_id());
    shash::Any object_id;
    bool retval = transport->ParseMsgHash(msg_req->object_id(), &object_id);
    if (!retval) {
      msg_reply.set_status(cvmfs::STATUS_MALFORMED);
    } else if (object_id != known_object) {
      if (object_id == new_object)
        msg_reply.set_status(cvmfs::STATUS_OK);
      else
        msg_reply.set_status(cvmfs::STATUS_NOENTRY);
    } else {
      if ((known_object_refcnt + msg_req->change_by()) < 0) {
        msg_reply.set_status(cvmfs::STATUS_BADCOUNT);
      } else {
        known_object_refcnt += msg_req->change_by();
        msg_reply.set_status(cvmfs::STATUS_OK);
      }
    }
    if (next_status >= 0)
      msg_reply.set_status(static_cast<cvmfs::EnumStatus>(next_status));
    CacheTransport::Frame frame_send(&msg_reply);
    transport->SendFrame(&frame_send);
  }

  void HandleObjectInfo(
    cvmfs::MsgObjectInfoReq *msg_req,
    CacheTransport *transport)
  {
    cvmfs::MsgObjectInfoReply msg_reply;
    msg_reply.set_req_id(msg_req->req_id());
    shash::Any object_id;
    bool retval = transport->ParseMsgHash(msg_req->object_id(), &object_id);
    if (!retval) {
      msg_reply.set_status(cvmfs::STATUS_MALFORMED);
    } else if (object_id == known_object) {
      msg_reply.set_status(cvmfs::STATUS_OK);
      msg_reply.set_object_type(cvmfs::OBJECT_REGULAR);
      msg_reply.set_nparts(1);
      msg_reply.set_size(known_object_content.length());
    } else if (object_id == new_object) {
      msg_reply.set_status(cvmfs::STATUS_OK);
      msg_reply.set_object_type(cvmfs::OBJECT_REGULAR);
      msg_reply.set_nparts(0); // <-- wrong but ignored
      msg_reply.set_size(new_object_content.length());
    } else {
      msg_reply.set_status(cvmfs::STATUS_NOENTRY);
    }
    if (next_status >= 0)
      msg_reply.set_status(static_cast<cvmfs::EnumStatus>(next_status));
    CacheTransport::Frame frame_send(&msg_reply);
    transport->SendFrame(&frame_send);
  }

  void HandleRead(cvmfs::MsgReadReq *msg_req, CacheTransport *transport) {
    cvmfs::MsgReadReply msg_reply;
    CacheTransport::Frame frame_send(&msg_reply);
    msg_reply.set_req_id(msg_req->req_id());
    shash::Any object_id;
    bool retval = transport->ParseMsgHash(msg_req->object_id(), &object_id);
    if (!retval) {
      msg_reply.set_status(cvmfs::STATUS_MALFORMED);
    } else if ((object_id != known_object) && (object_id != new_object)) {
      msg_reply.set_status(cvmfs::STATUS_NOENTRY);
    } else {
      const char *data;
      uint32_t data_size;
      if (object_id == known_object) {
        data = known_object_content.data();
        data_size = known_object_content.length();
      } else {
        data = new_object_content.data();
        data_size = new_object_content.length();
      }
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
    if (next_status >= 0)
      msg_reply.set_status(static_cast<cvmfs::EnumStatus>(next_status));
    transport->SendFrame(&frame_send);
  }

  void HandleStore(
    cvmfs::MsgStoreReq *msg_req,
    CacheTransport::Frame *frame_recv,
    CacheTransport *transport)
  {
    cvmfs::MsgStoreReply msg_reply;
    CacheTransport::Frame frame_send(&msg_reply);
    msg_reply.set_req_id(msg_req->req_id());
    msg_reply.set_part_nr(msg_req->part_nr());
    bool retval = transport->ParseMsgHash(msg_req->object_id(), &new_object);
    if (!retval) {
      msg_reply.set_status(cvmfs::STATUS_MALFORMED);
    } else {
      if (msg_req->part_nr() == 1) {
        new_object_content.clear();
        latest_part = 0;
      }
      if (frame_recv->att_size() > 0) {
        string data(reinterpret_cast<char *>(frame_recv->attachment()),
                    frame_recv->att_size());
        new_object_content += data;
      }
      assert(msg_req->part_nr() == (latest_part + 1));
      latest_part++;
      msg_reply.set_status(cvmfs::STATUS_OK);
    }
    transport->SendFrame(&frame_send);
  }

  void HandleStoreAbort(
    cvmfs::MsgStoreAbortReq *msg_req,
    CacheTransport *transport)
  {
    new_object_content.clear();
    latest_part = 0;
    cvmfs::MsgStoreReply msg_reply;
    CacheTransport::Frame frame_send(&msg_reply);
    msg_reply.set_req_id(msg_req->req_id());
    msg_reply.set_part_nr(0);
    msg_reply.set_status(cvmfs::STATUS_OK);
    transport->SendFrame(&frame_send);
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
        cache_plugin->HandleHandshake(&transport);
      } else if (msg_typed->GetTypeName() == "cvmfs.MsgQuit") {
        break;
      } else if (msg_typed->GetTypeName() == "cvmfs.MsgRefcountReq") {
        cvmfs::MsgRefcountReq *msg_req =
          reinterpret_cast<cvmfs::MsgRefcountReq *>(msg_typed);
        cache_plugin->HandleRefcountRequest(msg_req, &transport);
      } else if (msg_typed->GetTypeName() == "cvmfs.MsgObjectInfoReq") {
        cvmfs::MsgObjectInfoReq *msg_req =
          reinterpret_cast<cvmfs::MsgObjectInfoReq *>(msg_typed);
        cache_plugin->HandleObjectInfo(msg_req, &transport);
      } else if (msg_typed->GetTypeName() == "cvmfs.MsgReadReq") {
        cvmfs::MsgReadReq *msg_req =
          reinterpret_cast<cvmfs::MsgReadReq *>(msg_typed);
        cache_plugin->HandleRead(msg_req, &transport);
      } else if (msg_typed->GetTypeName() == "cvmfs.MsgStoreReq") {
        cvmfs::MsgStoreReq *msg_req =
          reinterpret_cast<cvmfs::MsgStoreReq *>(msg_typed);
        cache_plugin->HandleStore(msg_req, &frame_recv, &transport);
      } else if (msg_typed->GetTypeName() == "cvmfs.MsgStoreAbortReq") {
        cvmfs::MsgStoreAbortReq *msg_req =
          reinterpret_cast<cvmfs::MsgStoreAbortReq *>(msg_typed);
        cache_plugin->HandleStoreAbort(msg_req, &transport);
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
  string new_object_content;
  unsigned latest_part;
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
  EXPECT_EQ(-EINVAL, cache_mgr_->Pread(fd, buffer, 1, 64));
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
  unsigned char *buffer;
  uint64_t size;
  EXPECT_TRUE(cache_mgr_->Open2Mem(id, "test", &buffer, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buffer), size));
  free(buffer);

  content = "";
  HashString(content, &id);
  data = NULL;
  EXPECT_TRUE(
    cache_mgr_->CommitFromMem(id, data, content.length(), "test"));
  EXPECT_TRUE(cache_mgr_->Open2Mem(id, "test", &buffer, &size));
  EXPECT_EQ(0U, size);
  EXPECT_EQ(NULL, buffer);

  unsigned large_size = 50 * 1024 * 1024;
  unsigned char *large_buffer = reinterpret_cast<unsigned char *>(
    scalloc(large_size, 1));
  EXPECT_TRUE(
    cache_mgr_->CommitFromMem(id, large_buffer, large_size, "test"));
  unsigned char *large_buffer_verify = reinterpret_cast<unsigned char *>(
    smalloc(large_size));
  EXPECT_TRUE(cache_mgr_->Open2Mem(id, "test", &large_buffer_verify, &size));
  EXPECT_EQ(large_size, size);
  EXPECT_EQ(0, memcmp(large_buffer, large_buffer_verify, large_size));
  free(large_buffer_verify);
  free(large_buffer);

  large_size = 50 * 1024 * 1024 + 1;
  large_buffer = reinterpret_cast<unsigned char *>(scalloc(large_size, 1));
  EXPECT_TRUE(
    cache_mgr_->CommitFromMem(id, large_buffer, large_size, "test"));
  large_buffer_verify = reinterpret_cast<unsigned char *>(smalloc(large_size));
  EXPECT_TRUE(cache_mgr_->Open2Mem(id, "test", &large_buffer_verify, &size));
  EXPECT_EQ(large_size, size);
  EXPECT_EQ(0, memcmp(large_buffer, large_buffer_verify, large_size));
  free(large_buffer_verify);
  free(large_buffer);

  // test unordered upload of chunks (and failure inbetween)
}

TEST_F(T_ExternalCacheManager, TransactionAbort) {
  shash::Any id(shash::kSha1);
  string content = "foo";
  HashString(content, &id);
  void *txn = alloca(cache_mgr_->SizeOfTxn());
  EXPECT_EQ(0, cache_mgr_->StartTxn(id, content.length(), txn));
  EXPECT_EQ(0, cache_mgr_->Reset(txn));
  EXPECT_EQ(2, cache_mgr_->Write(content.data(), 2, txn));
  EXPECT_EQ(0, cache_mgr_->Reset(txn));
  EXPECT_EQ(3, cache_mgr_->Write(content.data(), 3, txn));
  EXPECT_EQ(0, cache_mgr_->CommitTxn(txn));

  unsigned char *buf;
  uint64_t size;
  EXPECT_TRUE(cache_mgr_->Open2Mem(id, "test", &buf, &size));
  EXPECT_EQ(content, string(reinterpret_cast<char *>(buf), size));
  free(buf);
}
