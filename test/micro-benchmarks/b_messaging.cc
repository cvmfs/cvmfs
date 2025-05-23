/**
 * This file is part of the CernVM File System.
 */
#include <benchmark/benchmark.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cassert>
#include <string>

#include "bm_util.h"
#include "cache_transport.h"
#include "util/posix.h"

using namespace std;  // NOLINT

class BM_Messaging : public benchmark::Fixture {
 protected:
  virtual void SetUp(const benchmark::State &st) { }

  virtual void TearDown(const benchmark::State &st) { }
};

BENCHMARK_DEFINE_F(BM_Messaging, CacheHandshake)(benchmark::State &st) {
  string socket_path = "/tmp/cvmfs_benchmark.socket";
  int fd_socket = MakeSocket(socket_path, 0600);
  int retval = listen(fd_socket, 1);
  assert(retval == 0);

  pid_t pid;
  switch (pid = fork()) {
    case -1:
      abort();
    case 0:
      struct sockaddr_un remote;
      socklen_t socket_size = sizeof(remote);
      int fd_connection = accept(
          fd_socket, (struct sockaddr *)&remote, &socket_size);
      assert(fd_connection >= 0);
      CacheTransport transport(fd_connection);
      char buffer[st.range(0)];
      while (true) {
        CacheTransport::Frame frame_recv;
        frame_recv.set_attachment(buffer, st.range(0));
        retval = transport.RecvFrame(&frame_recv);
        assert(retval);
        google::protobuf::MessageLite *msg_typed = frame_recv.GetMsgTyped();
        if (msg_typed->GetTypeName() == "cvmfs.MsgHandshake") {
          cvmfs::MsgHandshakeAck msg;
          msg.set_status(cvmfs::STATUS_OK);
          msg.set_name("unit test cache manager");
          msg.set_protocol_version(1);
          msg.set_session_id(42);
          msg.set_max_object_size(128 * 1024 * 1024);
          msg.set_capabilities(0);
          CacheTransport::Frame frame_send(&msg);
          transport.SendFrame(&frame_send);
        } else if (msg_typed->GetTypeName() == "cvmfs.MsgQuit") {
          break;
        }
      }
      shutdown(fd_connection, SHUT_RDWR);
      close(fd_connection);
      close(fd_socket);
      unlink(socket_path.c_str());
      exit(0);
  }

  int fd_client = ConnectSocket(socket_path);
  assert(fd_client >= 0);
  CacheTransport transport(fd_client);

  char buffer[st.range(0)];
  while (st.KeepRunning()) {
    cvmfs::MsgHandshake msg_handshake;
    msg_handshake.set_protocol_version(1);
    CacheTransport::Frame frame_send(&msg_handshake);
    frame_send.set_attachment(buffer, st.range(0));
    transport.SendFrame(&frame_send);

    CacheTransport::Frame frame_recv;
    bool retval = transport.RecvFrame(&frame_recv);
    assert(retval);
    google::protobuf::MessageLite *msg_typed = frame_recv.GetMsgTyped();
    assert(msg_typed->GetTypeName() == "cvmfs.MsgHandshakeAck");
    cvmfs::MsgHandshakeAck
        *msg_ack = reinterpret_cast<cvmfs::MsgHandshakeAck *>(msg_typed);
    assert(msg_ack->session_id() == 42);
  }
  st.SetItemsProcessed(st.iterations());
  st.SetBytesProcessed(int64_t(st.iterations()) * int64_t(st.range(0)));

  cvmfs::MsgQuit msg_quit;
  msg_quit.set_session_id(42);
  CacheTransport::Frame frame(&msg_quit);
  transport.SendFrame(&frame);
  close(fd_client);
  close(fd_socket);
  int statloc;
  waitpid(pid, &statloc, 0);
}
BENCHMARK_REGISTER_F(BM_Messaging, CacheHandshake)
    ->Repetitions(3)
    ->Arg(1024)
    ->Arg(128 * 1024)
    ->UseRealTime();
