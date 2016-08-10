/**
 * This file is part of the CernVM File System.
 */
#include <benchmark/benchmark.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "bm_util.h"
#include "platform.h"
#include "util/posix.h"

class BM_Syscalls : public benchmark::Fixture {
 protected:
  virtual void SetUp(const benchmark::State &st) {
  }

  virtual void TearDown(const benchmark::State &st) {
  }
};

BENCHMARK_DEFINE_F(BM_Syscalls, FileOpenClose)(benchmark::State &st) {
  while (st.KeepRunning()) {
    int fd = open("/dev/null", O_RDONLY);
    close(fd);
  }
  st.SetItemsProcessed(st.iterations());
  st.SetLabel("/dev/null");
}
BENCHMARK_REGISTER_F(BM_Syscalls, FileOpenClose)->Repetitions(3)->
  UseRealTime();


BENCHMARK_DEFINE_F(BM_Syscalls, Stat)(benchmark::State &st) {
  platform_stat64 info;
  while (st.KeepRunning()) {
    platform_stat("/dev/null", &info);
    ClobberMemory();
  }
  st.SetItemsProcessed(st.iterations());
  st.SetLabel("/dev/null");
}
BENCHMARK_REGISTER_F(BM_Syscalls, Stat)->Repetitions(3)->
  UseRealTime();


BENCHMARK_DEFINE_F(BM_Syscalls, Read)(benchmark::State &st) {
  int fd = open("/dev/zero", O_RDONLY);
  unsigned size = st.range_x();
  char *buf[size];
  assert(fd >= 0);

  while (st.KeepRunning()) {
    SafeRead(fd, buf, size);
    Escape(buf);
  }
  st.SetItemsProcessed(st.iterations());
  st.SetBytesProcessed(int64_t(st.iterations()) * int64_t(size));
  st.SetLabel("/dev/zero");

  close(fd);
}
BENCHMARK_REGISTER_F(BM_Syscalls, Read)->Repetitions(3)->
  UseRealTime()->Arg(4096)->Arg(128*1024);


/**
 * Command-response with 100B commands and 4k responses
 */
BENCHMARK_DEFINE_F(BM_Syscalls, Pipe)(benchmark::State &st) {
  int pipe_cmd[2];
  int pipe_data[2];
  char cmd_buf[100];
  char data_buf[st.range_x()];

  MakePipe(pipe_cmd);
  MakePipe(pipe_data);
  switch (fork()) {
    case -1:
      abort();
    case 0:
      close(pipe_cmd[1]);
      close(pipe_data[0]);
      while (true) {
        ReadPipe(pipe_cmd[0], cmd_buf, sizeof(cmd_buf));
        if (cmd_buf[0] == 'Q')
          exit(0);
        WritePipe(pipe_data[1], data_buf, sizeof(data_buf));
      }
  }
  close(pipe_cmd[0]);
  close(pipe_data[1]);
  cmd_buf[0] = 'C';

  while (st.KeepRunning()) {
    WritePipe(pipe_cmd[1], cmd_buf, sizeof(cmd_buf));
    SafeRead(pipe_data[0], data_buf, sizeof(data_buf));
    ClobberMemory();
  }
  st.SetItemsProcessed(st.iterations());
  st.SetBytesProcessed(int64_t(st.iterations()) * int64_t(st.range_x()));


  close(pipe_data[0]);
  cmd_buf[0] = 'Q';
  WritePipe(pipe_cmd[1], cmd_buf, sizeof(cmd_buf));
  close(pipe_cmd[1]);
}
BENCHMARK_REGISTER_F(BM_Syscalls, Pipe)->Repetitions(3)->
  UseRealTime()->Arg(4096)->Arg(128*1024)->Arg(1024*1024);

BENCHMARK_DEFINE_F(BM_Syscalls, Socket)(benchmark::State &st) {
  int rc;
  int sock_cmd[2];
  int sock_data[2];
  char cmd_buf[100];
  char data_buf[st.range_x()];

  rc = socketpair(AF_UNIX, SOCK_STREAM, 0, sock_cmd);
  assert(rc == 0);
  rc = socketpair(AF_UNIX, SOCK_STREAM, 0, sock_data);
  assert(rc == 0);
  switch (fork()) {
    case -1:
      abort();
    case 0:
      close(sock_cmd[1]);
      close(sock_data[0]);
      while (true) {
        ReadPipe(sock_cmd[0], cmd_buf, sizeof(cmd_buf));
        if (cmd_buf[0] == 'Q')
          exit(0);
        WritePipe(sock_data[1], data_buf, sizeof(data_buf));
      }
  }
  close(sock_cmd[0]);
  close(sock_data[1]);
  cmd_buf[0] = 'C';

  while (st.KeepRunning()) {
    WritePipe(sock_cmd[1], cmd_buf, sizeof(cmd_buf));
    SafeRead(sock_data[0], data_buf, sizeof(data_buf));
    ClobberMemory();
  }
  st.SetItemsProcessed(st.iterations());
  st.SetBytesProcessed(int64_t(st.iterations()) * int64_t(st.range_x()));


  close(sock_data[0]);
  cmd_buf[0] = 'Q';
  WritePipe(sock_cmd[1], cmd_buf, sizeof(cmd_buf));
  close(sock_cmd[1]);
}
BENCHMARK_REGISTER_F(BM_Syscalls, Socket)->Repetitions(3)->
  UseRealTime()->Arg(4*1024)->Arg(128*1024)->Arg(1024*1024);

BENCHMARK_DEFINE_F(BM_Syscalls, SocketFd)(benchmark::State &st) {
  int rc;
  int sock_cmd[2];
  int sock_data[2];
  char cmd_buf[100];
  char data_buf[st.range_x()];

  const char *shm_path = "/cvmfs_socketfd.test";
  int memfd = shm_open(shm_path, O_RDWR|O_CREAT|O_TRUNC, 0666);
  assert(memfd >= 0);
  rc = shm_unlink(shm_path);
  assert(rc == 0);
  rc = ftruncate(memfd, st.range_x());
  assert(rc == 0);

  rc = socketpair(SOL_SOCKET, SOCK_STREAM, 0, sock_cmd);
  assert(rc == 0);
  rc = socketpair(AF_UNIX, SOCK_STREAM, 0, sock_data);
  assert(rc == 0);

  struct msghdr msg = { 0 };
  struct cmsghdr *cmsg;
  int *fdptr;
  union {
    char buf[CMSG_SPACE(sizeof(int))];
    struct cmsghdr align;
  } u;
  struct iovec io = { .iov_base = const_cast<char *>(""), .iov_len = 1 };
  msg.msg_iov = &io;
  msg.msg_iovlen = 1;
  msg.msg_control = u.buf;
  msg.msg_controllen = sizeof(u.buf);
  cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(sizeof(int));
  fdptr = reinterpret_cast<int *>(CMSG_DATA(cmsg));
  memcpy(fdptr, &memfd, sizeof(int));


  rc = socketpair(AF_UNIX, SOCK_STREAM, 0, sock_cmd);
  assert(rc == 0);
  rc = socketpair(AF_UNIX, SOCK_STREAM, 0, sock_data);
  assert(rc == 0);
  switch (fork()) {
    case -1:
      abort();
    case 0:
      close(sock_cmd[1]);
      close(sock_data[0]);
      while (true) {
        ReadPipe(sock_cmd[0], cmd_buf, sizeof(cmd_buf));
        if (cmd_buf[0] == 'Q')
          exit(0);
        rc = sendmsg(sock_data[1], &msg, 0);
        assert(rc >= 0);
      }
  }
  close(sock_cmd[0]);
  close(sock_data[1]);
  close(memfd);
  cmd_buf[0] = 'C';

  memset(&msg, 0, sizeof(msg));
  char m_buffer[256];
  memset(&io, 0, sizeof(io));
  io.iov_base = m_buffer;
  io.iov_len = sizeof(m_buffer);
  msg.msg_iov = &io;
  msg.msg_iovlen = 1;

  char c_buffer[256];
  msg.msg_control = c_buffer;
  msg.msg_controllen = sizeof(c_buffer);

  while (st.KeepRunning()) {
    WritePipe(sock_cmd[1], cmd_buf, sizeof(cmd_buf));
    rc = recvmsg(sock_data[0], &msg, 0);
    assert(rc >= 0);
    for (cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL;
         cmsg = CMSG_NXTHDR(&msg, cmsg)) {
      if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS) {
        fdptr = reinterpret_cast<int *>(CMSG_DATA(cmsg));
        memfd = *fdptr;
        break;
      }
    }
    assert(cmsg);
    void *mapped_buf = mmap(NULL, st.range_x(), PROT_READ,
                            MAP_PRIVATE, memfd, 0);
    assert(mapped_buf != MAP_FAILED);
    close(memfd);
    memcpy(data_buf, mapped_buf, st.range_x());
    munmap(mapped_buf, st.range_x());
    ClobberMemory();
  }
  st.SetItemsProcessed(st.iterations());
  st.SetBytesProcessed(int64_t(st.iterations()) * int64_t(st.range_x()));


  close(sock_data[0]);
  cmd_buf[0] = 'Q';
  WritePipe(sock_cmd[1], cmd_buf, sizeof(cmd_buf));
  close(sock_cmd[1]);
}
BENCHMARK_REGISTER_F(BM_Syscalls, SocketFd)->Repetitions(3)->
  UseRealTime()->Arg(4*1024)->Arg(128*1024)->Arg(1024*1024);


BENCHMARK_DEFINE_F(BM_Syscalls, SocketFdRead)(benchmark::State &st) {
  int rc;
  int sock_cmd[2];
  int sock_data[2];
  char cmd_buf[100];
  char data_buf[st.range_x()];

  const char *shm_path = "/cvmfs_socketfd.test";
  int memfd = shm_open(shm_path, O_RDWR|O_CREAT|O_TRUNC, 0666);
  assert(memfd >= 0);
  rc = shm_unlink(shm_path);
  assert(rc == 0);
  rc = ftruncate(memfd, st.range_x());
  assert(rc == 0);

  rc = socketpair(SOL_SOCKET, SOCK_STREAM, 0, sock_cmd);
  assert(rc == 0);
  rc = socketpair(AF_UNIX, SOCK_STREAM, 0, sock_data);
  assert(rc == 0);

  struct msghdr msg = { 0 };
  struct cmsghdr *cmsg;
  int *fdptr;
  union {
    char buf[CMSG_SPACE(sizeof(int))];
    struct cmsghdr align;
  } u;
  struct iovec io = { .iov_base = const_cast<char *>(""), .iov_len = 1 };
  msg.msg_iov = &io;
  msg.msg_iovlen = 1;
  msg.msg_control = u.buf;
  msg.msg_controllen = sizeof(u.buf);
  cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(sizeof(int));
  fdptr = reinterpret_cast<int *>(CMSG_DATA(cmsg));
  memcpy(fdptr, &memfd, sizeof(int));


  rc = socketpair(AF_UNIX, SOCK_STREAM, 0, sock_cmd);
  assert(rc == 0);
  rc = socketpair(AF_UNIX, SOCK_STREAM, 0, sock_data);
  assert(rc == 0);
  switch (fork()) {
    case -1:
      abort();
    case 0:
      close(sock_cmd[1]);
      close(sock_data[0]);
      while (true) {
        ReadPipe(sock_cmd[0], cmd_buf, sizeof(cmd_buf));
        if (cmd_buf[0] == 'Q')
          exit(0);
        rc = sendmsg(sock_data[1], &msg, 0);
        assert(rc >= 0);
      }
  }
  close(sock_cmd[0]);
  close(sock_data[1]);
  close(memfd);
  cmd_buf[0] = 'C';

  memset(&msg, 0, sizeof(msg));
  char m_buffer[256];
  memset(&io, 0, sizeof(io));
  io.iov_base = m_buffer;
  io.iov_len = sizeof(m_buffer);
  msg.msg_iov = &io;
  msg.msg_iovlen = 1;

  char c_buffer[256];
  msg.msg_control = c_buffer;
  msg.msg_controllen = sizeof(c_buffer);

  while (st.KeepRunning()) {
    WritePipe(sock_cmd[1], cmd_buf, sizeof(cmd_buf));
    rc = recvmsg(sock_data[0], &msg, 0);
    assert(rc >= 0);
    for (cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL;
         cmsg = CMSG_NXTHDR(&msg, cmsg)) {
      if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS) {
        fdptr = reinterpret_cast<int *>(CMSG_DATA(cmsg));
        memfd = *fdptr;
        break;
      }
    }
    assert(cmsg);
    SafeRead(memfd, data_buf, sizeof(data_buf));
    close(memfd);
    ClobberMemory();
  }
  st.SetItemsProcessed(st.iterations());
  st.SetBytesProcessed(int64_t(st.iterations()) * int64_t(st.range_x()));


  close(sock_data[0]);
  cmd_buf[0] = 'Q';
  WritePipe(sock_cmd[1], cmd_buf, sizeof(cmd_buf));
  close(sock_cmd[1]);
}
BENCHMARK_REGISTER_F(BM_Syscalls, SocketFdRead)->Repetitions(3)->
  UseRealTime()->Arg(4*1024)->Arg(128*1024)->Arg(1024*1024);
