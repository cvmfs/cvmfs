/**
 * This file is part of the CernVM File System.
 */
#include <benchmark/benchmark.h>

#include <fcntl.h>
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


/**
 * Command-response with 100B commands and 4k responses
 */
BENCHMARK_DEFINE_F(BM_Syscalls, Pipe)(benchmark::State &st) {
  int pipe_cmd[2];
  int pipe_data[2];
  MakePipe(pipe_cmd);
  MakePipe(pipe_data);
  switch (fork()) {
    case -1:
      abort();
    case 0:
      close(pipe_cmd[1]);
      close(pipe_data[0]);
      char cmd_buf[100];
      char data_buf[4096];
      while (true) {
        ReadPipe(pipe_cmd[0], cmd_buf, sizeof(cmd_buf));
        if (pipe_cmd[0] == 'Q')
          exit(0);
        WritePipe(pipe_data[1], data_buf, sizeof(data_buf));
      }
  }
  close(pipe_cmd[0]);
  close(pipe_data[1]);
  char cmd_buf[100];
  char data_buf[4096];
  cmd_buf[0] = 'C';

  while (st.KeepRunning()) {
    WritePipe(pipe_cmd[1], cmd_buf, sizeof(cmd_buf));
    ReadPipe(pipe_data[0], data_buf, sizeof(data_buf));
    ClobberMemory();
  }
  st.SetItemsProcessed(st.iterations());

  close(pipe_data[0]);
  cmd_buf[0] = 'Q';
  WritePipe(pipe_cmd[1], cmd_buf, sizeof(cmd_buf));
  close(pipe_cmd[1]);
}
BENCHMARK_REGISTER_F(BM_Syscalls, Pipe)->Repetitions(3)->
  UseRealTime();
