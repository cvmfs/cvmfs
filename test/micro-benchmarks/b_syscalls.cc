/**
 * This file is part of the CernVM File System.
 */
#include <benchmark/benchmark.h>

#include <fcntl.h>
#include <unistd.h>

#include "bm_util.h"
#include "platform.h"

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
  st.SetLabel("/dev/null");
}
BENCHMARK_REGISTER_F(BM_Syscalls, FileOpenClose)->Repetitions(3);


BENCHMARK_DEFINE_F(BM_Syscalls, Stat)(benchmark::State &st) {
  platform_stat64 info;
  while (st.KeepRunning()) {
    platform_stat("/dev/null", &info);
    ClobberMemory();
  }
  st.SetLabel("/dev/null");
}
BENCHMARK_REGISTER_F(BM_Syscalls, Stat)->Repetitions(3);
