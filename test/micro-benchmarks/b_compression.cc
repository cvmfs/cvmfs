/**
 * This file is part of the CernVM File System.
 */
#include <benchmark/benchmark.h>
#include <inttypes.h>

#include <cstdlib>
#include <cstring>

#include "bm_util.h"
#include "compression/compression.h"

class BM_Compression : public benchmark::Fixture {
 protected:
  virtual void SetUp(const benchmark::State &st) { }

  virtual void TearDown(const benchmark::State &st) { }
};


BENCHMARK_DEFINE_F(BM_Compression, Zlib)(benchmark::State &st) {
  unsigned size = st.range(0);
  unsigned char buffer[size];
  while (st.KeepRunning()) {
    void *out_buf;
    uint64_t out_size;
    zlib::CompressMem2Mem(buffer, size, &out_buf, &out_size);
    free(out_buf);
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_Compression, Zlib)
    ->Repetitions(3)
    ->Arg(100)
    ->Arg(4096)
    ->Arg(100 * 1024);
