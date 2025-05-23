/**
 * This file is part of the CernVM File System.
 */
#include <benchmark/benchmark.h>

#include <cstdlib>
#include <cstring>

#include "bm_util.h"
#include "crypto/hash.h"
#include "util/string.h"

class BM_Hash : public benchmark::Fixture {
 protected:
  virtual void SetUp(const benchmark::State &st) {
    short_path_ = strdup("/short/path");
    long_path_ = strdup(
        "/this/is/a/longer/path/that/resembles"
        "/paths/found/in/some/or/many/experiment/software/frameworks");
  }

  virtual void TearDown(const benchmark::State &st) {
    free(short_path_);
    free(long_path_);
  }

  char *short_path_;
  char *long_path_;
};


BENCHMARK_DEFINE_F(BM_Hash, ShortPath)(benchmark::State &st) {
  size_t length = strlen(short_path_);
  while (st.KeepRunning()) {
    shash::Md5 md5(short_path_, length);
    ClobberMemory();
  }
  st.SetItemsProcessed(st.iterations());
  st.SetLabel((StringifyInt(length) + " characters").c_str());
}
BENCHMARK_REGISTER_F(BM_Hash, ShortPath)->Repetitions(3);


BENCHMARK_DEFINE_F(BM_Hash, LongPath)(benchmark::State &st) {
  size_t length = strlen(long_path_);
  while (st.KeepRunning()) {
    shash::Md5 md5(long_path_, length);
    Escape(&md5);
  }
  st.SetItemsProcessed(st.iterations());
  st.SetLabel((StringifyInt(length) + " characters").c_str());
}
BENCHMARK_REGISTER_F(BM_Hash, LongPath)->Repetitions(3);


BENCHMARK_DEFINE_F(BM_Hash, Sha1)(benchmark::State &st) {
  unsigned size = st.range(0);
  unsigned char buffer[size];
  shash::Any content_hash(shash::kSha1);
  while (st.KeepRunning()) {
    HashMem(buffer, size, &content_hash);
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_Hash, Sha1)
    ->Repetitions(3)
    ->Arg(100)
    ->Arg(4096)
    ->Arg(100 * 1024);
