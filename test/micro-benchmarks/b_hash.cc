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
    long_path_ = strdup("/this/is/a/longer/path/that/resembles"
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
BENCHMARK_REGISTER_F(BM_Hash, Sha1)->Repetitions(3)->Arg(100)->Arg(4096)->
  Arg(100*1024);

BENCHMARK_DEFINE_F(BM_Hash, Rmd160)(benchmark::State &st) {
  unsigned size = st.range(0);
  unsigned char buffer[size];
  shash::Any content_hash(shash::kRmd160);
  while (st.KeepRunning()) {
    HashMem(buffer, size, &content_hash);
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_Hash, Rmd160)->Repetitions(3)->Arg(100)->Arg(4096)->
  Arg(100*1024);

BENCHMARK_DEFINE_F(BM_Hash, Shake128)(benchmark::State &st) {
  unsigned size = st.range(0);
  unsigned char buffer[size];
  shash::Any content_hash(shash::kShake128);
  while (st.KeepRunning()) {
    HashMem(buffer, size, &content_hash);
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_Hash, Shake128)->Repetitions(3)->Arg(100)->Arg(4096)->
  Arg(100*1024);

BENCHMARK_DEFINE_F(BM_Hash, Sha256)(benchmark::State &st) {
  unsigned size = st.range(0);
  unsigned char buffer[size];
  std::string hex_digest;
  while (st.KeepRunning()) {
    hex_digest = shash::Sha256Mem(buffer, size);
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_Hash, Sha256)->Repetitions(3)->Arg(100)->Arg(4096)->
  Arg(100*1024);

BENCHMARK_DEFINE_F(BM_Hash, Hmac256)(benchmark::State &st) {
  unsigned size = st.range(0);
  std::string content;
  content.resize(size);
  const std::string key = "0123456789abcdefghij";
  std::string hex_digest;
  while (st.KeepRunning()) {
    hex_digest = shash::Hmac256(key, content, false /* raw_output */);
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_Hash, Hmac256)->Repetitions(3)->Arg(100)->Arg(4096)->
  Arg(100*1024);
