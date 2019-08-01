/**
 * This file is part of the CernVM File System.
 */

#include <benchmark/benchmark.h>

#include "bm_util.h"
#include "platform.h"

class BM_Utils : public benchmark::Fixture {
 protected:
  virtual void SetUp(const benchmark::State &st) {
  }

  virtual void TearDown(const benchmark::State &st) {
  }
};

BENCHMARK_DEFINE_F(BM_Utils, GetPreciseTime)(benchmark::State &st) {
  while (st.KeepRunning()) {
    platform_monotonic_time_ns();
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_Utils, GetPreciseTime)->Repetitions(3)->
  UseRealTime();

