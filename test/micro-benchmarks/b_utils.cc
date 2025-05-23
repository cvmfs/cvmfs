/**
 * This file is part of the CernVM File System.
 */

#include <benchmark/benchmark.h>

#include "bm_util.h"
#include "util/algorithm.h"
#include "util/platform.h"

class BM_Utils : public benchmark::Fixture {
 protected:
  virtual void SetUp(const benchmark::State &st) { }

  virtual void TearDown(const benchmark::State &st) { }
};


BENCHMARK_DEFINE_F(BM_Utils, GetMonotonicTime)(benchmark::State &st) {
  uint64_t now;
  while (st.KeepRunning()) {
    now = platform_monotonic_time();
    Escape(&now);
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_Utils, GetMonotonicTime)->Repetitions(3)->UseRealTime();


BENCHMARK_DEFINE_F(BM_Utils, GetPreciseTime)(benchmark::State &st) {
  uint64_t now;
  while (st.KeepRunning()) {
    now = platform_monotonic_time_ns();
    Escape(&now);
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_Utils, GetPreciseTime)->Repetitions(3)->UseRealTime();


BENCHMARK_DEFINE_F(BM_Utils, HighPrecisionTimer)(benchmark::State &st) {
  HighPrecisionTimer::g_is_enabled = true;
  Log2Histogram recorder(30);
  while (st.KeepRunning()) {
    {
      HighPrecisionTimer timer(&recorder);
    }
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_Utils, HighPrecisionTimer)
    ->Repetitions(3)
    ->UseRealTime();


BENCHMARK_DEFINE_F(BM_Utils, HighPrecisionTimerIdle)(benchmark::State &st) {
  HighPrecisionTimer::g_is_enabled = false;
  Log2Histogram recorder(30);
  while (st.KeepRunning()) {
    {
      HighPrecisionTimer timer(&recorder);
    }
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_Utils, HighPrecisionTimerIdle)
    ->Repetitions(3)
    ->UseRealTime();
