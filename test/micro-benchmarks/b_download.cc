/**
 * This file is part of the CernVM File System.
 */
#include <benchmark/benchmark.h>

#include "bm_util.h"
#include "network/download.h"
#include "statistics.h"

class BM_Download : public benchmark::Fixture {
 protected:
  virtual void SetUp(const benchmark::State &st) {
  }

  virtual void TearDown(const benchmark::State &st) {
  }
  perf::Statistics *stats{nullptr};
};

BENCHMARK_DEFINE_F(BM_Download, DownloadManagerCtor)(benchmark::State &st) {
  while (st.KeepRunning()) {
    // Clean up from the previous iteration (does nothing if it is the first iteration)
    st.PauseTiming(); // Pause timing, because deleting and creating stats takes long and is not part of the test
    delete stats;
    stats = new perf::Statistics;
    st.ResumeTiming();
    download::DownloadManager(1, perf::StatisticsTemplate("download", stats));
  }
  st.SetItemsProcessed(st.iterations());
}

BENCHMARK_REGISTER_F(BM_Download, DownloadManagerCtor)->Repetitions(3);