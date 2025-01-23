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
};


BENCHMARK_DEFINE_F(BM_Download, DownloadManagerCtor)(benchmark::State &st) {
  perf::Statistics stats;
  download::DownloadManager * dlMan = new download::DownloadManager(1, perf::StatisticsTemplate("download", &stats));
  st.SetItemsProcessed(st.iterations());
}

BENCHMARK_REGISTER_F(BM_Download, DownloadManagerCtor)->Repetitions(3);