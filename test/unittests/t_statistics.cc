/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"

#include "../../cvmfs/platform.h"
#include "../../cvmfs/statistics.h"

using namespace std;  // NOLINT

namespace perf {

TEST(T_Statistics, Counter) {
  Counter counter;
  EXPECT_EQ(0, counter.Get());
  counter.Set(1);
  EXPECT_EQ(1, counter.Get());
  counter.Inc();
  EXPECT_EQ(2, counter.Get());
  counter.Dec();
  EXPECT_EQ(1, counter.Get());
  EXPECT_EQ(1, counter.Xadd(-1));
  EXPECT_EQ(0, counter.Get());
  counter.Dec();
  EXPECT_EQ(-1, counter.Get());

  counter.Set(1024*1024);
  EXPECT_EQ("1048576", counter.Print());
  EXPECT_EQ("1024", counter.PrintKi());
  EXPECT_EQ("1048", counter.PrintK());
  EXPECT_EQ("1", counter.PrintM());
  EXPECT_EQ("1", counter.PrintMi());

  Counter counter2;
  EXPECT_EQ("inf", counter.PrintRatio(counter2));
  counter2.Set(1024);
  EXPECT_EQ("1024.000", counter.PrintRatio(counter2));
}


TEST(T_Statistics, Statistics) {
  Statistics statistics;

  Counter *counter = statistics.Register("test.counter", "a test counter");
  ASSERT_TRUE(counter != NULL);
  EXPECT_EQ(0, counter->Get());

  ASSERT_DEATH(statistics.Register("test.counter", "Name Clash"), ".*");
  EXPECT_EQ(0, statistics.Lookup("test.counter")->Get());
  EXPECT_EQ("a test counter", statistics.LookupDesc("test.counter"));

  EXPECT_EQ(NULL, statistics.Lookup("test.unknown"));

  EXPECT_EQ("test.counter|0|a test counter\n",
            statistics.PrintList(Statistics::kPrintSimple));
}


TEST(T_Statistics, RecorderConstruct) {
  Recorder recorder(5, 10);
  EXPECT_EQ(10U, recorder.capacity_s());
  Recorder recorder2(5, 9);
  EXPECT_EQ(10U, recorder2.capacity_s());
  Recorder recorder3(5, 6);
  EXPECT_EQ(10U, recorder3.capacity_s());
  Recorder recorder4(1, 10);
  EXPECT_EQ(10U, recorder4.capacity_s());
}


TEST(T_Statistics, RecorderTick) {
  Recorder recorder(1, 10);

  EXPECT_EQ(0U, recorder.GetNoTicks(0));
  EXPECT_EQ(0U, recorder.GetNoTicks(1));
  EXPECT_EQ(0U, recorder.GetNoTicks(uint32_t(-1)));

  recorder.Tick();
  recorder.TickAt(platform_monotonic_time() - 5);
  EXPECT_EQ(1U, recorder.GetNoTicks(1));
  EXPECT_EQ(2U, recorder.GetNoTicks(uint32_t(-1)));

  // Don't record tick in the distant past
  recorder.TickAt(0);
  EXPECT_EQ(2U, recorder.GetNoTicks(uint32_t(-1)));

  // Many ticks in a past period
  Recorder recorder2(1, 10);
  for (unsigned i = 0; i < 10; ++i)
    recorder2.TickAt(i);
  EXPECT_EQ(0U, recorder2.GetNoTicks(1));
  EXPECT_EQ(10U, recorder2.GetNoTicks(uint32_t(-1)));
  // Long gap with no ticks
  recorder2.Tick();
  EXPECT_EQ(1U, recorder2.GetNoTicks(uint32_t(-1)));

  // Ticks not starting at zero
  Recorder recorder3(1, 10);
  for (unsigned i = 2; i < 12; ++i)
    recorder3.TickAt(i);
  EXPECT_EQ(0U, recorder3.GetNoTicks(1));
  EXPECT_EQ(10U, recorder3.GetNoTicks(uint32_t(-1)));

  // More coarse-grained binning, ring buffer overflow
  Recorder recorder4(2, 10);
  for (unsigned i = 2; i < 22; ++i)
    recorder4.TickAt(i);
  EXPECT_EQ(0U, recorder4.GetNoTicks(1));
  EXPECT_EQ(10U, recorder4.GetNoTicks(uint32_t(-1)));

  // Clear bins
  Recorder recorder5(1, 10);
  for (unsigned i = 2; i < 12; ++i)
    recorder5.TickAt(i);
  recorder5.TickAt(14);
  EXPECT_EQ(8U, recorder5.GetNoTicks(uint32_t(-1)));
}


TEST(T_Statistics, MultiRecorder) {
  MultiRecorder recorder;
  recorder.Tick();
  EXPECT_EQ(0U, recorder.GetNoTicks(uint32_t(-1)));

  recorder.AddRecorder(1, 10);
  recorder.AddRecorder(2, 20);
  for (unsigned i = 2; i < 22; ++i)
    recorder.TickAt(i);
  EXPECT_EQ(20U, recorder.GetNoTicks(uint32_t(-1)));
  recorder.Tick();
  EXPECT_EQ(1U, recorder.GetNoTicks(1));
  EXPECT_EQ(1U, recorder.GetNoTicks(uint32_t(-1)));
}

}  // namespace perf
