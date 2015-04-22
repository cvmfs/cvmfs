/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"

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

  ASSERT_EQ(statistics.Register("test.counter", "Name Clash"), counter);
  EXPECT_EQ(0, statistics.Lookup("test.counter")->Get());
  EXPECT_EQ("a test counter", statistics.LookupDesc("test.counter"));

  EXPECT_EQ(NULL, statistics.Lookup("test.unknown"));

  EXPECT_EQ("test.counter|0|a test counter\n",
            statistics.PrintList(Statistics::kPrintSimple));
}

}  // namespace perf
