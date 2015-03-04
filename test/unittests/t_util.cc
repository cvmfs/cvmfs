/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <pthread.h>
#include <tbb/tbb_thread.h>

#include <vector>

#include "../../cvmfs/util.h"

using namespace std;  // NOLINT

class ThreadDummy {
 public:
  explicit ThreadDummy(int canary_value)
    : result_value(0)
    , value_(canary_value)
  { }

  void OtherThread() {
    result_value = value_;
  }

  int result_value;

 private:
  const int value_;
};


TEST(T_Util, ThreadProxy) {
  const int canary = 1337;

  ThreadDummy dummy(canary);
  tbb::tbb_thread thread(&ThreadProxy<ThreadDummy>,
                         &dummy,
                         &ThreadDummy::OtherThread);
  thread.join();

  EXPECT_EQ(canary, dummy.result_value);
}


TEST(T_Util, GetUidOf) {
  uid_t uid;
  gid_t gid;
  EXPECT_TRUE(GetUidOf("root", &uid, &gid));
  EXPECT_EQ(0U, uid);
  EXPECT_EQ(0U, gid);
  EXPECT_FALSE(GetUidOf("no-such-user", &uid, &gid));
}


TEST(T_Util, GetGidOf) {
  gid_t gid;
  EXPECT_TRUE(GetGidOf("root", &gid));
  EXPECT_EQ(0U, gid);
  EXPECT_FALSE(GetGidOf("no-such-group", &gid));
}


TEST(T_Util, IsAbsolutePath) {
  const bool empty = IsAbsolutePath("");
  EXPECT_FALSE(empty) << "empty path string treated as absolute";

  const bool relative = IsAbsolutePath("foo.bar");
  EXPECT_FALSE(relative) << "relative path treated as absolute";
  const bool absolute = IsAbsolutePath("/tmp/foo.bar");
  EXPECT_TRUE(absolute) << "absolute path not recognized";
}


TEST(T_Util, HasSuffix) {
  EXPECT_TRUE(HasSuffix("abc-foo", "-foo", false));
  EXPECT_FALSE(HasSuffix("abc-foo", "-FOO", false));
  EXPECT_TRUE(HasSuffix("abc-foo", "-FOO", true));
  EXPECT_TRUE(HasSuffix("", "", false));
  EXPECT_TRUE(HasSuffix("abc", "", false));
  EXPECT_TRUE(HasSuffix("-foo", "-foo", false));
  EXPECT_FALSE(HasSuffix("abc+foo", "-foo", false));
  EXPECT_FALSE(HasSuffix("foo", "-foo", false));
}


TEST(T_Util, Shuffle) {
  vector<int> v;
  Prng prng;
  vector<int> shuffled = Shuffle(v, &prng);
  EXPECT_EQ(v, shuffled);

  v.push_back(2);
  v.push_back(3);
  v.push_back(5);
  v.push_back(7);
  shuffled = Shuffle(v, &prng);
  ASSERT_EQ(v.size(), shuffled.size());
  EXPECT_NE(v, shuffled);
  int prod_v = 1;
  int prod_shuffled = 1;
  for (unsigned i = 0; i < shuffled.size(); ++i) {
    prod_v *= v[i];
    prod_shuffled *= shuffled[i];
  }
  EXPECT_EQ(prod_shuffled, prod_v);
}


TEST(T_Util, SortTeam) {
  vector<int> tractor;
  vector<string> towed;

  SortTeam(&tractor, &towed);
  ASSERT_TRUE(tractor.empty());
  ASSERT_TRUE(towed.empty());

  tractor.push_back(1);
  towed.push_back("one");
  SortTeam(&tractor, &towed);
  ASSERT_EQ(tractor.size(), towed.size());
  ASSERT_EQ(tractor.size(), 1U);
  EXPECT_EQ(tractor[0], 1);
  EXPECT_EQ(towed[0], "one");

  tractor.push_back(2);
  towed.push_back("two");
  SortTeam(&tractor, &towed);
  ASSERT_EQ(tractor.size(), towed.size());
  ASSERT_EQ(tractor.size(), 2U);
  EXPECT_EQ(tractor[0], 1);
  EXPECT_EQ(tractor[1], 2);
  EXPECT_EQ(towed[0], "one");
  EXPECT_EQ(towed[1], "two");

  tractor.push_back(3);
  towed.push_back("three");
  SortTeam(&tractor, &towed);
  ASSERT_EQ(tractor.size(), towed.size());
  ASSERT_EQ(tractor.size(), 3U);
  EXPECT_EQ(tractor[0], 1);
  EXPECT_EQ(tractor[1], 2);
  EXPECT_EQ(tractor[2], 3);
  EXPECT_EQ(towed[0], "one");
  EXPECT_EQ(towed[1], "two");
  EXPECT_EQ(towed[2], "three");

  tractor.clear();
  towed.clear();
  tractor.push_back(3);
  tractor.push_back(2);
  tractor.push_back(1);
  towed.push_back("three");
  towed.push_back("two");
  towed.push_back("one");
  SortTeam(&tractor, &towed);
  ASSERT_EQ(tractor.size(), towed.size());
  ASSERT_EQ(tractor.size(), 3U);
  EXPECT_EQ(tractor[0], 1);
  EXPECT_EQ(tractor[1], 2);
  EXPECT_EQ(tractor[2], 3);
  EXPECT_EQ(towed[0], "one");
  EXPECT_EQ(towed[1], "two");
  EXPECT_EQ(towed[2], "three");
}


TEST(T_Util, String2Uint64) {
  EXPECT_EQ(String2Uint64("0"), 0U);
  EXPECT_EQ(String2Uint64("10"), 10U);
  EXPECT_EQ(String2Uint64("18446744073709551615000"), 18446744073709551615LLU);
  EXPECT_EQ(String2Uint64("1a"), 1U);
}


TEST(T_Util, IsHttpUrl) {
  EXPECT_TRUE(IsHttpUrl("http://cvmfs-stratum-one.cern.ch/cvmfs/cms.cern.ch"));
  EXPECT_TRUE(IsHttpUrl("http://"));
  EXPECT_TRUE(IsHttpUrl("http://foobar"));
  EXPECT_TRUE(IsHttpUrl("HTTP://www.google.com"));
  EXPECT_TRUE(IsHttpUrl("HTtP://cvmfs-stratum-zero.cern.ch/ot/atlas"));
  EXPECT_FALSE(IsHttpUrl("http:/foobar"));
  EXPECT_FALSE(IsHttpUrl("http"));
  EXPECT_FALSE(IsHttpUrl("/srv/cvmfs/cms.cern.ch"));
  EXPECT_FALSE(IsHttpUrl("srv/cvmfs/cms.cern.ch"));
  EXPECT_FALSE(IsHttpUrl("http//foobar"));
}
