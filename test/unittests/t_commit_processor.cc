/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <ctime>
#include <string>

#include "receiver/commit_processor.h"

using receiver::ParseRelativeTimespan;

namespace {
// Fixed reference point so the parser is deterministic: 2023-11-14T22:13:20Z.
const time_t kNow = 1700000000;
}  // anonymous namespace

class T_ParseRelativeTimespan : public ::testing::Test {};

TEST_F(T_ParseRelativeTimespan, FixedLengthUnits) {
  EXPECT_EQ(kNow - 1, ParseRelativeTimespan("1 second ago", kNow));
  EXPECT_EQ(kNow - 45, ParseRelativeTimespan("45 seconds ago", kNow));
  EXPECT_EQ(kNow - 90 * 60, ParseRelativeTimespan("90 minutes ago", kNow));
  EXPECT_EQ(kNow - 3 * 3600, ParseRelativeTimespan("3 hours ago", kNow));
  EXPECT_EQ(kNow - 30 * 86400, ParseRelativeTimespan("30 days ago", kNow));
  EXPECT_EQ(kNow - 4 * 604800, ParseRelativeTimespan("4 weeks ago", kNow));
  // "0 ... ago" resolves to exactly now (everything strictly older is removed).
  EXPECT_EQ(kNow, ParseRelativeTimespan("0 days ago", kNow));
}

TEST_F(T_ParseRelativeTimespan, AbbreviatedUnits) {
  EXPECT_EQ(kNow - 10, ParseRelativeTimespan("10 sec ago", kNow));
  EXPECT_EQ(kNow - 5 * 60, ParseRelativeTimespan("5 min ago", kNow));
}

TEST_F(T_ParseRelativeTimespan, CaseAndWhitespaceInsensitive) {
  EXPECT_EQ(kNow - 30 * 86400, ParseRelativeTimespan("30 DAYS AGO", kNow));
  EXPECT_EQ(kNow - 7 * 86400, ParseRelativeTimespan("7 Days Ago", kNow));
  EXPECT_EQ(kNow - 30 * 86400,
            ParseRelativeTimespan("  30   days   ago  ", kNow));
}

TEST_F(T_ParseRelativeTimespan, CalendarUnitsAreCalendarAware) {
  // Months and years go through mktime() to stay calendar-accurate, matching
  // GNU date. Compare against the same broken-down-time arithmetic.
  struct tm broken_time;

  localtime_r(&kNow, &broken_time);
  broken_time.tm_mon -= 1;
  EXPECT_EQ(mktime(&broken_time), ParseRelativeTimespan("1 month ago", kNow));

  localtime_r(&kNow, &broken_time);
  broken_time.tm_mon -= 6;
  EXPECT_EQ(mktime(&broken_time), ParseRelativeTimespan("6 months ago", kNow));

  localtime_r(&kNow, &broken_time);
  broken_time.tm_year -= 2;
  EXPECT_EQ(mktime(&broken_time), ParseRelativeTimespan("2 years ago", kNow));
}

TEST_F(T_ParseRelativeTimespan, RejectsUnsupportedInput) {
  EXPECT_EQ(0, ParseRelativeTimespan("", kNow));
  EXPECT_EQ(0, ParseRelativeTimespan("garbage", kNow));
  EXPECT_EQ(0, ParseRelativeTimespan("30 days", kNow));       // missing "ago"
  EXPECT_EQ(0, ParseRelativeTimespan("tomorrow", kNow));      // single token
  EXPECT_EQ(0, ParseRelativeTimespan("2024-01-01", kNow));    // absolute date
  EXPECT_EQ(0, ParseRelativeTimespan("days ago", kNow));      // no number
  EXPECT_EQ(0, ParseRelativeTimespan("3x days ago", kNow));   // non-numeric N
  EXPECT_EQ(0, ParseRelativeTimespan("30 fortnights ago", kNow));  // bad unit
  EXPECT_EQ(0, ParseRelativeTimespan("30 days hence", kNow));      // bad tail
}
