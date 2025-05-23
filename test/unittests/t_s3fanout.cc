/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdio>

#include "network/s3fanout.h"
#include "util/file_backed_buffer.h"

using namespace std;  // NOLINT

TEST(T_S3Fanout, DetectThrottleIndicator) {
  FileBackedBuffer *buf = FileBackedBuffer::Create(1024);
  s3fanout::JobInfo info("", NULL, buf);
  info.throttle_ms = 1;

  s3fanout::S3FanoutManager::DetectThrottleIndicator("", &info);
  EXPECT_EQ(1U, info.throttle_ms);
  s3fanout::S3FanoutManager::DetectThrottleIndicator("retry-after", &info);
  EXPECT_EQ(1U, info.throttle_ms);
  s3fanout::S3FanoutManager::DetectThrottleIndicator("retry-after:", &info);
  EXPECT_EQ(1U, info.throttle_ms);
  s3fanout::S3FanoutManager::DetectThrottleIndicator("x-retry-in:", &info);
  EXPECT_EQ(1U, info.throttle_ms);

  s3fanout::S3FanoutManager::DetectThrottleIndicator("retry-after: 1", &info);
  EXPECT_EQ(1000U, info.throttle_ms);
  s3fanout::S3FanoutManager::DetectThrottleIndicator("retry-after:5", &info);
  EXPECT_EQ(5000U, info.throttle_ms);
  s3fanout::S3FanoutManager::DetectThrottleIndicator("retry-after:42", &info);
  EXPECT_EQ(10000U, info.throttle_ms);
  s3fanout::S3FanoutManager::DetectThrottleIndicator("x-retry-in:2", &info);
  EXPECT_EQ(2000U, info.throttle_ms);
  s3fanout::S3FanoutManager::DetectThrottleIndicator("x-retry-in:0", &info);
  EXPECT_EQ(2000U, info.throttle_ms);

  s3fanout::S3FanoutManager::DetectThrottleIndicator("retry-after:13ms", &info);
  EXPECT_EQ(13U, info.throttle_ms);
  s3fanout::S3FanoutManager::DetectThrottleIndicator("retry-after:27Ms", &info);
  EXPECT_EQ(27U, info.throttle_ms);
  s3fanout::S3FanoutManager::DetectThrottleIndicator("retry-after:12000ms",
                                                     &info);
  EXPECT_EQ(10000U, info.throttle_ms);

  s3fanout::S3FanoutManager::DetectThrottleIndicator("X-Retry-In: 10ms\n",
                                                     &info);
  EXPECT_EQ(10U, info.throttle_ms);
  s3fanout::S3FanoutManager::DetectThrottleIndicator("X-Retry-In: 12ms\r\n",
                                                     &info);
  EXPECT_EQ(12U, info.throttle_ms);
}
