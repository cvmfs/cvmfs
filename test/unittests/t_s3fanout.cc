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


TEST(T_S3Fanout, ComposeDeleteMultiXmlSingleKey) {
  vector<string> keys;
  keys.push_back("data/ab/file1");
  const string xml = s3fanout::ComposeDeleteMultiXml(keys);

  EXPECT_NE(string::npos, xml.find("<?xml version=\"1.0\""));
  EXPECT_NE(string::npos, xml.find("<Delete>"));
  EXPECT_NE(string::npos, xml.find("<Quiet>true</Quiet>"));
  EXPECT_NE(string::npos, xml.find("<Object><Key>data/ab/file1</Key></Object>"));
  EXPECT_NE(string::npos, xml.find("</Delete>"));
}


TEST(T_S3Fanout, ComposeDeleteMultiXmlMultipleKeys) {
  vector<string> keys;
  keys.push_back("data/ab/file1");
  keys.push_back("data/cd/file2");
  keys.push_back("data/ef/file3");
  const string xml = s3fanout::ComposeDeleteMultiXml(keys);

  EXPECT_NE(string::npos, xml.find("<Object><Key>data/ab/file1</Key></Object>"));
  EXPECT_NE(string::npos, xml.find("<Object><Key>data/cd/file2</Key></Object>"));
  EXPECT_NE(string::npos, xml.find("<Object><Key>data/ef/file3</Key></Object>"));
}


TEST(T_S3Fanout, ComposeDeleteMultiXmlEmptyKeys) {
  vector<string> keys;
  const string xml = s3fanout::ComposeDeleteMultiXml(keys);

  EXPECT_NE(string::npos, xml.find("<Delete>"));
  EXPECT_NE(string::npos, xml.find("<Quiet>true</Quiet>"));
  EXPECT_NE(string::npos, xml.find("</Delete>"));
  EXPECT_EQ(string::npos, xml.find("<Object>"));
}


TEST(T_S3Fanout, ComposeDeleteMultiXmlEscaping) {
  vector<string> keys;
  keys.push_back("repo&name/data/ab/file1");
  keys.push_back("repo<name/data/cd/file2");
  keys.push_back("normal/data/ef/file3");
  const string xml = s3fanout::ComposeDeleteMultiXml(keys);

  EXPECT_NE(string::npos,
            xml.find("<Key>repo&amp;name/data/ab/file1</Key>"));
  EXPECT_NE(string::npos,
            xml.find("<Key>repo&lt;name/data/cd/file2</Key>"));
  EXPECT_NE(string::npos,
            xml.find("<Key>normal/data/ef/file3</Key>"));
  // Raw & and < must not appear unescaped in key values
  EXPECT_EQ(string::npos, xml.find("repo&name"));
  EXPECT_EQ(string::npos, xml.find("repo<name"));
}


TEST(T_S3Fanout, ParseDeleteMultiResponseEmpty) {
  vector<string> error_keys, error_codes, error_messages;
  const unsigned n = s3fanout::ParseDeleteMultiResponse(
      "", &error_keys, &error_codes, &error_messages);

  EXPECT_EQ(0U, n);
  EXPECT_TRUE(error_keys.empty());
}


TEST(T_S3Fanout, ParseDeleteMultiResponseSingleError) {
  const string response =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
      "<DeleteResult>"
      "<Error>"
      "<Key>data/ab/file1</Key>"
      "<Code>AccessDenied</Code>"
      "<Message>Access Denied</Message>"
      "</Error>"
      "</DeleteResult>";

  vector<string> error_keys, error_codes, error_messages;
  const unsigned n = s3fanout::ParseDeleteMultiResponse(
      response, &error_keys, &error_codes, &error_messages);

  EXPECT_EQ(1U, n);
  ASSERT_EQ(1U, error_keys.size());
  EXPECT_EQ("data/ab/file1", error_keys[0]);
  EXPECT_EQ("AccessDenied", error_codes[0]);
  EXPECT_EQ("Access Denied", error_messages[0]);
}


TEST(T_S3Fanout, ParseDeleteMultiResponseMultipleErrors) {
  const string response =
      "<DeleteResult>"
      "<Error><Key>key1</Key><Code>NoSuchKey</Code>"
      "<Message>Not found</Message></Error>"
      "<Error><Key>key2</Key><Code>InternalError</Code>"
      "<Message>Internal</Message></Error>"
      "</DeleteResult>";

  vector<string> error_keys, error_codes, error_messages;
  const unsigned n = s3fanout::ParseDeleteMultiResponse(
      response, &error_keys, &error_codes, &error_messages);

  EXPECT_EQ(2U, n);
  ASSERT_EQ(2U, error_keys.size());
  EXPECT_EQ("key1", error_keys[0]);
  EXPECT_EQ("NoSuchKey", error_codes[0]);
  EXPECT_EQ("key2", error_keys[1]);
  EXPECT_EQ("InternalError", error_codes[1]);
}


TEST(T_S3Fanout, ParseDeleteMultiResponseMalformed) {
  const string response = "<DeleteResult><Error><Key>k1</Key>";

  vector<string> error_keys, error_codes, error_messages;
  const unsigned n = s3fanout::ParseDeleteMultiResponse(
      response, &error_keys, &error_codes, &error_messages);

  EXPECT_EQ(0U, n);
}
