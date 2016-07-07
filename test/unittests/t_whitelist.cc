/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <ctime>

#include "util/string.h"
#include "whitelist.h"

using namespace std;  // NOLINT

namespace whitelist {

class T_Whitelist : public ::testing::Test {
 protected:
  virtual void SetUp() {
    wl_ = NULL;
  }

  virtual void TearDown() {
    delete wl_;
  }

  Whitelist *wl_;
};

TEST_F(T_Whitelist, ParseWhitelist) {
  wl_ = new Whitelist();  // private constructor;
  wl_->fqrn_ = "abc";

  string text;
  EXPECT_EQ(kFailMalformed, wl_->ParseWhitelist(
    reinterpret_cast<const unsigned char *>(text.data()), text.size()));
  text = "XXXXXXXXXXXXXX\n19840403000000";
  EXPECT_EQ(kFailMalformed, wl_->ParseWhitelist(
    reinterpret_cast<const unsigned char *>(text.data()), text.size()));
  text = "XXXXXXXXXXXXXX\nE19840403000000";
  EXPECT_EQ(kFailExpired, wl_->ParseWhitelist(
    reinterpret_cast<const unsigned char *>(text.data()), text.size()));
  time_t timestamp_earlier = time(NULL) - 7200;
  struct tm now;
  ASSERT_TRUE(gmtime_r(&timestamp_earlier, &now) != NULL);
  char buffer[16];
  snprintf(buffer, sizeof(buffer), "%04d%02d%02d%02d%02d%02d",
           now.tm_year + 1900, now.tm_mon + 1, now.tm_mday,
           now.tm_hour, now.tm_min, now.tm_sec);
  text = "XXXXXXXXXXXXXX\nE" + string(buffer);
  EXPECT_EQ(kFailExpired, wl_->ParseWhitelist(
    reinterpret_cast<const unsigned char *>(text.data()), text.size()));
  time_t timestamp_later = time(NULL) + 7200;
  ASSERT_TRUE(gmtime_r(&timestamp_later, &now) != NULL);
  snprintf(buffer, sizeof(buffer), "%04d%02d%02d%02d%02d%02d",
           now.tm_year + 1900, now.tm_mon + 1, now.tm_mday,
           now.tm_hour, now.tm_min, now.tm_sec);
  text = "XXXXXXXXXXXXXX\nE" + string(buffer);
  EXPECT_EQ(kFailNameMismatch, wl_->ParseWhitelist(
    reinterpret_cast<const unsigned char *>(text.data()), text.size()));
  text += "\nNabc";
  EXPECT_EQ(kFailOk, wl_->ParseWhitelist(
    reinterpret_cast<const unsigned char *>(text.data()), text.size()));
}

}  // namespace whitelist
