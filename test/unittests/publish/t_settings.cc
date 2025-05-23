/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <stdint.h>

#include <string>

#include "publish/settings.h"

using namespace std;  // NOLINT

namespace publish {

class T_Settings : public ::testing::Test {
 protected:
  virtual void SetUp() { }

 protected:
};

TEST_F(T_Settings, Setting) {
  Setting<int32_t> sint;
  EXPECT_TRUE(sint.is_default());
  EXPECT_EQ(0, sint());
  sint = 42;
  EXPECT_FALSE(sint.is_default());
  EXPECT_EQ(42, sint());
  EXPECT_FALSE(sint.SetIfDefault(1));
  EXPECT_EQ(42, sint());

  Setting<std::string> sstring("abc");
  EXPECT_TRUE(sstring.is_default());
  EXPECT_STREQ("abc", sstring().c_str());
}

}  // namespace publish
