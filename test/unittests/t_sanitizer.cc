/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/sanitizer.h"

class T_Sanitizer : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }
};


TEST_F(T_Sanitizer, AlphaNum) {
  sanitizer::AlphaNumSanitizer test_sanitizer;

  EXPECT_TRUE(test_sanitizer.IsValid("abcABC012"));
  EXPECT_FALSE(test_sanitizer.IsValid("abcABC-012"));
  EXPECT_EQ(test_sanitizer.Filter("abcABC-012"), "abcABC012");
}


TEST_F(T_Sanitizer, Empty) {
  sanitizer::InputSanitizer test_sanitizer("");

  EXPECT_FALSE(test_sanitizer.IsValid("abc"));
  EXPECT_EQ(test_sanitizer.Filter("abc"), "");
}


TEST_F(T_Sanitizer, Single) {
  sanitizer::InputSanitizer test_sanitizer("-");

  EXPECT_FALSE(test_sanitizer.IsValid("a-b"));
  EXPECT_EQ(test_sanitizer.Filter("a-b"), "-");
}

TEST_F(T_Sanitizer, Space) {
  sanitizer::InputSanitizer test_sanitizer("  az AZ");

  EXPECT_TRUE(test_sanitizer.IsValid("abc ABC"));
}
