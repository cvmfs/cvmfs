#include <gtest/gtest.h>

#include "../../cvmfs/hash.h"

TEST(T_Shash, VerifyHex) {
  EXPECT_EQ(shash::HexPtr("").IsValid(), false);
  EXPECT_EQ(shash::HexPtr("012abc").IsValid(), false);
  EXPECT_EQ(shash::HexPtr("A68b329da9893e34099c7d8ad5cb9c94").IsValid(), false);
  EXPECT_EQ(shash::HexPtr("68b329da9893e34099c7d8ad5cb9c9400").IsValid(), false);
  EXPECT_EQ(shash::HexPtr("8b329da9893e34099c7d8ad5cb9c940-").IsValid(), false);
  EXPECT_EQ(shash::HexPtr("8b329da9893e34099c7d8ad5cb9c940-rmd160").IsValid(),
                          false);
  EXPECT_EQ(shash::HexPtr("68b329da9893e34099c7d8ad5cb9c940").IsValid(),
            true);

  EXPECT_EQ(shash::HexPtr("adc83b19e793491b1c6ea0fd8b46cd9f32e592fcX").IsValid(),
            false);
  EXPECT_EQ(shash::HexPtr(
    "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc-NO").IsValid(),
    false);
  EXPECT_EQ(shash::HexPtr(
    "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc-longsuffix").IsValid(),
    false);
  EXPECT_EQ(shash::HexPtr(
            "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc-rmd161").IsValid(),
            false);

  EXPECT_EQ(shash::HexPtr("adc83b19e793491b1c6ea0fd8b46cd9f32e592fc").IsValid(),
            true);
  EXPECT_EQ(shash::HexPtr(
    "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc-rmd160").IsValid(),
    true);
  EXPECT_EQ(shash::HexPtr(
    "adc83b19e793491b1c6ea0fd8b46cd9f32e592f-rmd160").IsValid(),
    false);
  EXPECT_EQ(shash::HexPtr(
    "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc-rmd1600").IsValid(),
    false);
}

