/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <state.h>

#include <cstdint>

TEST(T_State, OpenFilesCounter) {
  unsigned char buffer[4];
  EXPECT_EQ(4u, StateSerializer::SerializeOpenFilesCounter(42, NULL));
  EXPECT_EQ(4u, StateSerializer::SerializeOpenFilesCounter(42, buffer));
  uint32_t check;
  EXPECT_EQ(4u, StateSerializer::DeserializeOpenFilesCounter(buffer, &check));
  EXPECT_EQ(42u, check);
}
