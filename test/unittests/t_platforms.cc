/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "util/platform.h"

// TODO(Radu): Could add some unit tests for all the functions in
//             platform_{linux,osx}.h

class T_Platform : public ::testing::Test { };

TEST_F(T_Platform, Spinlock) {
  platform_spinlock lock;
  platform_spinlock_init(&lock, PTHREAD_PROCESS_PRIVATE);
  ASSERT_EQ(0, platform_spinlock_trylock(&lock));
  ASSERT_NE(0, platform_spinlock_trylock(&lock));
  platform_spinlock_unlock(&lock);
  ASSERT_EQ(0, platform_spinlock_trylock(&lock));
}
