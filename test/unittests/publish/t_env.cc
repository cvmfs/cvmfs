/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "publish/repository.h"

namespace publish {

TEST(T_Env, GetEnterSessionDir) {
  EXPECT_TRUE(Env::GetEnterSessionDir().empty());
}

}  // namespace publish
