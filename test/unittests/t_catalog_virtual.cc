/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "catalog_virtual.h"

namespace catalog {

class T_VirtualCatalog : public ::testing::Test {
 protected:
  virtual void SetUp() { }

  virtual void TearDown() { }
};


TEST_F(T_VirtualCatalog, ParseActions) {
  int actions;
  EXPECT_TRUE(VirtualCatalog::ParseActions("", &actions));
  EXPECT_EQ(VirtualCatalog::kActionNone, actions);
  EXPECT_FALSE(VirtualCatalog::ParseActions("abc", &actions));
  EXPECT_TRUE(VirtualCatalog::ParseActions("remove", &actions));
  EXPECT_EQ(VirtualCatalog::kActionRemove, actions);
  EXPECT_TRUE(VirtualCatalog::ParseActions("snapshots", &actions));
  EXPECT_EQ(VirtualCatalog::kActionGenerateSnapshots, actions);
  EXPECT_TRUE(VirtualCatalog::ParseActions("snapshots,remove", &actions));
  EXPECT_EQ(
      VirtualCatalog::kActionGenerateSnapshots | VirtualCatalog::kActionRemove,
      actions);
  EXPECT_TRUE(
      VirtualCatalog::ParseActions("snapshots,remove,remove", &actions));
  EXPECT_EQ(
      VirtualCatalog::kActionGenerateSnapshots | VirtualCatalog::kActionRemove,
      actions);
  EXPECT_FALSE(VirtualCatalog::ParseActions("snapshots,abc", &actions));
}

}  // namespace catalog
