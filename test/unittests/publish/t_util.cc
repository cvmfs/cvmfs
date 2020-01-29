/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "publish/repository_util.h"

using namespace std;  // NOLINT

namespace publish {

class T_Util : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }

 protected:
};

TEST_F(T_Util, CheckoutMarker) {
  EXPECT_EQ(NULL, CheckoutMarker::CreateFrom("/no/such/path"));

  shash::Any hash = shash::MkFromHexPtr(
    shash::HexPtr("0123456789abcdef0123456789abcdef01234567"),
    shash::kSuffixCatalog);
  CheckoutMarker m("tag", "branch", hash);
  m.SaveAs("cvmfs_test_checkout_marker");

  CheckoutMarker *l = CheckoutMarker::CreateFrom("cvmfs_test_checkout_marker");
  ASSERT_TRUE(l != NULL);
  EXPECT_EQ(m.tag(), l->tag());
  EXPECT_EQ(m.branch(), l->branch());
  EXPECT_EQ(m.hash(), l->hash());
  delete l;
}

}  // namespace publish
