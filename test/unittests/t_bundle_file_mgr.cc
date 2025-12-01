/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "file_bundle.h"
#include "json_document.h"
#include "shortstring.h"
#include "util/pointer.h"

class T_BundleFileMgr : public ::testing::Test {
 protected:
  virtual void SetUp() {
    UniquePtr<JsonDocument> json_doc(JsonDocument::Create("{}"));
    EXPECT_TRUE(json_doc->IsValid());
    bfm_.Manage(json_doc);
    EXPECT_TRUE(bfm_);
  }
  virtual void TearDown() { }
  virtual ~T_BundleFileMgr() { }

  BundleFileMgr bfm_;
};

TEST_F(T_BundleFileMgr, Construction) {
  PathString path("test_string");
  BundleFileMgr mgr(path);
}

