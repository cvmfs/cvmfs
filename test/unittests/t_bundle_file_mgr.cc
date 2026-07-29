/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <vector>

#include "file_bundle.h"
#include "json_document.h"

class T_BundleFileMgr : public ::testing::Test {
 protected:
  virtual void SetUp() {
    bfm_ = new BundleFileMgr(JsonDocument::Create(CreateJsonTxt()));
    EXPECT_TRUE(bfm_);
    EXPECT_TRUE(*bfm_);
  }

  virtual void TearDown() { delete bfm_; }
  virtual ~T_BundleFileMgr() { }

  BundleFileMgr* bfm_;
  const std::vector<std::string> paths_{"a_file_without_extension",
                                        "a_file_with.extension",
                                        "a/file/within/a/directory.foo"};

 private:
  std::string CreateJsonTxt() {
    std::ostringstream json;
    json << "{";
    json << "  \"name\": \"CVMFS_BUNDLE\",\n";
    json << "  \"version\": \"1.0.0\",\n";
    json << "  \"encoding\": \"UTF-8\",\n";
    json << "  \"dependencies\": [\n";
    for (size_t i = 0; i < paths_.size(); ++i) {
      json << "    \"" << paths_[i] << "\"";
      if (i < paths_.size() - 1) {
        json << ",\n";
      }
    }
    json << "]}";
    return json.str();
  }
};

TEST_F(T_BundleFileMgr, TestSize) { EXPECT_EQ(paths_.size(), bfm_->Size()); }


TEST_F(T_BundleFileMgr, TestVersion) { EXPECT_EQ(bfm_->GetVersion(), "1.0.0"); }

TEST_F(T_BundleFileMgr, TestEncoding) {
  EXPECT_EQ(bfm_->GetEncoding(), "UTF-8");
}

TEST_F(T_BundleFileMgr, TestGetNext) {
  for (size_t i = 0; i < paths_.size(); ++i) {
    EXPECT_EQ(bfm_->GetNext().ToString(), paths_[i]);
  }
  EXPECT_TRUE(bfm_->GetNext().IsEmpty());
}

