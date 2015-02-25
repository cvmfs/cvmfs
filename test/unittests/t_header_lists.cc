/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"

#include "../../cvmfs/download.h"

namespace download {

class T_HeaderLists : public ::testing::Test {
 protected:
  virtual void SetUp() {
    header_lists = new download::HeaderLists();
  }

  download::HeaderLists *header_lists;
};

TEST_F(T_HeaderLists, Basic) {
  curl_slist *header = header_lists->GetList("First: Line");
  EXPECT_EQ(header_lists->Print(header), "First: Line\n");

  header_lists->AppendHeader(header, "Second: Line");
  EXPECT_EQ(header_lists->Print(header), "First: Line\nSecond: Line\n");

  header_lists->PutList(header);
  delete header_lists;
}

TEST_F(T_HeaderLists, Intrinsics) {
  for (unsigned i = 0; i < header_lists->kBlockSize; ++i)
    header_lists->GetList("Some: Header");
  EXPECT_EQ(header_lists->blocks_.size(), 1U);

  header_lists->PutList(&(header_lists->blocks_[0][header_lists->kBlockSize-1]));
  header_lists->GetList("Some: Header");
  EXPECT_EQ(header_lists->blocks_.size(), 1U);

  for (unsigned i = 0; i < header_lists->kBlockSize; ++i)
    header_lists->PutList(&(header_lists->blocks_[0][i]));
  for (unsigned i = 0; i < header_lists->kBlockSize; ++i)
    header_lists->GetList("Some: Header");
  EXPECT_EQ(header_lists->blocks_.size(), 1U);

  header_lists->GetList("Some: Header");
  EXPECT_EQ(header_lists->blocks_.size(), 2U);

  delete header_lists;
}

}  // namespace download
