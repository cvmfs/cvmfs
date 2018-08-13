/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <fstream>
#include <iostream>

#include "shrinkwrap/spec_tree.h"
#include "test-util.h"

class T_Spec_Tree :
  public ::testing::Test {
 protected:
  virtual void SetUp() {
    std::ofstream f;
    f.open("/tmp/cvmfs-spec-tree-test.spec.txt");
    f << "^/*\n";
    f << "/foo/bar\n";
    f << "/foo/bar/abc/*\n";
    f << "!/foo/bar/abc/def/hij\n";
    f << "^/bar\n";
    f << "^/bar/abc/def/*\n";
    f << "!/bar/abc/def/ghj\n";
    f.close();
    specs_ = SpecTree::Create("/tmp/cvmfs-spec-tree-test.spec.txt");
  }

  virtual void TearDown() {
    unlink("/tmp/cvmfs-spec-tree-test.spec.txt");
    delete specs_;
  }
  SpecTree *specs_;
};

TEST_F(T_Spec_Tree, BasicMatchTest) {
  EXPECT_TRUE(specs_->IsMatching(""));
  EXPECT_TRUE(specs_->IsMatching("/"));
  EXPECT_TRUE(specs_->IsMatching("/asdf"));
  EXPECT_TRUE(specs_->IsMatching("/foo/"));
  EXPECT_TRUE(specs_->IsMatching("/foo/bar"));
  EXPECT_TRUE(specs_->IsMatching("/foo/bar/abc/foo/foo/foo.txt"));

  EXPECT_TRUE(specs_->IsMatching("/bar"));
  EXPECT_TRUE(specs_->IsMatching("/bar/abc/def/def"));
  EXPECT_TRUE(specs_->IsMatching("/bar/abc/def"));

  EXPECT_FALSE(specs_->IsMatching("/bar/abc/def/ghj"));
  EXPECT_FALSE(specs_->IsMatching("/bar/abc/def/ghj/def/def"));
  EXPECT_FALSE(specs_->IsMatching("/bar/def"));
  EXPECT_FALSE(specs_->IsMatching("/bar/abc/abc"));
  EXPECT_FALSE(specs_->IsMatching("/foobar/bar"));
  EXPECT_FALSE(specs_->IsMatching("/foo/bar/def"));
  EXPECT_FALSE(specs_->IsMatching("/foo/bar/abc/def/hij"));
  EXPECT_FALSE(specs_->IsMatching("/foo/bar/abc/def/hij/def.txt"));
}

TEST_F(T_Spec_Tree, CheckListings) {
  size_t listLen = 0;
  char **dirList = NULL;
  ASSERT_EQ(SPEC_READ_FS, specs_->ListDir(
    "/",
    &dirList,
    &listLen));

  ASSERT_EQ(SPEC_READ_FS, specs_->ListDir(
    "",
    &dirList,
    &listLen));

  ASSERT_EQ(0, specs_->ListDir(
    "/foo",
    &dirList,
    &listLen));
  ASSERT_EQ(1, listLen);
  AssertListHas("bar", dirList, listLen);
  FreeList(dirList, listLen);
  listLen = 0;

  ASSERT_EQ(0, specs_->ListDir(
    "/foo/bar",
    &dirList,
    &listLen));
  ASSERT_EQ(1, listLen);
  AssertListHas("abc", dirList, listLen);
  FreeList(dirList, listLen);
  listLen = 0;

  ASSERT_EQ(SPEC_READ_FS, specs_->ListDir(
    "/foo/bar/abc",
    &dirList,
    &listLen));

  ASSERT_EQ(SPEC_READ_FS, specs_->ListDir(
    "/foo/bar/abc/def",
    &dirList,
    &listLen));

  ASSERT_EQ(-1, specs_->ListDir(
    "/foo/bar/abc/def/hij",
    &dirList,
    &listLen));

  ASSERT_EQ(0, specs_->ListDir(
    "/bar",
    &dirList,
    &listLen));
  ASSERT_EQ(1, listLen);
  AssertListHas("abc", dirList, listLen);
  FreeList(dirList, listLen);
  listLen = 0;

  ASSERT_EQ(0, specs_->ListDir(
    "/bar/abc",
    &dirList,
    &listLen));
  ASSERT_EQ(1, listLen);
  AssertListHas("def", dirList, listLen);
  FreeList(dirList, listLen);
  listLen = 0;

  ASSERT_EQ(SPEC_READ_FS, specs_->ListDir(
    "/foo/bar/abc/def",
    &dirList,
    &listLen));

  ASSERT_EQ(-1, specs_->ListDir(
    "/bar/abc/def/foo",
    &dirList,
    &listLen));
  ASSERT_EQ(-1, specs_->ListDir(
    "/bar/abc/def/foo/foo",
    &dirList,
    &listLen));

  ASSERT_EQ(-1, specs_->ListDir(
    "/bar/abc/def/ghj",
    &dirList,
    &listLen));
}
