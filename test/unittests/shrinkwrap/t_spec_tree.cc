/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "shrinkwrap/spec_tree.h"
#include "testutil_shrinkwrap.h"
#include "util/posix.h"


class T_SpecTree : public ::testing::Test {
 protected:
  virtual void SetUp() {
    std::string content;
    content = "^/*\n"
              "/foo/bar\n"
              "/foo/bar/abc/*\n"
              "!/foo/bar/abc/def/hij\n"
              "^/bar\n"
              "^/bar/abc/def/*\n"
              "!/bar/abc/def/ghj\n";
    ASSERT_TRUE(
        SafeWriteToFile(content, "./cvmfs-spec-tree-test.spec.txt", 0600));
    specs_ = SpecTree::Create("./cvmfs-spec-tree-test.spec.txt");
  }

  virtual void TearDown() {
    unlink("cvmfs-spec-tree-test.spec.txt");
    delete specs_;
  }

  SpecTree *specs_;
};


TEST_F(T_SpecTree, BasicMatchTest) {
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


TEST(T_SpecTreeStandalone, GitignoreStyleReinclude) {
  const std::string spec_file = "./cvmfs-spec-tree-gitignore-test.spec.txt";
  const std::string content = "/base*\n"
                              "!/base/accel/amd/*\n"
                              "/base/accel/amd/gfx90a\n"
                              "!/base/accel/amd/gfx90a/excluded\n";
  ASSERT_TRUE(SafeWriteToFile(content, spec_file, 0600));

  SpecTree *specs = SpecTree::Create(spec_file);

  EXPECT_TRUE(specs->IsMatching("/base/accel/amd"));
  EXPECT_FALSE(specs->IsMatching("/base/accel/amd/gfx900"));
  EXPECT_FALSE(specs->IsMatching("/base/accel/amd/gfx900/file"));
  EXPECT_TRUE(specs->IsMatching("/base/accel/amd/gfx90a"));
  EXPECT_TRUE(specs->IsMatching("/base/accel/amd/gfx90a/file"));
  EXPECT_FALSE(specs->IsMatching("/base/accel/amd/gfx90a/excluded"));
  EXPECT_FALSE(specs->IsMatching("/base/accel/amd/gfx90a/excluded/file"));

  size_t listLen = 0;
  char **dirList = NULL;
  EXPECT_EQ(0, specs->ListDir("/base/accel/amd", &dirList, &listLen));
  EXPECT_EQ(1U, listLen);
  ExpectListHas("gfx90a", dirList);
  FreeList(dirList, listLen);
  dirList = NULL;
  listLen = 0;

  EXPECT_EQ(SPEC_READ_FS,
            specs->ListDir("/base/accel/amd/gfx90a", &dirList, &listLen));
  EXPECT_EQ(-1, specs->ListDir("/base/accel/amd/gfx900", &dirList, &listLen));

  delete specs;
  unlink(spec_file.c_str());
}


TEST(T_SpecTreeStandalone, ExcludeChildrenHonorsOrder) {
  const std::string spec_file = "./cvmfs-spec-tree-order-test.spec.txt";
  const std::string content = "/base*\n"
                              "/base/accel/amd/gfx90a\n"
                              "!/base/accel/amd/*\n";
  ASSERT_TRUE(SafeWriteToFile(content, spec_file, 0600));

  SpecTree *specs = SpecTree::Create(spec_file);

  EXPECT_TRUE(specs->IsMatching("/base/accel/amd"));
  EXPECT_FALSE(specs->IsMatching("/base/accel/amd/gfx90a"));
  EXPECT_FALSE(specs->IsMatching("/base/accel/amd/gfx90a/file"));

  size_t listLen = 0;
  char **dirList = NULL;
  EXPECT_EQ(-1, specs->ListDir("/base/accel/amd", &dirList, &listLen));
  EXPECT_EQ(NULL, dirList);

  delete specs;
  unlink(spec_file.c_str());
}


TEST_F(T_SpecTree, CheckListings) {
  size_t listLen = 0;
  char **dirList = NULL;
  EXPECT_EQ(SPEC_READ_FS, specs_->ListDir("/", &dirList, &listLen));

  EXPECT_EQ(SPEC_READ_FS, specs_->ListDir("", &dirList, &listLen));

  EXPECT_EQ(0, specs_->ListDir("/foo", &dirList, &listLen));
  EXPECT_EQ(1U, listLen);
  ExpectListHas("bar", dirList);
  FreeList(dirList, listLen);
  listLen = 0;

  EXPECT_EQ(0, specs_->ListDir("/foo/bar", &dirList, &listLen));
  EXPECT_EQ(1U, listLen);
  ExpectListHas("abc", dirList);
  FreeList(dirList, listLen);
  listLen = 0;

  EXPECT_EQ(SPEC_READ_FS, specs_->ListDir("/foo/bar/abc", &dirList, &listLen));

  EXPECT_EQ(SPEC_READ_FS,
            specs_->ListDir("/foo/bar/abc/def", &dirList, &listLen));

  EXPECT_EQ(-1, specs_->ListDir("/foo/bar/abc/def/hij", &dirList, &listLen));

  EXPECT_EQ(0, specs_->ListDir("/bar", &dirList, &listLen));
  EXPECT_EQ(1U, listLen);
  ExpectListHas("abc", dirList);
  FreeList(dirList, listLen);
  listLen = 0;

  EXPECT_EQ(0, specs_->ListDir("/bar/abc", &dirList, &listLen));
  EXPECT_EQ(1U, listLen);
  ExpectListHas("def", dirList);
  FreeList(dirList, listLen);
  listLen = 0;

  EXPECT_EQ(SPEC_READ_FS,
            specs_->ListDir("/foo/bar/abc/def", &dirList, &listLen));

  EXPECT_EQ(-1, specs_->ListDir("/bar/abc/def/foo", &dirList, &listLen));
  EXPECT_EQ(-1, specs_->ListDir("/bar/abc/def/foo/foo", &dirList, &listLen));

  EXPECT_EQ(-1, specs_->ListDir("/bar/abc/def/ghj", &dirList, &listLen));
}
