/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "catalog_test_tools.h"

namespace {
const char* hashes[] = {
  "b026324c6904b2a9cb4b88d6d61c81d1000000",
  "26ab0db90d72e28ad0ba1e22ee510510000000",
  "6d7fce9fee471194aa8b5b6e47267f03000000",
  "48a24b70a0b376535542b996af517398000000",
  "1dcca23355272056f04fe8bf20edfce0000000",
  "11111111111111111111111111111111111111"
};
}  // namespace

class T_CatalogMergeTool : public ::testing::Test {
};

TEST_F(T_CatalogMergeTool, Basic) {
  DirSpecEntryList entries;

  const size_t file_size = 4096;
  char suffix = shash::kSha1;
  shash::Any hash;
  const shash::Any empty_content;

  // adding ""
  entries.push_back(DirSpecEntry(empty_content, file_size, "", ""));

  // adding "/dir"
  entries.push_back(DirSpecEntry(empty_content, file_size, "", "dir"));

  // adding "/file1"
  hash = shash::Any(shash::kSha1,
                    reinterpret_cast<const unsigned char*>(hashes[0]),
                    suffix);
  entries.push_back(DirSpecEntry(hash, file_size, "", "file1"));

  // adding "/dir/dir"
  entries.push_back(DirSpecEntry(empty_content, file_size, "/dir", "dir"));

  // adding "/dir/dir/file2"
  hash = shash::Any(shash::kSha1,
                    reinterpret_cast<const unsigned char*>(hashes[1]),
                    suffix);
  entries.push_back(DirSpecEntry(hash, file_size, "/dir/dir", "file2"));

  // adding "/dir/dir/dir"
  entries.push_back(DirSpecEntry(empty_content, file_size, "/dir/dir", "dir"));

  DirSpec spec(entries);
  CatalogTestTool tester(spec);

  //MockCatalog* root_catalog = catalog_mgr_->RetrieveRootCatalog();
}
