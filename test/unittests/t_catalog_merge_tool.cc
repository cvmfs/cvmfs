/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "catalog_test_tools.h"
#include "receiver/catalog_merge_tool.h"
#include "receiver/params.h"
#include "testutil.h"
#include "xattr.h"

namespace {
const char* hashes[] = {"b026324c6904b2a9cb4b88d6d61c81d1000000",
                        "26ab0db90d72e28ad0ba1e22ee510510000000",
                        "6d7fce9fee471194aa8b5b6e47267f03000000",
                        "48a24b70a0b376535542b996af517398000000",
                        "1dcca23355272056f04fe8bf20edfce0000000",
                        "11111111111111111111111111111111111111"};

DirSpec MakeBaseSpec() {
  DirSpec spec;

  const size_t file_size = 4096;
  char suffix = shash::kSha1;
  shash::Any hash;
  const shash::Any empty_content;

  // adding "/dir"
  {
    DirSpecItem item(
        catalog::DirectoryEntryTestFactory::Directory("dir", file_size),
        XattrList(), "");
    spec.push_back(item);
  }

  // adding "/file1"
  {
    hash =
        shash::Any(shash::kSha1,
                   reinterpret_cast<const unsigned char*>(hashes[0]), suffix);
    DirSpecItem item(catalog::DirectoryEntryTestFactory::RegularFile(
                         "file1", file_size, hash),
                     XattrList(), "");
    spec.push_back(item);
  }

  // adding "/dir/dir/file2"
  {
    hash =
        shash::Any(shash::kSha1,
                   reinterpret_cast<const unsigned char*>(hashes[1]), suffix);
    DirSpecItem item(catalog::DirectoryEntryTestFactory::RegularFile(
                         "file2", file_size, hash),
                     XattrList(), "dir");
    spec.push_back(item);
  }

  // adding "/dir/dir"
  {
    DirSpecItem item(
        catalog::DirectoryEntryTestFactory::Directory("dir", file_size),
        XattrList(), "dir");
    spec.push_back(item);
  }

  // adding "/dir/dir/file2"
  {
    hash =
        shash::Any(shash::kSha1,
                   reinterpret_cast<const unsigned char*>(hashes[2]), suffix);
    DirSpecItem item(catalog::DirectoryEntryTestFactory::RegularFile(
                         "file3", file_size, hash),
                     XattrList(), "dir/dir");
    spec.push_back(item);
  }

  return spec;
}

receiver::Params MakeMergeToolParams(const std::string& name) {
  receiver::Params params;

  const std::string sandbox_root = GetCurrentWorkingDirectory();
  const std::string stratum0 = sandbox_root + "/" + name + "_stratum0";
  const std::string temp_dir = stratum0 + "/data/txn";

  params.stratum0 = "file://" + stratum0;
  params.spooler_configuration = "local," + temp_dir + "," + stratum0;
  params.hash_alg = shash::kSha1;
  params.compression_alg = zlib::kZlibDefault;
  params.generate_legacy_bulk_chunks = false;
  params.use_file_chunking = true;
  params.min_chunk_size = 4194304;
  params.avg_chunk_size = 8388608;
  params.max_chunk_size = 16777216;
  params.enforce_limits = false;
  params.nested_kcatalog_limit = 0;
  params.root_kcatalog_limit = 0;
  params.file_mbyte_limit = 0;
  params.use_autocatalogs = false;
  params.max_weight = 0;
  params.min_weight = 0;

  return params;
}

}  // namespace

class T_CatalogMergeTool : public ::testing::Test {};

TEST_F(T_CatalogMergeTool, Basic) {

  DirSpec spec = MakeBaseSpec();

  CatalogTestTool tester("test");
  EXPECT_TRUE(tester.Init());

  manifest::Manifest original_manifest = *(tester.manifest());

  EXPECT_TRUE(tester.Apply("first", spec));

  UniquePtr<ServerTool> server_tool(new ServerTool());
  EXPECT_TRUE(server_tool->InitDownloadManager(true));

  receiver::Params params = MakeMergeToolParams("test");

  CatalogTestTool::History history = tester.history();

  receiver::CatalogMergeTool<catalog::WritableCatalogManager,
                             catalog::SimpleCatalogManager>
      merge_tool(params.stratum0, history[0].second, history[1].second,
                 PathString(""), GetCurrentWorkingDirectory(),
                 server_tool->download_manager(),
                 &original_manifest);
  EXPECT_TRUE(merge_tool.Init());

  std::string new_manifest_path;
  EXPECT_TRUE(merge_tool.Run(params, &new_manifest_path));
}
