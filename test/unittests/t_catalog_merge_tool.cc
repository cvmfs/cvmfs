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

  // adding "/dir/file1"
  {
    hash =
        shash::Any(shash::kSha1,
                   reinterpret_cast<const unsigned char*>(hashes[0]), suffix);
    DirSpecItem item(catalog::DirectoryEntryTestFactory::RegularFile(
                         "file1", file_size, hash),
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
                   reinterpret_cast<const unsigned char*>(hashes[1]), suffix);
    DirSpecItem item(catalog::DirectoryEntryTestFactory::RegularFile(
                         "file2", file_size, hash),
                     XattrList(), "dir/dir");
    spec.push_back(item);
  }

  // adding "/file3"
  {
    hash =
        shash::Any(shash::kSha1,
                   reinterpret_cast<const unsigned char*>(hashes[2]), suffix);
    DirSpecItem item(catalog::DirectoryEntryTestFactory::RegularFile(
                         "file3", file_size, hash),
                     XattrList(), "");
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

TEST_F(T_CatalogMergeTool, BasicTwoCommits) {
  DirSpec spec1 = MakeBaseSpec();

  CatalogTestTool tester("test");
  EXPECT_TRUE(tester.Init());

  manifest::Manifest original_manifest = *(tester.manifest());

  EXPECT_TRUE(tester.Apply("first", spec1));

  manifest::Manifest first_manifest = *(tester.manifest());

  {
    DirSpec spec2 = spec1;
    // adding "/dir/dir/file4" to spec1

    const size_t file_size = 4096;
    shash::Any hash = shash::Any(
        shash::kSha1, reinterpret_cast<const unsigned char*>(hashes[3]),
        shash::kSha1);
    DirSpecItem item(catalog::DirectoryEntryTestFactory::RegularFile(
                         "file4", file_size, hash),
                     XattrList(), "dir/dir");
    spec2.push_back(item);

    EXPECT_TRUE(tester.Apply("second", spec2));
  }

  UniquePtr<ServerTool> server_tool(new ServerTool());
  EXPECT_TRUE(server_tool->InitDownloadManager(true));

  receiver::Params params = MakeMergeToolParams("test");

  CatalogTestTool::History history = tester.history();

  receiver::CatalogMergeTool<catalog::WritableCatalogManager,
                             catalog::SimpleCatalogManager>
      merge_tool(params.stratum0, history[1].second, history[2].second,
                 PathString(""), GetCurrentWorkingDirectory(),
                 server_tool->download_manager(), &first_manifest);
  EXPECT_TRUE(merge_tool.Init());

  std::string output_manifest_path;
  EXPECT_TRUE(merge_tool.Run(params, &output_manifest_path));

  UniquePtr<manifest::Manifest> output_manifest(
      manifest::Manifest::LoadFile(output_manifest_path));

  EXPECT_TRUE(output_manifest.IsValid());

  DirSpec output_spec;
  EXPECT_TRUE(tester.DirSpecAtRootHash(output_manifest->catalog_hash(), &output_spec));

  EXPECT_EQ(5u, spec1.size());
  EXPECT_EQ(6u, output_spec.size());
  EXPECT_EQ(0, strcmp("file4", output_spec[4].entry_base().name().c_str()));

  /*
  std::string spec_str1;
  PrintDirSpecToString(spec1, &spec_str1);
  std::string out_spec_str;
  PrintDirSpecToString(output_spec, &out_spec_str);

  LogCvmfs(kLogCvmfs, kLogStdout, "Spec1:\n%s", spec_str1.c_str());
  LogCvmfs(kLogCvmfs, kLogStdout, "Output spec:\n%s", out_spec_str.c_str());
  */
}
