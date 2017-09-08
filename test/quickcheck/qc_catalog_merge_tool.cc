/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

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

  // adding "/dir"
  spec.AddDirectory("dir", "", file_size);

  // adding "/dir/file1"
  spec.AddFile("file1", "dir", hashes[0], file_size);

  // adding "/dir/dir"
  spec.AddDirectory("dir", "dir", file_size);

  // adding "/dir/dir/file2"
  spec.AddFile("file2", "dir/dir", hashes[1], file_size);

  // adding "/file3"
  spec.AddFile("file3", "", hashes[2], file_size);

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

RC_GTEST_FIXTURE_PROP(T_CatalogMergeTool, InOut, ()) {
  DirSpec spec1 = MakeBaseSpec();

  CatalogTestTool tester("test");
  RC_ASSERT(tester.Init());

  RC_ASSERT(tester.Apply("first", spec1));

  manifest::Manifest first_manifest = *(tester.manifest());

  DirSpec spec2 = spec1;

  // add "dir/new_dir" and "dir/new_dir/new_file.txt"
  spec2.AddDirectory("new_dir", "dir", 4096);
  spec2.AddFile("new_file.txt", "dir/new_dir", hashes[3], 1024);

  // enlarge "/dir/file1" in spec2
  DirSpecItem item = spec2.Item(1);
  catalog::DirectoryEntryBase entry = item.entry_base();
  XattrList xattrs = item.xattrs();
  const std::string parent = item.parent();

  catalog::DirectoryEntry updated_entry =
      catalog::DirectoryEntryTestFactory::RegularFile(
        entry.name().c_str(), entry.size() * 2, entry.checksum());
  spec2.SetItem(DirSpecItem(updated_entry, xattrs, parent), 1);

  // remove "dir/dir" and "dir/dir/file2"
  spec2.RemoveItem(3);
  spec2.RemoveItem(2);

  RC_ASSERT(tester.Apply("second", spec2));

  UniquePtr<ServerTool> server_tool(new ServerTool());
  RC_ASSERT(server_tool->InitDownloadManager(true));

  receiver::Params params = MakeMergeToolParams("test");

  CatalogTestTool::History history = tester.history();

  receiver::CatalogMergeTool<catalog::WritableCatalogManager,
                             catalog::SimpleCatalogManager>
      merge_tool(params.stratum0, history[1].second, history[2].second,
                 PathString(""), GetCurrentWorkingDirectory() + "/merge_tool",
                 server_tool->download_manager(), &first_manifest);
  RC_ASSERT(merge_tool.Init());

  std::string output_manifest_path;
  RC_ASSERT(merge_tool.Run(params, &output_manifest_path));

  UniquePtr<manifest::Manifest> output_manifest(
      manifest::Manifest::LoadFile(output_manifest_path));

  RC_ASSERT(output_manifest.IsValid());

  DirSpec output_spec;
  RC_ASSERT(
      tester.DirSpecAtRootHash(output_manifest->catalog_hash(), &output_spec));

  spec2.Sort();
  std::string spec2_str;
  spec2.ToString(&spec2_str);
  output_spec.Sort();
  std::string out_spec_str;
  output_spec.ToString(&out_spec_str);

  /*
  LogCvmfs(kLogCvmfs, kLogStdout, "Target spec:\n%s", spec2_str.c_str());
  LogCvmfs(kLogCvmfs, kLogStdout, "Output spec:\n%s", out_spec_str.c_str());
  */

  // the printed form of the target and output dir specs should be the same
  RC_ASSERT(0 == strcmp(spec2_str.c_str(), out_spec_str.c_str()));

  // check size of "/dir/file1"
  RC_ASSERT(8192u == output_spec.Item(1).entry_base().size());

  // check size of "/dir/new_dir/new_file.txt"
  RC_ASSERT(1024u == output_spec.Item(3).entry_base().size());
}
