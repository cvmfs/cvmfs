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

// Randomly generate a base dir spec
DirSpec MakeBaseSpec() {
  DirSpec spec;

  const auto num_entries = *rc::gen::arbitrary<uint64_t>();
  for (auto i = 0u; i < num_entries; ++i) {
    const auto file_name = "file" + std::to_string(i);
    const auto file_size = *rc::gen::arbitrary<uint64_t>();
    RC_ASSERT(spec.AddFile(file_name, "", hashes[0], file_size));
  }

  return spec;
}

// TODO(radu): Randomly modify the input spec
DirSpec ModifySpec(const DirSpec& in) {
  return in;
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
  // "Initial commit"
  DirSpec spec0;

  CatalogTestTool tester("test");
  RC_ASSERT(tester.Init());

  RC_ASSERT(tester.Apply("initial", spec0));

  manifest::Manifest first_manifest = *(tester.manifest());

  // First actual commit
  DirSpec spec1 = MakeBaseSpec();

  RC_ASSERT(tester.Apply("second", spec1));

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

  spec1.Sort();
  std::string spec1_str;
  spec1.ToString(&spec1_str);
  output_spec.Sort();
  std::string out_spec_str;
  output_spec.ToString(&out_spec_str);

  /*
  LogCvmfs(kLogCvmfs, kLogStdout, "Target spec:\n%s", spec1_str.c_str());
  LogCvmfs(kLogCvmfs, kLogStdout, "Output spec:\n%s", out_spec_str.c_str());
  */

  // the printed form of the target and output dir specs should be the same
  RC_ASSERT(0 == strcmp(spec1_str.c_str(), out_spec_str.c_str()));
}
