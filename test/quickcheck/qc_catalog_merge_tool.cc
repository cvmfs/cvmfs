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
#include "util/exception.h"
#include "xattr.h"

namespace {
const char *test_file_hash = "b026324c6904b2a9cb4b88d6d61c81d1000000";

enum class ItemType : int {
  File,
  Directory
};

enum class ChangeType : int {
  AddFile,
  AddDir,
  RemoveItem,
  ModifyFile
};

// Randomly generate a base dir spec
// TODO(radu): this function can become a template specialization of
//             Arbitrary for DirSpec
DirSpec MakeBaseSpec() {
  DirSpec spec;

  // Choose number of entries to generate
  const auto num_entries = *rc::gen::arbitrary<size_t>();

  for (auto i = 0u; i < num_entries; ++i) {
    // Choose an existing directory as parent;
    const auto dirs = spec.GetDirs();
    const auto parent_index = *rc::gen::arbitrary<size_t>() % dirs.size();
    const auto parent = dirs[parent_index];

    // Are we generating a file or a directory?
    const auto entry_type = *rc::gen::arbitrary<size_t>()
                            % 2;  // 0 == file, 1 == dir,
    if (entry_type == static_cast<int>(ItemType::File)) {
      const auto file_name = "file" + std::to_string(i);
      const auto file_size = *rc::gen::arbitrary<size_t>();
      RC_ASSERT(spec.AddFile(file_name, parent, test_file_hash, file_size));
    } else if (entry_type == static_cast<int>(ItemType::Directory)) {
      const auto dir_name = "dir" + std::to_string(i);
      RC_ASSERT(spec.AddDirectory(dir_name, parent, 1));
    }
  }

  return spec;
}

// TODO(radu): Randomly modify the input spec
DirSpec ModifySpec(const DirSpec &in) {
  DirSpec out(in);

  // Choose number of changes to perform
  const auto num_changes = *rc::gen::arbitrary<size_t>();

  for (auto i = 0u; i < num_changes; ++i) {
    const auto change_type = *rc::gen::arbitrary<size_t>()
                             % 4;  // 0 == AddFile , 1 == AddDir etc.
    switch (static_cast<ChangeType>(change_type)) {
      case ChangeType::AddFile: {
        const auto dirs = out.GetDirs();
        const auto parent_index = *rc::gen::arbitrary<size_t>() % dirs.size();
        const auto parent = dirs[parent_index];
        const auto file_name = "new_file" + std::to_string(i);
        const auto file_size = *rc::gen::arbitrary<size_t>();
        RC_ASSERT(out.AddFile(file_name, parent, test_file_hash, file_size));
      } break;
      case ChangeType::AddDir: {
        const auto dirs = out.GetDirs();
        const auto parent_index = *rc::gen::arbitrary<size_t>() % dirs.size();
        const auto parent = dirs[parent_index];
        const auto dir_name = "new_dir" + std::to_string(i);
        RC_ASSERT(out.AddDirectory(dir_name, parent, 1));
      } break;
      case ChangeType::RemoveItem: {
        if (out.NumItems() > 0) {
          const auto item_index = *rc::gen::arbitrary<size_t>()
                                  % out.NumItems();
          size_t idx = 0;
          auto it = out.items().begin();
          while (idx < item_index) {
            ++it;
            ++idx;
          }
          out.RemoveItemRec(it->first);
        }
      } break;
      case ChangeType::ModifyFile:
        // TODO(radu): Implement file content modifications
        break;
      default:
        PANIC(kLogStderr, "Unknown change type. Aborting.");
        break;
    }
  }

  return out;
}

receiver::Params MakeMergeToolParams(const std::string &name) {
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

class T_CatalogMergeTool : public ::testing::Test { };

/**
 * This is a basic "what goes in, must also come out" test, implemented with
 * random data generation from RapidCheck:
 *
 * 1. A directory tree specification (DirSpec) is randomly generated in
 *    the MakeBaseSpec function (state_1)
 * 2. The DirSpec object created at the previous step is randomly modified
 *    in the ModifySpec function (state_2)
 * 3. The two DirSpec objects are applied to the CatalogTestTool as sequential
 "states" of
 *    of the test repository. The CatalogTestTool object will create catalogs
 corresponding to these states.
 * 4. A CatalogMergeTool is created to merge the changes of state_2 - state_1
 onto state_1
 *    The resulting catalog, corresponding to state_3 should be equivalent to
 state_2.
 *
 * Note: This testing strategy can be later expanded to to a three-way merge.
 */
RC_GTEST_FIXTURE_PROP(T_CatalogMergeTool, InOut, ()) {
  CatalogTestTool tester("test");
  RC_ASSERT(tester.Init());

  // First actual commit
  DirSpec spec1 = MakeBaseSpec();
  RC_ASSERT(tester.Apply("first", spec1));

  manifest::Manifest first_manifest = *(tester.manifest());

  // Second commit with (target) modified director spec
  DirSpec spec2 = ModifySpec(spec1);
  RC_ASSERT(tester.Apply("target", spec2));

  UniquePtr<ServerTool> server_tool(new ServerTool());
  RC_ASSERT(server_tool->InitDownloadManager(true));

  receiver::Params params = MakeMergeToolParams("test");

  CatalogTestTool::History history = tester.history();

  perf::Statistics statistics;

  receiver::CatalogMergeTool<catalog::WritableCatalogManager,
                             catalog::SimpleCatalogManager>
      merge_tool(params.stratum0, history[1].second, history[2].second,
                 PathString(""), GetCurrentWorkingDirectory() + "/merge_tool",
                 server_tool->download_manager(), &first_manifest, &statistics);
  RC_ASSERT(merge_tool.Init());

  std::string output_manifest_path;
  RC_ASSERT(merge_tool.Run(params, &output_manifest_path));

  UniquePtr<manifest::Manifest> output_manifest(
      manifest::Manifest::LoadFile(output_manifest_path));

  RC_ASSERT(output_manifest.IsValid());

  DirSpec output_spec;
  RC_ASSERT(
      tester.DirSpecAtRootHash(output_manifest->catalog_hash(), &output_spec));

  std::string target_spec_str;
  spec2.ToString(&target_spec_str);
  std::string out_spec_str;
  output_spec.ToString(&out_spec_str);

  /*
  LogCvmfs(kLogCvmfs, kLogStdout, "Target spec:\n%s", target_spec_str.c_str());
  LogCvmfs(kLogCvmfs, kLogStdout, "Output spec:\n%s", out_spec_str.c_str());
  */

  // the printed form of the target and output dir specs should be the same
  RC_ASSERT(0 == strcmp(target_spec_str.c_str(), out_spec_str.c_str()));
}
