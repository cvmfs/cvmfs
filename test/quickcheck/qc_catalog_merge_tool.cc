/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "fuzztest/fuzztest.h"

#include <cstdint>
#include <string>
#include <vector>

#include "catalog_test_tools.h"
#include "receiver/catalog_merge_tool.h"
#include "receiver/params.h"
#include "testutil.h"
#include "util/exception.h"
#include "xattr.h"

namespace {
const char *test_file_hash = "b026324c6904b2a9cb4b88d6d61c81d100000000";

enum class ChangeType : int {
  AddFile,
  AddDir,
  RemoveItem,
  ModifyFile
};

// Descriptor for one entry of the randomly generated base directory tree.
// Unlike RapidCheck (which sampled generators imperatively while building the
// tree), FuzzTest declares the randomness as input domains; MakeBaseSpec then
// consumes an already-generated vector of these descriptors.
struct EntrySpec {
  bool is_dir;             // false == file, true == directory
  uint32_t parent_choice;  // reduced modulo the current number of dirs
  uint32_t size;           // file size (ignored for directories)
};

// Descriptor for one random modification applied on top of the base tree.
struct ChangeSpec {
  uint32_t kind;           // reduced modulo the number of ChangeType values
  uint32_t parent_choice;  // reduced modulo the current number of dirs
  uint32_t item_choice;    // reduced modulo the current number of items
  uint32_t size;           // file size for newly added files
};

// Build a base dir spec from a generated list of entry descriptors.
DirSpec MakeBaseSpec(const std::vector<EntrySpec> &entries) {
  DirSpec spec;

  for (size_t i = 0; i < entries.size(); ++i) {
    const EntrySpec &entry = entries[i];

    // Choose an existing directory as parent (the root always exists).
    const std::vector<std::string> dirs = spec.GetDirs();
    const std::string parent = dirs[entry.parent_choice % dirs.size()];

    if (!entry.is_dir) {
      const std::string file_name = "file" + std::to_string(i);
      EXPECT_TRUE(spec.AddFile(file_name, parent, test_file_hash, entry.size));
    } else {
      const std::string dir_name = "dir" + std::to_string(i);
      EXPECT_TRUE(spec.AddDirectory(dir_name, parent, 1));
    }
  }

  return spec;
}

// Apply a generated list of modifications on top of the base spec.
// TODO(radu): Implement ChangeType::ModifyFile content modifications
DirSpec ModifySpec(const DirSpec &in, const std::vector<ChangeSpec> &changes) {
  DirSpec out(in);

  for (size_t i = 0; i < changes.size(); ++i) {
    const ChangeSpec &change = changes[i];
    const ChangeType change_type = static_cast<ChangeType>(change.kind % 4);
    switch (change_type) {
      case ChangeType::AddFile: {
        const std::vector<std::string> dirs = out.GetDirs();
        const std::string parent = dirs[change.parent_choice % dirs.size()];
        const std::string file_name = "new_file" + std::to_string(i);
        EXPECT_TRUE(
            out.AddFile(file_name, parent, test_file_hash, change.size));
      } break;
      case ChangeType::AddDir: {
        const std::vector<std::string> dirs = out.GetDirs();
        const std::string parent = dirs[change.parent_choice % dirs.size()];
        const std::string dir_name = "new_dir" + std::to_string(i);
        EXPECT_TRUE(out.AddDirectory(dir_name, parent, 1));
      } break;
      case ChangeType::RemoveItem: {
        if (out.NumItems() > 0) {
          const size_t item_index = change.item_choice % out.NumItems();
          size_t idx = 0;
          DirSpec::ItemList::const_iterator it = out.items().begin();
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

  // Point at the repository the CatalogTestTool actually created (its stratum0
  // is sandbox_root/name). The merge tool reads the base/target catalogs from
  // this path and writes the merged result back into the same repository, so
  // DirSpecAtRootHash() can read the output catalog afterwards.
  const std::string sandbox_root = GetCurrentWorkingDirectory();
  const std::string stratum0 = sandbox_root + "/" + name;
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

// Domain for a single base-tree entry.
fuzztest::Domain<EntrySpec> EntrySpecDomain() {
  return fuzztest::StructOf<EntrySpec>(fuzztest::Arbitrary<bool>(),
                                       fuzztest::Arbitrary<uint32_t>(),
                                       fuzztest::Arbitrary<uint32_t>());
}

// Domain for a single modification.
fuzztest::Domain<ChangeSpec> ChangeSpecDomain() {
  return fuzztest::StructOf<ChangeSpec>(
      fuzztest::Arbitrary<uint32_t>(), fuzztest::Arbitrary<uint32_t>(),
      fuzztest::Arbitrary<uint32_t>(), fuzztest::Arbitrary<uint32_t>());
}

/**
 * This is a basic "what goes in, must also come out" test, implemented with
 * random data generation from FuzzTest:
 *
 * 1. A directory tree specification (DirSpec) is randomly generated in
 *    the MakeBaseSpec function from the generated `base_entries` (state_1)
 * 2. The DirSpec object created at the previous step is randomly modified
 *    in the ModifySpec function from the generated `changes` (state_2)
 * 3. The two DirSpec objects are applied to the CatalogTestTool as sequential
 *    "states" of the test repository. The CatalogTestTool object will create
 *    catalogs corresponding to these states.
 * 4. A CatalogMergeTool is created to merge the changes of state_2 - state_1
 *    onto state_1. The resulting catalog, corresponding to state_3 should be
 *    equivalent to state_2.
 *
 * Note: This testing strategy can be later expanded to to a three-way merge.
 */
void CatalogMergeInOut(const std::vector<EntrySpec> &base_entries,
                       const std::vector<ChangeSpec> &changes) {
  CatalogTestTool tester("test");
  ASSERT_TRUE(tester.Init());

  // First actual commit
  DirSpec spec1 = MakeBaseSpec(base_entries);
  ASSERT_TRUE(tester.Apply("first", spec1));

  manifest::Manifest first_manifest = *(tester.manifest());

  // Second commit with (target) modified director spec
  DirSpec spec2 = ModifySpec(spec1, changes);
  ASSERT_TRUE(tester.Apply("target", spec2));

  UniquePtr<ServerTool> server_tool(new ServerTool());
  ASSERT_TRUE(server_tool->InitDownloadManager(true, ""));

  receiver::Params params = MakeMergeToolParams("test");

  CatalogTestTool::History history = tester.history();

  perf::Statistics statistics;

  receiver::CatalogMergeTool<catalog::WritableCatalogManager,
                             catalog::SimpleCatalogManager>
      merge_tool(params.stratum0, history[1].second, history[2].second,
                 PathString(""), GetCurrentWorkingDirectory() + "/merge_tool",
                 server_tool->download_manager(), &first_manifest, &statistics,
                 "");
  ASSERT_TRUE(merge_tool.Init());

  std::string output_manifest_path;
  shash::Any output_manifest_hash;
  uint64_t final_rev;
  ASSERT_TRUE(merge_tool.Run(params, &output_manifest_path,
                             &output_manifest_hash, &final_rev));

  std::unique_ptr<manifest::Manifest> output_manifest(
      manifest::Manifest::LoadFile(output_manifest_path));

  ASSERT_TRUE(output_manifest.IsValid());

  DirSpec output_spec;
  ASSERT_TRUE(
      tester.DirSpecAtRootHash(output_manifest->catalog_hash(), &output_spec));

  std::string target_spec_str;
  spec2.ToString(&target_spec_str);
  std::string out_spec_str;
  output_spec.ToString(&out_spec_str);

  // the printed form of the target and output dir specs should be the same
  EXPECT_EQ(target_spec_str, out_spec_str);
}

}  // namespace

FUZZ_TEST(T_CatalogMergeTool, CatalogMergeInOut)
    .WithDomains(fuzztest::VectorOf(EntrySpecDomain()).WithMaxSize(64),
                 fuzztest::VectorOf(ChangeSpecDomain()).WithMaxSize(64));
