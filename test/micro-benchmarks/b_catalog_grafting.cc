/**
 * This file is part of the CernVM File System.
 */
#define __STDC_FORMAT_MACROS
#include <benchmark/benchmark.h>

#include <inttypes.h>
#include <stdint.h>

#include <cstdio>
#include <iostream>

#include "bm_util.h"
#include "catalog_test_tools.h"
#include "crypto/hash.h"
#include "directory_entry.h"
#include "shortstring.h"
#include "util/murmur.hxx"
#include "util/prng.h"
#include "receiver/catalog_merge_tool.h"
#include "receiver/params.h"
#include "testutil.h"
#include "xattr.h"

namespace {  
  receiver::Params MakeMergeToolParams(const std::string& name) {
    receiver::Params params;
  
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
  
}  // namespace

class BM_CatalogGrafting : public benchmark::Fixture {
 protected:
  virtual void SetUp(const benchmark::State &st) {
  }

  virtual void TearDown(const benchmark::State &st) {
  }
  const char* file_hash{"b026324c6904b2a9cb4b88d6d61c81d100000000"};
  const size_t size{4096};
};

BENCHMARK_DEFINE_F(BM_CatalogGrafting, Baseline)(benchmark::State &st) {
  while (st.KeepRunning()) {
    st.PauseTiming();

    // Create initial spec with only 1 dir
    DirSpec spec1;
    spec1.AddDirectory("dir", "", size);

    CatalogTestTool tester("test");
    tester.Init();  
    tester.Apply("first", spec1);
    
    manifest::Manifest first_manifest = *(tester.manifest());
  
    DirSpec spec2{spec1};
    // add nested directories
    spec2.AddDirectory("1", "dir", size);
    spec2.AddDirectory("2", "dir/1", size);
    spec2.AddDirectory("3", "dir/1/2", size);
    spec2.AddDirectory("4", "dir/1/2/3", size);
    spec2.AddDirectory("5", "dir/1/2/3/4", size);
    // Add files to each of the added directories
    spec2.AddFile("file1", "dir/1", file_hash, size);
    spec2.AddFile("file2", "dir/1/2", file_hash, size);
    spec2.AddFile("file3", "dir/1/2/3", file_hash, size);
    spec2.AddFile("file4", "dir/1/2/3/4", file_hash, size);
    spec2.AddFile("file5", "dir/1/2/3/4/5", file_hash, size);
    // Add catalogs to each of the added directories
    spec2.AddNestedCatalog("dir");
    spec2.AddNestedCatalog("dir/1");
    spec2.AddNestedCatalog("dir/1/2");
    spec2.AddNestedCatalog("dir/1/2/3");
    spec2.AddNestedCatalog("dir/1/2/3/4");
    spec2.AddNestedCatalog("dir/1/2/3/4/5");
  
    tester.Apply("second", spec2);
  
    UniquePtr<ServerTool> server_tool(new ServerTool());
    server_tool->InitDownloadManager(true, "");
  
    receiver::Params params = MakeMergeToolParams("test");
  
    CatalogTestTool::History history = tester.history();
  
    perf::Statistics statistics;
  
    receiver::CatalogMergeTool<catalog::WritableCatalogManager,
                              catalog::SimpleCatalogManager>
       merge_tool(params.stratum0, history[1].second, history[2].second,
                  PathString(""), GetCurrentWorkingDirectory() + "/merge_tool",
                  server_tool->download_manager(), &first_manifest, &statistics, "");
    merge_tool.Init();

    std::string output_manifest_path;
    shash::Any output_manifest_hash;
    uint64_t final_rev;

    // Measure only the merge tool's time
    st.ResumeTiming();
    merge_tool.Run(params, &output_manifest_path, &output_manifest_hash, &final_rev);
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_CatalogGrafting, Baseline)->Repetitions(3);