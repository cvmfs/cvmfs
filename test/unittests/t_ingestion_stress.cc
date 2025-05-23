/**
 * This file is part of the CernVM File System.
 */

#include <cassert>
#include <cstdlib>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "c_file_sandbox.h"
#include "c_mock_uploader.h"
#include "crypto/hash.h"
#include "file_chunk.h"
#include "gtest/gtest.h"
#include "ingestion/item_mem.h"
#include "ingestion/pipeline.h"
#include "testutil.h"
#include "util/atomic.h"
#include "util/prng.h"
#include "util/smalloc.h"
#include "util/string.h"


class T_IngestionStress : public FileSandbox {
 public:
  typedef std::vector<ExpectedHashString> ExpectedHashStrings;

  T_IngestionStress()
      : FileSandbox(IngestionMockUploader::sandbox_path), uploader_(NULL) { }

 protected:
  virtual void SetUp() {
    uploader_ = IngestionMockUploader::MockConstruct();
    ASSERT_TRUE(uploader_ != NULL);
    CreateSandbox(IngestionMockUploader::sandbox_tmp_dir);
  }

  virtual void TearDown() {
    RemoveSandbox(IngestionMockUploader::sandbox_tmp_dir);
    uploader_->TearDown();
    delete uploader_;
  }

  template<class VectorT>
  void AppendVectorToVector(const VectorT &appendee, VectorT *vector) const {
    vector->insert(vector->end(), appendee.begin(), appendee.end());
  }

  ExpectedHashString GetEmptyFileBulkHash(
      const shash::Suffix suffix = shash::kSuffixNone) const {
    return std::make_pair("e8ec3d88b62ebf526e4e5a4ff6162a3aa48a6b78", suffix);
  }

  ExpectedHashString GetSmallFileBulkHash(
      const shash::Suffix suffix = shash::kSuffixNone) const {
    return std::make_pair("7935fe23e1f9959b176999d63f6b8ccacc7c6eff", suffix);
  }

  ExpectedHashString GetSmallZeroFileBulkHash(
      const shash::Suffix suffix = shash::kSuffixNone) const {
    return std::make_pair("b05b2dd3d5336b9ef1e4ce01b70bc40a9b676754", suffix);
  }

  ExpectedHashString GetBigFileBulkHash(
      const shash::Suffix suffix = shash::kSuffixNone) const {
    return std::make_pair("32f526af5fb573ee70925b108310447529d9b9eb", suffix);
  }

  ExpectedHashStrings GetBigFileChunkHashes() const {
    ExpectedHashStrings h;
    const shash::Suffix P = shash::kSuffixPartial;
    h.push_back(std::make_pair("b1ed4a3c29df719de4e29d8afe0e16f968226a8e", P));
    h.push_back(std::make_pair("fdc1bc2795cac33e9ff61f0c1c5f1815498362f6", P));
    h.push_back(std::make_pair("b928e5935b6a35278a81f9f859b13d3e83d88052", P));
    h.push_back(std::make_pair("bbd189584d78de0ad603ef5a7de3f770ab0bf3f8", P));
    h.push_back(std::make_pair("c8b7e7c3595244afe1cd946e4fd90ecbea34967f", P));
    return h;
  }

  ExpectedHashString GetBigZeroFileBulkHash(
      const shash::Suffix suffix = shash::kSuffixNone) const {
    return std::make_pair("7bda09b8a07d28b293ff67a32b763d8c86d78a5c", suffix);
  }

  ExpectedHashStrings GetBigZeroFileChunkHashes() const {
    ExpectedHashStrings h;
    const shash::Suffix P = shash::kSuffixPartial;
    h.push_back(std::make_pair("9a3cc4a3dd5f68bbd3a959359f213fda59b8c319", P));
    h.push_back(std::make_pair("e5cb4b67a2f23f95ac6b55f2a9a265520abdcf77", P));
    return h;
  }

  ExpectedHashString GetHugeFileBulkHash(
      const shash::Suffix suffix = shash::kSuffixNone) const {
    return std::make_pair("40a3de440ed2684253ca119cde8884144e7aaa1c", suffix);
  }

  ExpectedHashStrings GetHugeFileChunkHashes() const {
    ExpectedHashStrings h;
    const shash::Suffix P = shash::kSuffixPartial;
    h.push_back(std::make_pair("5f59af1e046b05e512102c598a644feaafb7a103", P));
    h.push_back(std::make_pair("d898735fc27d1e5d081f0b6ad3cf0f7d71978304", P));
    h.push_back(std::make_pair("e1c43da8ce10d6039c86755d30036625342ddbac", P));
    h.push_back(std::make_pair("94e8266eb774a2107b14883751206bda7f4913dd", P));
    h.push_back(std::make_pair("1218cd9840f03b4ab3c0ed58cf78fc39c93db320", P));
    h.push_back(std::make_pair("4ad2b87e457e1fb4ab88aa88bfd7ebce9c5ca227", P));
    h.push_back(std::make_pair("59ec5f1a966d16e4f20b60c92f530331eeb76056", P));
    h.push_back(std::make_pair("e297fa139a837d7b9de1f21efc024dd7cedd9f99", P));
    h.push_back(std::make_pair("8a0f8511afc80eb0382b5e454ccdd847751ca914", P));
    h.push_back(std::make_pair("78e6966a426088eeaea9c8221b4bcf785bf6a1fd", P));
    h.push_back(std::make_pair("bd12e4a53157393ac2636922a89f979edc0444a2", P));
    h.push_back(std::make_pair("94d9a6ef192885c2880f4bd717c5e6fd1b322bd6", P));
    h.push_back(std::make_pair("91da9fafce3a1b791e473519d0330f10f43959ed", P));
    h.push_back(std::make_pair("f82dd934989b99dcb9c7b3e77eed2da89d852543", P));
    h.push_back(std::make_pair("ce23d12cf133a9b4eb36a2222c18919248c83222", P));
    h.push_back(std::make_pair("9eab18a2c6d7c38a381e5650324c832d80128375", P));
    h.push_back(std::make_pair("da8aa7fc16c2c8eba6990acea79d7527505ac85d", P));
    h.push_back(std::make_pair("7777fcaceae803568c25e3b028dbc78601264c23", P));
    h.push_back(std::make_pair("e8d21488e822a5ca7c4fa01ddb90020d46ce254a", P));
    h.push_back(std::make_pair("cc2adf011cd610c1a54f6978c179d112d046f8c8", P));
    h.push_back(std::make_pair("6cfe6ccf9dbd967ccc41b6ea02c5c4ac55e4acec", P));
    h.push_back(std::make_pair("7f920c74a0c0ed6e6e7e0461d2bae024de71fd35", P));
    h.push_back(std::make_pair("89c04af09410412e94094a2643e4d075f0c9b5ea", P));
    h.push_back(std::make_pair("b43ff42f85c61d593c2ec623dc33b4e2ed5186a1", P));
    h.push_back(std::make_pair("5edf0d35f748aa7889965268691215a895bf20dd", P));
    h.push_back(std::make_pair("6bc191d38680aa3826a034ef85858b598072f2cd", P));
    h.push_back(std::make_pair("a86aef847fb57e9d97116a48ea96b4bc595abcf0", P));
    h.push_back(std::make_pair("477f0c4bedbdb6ad6378395a6630a5b51ba83faf", P));
    h.push_back(std::make_pair("53b0f1f1b48084122e826c472a6bcdcc27469c76", P));
    h.push_back(std::make_pair("a01bda9eacbb0e3e907a2d42e31fb834e4813786", P));
    h.push_back(std::make_pair("5861c8f0d79eba277df8f234af61ff0ed630ce0a", P));
    h.push_back(std::make_pair("622dbca4284a718566e1800e7fe3143818ce82a2", P));
    h.push_back(std::make_pair("08ee0ecd11badd592fae70be2a463c00967e7ad2", P));
    h.push_back(std::make_pair("e18299d4b826792453b553950c9881cf1e32518f", P));
    h.push_back(std::make_pair("da6c557d7c4c0ca5cd52e607c08af51fc2932917", P));
    h.push_back(std::make_pair("0c9ae2edbb9e211603d88152fc6da09bbd3fb5a3", P));
    h.push_back(std::make_pair("621b6408362964c9975350c48e9df98c68ca37b4", P));
    h.push_back(std::make_pair("1b2fd0513bf738b6c3784c7211ccf253d1fdd7b4", P));
    h.push_back(std::make_pair("2c9c414a94a32ea61f87dd76444f2704b3025efb", P));
    h.push_back(std::make_pair("f215b9ddcf6a614e1c71e533bf1bcd1130902776", P));
    h.push_back(std::make_pair("340bf76ed5f34a1147ec57018c70387d275a66ed", P));
    h.push_back(std::make_pair("26ea7c4bb725f778bc771a156d981e2dd7edab50", P));
    h.push_back(std::make_pair("a381169605b795e5b5991acc3f019cb9cd3f1727", P));
    h.push_back(std::make_pair("8c82806ce94a753e4282bbc733ffd47ac3e70306", P));
    h.push_back(std::make_pair("96d88b72077b4907a2308a1b9dbb0ef4299c933a", P));
    h.push_back(std::make_pair("d7e927f1c74f5e7819b789b003437a52485699a6", P));
    h.push_back(std::make_pair("336455de0b6a008cb252dfe93edfcea8069d75c2", P));
    h.push_back(std::make_pair("88a8eefb5383ec676870338b3dbe394e80f7b0de", P));
    h.push_back(std::make_pair("2bc320860bd3b1fc0fb2df3a03cd7d2daf2cc177", P));
    h.push_back(std::make_pair("91ded4765294a9b7e34d06f57a5a5a9a21b1ee64", P));
    h.push_back(std::make_pair("beadda974fd09806ea7776e0295f498851351269", P));
    h.push_back(std::make_pair("78ddd560f249dd79a3f86eeae9adb2c5eb4635df", P));
    h.push_back(std::make_pair("653d3026a54e48cb6d2010e1a388eef0a37a6180", P));
    h.push_back(std::make_pair("1ab877f51016f832391fcae1ea251a0ce61fb1b6", P));
    h.push_back(std::make_pair("7260fdc231aa9819b0f1861cd1e1345f50875c10", P));
    h.push_back(std::make_pair("8bc6f847e0607ecd753c5e08d0bee639c5b9447b", P));
    h.push_back(std::make_pair("9836e4237e0b6630f1b0450ec065ae3b658318e8", P));
    h.push_back(std::make_pair("358c0b5ce237578d238c0ec11f109462213c12fa", P));
    h.push_back(std::make_pair("36b4b6d69cdca800cc814ad40a3fcea1aff6c6a3", P));
    h.push_back(std::make_pair("e012ac32f8d8d5ed9a6a5a92564d21fd4d03dcc4", P));
    h.push_back(std::make_pair("60a3c4c2c5df7cc4010158e243d023b7525602f2", P));
    h.push_back(std::make_pair("e990ca08098bb8bdbf291a397f9338d18f9db554", P));
    h.push_back(std::make_pair("74109700ec3804bf3616c9d80e6acf0c9f8fe4ee", P));
    h.push_back(std::make_pair("bb002635ba0690f911f81a6f63dd00a5ad5f25ae", P));
    h.push_back(std::make_pair("b9db553972a21d0f856a508e235f65e6693e342e", P));
    h.push_back(std::make_pair("d35bda0cd389113d45bf264dbafd62e3b5b2cbf6", P));
    h.push_back(std::make_pair("93b15e04be918f4fc8eafe1e9bb7922902637ab0", P));
    h.push_back(std::make_pair("04286a3d861abe0d411ea6139f2529d87c2d03bd", P));
    h.push_back(std::make_pair("23c5706471c2228f5be3c5e1a6a1cfbc9b8f1258", P));
    h.push_back(std::make_pair("b810304158612ca13b87e9bcb842b25e07ed2e6e", P));
    h.push_back(std::make_pair("88d1bb0deef7f878868172d2e8a51a400d70895e", P));
    h.push_back(std::make_pair("1394b54a1c998a3ef08e49b4b3b0549ab8c31530", P));
    h.push_back(std::make_pair("e29edd5049e47fb04327c8f569c7906247169f85", P));
    h.push_back(std::make_pair("5cc73345f165b16c6bcb1cc812505459399ec371", P));
    h.push_back(std::make_pair("bc954f87b62a96e0c8d245fe0120e5e019ba8564", P));
    h.push_back(std::make_pair("34a58e8dfd9f29c8b6a21bed281bd0ac31cb1a22", P));
    h.push_back(std::make_pair("9616141471575586fb2e2a37aa0669b3bae3ff12", P));
    h.push_back(std::make_pair("ff03320fe5d23a55c1d7d9e840576454a5678a6f", P));
    h.push_back(std::make_pair("26a4926e3bfef1cc3ec5b57058566409d4b24e7c", P));
    h.push_back(std::make_pair("6ebeb7e0fbda3807fe41c36c3410a66fb9c7d12e", P));
    h.push_back(std::make_pair("feb29285ecaf1bff2765b93c3bcdd6d767677876", P));
    h.push_back(std::make_pair("47f39515f0752b7b709c69198525f56a787521b3", P));
    h.push_back(std::make_pair("8cedc81fa643344a02d97396c96ff1f53594f15e", P));
    h.push_back(std::make_pair("0b0bb1adfc98cb59b6188e5ce15b4f80ce13ff05", P));
    h.push_back(std::make_pair("04eca4afa27e11ca22f65b1a9860701828cde7ef", P));
    h.push_back(std::make_pair("b89bab0c79d131c902b152759749d91d1d607429", P));
    h.push_back(std::make_pair("95a5c16a9d8d2d75377f5aaca8cca8a7a6ba45e5", P));
    h.push_back(std::make_pair("0469f502c017cbd062b36ab362e1a55ef2a43bd5", P));
    h.push_back(std::make_pair("966f9289f379be2596d99d5df3732c013bbe0ebf", P));
    h.push_back(std::make_pair("94bd02bd9f74af14f0bd38013e410fe122d7aafb", P));
    h.push_back(std::make_pair("f58c84765e26f488a7a764dc264173d3151b533e", P));
    h.push_back(std::make_pair("bea982f20a74d304a356c68dc871ea81f621c69e", P));
    h.push_back(std::make_pair("5be00fd3e8ea277520672a7007aa13730ad0943d", P));
    h.push_back(std::make_pair("43216bc79ea767943103866b222938ec424f93d8", P));
    h.push_back(std::make_pair("aa4c65b5bb52f072e0aeafc2a6e2e4731dafb1c5", P));
    h.push_back(std::make_pair("f8b737ce0f65549dbe03f0a26741e277a7afbd49", P));
    h.push_back(std::make_pair("1c0d64d7282ecd21faf5bf83170d651c45105deb", P));
    h.push_back(std::make_pair("89c8ced4578651823580f3778a7b122e8fdcb044", P));
    h.push_back(std::make_pair("8259a641794f51c4e23d788222f30223f14e355b", P));
    h.push_back(std::make_pair("b52db5fdb938e535b5a4d34e5347aed97f537c4a", P));
    h.push_back(std::make_pair("9883edacdb12fbe68935d51050fd03d403164e53", P));
    h.push_back(std::make_pair("04b1ea7fce402d7a7e9ae70c830d1e19748e2eb4", P));
    h.push_back(std::make_pair("f9cff039047aa2ac3de1c86bd42b6a6dc263c15b", P));
    h.push_back(std::make_pair("457e220a21765f851851a0e51d567d797390a9b1", P));
    h.push_back(std::make_pair("a686de7b0e34aa3203c315b6cb6c6f20dc673b09", P));
    h.push_back(std::make_pair("4d0a49e9d41a974003e756bee9529858703f78b5", P));
    h.push_back(std::make_pair("7efc06549b4b427a712c4d92d6038840358cf098", P));
    h.push_back(std::make_pair("39edd8ce508240e59a82b206c18a724483349d62", P));
    return h;
  }

  ExpectedHashString GetHugeZeroFileBulkHash(
      const shash::Suffix suffix = shash::kSuffixNone) const {
    return std::make_pair("e80663bfedae90ad6a7b841ac915cb29223f8256", suffix);
  }

  ExpectedHashStrings GetHugeZeroFileChunkHashes() const {
    ExpectedHashStrings h;
    const shash::Suffix P = shash::kSuffixPartial;
    h.push_back(std::make_pair("9a3cc4a3dd5f68bbd3a959359f213fda59b8c319", P));
    h.push_back(std::make_pair("f62536cf1fa2b38fcaf2d181a6d595feca0cd100", P));
    return h;
  }

  void TestProcessFile(const std::string &file_path,
                       const ExpectedHashString &reference_hash,
                       const bool generate_legacy_bulk_hashes = true,
                       const bool use_chunking = true) {
    ExpectedHashStrings reference_hash_strings;
    reference_hash_strings.push_back(reference_hash);
    TestProcessFile(file_path,
                    reference_hash_strings,
                    generate_legacy_bulk_hashes,
                    use_chunking);
  }

  void TestProcessFile(const std::string &file_path,
                       const ExpectedHashStrings &reference_hash_strings,
                       const bool generate_legacy_bulk_hashes = true,
                       const bool use_chunking = true) {
    std::vector<std::string> file_paths;
    file_paths.push_back(file_path);
    TestProcessFiles(file_paths,
                     reference_hash_strings,
                     generate_legacy_bulk_hashes,
                     use_chunking);
  }

  void TestProcessFiles(const std::vector<std::string> &file_paths,
                        const ExpectedHashStrings &reference_hash_strings,
                        const bool generate_legacy_bulk_hashes = true,
                        const bool use_chunking = true) {
    IngestionPipeline pipeline(
        uploader_, MockSpoolerDefinition(generate_legacy_bulk_hashes));
    pipeline.Spawn();

    for (unsigned i = 0; i < file_paths.size(); ++i) {
      pipeline.Process(new FileIngestionSource(file_paths[i]), use_chunking);
    }
    pipeline.WaitFor();

    CheckHashes(uploader_->results, reference_hash_strings);
  }

  void CheckHash(const IngestionMockUploader::Results &results,
                 const ExpectedHashString &expected_hash) const {
    ExpectedHashStrings expected_hashes;
    expected_hashes.push_back(expected_hash);
    CheckHashes(results, expected_hashes);
  }

  void CheckHashes(const IngestionMockUploader::Results &results,
                   const ExpectedHashStrings &reference_hash_strings) const {
    std::set<std::string> reference_set;
    std::set<std::string> result_set;
    for (ExpectedHashStrings::const_iterator
             i = reference_hash_strings.begin(),
             i_end = reference_hash_strings.end();
         i != i_end;
         ++i) {
      reference_set.insert(i->first);
    }
    for (IngestionMockUploader::Results::const_iterator i = results.begin(),
                                                        i_end = results.end();
         i != i_end;
         ++i) {
      result_set.insert(i->computed_hash.ToString());
    }
    EXPECT_EQ(reference_set.size(), result_set.size())
        << "number of generated chunks did not match";

    // convert hash strings into shash::Any structs
    typedef std::vector<std::pair<shash::Any, shash::Suffix> > ExpectedHashes;
    ExpectedHashes reference_hashes;
    ExpectedHashStrings::const_iterator i = reference_hash_strings.begin();
    ExpectedHashStrings::const_iterator iend = reference_hash_strings.end();
    for (; i != iend; ++i) {
      reference_hashes.push_back(std::make_pair(
          shash::Any(shash::kSha1, shash::HexPtr(i->first)), i->second));
    }

    // check if we can find the computed hashes in the expected hashes
    // O(n^2) but works
    IngestionMockUploader::Results::const_iterator j = results.begin();
    IngestionMockUploader::Results::const_iterator jend = results.end();
    for (; j != jend; ++j) {
      bool found = false;
      ExpectedHashes::const_iterator k = reference_hashes.begin();
      ExpectedHashes::const_iterator kend = reference_hashes.end();
      for (; k != kend; ++k) {
        if (k->first == j->computed_hash) {
          EXPECT_EQ(k->second, j->computed_hash.suffix)
              << "hash suffix does not fit";
          found = true;
          break;
        }
      }

      EXPECT_TRUE(found) << "did not find generated hash "
                         << j->computed_hash.ToString() << " "
                         << "in the provided reference hashes";
    }
  }

  IngestionMockUploader *uploader_;
};

//------------------------------------------------------------------------------


TEST_F(T_IngestionStress, ProcessEmptyFile) {
  TestProcessFile(GetEmptyFile(), GetEmptyFileBulkHash());
}

TEST_F(T_IngestionStress, ProcessSmallFile) {
  const std::string &path = GetSmallFile();
  TestProcessFile(path, GetSmallFileBulkHash());
}

TEST_F(T_IngestionStress, ProcessSmallZeroFile) {
  const std::string &path = GetSmallZeroFile();
  TestProcessFile(path, GetSmallZeroFileBulkHash());
}

TEST_F(T_IngestionStress, ProcessSmallFileForcedBulk) {
  // Only one chunk created, promoted to bulk chunk
  TestProcessFile(GetSmallFile(), GetSmallFileBulkHash(),
                  false, /* legacy bulk hash */
                  true /* chunking */);
}

TEST_F(T_IngestionStress, ProcessBigFile) {
  ExpectedHashStrings hs = GetBigFileChunkHashes();
  hs.push_back(GetBigFileBulkHash());
  TestProcessFile(GetBigFile(), hs);
}

TEST_F(T_IngestionStress, ProcessBigZeroFile) {
  ExpectedHashStrings hs = GetBigZeroFileChunkHashes();
  hs.push_back(GetBigZeroFileBulkHash());
  TestProcessFile(GetBigZeroFile(), hs);
}

TEST_F(T_IngestionStress, ProcessBigFileForcedBulk) {
  // No chunking, hence bulk chunk must be created
  TestProcessFile(GetBigFile(), GetBigFileBulkHash(),
                  false, /* legacy bulk hash */
                  false /* chunking */);
}

TEST_F(T_IngestionStress, ProcessBigFileOnlyChunks) {
  // No bulk hash in the reference list
  ExpectedHashStrings hs = GetBigFileChunkHashes();
  TestProcessFile(GetBigFile(), hs, false, /* legacy bulk hash */
                  true /* chunking */);
}

TEST_F(T_IngestionStress, ProcessHugeFileSlow) {
  ExpectedHashStrings hs = GetHugeFileChunkHashes();
  hs.push_back(GetHugeFileBulkHash());
  TestProcessFile(GetHugeFile(), hs);
}

TEST_F(T_IngestionStress, ProcessHugeZeroFileSlow) {
  ExpectedHashStrings hs = GetHugeZeroFileChunkHashes();
  hs.push_back(GetHugeZeroFileBulkHash());
  TestProcessFile(GetHugeZeroFile(), hs);
}

TEST_F(T_IngestionStress, ProcessBigFileWithoutChunks) {
  TestProcessFile(GetBigFile(), GetBigFileBulkHash(),
                  true, /* legacy bulk hash */
                  false /* chunking */);
}

TEST_F(T_IngestionStress, ProcessMultipleFilesSlow) {
  std::vector<std::string> pathes;
  ExpectedHashStrings hs;

  pathes.push_back(GetSmallFile());
  hs.push_back(GetSmallFileBulkHash());

  pathes.push_back(GetEmptyFile());
  hs.push_back(GetEmptyFileBulkHash());

  pathes.push_back(GetBigFile());
  hs.push_back(GetBigFileBulkHash());
  AppendVectorToVector(GetBigFileChunkHashes(), &hs);

  pathes.push_back(GetHugeFile());
  hs.push_back(GetHugeFileBulkHash());
  AppendVectorToVector(GetHugeFileChunkHashes(), &hs);

  pathes.push_back(GetHugeFile());
  hs.push_back(GetHugeFileBulkHash());
  AppendVectorToVector(GetHugeFileChunkHashes(), &hs);

  pathes.push_back(GetSmallFile());
  hs.push_back(GetSmallFileBulkHash());

  pathes.push_back(GetBigFile());
  hs.push_back(GetBigFileBulkHash());
  AppendVectorToVector(GetBigFileChunkHashes(), &hs);

  TestProcessFiles(pathes, hs);
}

TEST_F(T_IngestionStress, ProcessMultipeFilesWithoutChunkingSlow) {
  std::vector<std::string> pathes;
  ExpectedHashStrings hs;

  pathes.push_back(GetEmptyFile());
  hs.push_back(GetEmptyFileBulkHash());

  pathes.push_back(GetHugeFile());
  hs.push_back(GetHugeFileBulkHash());

  pathes.push_back(GetBigFile());
  hs.push_back(GetBigFileBulkHash());

  pathes.push_back(GetHugeFile());
  hs.push_back(GetHugeFileBulkHash());

  pathes.push_back(GetHugeFile());
  hs.push_back(GetHugeFileBulkHash());

  pathes.push_back(GetSmallFile());
  hs.push_back(GetSmallFileBulkHash());

  pathes.push_back(GetBigFile());
  hs.push_back(GetBigFileBulkHash());

  TestProcessFiles(pathes, hs, true, /* legacy bulk */
                   false /* chunking */);
}

TEST_F(T_IngestionStress, ProcessMultipleFilesInSeparateWavesSlow) {
  IngestionPipeline pipeline(uploader_, MockSpoolerDefinition());
  pipeline.Spawn();

  // first wave...
  pipeline.Process(new FileIngestionSource(GetEmptyFile()), true);
  pipeline.WaitFor();
  CheckHash(uploader_->results, GetEmptyFileBulkHash());
  uploader_->ClearResults();

  // second wave...
  // some small and medium sized files with file chunking enabled
  // one big file without file chunking
  pipeline.Process(new FileIngestionSource(GetEmptyFile()), true);
  pipeline.Process(new FileIngestionSource(GetSmallFile()), true);
  pipeline.Process(new FileIngestionSource(GetBigFile()), true);
  pipeline.Process(new FileIngestionSource(GetHugeFile()), false,
                   shash::kSuffixCatalog);
  ExpectedHashStrings hs;
  hs.push_back(GetEmptyFileBulkHash());
  hs.push_back(GetSmallFileBulkHash());
  hs.push_back(GetBigFileBulkHash());
  AppendVectorToVector(GetBigFileChunkHashes(), &hs);
  hs.push_back(GetHugeFileBulkHash(shash::kSuffixCatalog));
  pipeline.WaitFor();
  CheckHashes(uploader_->results, hs);
  hs.clear();
  uploader_->ClearResults();

  // third wave...
  pipeline.Process(new FileIngestionSource(GetSmallFile()), true,
                   shash::kSuffixCertificate);
  pipeline.WaitFor();
  CheckHash(uploader_->results,
            GetSmallFileBulkHash(shash::kSuffixCertificate));
  uploader_->ClearResults();
}

namespace {
struct CallbackTest {
  static void CallbackFn(const upload::SpoolerResult &result) {
    EXPECT_EQ(0, result.return_code);

    result_content_hash = result.content_hash;
    result_local_path = result.local_path;
    result_chunk_list = result.file_chunks;
  }

  static void CallbackCount(const upload::SpoolerResult &result) {
    EXPECT_EQ(0, result.return_code);
    atomic_inc64(&counter);
  }

  static atomic_int64 counter;
  static shash::Any result_content_hash;
  static std::string result_local_path;
  static FileChunkList result_chunk_list;
};
shash::Any CallbackTest::result_content_hash;
std::string CallbackTest::result_local_path;
FileChunkList CallbackTest::result_chunk_list;
atomic_int64 CallbackTest::counter = 0;
}  // anonymous namespace

TEST_F(T_IngestionStress, ProcessingCallbackForSmallFile) {
  IngestionPipeline pipeline(uploader_, MockSpoolerDefinition());
  pipeline.Spawn();
  pipeline.RegisterListener(&CallbackTest::CallbackFn);

  pipeline.Process(new FileIngestionSource(GetSmallFile()), true,
                   shash::kSuffixHistory);
  pipeline.WaitFor();

  shash::Any expected_content_hash(shash::kSha1,
                                   shash::HexPtr(GetSmallFileBulkHash().first));
  EXPECT_EQ(expected_content_hash, CallbackTest::result_content_hash);
  EXPECT_EQ(GetSmallFile(), CallbackTest::result_local_path);
  EXPECT_EQ(0u, CallbackTest::result_chunk_list.size());
}

TEST_F(T_IngestionStress, ProcessingCallbackForBigFile) {
  IngestionPipeline pipeline(uploader_, MockSpoolerDefinition());
  pipeline.Spawn();
  pipeline.RegisterListener(&CallbackTest::CallbackFn);

  pipeline.Process(new FileIngestionSource(GetBigFile()), true,
                   shash::kSuffixCatalog);
  pipeline.WaitFor();

  shash::Any expected_content_hash(shash::kSha1,
                                   shash::HexPtr(GetBigFileBulkHash().first));
  const size_t number_of_chunks = GetBigFileChunkHashes().size();
  EXPECT_EQ(expected_content_hash, CallbackTest::result_content_hash);
  EXPECT_EQ(GetBigFile(), CallbackTest::result_local_path);
  EXPECT_EQ(number_of_chunks, CallbackTest::result_chunk_list.size());
}

TEST_F(T_IngestionStress, RealWorldSlow) {
  Prng prng;
  prng.InitSeed(42);

  // Prepare 100 random files roughly normally distributed around 4k
  unsigned S = 100;
  unsigned T = 12;
  unsigned mean = 4192;
  std::vector<unsigned char *> buffers;
  std::vector<unsigned> buffer_sizes;
  for (unsigned i = 0; i < S; ++i) {
    uint32_t size = 0;
    for (unsigned j = 0; j < T; ++j) {
      size += prng.Next(2 * mean);
    }
    size /= T;
    buffers.push_back(static_cast<unsigned char *>(smalloc(size)));
    buffer_sizes.push_back(size);

    for (unsigned j = 0; j < size; ++j) {
      buffers[i][j] = prng.Next(256);
    }
  }

  uploader_->keep_results = false;
  IngestionPipeline pipeline(uploader_,
                             MockSpoolerDefinition(false /* bulk chunks */));

  int N = 250000;
  pipeline.Spawn();
  pipeline.RegisterListener(&CallbackTest::CallbackCount);

  // Process
  for (int i = 0; i < N; ++i) {
    MemoryIngestionSource *s = new MemoryIngestionSource(
        StringifyInt(i), buffers[i % S], buffer_sizes[i % S]);
    pipeline.Process(s, true /* allow_chunking */);
  }
  pipeline.WaitFor();

  EXPECT_EQ(N, CallbackTest::counter);

  for (unsigned i = 0; i < S; ++i) {
    free(buffers[i]);
  }
}
