#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <cerrno>

#include <openssl/sha.h>

#include <iostream>

#include "../../cvmfs/util.h"
#include "../../cvmfs/prng.h"
#include "../../cvmfs/upload_facility.h"
#include "../../cvmfs/upload_spooler_definition.h"
#include "../../cvmfs/upload_spooler_result.h"
#include "../../cvmfs/upload_file_processing/file_processor.h"
#include "../../cvmfs/upload_file_processing/char_buffer.h"

struct MockStreamHandle : public upload::UploadStreamHandle {
  MockStreamHandle(const callback_t   *commit_callback) :
    UploadStreamHandle(commit_callback),
    data(NULL), nbytes(0), marker(0) {}

  ~MockStreamHandle() {
    if (data != NULL) {
      free(data);
      data   = NULL;
      nbytes = 0;
      marker = 0;
    }
  }

  void Extend(const size_t bytes) {
    if (data == NULL) {
      data = (char*)malloc(bytes);
      ASSERT_NE(static_cast<char*>(0), data)
        << "failed to malloc " << bytes << " bytes";
    } else {
      data = (char*)realloc(data, nbytes + bytes);
      ASSERT_NE(static_cast<char*>(0), data)
        << "failed to realloc to extend by " << bytes << " bytes";
    }

    nbytes += bytes;
  }

  void Append(const upload::CharBuffer *buffer) {
    const size_t bytes = buffer->used_bytes();
    Extend(bytes);
    memcpy(data + marker, buffer->ptr(), bytes);
    marker += bytes;
  }

  char*  data;
  size_t nbytes;
  off_t  marker;
};

/**
 * Mocked uploader that just keeps the processing results in memory for later
 * inspection.
 */
class MockUploader : public upload::AbstractUploader {
 private:
  static const bool not_implemented = false;

 public:
  static const std::string sandbox_path;
  static const std::string sandbox_tmp_dir;
  static const size_t      min_chunk_size = 512000;
  static const size_t      avg_chunk_size = min_chunk_size * 2;
  static const size_t      max_chunk_size = min_chunk_size * 4;

 public:
  struct Result {
    Result(MockStreamHandle  *handle,
           const hash::Any   &computed_content_hash,
           const std::string &hash_suffix) :
      computed_content_hash(computed_content_hash),
      hash_suffix(hash_suffix)
    {
      RecomputeContentHash(handle->data, handle->nbytes);

      EXPECT_EQ (recomputed_content_hash, computed_content_hash)
        << "returned content hash differs from recomputed content hash";
    }

    void RecomputeContentHash(const char* data, const size_t nbytes) {
      SHA_CTX sha_context;
      int sha1_retval;

      sha1_retval = SHA1_Init(&sha_context);
      ASSERT_EQ (1, sha1_retval) << "failed to initalize SHA1 context";

      sha1_retval = SHA1_Update(&sha_context, data, nbytes);
      ASSERT_EQ (1, sha1_retval) << "failed to compute SHA1 checksum";

      unsigned char sha1_digest_[SHA_DIGEST_LENGTH];
      sha1_retval = SHA1_Final(sha1_digest_, &sha_context);
      ASSERT_EQ (1, sha1_retval) << "failed to finalize SHA1 checksum";

      recomputed_content_hash = hash::Any(hash::kSha1,
                                          sha1_digest_,
                                          SHA_DIGEST_LENGTH);
    }

    hash::Any   computed_content_hash;
    hash::Any   recomputed_content_hash;
    std::string hash_suffix;
  };
  typedef std::vector<Result> Results;

 public:
  MockUploader() :
    AbstractUploader(upload::SpoolerDefinition("mock," + sandbox_path + "," +
                                                         sandbox_tmp_dir,
                                               true,
                                               min_chunk_size,
                                               avg_chunk_size,
                                               max_chunk_size)) {}

  const Results& results() const { return results_; }

  void ClearResults() {
    results_.clear();
  }

  void Upload(const std::string  &local_path,
              const std::string  &remote_path,
              const callback_t   *callback = NULL) {
    assert (MockUploader::not_implemented);
  }

  upload::UploadStreamHandle* InitStreamedUpload(
                                            const callback_t *callback = NULL) {
    return new MockStreamHandle(callback);
  }

  void Upload(upload::UploadStreamHandle  *handle,
              upload::CharBuffer          *buffer,
              const callback_t            *callback = NULL) {
    MockStreamHandle *local_handle = static_cast<MockStreamHandle*>(handle);
    local_handle->Append(buffer);

    Respond(callback, upload::UploaderResults(0, buffer));
  }

  void FinalizeStreamedUpload(upload::UploadStreamHandle *handle,
                              const hash::Any             content_hash,
                              const std::string           hash_suffix) {
    MockStreamHandle *local_handle = static_cast<MockStreamHandle*>(handle);

    // summarize the results produced by the FileProcessor
    results_.push_back(Result(local_handle, content_hash, hash_suffix));

    // remove the stream handle and fire callback
    const callback_t *callback = local_handle->commit_callback;
    delete local_handle;
    Respond(callback, upload::UploaderResults(0));
  }

  bool Remove(const std::string &file_to_delete) {
    assert (MockUploader::not_implemented);
  }

  bool Peek(const std::string &path) const {
    assert (MockUploader::not_implemented);
  }

  unsigned int GetNumberOfErrors() const {
    assert (MockUploader::not_implemented);
  }

 protected:
  Results results_;
};

const std::string MockUploader::sandbox_path    = "/tmp/cvmfs_ut_fileprocessing";
const std::string MockUploader::sandbox_tmp_dir = MockUploader::sandbox_path + "/tmp";


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


class T_FileProcessing : public ::testing::Test {
 public:
  typedef std::pair<std::string, std::string> ExpectedHashString;
  typedef std::vector<ExpectedHashString>     ExpectedHashStrings;

 protected:
  void SetUp() {
    CreateSandbox(MockUploader::sandbox_path,
                  MockUploader::sandbox_tmp_dir);
  }

  void TearDown() {
    RemoveSandbox(MockUploader::sandbox_path,
                  MockUploader::sandbox_tmp_dir);
  }

  const std::string& GetEmptyFile() {
    LazilyCreateDummyFile(MockUploader::sandbox_path, 0, &empty_file_, 42);
    return empty_file_;
  }

  ExpectedHashString GetEmptyFileBulkHash(const std::string suffix = "") const {
    return std::make_pair("e8ec3d88b62ebf526e4e5a4ff6162a3aa48a6b78", suffix);
  }

  const std::string GetSmallFile() {
    LazilyCreateDummyFile(MockUploader::sandbox_path, 50, &small_file_, 314);
    return small_file_;
  }

  ExpectedHashString GetSmallFileBulkHash(const std::string suffix = "") const {
    return std::make_pair("7935fe23e1f9959b176999d63f6b8ccacc7c6eff", suffix);
  }

  const std::string GetBigFile() {
    LazilyCreateDummyFile(MockUploader::sandbox_path, 4*1024, &big_file_, 1337);
    return big_file_;
  }

  ExpectedHashString GetBigFileBulkHash(const std::string suffix = "") const {
    return std::make_pair("32f526af5fb573ee70925b108310447529d9b9eb", suffix);
  }

  ExpectedHashStrings GetBigFileChunkHashes() const {
    ExpectedHashStrings h;
    h.push_back(std::make_pair("eef05f8b57bfd1178e761e1ea4cf02d0409e5a63", "P"));
    h.push_back(std::make_pair("aa965b9d3c790713320a9358c09ab2d1819d35e3", "P"));
    return h;
  }

  const std::string GetHugeFile() {
    LazilyCreateDummyFile(MockUploader::sandbox_path, 100*1024, &huge_file_, 1);
    return huge_file_;
  }

  ExpectedHashString GetHugeFileBulkHash(const std::string suffix = "") const {
    return std::make_pair("40a3de440ed2684253ca119cde8884144e7aaa1c", suffix);
  }

  ExpectedHashStrings GetHugeFileChunkHashes() const {
    ExpectedHashStrings h;
    h.push_back(std::make_pair("4bee676ffd0d311db73eba75e4a465436e966601", "P"));
    h.push_back(std::make_pair("0c4e853758b11d4056d9271a436124fcd2044bc5", "P"));
    h.push_back(std::make_pair("f81e89aefef730e31d1ede44bed1041766e592ac", "P"));
    h.push_back(std::make_pair("5bddaa73a861157f77b07e334514517370601760", "P"));
    h.push_back(std::make_pair("b005701a6f772e92a68927dd1a07bd6de928deee", "P"));
    h.push_back(std::make_pair("aeb08953ac450f250f95acdedae5f4858f7abf43", "P"));
    h.push_back(std::make_pair("ec4610784f80d2f2522c37f134d22dfa10d39da6", "P"));
    h.push_back(std::make_pair("c2b5ebc14c405c9787c6e16a66d0a28b73cae833", "P"));
    h.push_back(std::make_pair("daba9e1c983c65ef9a23043ee376c145d61ccc1b", "P"));
    h.push_back(std::make_pair("e287f1bff654cb4a83b29ea08813e2c1bd285f2e", "P"));
    h.push_back(std::make_pair("bb9455534f5d9bbb642033fcdeb61e12c63aeb7f", "P"));
    h.push_back(std::make_pair("101361a4995d5cfbd37401f57f45227c1da4145a", "P"));
    h.push_back(std::make_pair("ad56161c6884a81e3fa7e0b2496e8ddbcaff457a", "P"));
    h.push_back(std::make_pair("be9873d23f880799e74c0bc55fa0619de073d31c", "P"));
    h.push_back(std::make_pair("b7b8996e8a5e537bd22fa3a16040910089455036", "P"));
    h.push_back(std::make_pair("5b086558e63c486efe951af118deee3b78132246", "P"));
    h.push_back(std::make_pair("1acc092c0cae7cf86cb7cbce0b0faf3df72ecb8b", "P"));
    h.push_back(std::make_pair("be48464ac14395321c71c000d25c3835a6dbfed1", "P"));
    h.push_back(std::make_pair("c3a18c087b2c42bc9fa4e307fb5dbb13a6aaceb2", "P"));
    h.push_back(std::make_pair("bc8cf85e6d29e05b90dac42ce0bc474ea92648e5", "P"));
    h.push_back(std::make_pair("89065b8f67172f07ff0b794cfc975b1bc0520794", "P"));
    h.push_back(std::make_pair("ce2d607984eabad83acb3242b8f43c8f51337871", "P"));
    h.push_back(std::make_pair("5dd076bf24590b966ebf29f252ac119fad80face", "P"));
    h.push_back(std::make_pair("c79d08a1689889ef064774c4c3d2ef3378a69ff6", "P"));
    return h;
  }

  template <class VectorT>
  void AppendVectorToVector(VectorT &vector, const VectorT &appendee) const {
    vector.insert(vector.end(), appendee.begin(), appendee.end());
  }

  void TestProcessFile(const std::string        &file_path,
                       const ExpectedHashString &reference_hash,
                       const bool                use_chunking = true) {
    ExpectedHashStrings reference_hash_strings;
    reference_hash_strings.push_back(reference_hash);
    TestProcessFile(file_path, reference_hash_strings, use_chunking);
  }

  void TestProcessFile(const std::string         &file_path,
                       const ExpectedHashStrings &reference_hash_strings,
                       const bool                 use_chunking = true) {
    std::vector<std::string> file_pathes;
    file_pathes.push_back(file_path);
    TestProcessFiles(file_pathes, reference_hash_strings, use_chunking);
  }

  void TestProcessFiles(const std::vector<std::string> &file_pathes,
                        const ExpectedHashStrings      &reference_hash_strings,
                        const bool                      use_chunking = true) {
    upload::FileProcessor processor(&uploader_, use_chunking);

    std::vector<std::string>::const_iterator i    = file_pathes.begin();
    std::vector<std::string>::const_iterator iend = file_pathes.end();
    for (; i != iend; ++i) {
      processor.Process(*i, use_chunking);
    }

    processor.WaitForProcessing();

    const MockUploader::Results &results = uploader_.results();
    CheckHashes(results, reference_hash_strings);
  }

  void CheckHash(const MockUploader::Results &results,
                 const ExpectedHashString    &expected_hash) const {
    ExpectedHashStrings expected_hashes;
    expected_hashes.push_back(expected_hash);
    CheckHashes(results, expected_hashes);
  }

  void CheckHashes(const MockUploader::Results &results,
                   const ExpectedHashStrings   &reference_hash_strings) const {
    EXPECT_EQ (reference_hash_strings.size(), results.size())
      << "number of generated chunks did not match";

    // convert hash strings into hash::Any structs
    typedef std::vector<std::pair<hash::Any, std::string> > ExpectedHashes;
    ExpectedHashes reference_hashes;
    ExpectedHashStrings::const_iterator i    = reference_hash_strings.begin();
    ExpectedHashStrings::const_iterator iend = reference_hash_strings.end();
    for (; i != iend; ++i) {
      reference_hashes.push_back(
        std::make_pair(hash::Any(hash::kSha1, hash::HexPtr(i->first)),
                       i->second)
      );
    }

    // check if we can find the computed hashes in the expected hashes
    // Note: I know that this is O(n^2)... ;-)
    MockUploader::Results::const_iterator j    = results.begin();
    MockUploader::Results::const_iterator jend = results.end();
    for (; j != jend; ++j) {
      bool found = false;
      ExpectedHashes::const_iterator k    = reference_hashes.begin();
      ExpectedHashes::const_iterator kend = reference_hashes.end();
      for (; k != kend; ++k) {
        if (k->first == j->computed_content_hash) {
          EXPECT_EQ (k->second, j->hash_suffix)
            << "hash suffix does not fit";
          found = true;
          break;
        }
      }

      EXPECT_TRUE (found)
        << "did not find generated hash "
        << j->computed_content_hash.ToString() << " "
        << "in the provided reference hashes";
    }
  }

 private:
  void CreateSandbox(const std::string &sandbox_path,
                     const std::string &sandbox_tmp_dir) {
    bool retval;

    retval = MkdirDeep(sandbox_path, 0700);
    ASSERT_TRUE (retval) << "failed to create sandbox";

    retval = MkdirDeep(sandbox_tmp_dir, 0700);
    ASSERT_TRUE (retval) << "failed to create sandbox tmp directory";
  }

  void RemoveSandbox(const std::string &sandbox_path,
                     const std::string &sandbox_tmp_dir) {
    bool retval;

    retval = RemoveTree(sandbox_tmp_dir);
    ASSERT_TRUE (retval) << "failed to remove sandbox tmp directory";

    retval = RemoveTree(sandbox_path);
    ASSERT_TRUE (retval) << "failed to remove sandbox";
  }

  void CreateRandomBuffer(char *buffer, const size_t nbytes, Prng &rng) {
    for (size_t i = 0; i < nbytes; ++i) {
      buffer[i] = rng.Next(256);
    }
  }

  void LazilyCreateDummyFile(const std::string &sandbox_path,
                             const size_t       file_size_kb,
                                   std::string *file_name,
                             const uint64_t     seed) {
    static const size_t kb = 1024;

    // if file was already created, we do not do it again!
    if (! file_name->empty()) {
      return;
    }

    // create a temporary file
    FILE *file = CreateTempFile(sandbox_path + "/dummy", 0600, "r+", file_name);
    ASSERT_NE (static_cast<FILE*>(0), file) << "failed to create tmp file";

    // file the temporary file with the requested number of (pseudo) random data
    Prng rng;
    rng.InitSeed(seed);
    for (size_t i = 0; i < file_size_kb; ++i) {
      typedef char buffer_type;
      buffer_type buffer[kb];
      CreateRandomBuffer(buffer, kb, rng);
      const size_t written = fwrite(buffer, sizeof(buffer_type), kb, file);
      ASSERT_EQ (written, kb * sizeof(buffer_type))
        << "failed to write to tmp (errno: " << errno << ")";
    }

    // close the generated dummy file
    const int retval = fclose(file);
    ASSERT_EQ (0, retval) << "failed to close tmp file";
  }

 protected:
  MockUploader uploader_;

  std::string  empty_file_;
  std::string  small_file_;
  std::string  big_file_;
  std::string  huge_file_;
};


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_FileProcessing, Initialize) {
  upload::FileProcessor processor(&uploader_, true);
  processor.WaitForProcessing();
}


TEST_F(T_FileProcessing, ProcessEmptyFile) {
  TestProcessFile(GetEmptyFile(), GetEmptyFileBulkHash());
}


TEST_F(T_FileProcessing, ProcessSmallFile) {
  const std::string &path = GetSmallFile();
  TestProcessFile(path, GetSmallFileBulkHash());
}


TEST_F(T_FileProcessing, ProcessBigFile) {
  ExpectedHashStrings hs = GetBigFileChunkHashes();
  hs.push_back(GetBigFileBulkHash());
  TestProcessFile(GetBigFile(), hs);
}


TEST_F(T_FileProcessing, ProcessHugeFile) {
  ExpectedHashStrings hs = GetHugeFileChunkHashes();
  hs.push_back(GetHugeFileBulkHash());
  TestProcessFile(GetHugeFile(), hs);
}


TEST_F(T_FileProcessing, ProcessBigFileWithoutChunks) {
  TestProcessFile(GetBigFile(), GetBigFileBulkHash(), false);
}


TEST_F(T_FileProcessing, ProcessMultipleFiles) {
  std::vector<std::string> pathes;
  ExpectedHashStrings hs;

  pathes.push_back(GetSmallFile());
  hs.push_back(GetSmallFileBulkHash());

  pathes.push_back(GetEmptyFile());
  hs.push_back(GetEmptyFileBulkHash());

  pathes.push_back(GetBigFile());
  hs.push_back(GetBigFileBulkHash());
  AppendVectorToVector(hs, GetBigFileChunkHashes());

  pathes.push_back(GetHugeFile());
  hs.push_back(GetHugeFileBulkHash());
  AppendVectorToVector(hs, GetHugeFileChunkHashes());

  pathes.push_back(GetHugeFile());
  hs.push_back(GetHugeFileBulkHash());
  AppendVectorToVector(hs, GetHugeFileChunkHashes());

  pathes.push_back(GetSmallFile());
  hs.push_back(GetSmallFileBulkHash());

  pathes.push_back(GetBigFile());
  hs.push_back(GetBigFileBulkHash());
  AppendVectorToVector(hs, GetBigFileChunkHashes());

  TestProcessFiles(pathes, hs);
}


TEST_F(T_FileProcessing, ProcessMultipeFilesWithoutChunking) {
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

  TestProcessFiles(pathes, hs, false);
}


TEST_F(T_FileProcessing, ProcessMultipleFilesInSeparateWaves) {
  const bool use_chunking = true;
  upload::FileProcessor processor(&uploader_, use_chunking);

  // first wave...
  processor.Process(GetEmptyFile(), true);
  processor.WaitForProcessing();
  CheckHash(uploader_.results(), GetEmptyFileBulkHash());
  uploader_.ClearResults();

  // second wave...
  // some small and medium sized files with file chunking enabled
  // one big file without file chunking
  processor.Process(GetEmptyFile(), true);
  processor.Process(GetSmallFile(), true);
  processor.Process(GetBigFile(), true);
  processor.Process(GetHugeFile(), false, "C");
  ExpectedHashStrings hs;
  hs.push_back(GetEmptyFileBulkHash());
  hs.push_back(GetSmallFileBulkHash());
  hs.push_back(GetBigFileBulkHash());
  AppendVectorToVector(hs, GetBigFileChunkHashes());
  hs.push_back(GetHugeFileBulkHash("C"));
  processor.WaitForProcessing();
  CheckHashes(uploader_.results(), hs);
  hs.clear();
  uploader_.ClearResults();

  // third wave...
  processor.Process(GetSmallFile(), true, "X");
  processor.WaitForProcessing();
  CheckHash(uploader_.results(), GetSmallFileBulkHash("X"));
  uploader_.ClearResults();
}


struct CallbackTest {
  static void CallbackFn(const upload::SpoolerResult &result) {
    EXPECT_EQ (0,  result.return_code);

    result_content_hash = result.content_hash;
    result_local_path   = result.local_path;
    result_chunk_list   = result.file_chunks;
  }

  static hash::Any     result_content_hash;
  static std::string   result_local_path;
  static FileChunkList result_chunk_list;
};
hash::Any     CallbackTest::result_content_hash;
std::string   CallbackTest::result_local_path;
FileChunkList CallbackTest::result_chunk_list;

TEST_F(T_FileProcessing, ProcessingCallbackForSmallFile) {
  const bool use_chunking = true;
  upload::FileProcessor processor(&uploader_, use_chunking);
  processor.RegisterListener(&CallbackTest::CallbackFn);

  processor.Process(GetSmallFile(), true, "T");
  processor.WaitForProcessing();

  hash::Any expected_content_hash(
    hash::kSha1,
    hash::HexPtr(GetSmallFileBulkHash().first));
  EXPECT_EQ (expected_content_hash, CallbackTest::result_content_hash);
  EXPECT_EQ (GetSmallFile(),        CallbackTest::result_local_path);
  EXPECT_EQ (0u,                    CallbackTest::result_chunk_list.size());
}


TEST_F(T_FileProcessing, ProcessingCallbackForBigFile) {
  const bool use_chunking = true;
  upload::FileProcessor processor(&uploader_, use_chunking);
  processor.RegisterListener(&CallbackTest::CallbackFn);

  processor.Process(GetBigFile(), true, "T");
  processor.WaitForProcessing();

  hash::Any expected_content_hash(
    hash::kSha1,
    hash::HexPtr(GetBigFileBulkHash().first));
  const size_t number_of_chunks = GetBigFileChunkHashes().size();
  EXPECT_EQ (expected_content_hash, CallbackTest::result_content_hash);
  EXPECT_EQ (GetBigFile(),          CallbackTest::result_local_path);
  EXPECT_EQ (number_of_chunks,      CallbackTest::result_chunk_list.size());
}
