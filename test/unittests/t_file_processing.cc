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
           const hash::Any   &computed_content_hash) :
      computed_content_hash(computed_content_hash)
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

    hash::Any computed_content_hash;
    hash::Any recomputed_content_hash;
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
    results_.push_back(Result(local_handle, content_hash));

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

  const std::string GetSmallFile() {
    LazilyCreateDummyFile(MockUploader::sandbox_path, 50, &small_file_, 314);
    return small_file_;
  }

  const std::string GetBigFile() {
    LazilyCreateDummyFile(MockUploader::sandbox_path, 4*1024, &big_file_, 1337);
    return big_file_;
  }

  const std::string GetHugeFile() {
    LazilyCreateDummyFile(MockUploader::sandbox_path, 100*1024, &huge_file_, 1);
    return huge_file_;
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
    FILE *file = CreateTempFile(sandbox_path + "/dummy", 0600, "rw+", file_name);
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
  upload::FileProcessor processor(&uploader_, true);
  const std::string &path = GetEmptyFile();
  const hash::Any empty_content_hash(
                      hash::kSha1,
                      hash::HexPtr("e8ec3d88b62ebf526e4e5a4ff6162a3aa48a6b78"));

  processor.Process(path, true);
  processor.WaitForProcessing();

  const MockUploader::Results &results = uploader_.results();
  EXPECT_EQ (1u, results.size());

  EXPECT_EQ (empty_content_hash, results.front().computed_content_hash)
    << "empty hash should be " << empty_content_hash.ToString() << " "
    << "but was " << results.front().computed_content_hash.ToString();
}
