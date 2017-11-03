/**
 * This file is part of the CernVM File System.
 */

#ifndef TEST_UNITTESTS_C_MOCK_UPLOADER_H_
#define TEST_UNITTESTS_C_MOCK_UPLOADER_H_

#include <cassert>
#include <cstdlib>
#include <string>

#include "hash.h"
#include "testutil.h"

struct MockStreamHandle : public upload::UploadStreamHandle {
  explicit MockStreamHandle(const CallbackTN *commit_callback)
    : UploadStreamHandle(commit_callback)
    , data(NULL)
    , nbytes(0)
    , marker(0)
  { }

  virtual ~MockStreamHandle() {
    free(data);
    data = NULL;
    nbytes = 0;
    marker = 0;
  }

  void Extend(const size_t bytes) {
    if (data == NULL) {
      data = reinterpret_cast<unsigned char *>(malloc(bytes));
    } else {
      data = reinterpret_cast<unsigned char *>(srealloc(data, nbytes + bytes));
    }
    nbytes += bytes;
  }

  void Append(upload::AbstractUploader::UploadBuffer buffer) {
    Extend(buffer.size);
    void *b_ptr = buffer.data;
    unsigned char *r_ptr = data + marker;
    memcpy(r_ptr, b_ptr, buffer.size);
    marker += buffer.size;
  }

  unsigned char *data;
  size_t nbytes;
  off_t marker;
};


/**
 * Mocked uploader that just keeps the processing results in memory for later
 * inspection.
 */
class IngestionMockUploader
  : public AbstractMockUploader<IngestionMockUploader> {
 public:
  struct Result {
    Result(MockStreamHandle *handle, const shash::Any &computed_hash)
      : computed_hash(computed_hash)
    {
      EXPECT_EQ(RecomputeContentHash(handle->data, handle->nbytes),
                computed_hash)
        << "returned content hash differs from recomputed content hash";
    }

    shash::Any RecomputeContentHash(unsigned char *data, const size_t sz) {
      shash::Any recomputed_content_hash(computed_hash.algorithm);
      HashMem(data, sz, &recomputed_content_hash);
      return recomputed_content_hash;
    }

    shash::Any computed_hash;
  };
  typedef std::vector<Result> Results;

 public:
  explicit IngestionMockUploader(
    const upload::SpoolerDefinition &spooler_definition)
    : AbstractMockUploader<IngestionMockUploader>(spooler_definition)
   { }

  virtual std::string name() const { return "IngestionMockUploader"; }
  void ClearResults() { results.clear(); }

  upload::UploadStreamHandle *InitStreamedUpload(
    const CallbackTN *callback = NULL)
  {
    return new MockStreamHandle(callback);
  }

  void StreamedUpload(
    upload::UploadStreamHandle *handle,
    upload::AbstractUploader::UploadBuffer buffer,
    const CallbackTN *callback = NULL)
  {
    MockStreamHandle *local_handle = dynamic_cast<MockStreamHandle *>(handle);
    assert(local_handle != NULL);
    local_handle->Append(buffer);
    Respond(callback,
            upload::UploaderResults(upload::UploaderResults::kBufferUpload, 0));
  }

  void FinalizeStreamedUpload(
    upload::UploadStreamHandle *handle,
    const shash::Any &content_hash)
  {
    MockStreamHandle *local_handle = dynamic_cast<MockStreamHandle *>(handle);
    assert(local_handle != NULL);
    results.push_back(Result(local_handle, content_hash));
    const CallbackTN *callback = local_handle->commit_callback;
    delete handle;
    Respond(callback,
            upload::UploaderResults(upload::UploaderResults::kChunkCommit, 0));
  }

  Results results;
};

#endif  // TEST_UNITTESTS_C_MOCK_UPLOADER_H_
