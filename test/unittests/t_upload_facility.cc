/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdlib>

#include "crypto/hash.h"
#include "testutil.h"
#include "util/smalloc.h"

namespace upload {

struct UF_MockStreamHandle : public upload::UploadStreamHandle {
  explicit UF_MockStreamHandle(const CallbackTN *commit_callback)
      : UploadStreamHandle(commit_callback), commits(0), uploads(0) {
    ++instances;
  }

  ~UF_MockStreamHandle() { --instances; }

  int commits;
  int uploads;

  static int instances;
};

int UF_MockStreamHandle::instances = 0;

/**
 * Mocked uploader that just keeps the processing results in memory for later
 * inspection.
 */
class UF_MockUploader : public AbstractMockUploader<UF_MockUploader> {
 public:
  explicit UF_MockUploader(const SpoolerDefinition &spooler_definition)
      : AbstractMockUploader<UF_MockUploader>(spooler_definition)
      , initialize_called(false) { }

  virtual std::string name() const { return "UFMock"; }

  virtual bool Create() { return true; }

  upload::UploadStreamHandle *InitStreamedUpload(
      const CallbackTN *callback = NULL) {
    last_offset = 0;
    return new UF_MockStreamHandle(callback);
  }

  bool Initialize() {
    AbstractUploader::Initialize();
    initialize_called = true;
    return true;
  }

  void StreamedUpload(upload::UploadStreamHandle *abstract_handle,
                      upload::AbstractUploader::UploadBuffer buffer,
                      const CallbackTN *callback = NULL) {
    UF_MockStreamHandle *handle = static_cast<UF_MockStreamHandle *>(
        abstract_handle);
    handle->uploads++;
    last_buffer = buffer;
    Respond(callback, UploaderResults(UploaderResults::kBufferUpload, 0));
    last_offset += buffer.size;
  }

  void FinalizeStreamedUpload(upload::UploadStreamHandle *abstract_handle,
                              const shash::Any &content_hash) {
    UF_MockStreamHandle *handle = static_cast<UF_MockStreamHandle *>(
        abstract_handle);
    handle->commits++;
    const UF_MockStreamHandle::CallbackTN *callback = handle->commit_callback;
    delete handle;
    Respond(callback, UploaderResults(UploaderResults::kChunkCommit, 0));
  }

  virtual int64_t DoGetObjectSize(const std::string &file_name) { return 0; }

 public:
  static upload::AbstractUploader::UploadBuffer last_buffer;
  static unsigned last_offset;
  bool initialize_called;
};

unsigned UF_MockUploader::last_offset = 0;
upload::AbstractUploader::UploadBuffer UF_MockUploader::last_buffer;

//------------------------------------------------------------------------------

TEST(T_UploadFacility, InitializeAndTearDown) {
  UF_MockUploader *uploader = UF_MockUploader::MockConstruct();

  ASSERT_NE(static_cast<UF_MockUploader *>(NULL), uploader);
  EXPECT_TRUE(uploader->initialize_called);

  uploader->TearDown();
  delete uploader;
}

//------------------------------------------------------------------------------

volatile int chunk_upload_complete_callback_calls = 0;
void ChunkUploadCompleteCallback_T_Callbacks(const UploaderResults &results) {
  EXPECT_EQ(UploaderResults::kChunkCommit, results.type);
  EXPECT_EQ(0, results.return_code);
  EXPECT_EQ("", results.local_path);
  ++chunk_upload_complete_callback_calls;
}

volatile int buffer_upload_complete_callback_calls = 0;
void BufferUploadCompleteCallback_T_Callbacks(const UploaderResults &results) {
  EXPECT_EQ(UploaderResults::kBufferUpload, results.type);
  EXPECT_EQ(0, results.return_code);
  EXPECT_EQ("", results.local_path);
  ++buffer_upload_complete_callback_calls;
}

TEST(T_UploadFacility, CallbacksSlow) {
  UF_MockUploader *uploader = UF_MockUploader::MockConstruct();

  ASSERT_NE(static_cast<UF_MockUploader *>(NULL), uploader);
  EXPECT_TRUE(uploader->initialize_called);
  EXPECT_EQ(0, chunk_upload_complete_callback_calls);
  EXPECT_EQ(0, buffer_upload_complete_callback_calls);
  EXPECT_EQ(0, UF_MockStreamHandle::instances);

  UploadStreamHandle *handle = uploader->InitStreamedUpload(
      AbstractUploader::MakeCallback(&ChunkUploadCompleteCallback_T_Callbacks));
  ASSERT_NE(static_cast<void *>(NULL), handle);

  EXPECT_EQ(1, UF_MockStreamHandle::instances);

  sleep(1);

  EXPECT_EQ(0, chunk_upload_complete_callback_calls);
  EXPECT_EQ(0, buffer_upload_complete_callback_calls);

  unsigned char b1[1000];
  uploader->ScheduleUpload(handle,
                           AbstractUploader::UploadBuffer(1000, b1),
                           AbstractUploader::MakeCallback(
                               &BufferUploadCompleteCallback_T_Callbacks));

  sleep(1);

  EXPECT_EQ(0, chunk_upload_complete_callback_calls);
  EXPECT_EQ(1, buffer_upload_complete_callback_calls);

  unsigned char b2[1000];
  uploader->ScheduleUpload(handle,
                           AbstractUploader::UploadBuffer(1000, b2),
                           AbstractUploader::MakeCallback(
                               &BufferUploadCompleteCallback_T_Callbacks));

  sleep(1);

  EXPECT_EQ(0, chunk_upload_complete_callback_calls);
  EXPECT_EQ(2, buffer_upload_complete_callback_calls);

  uploader->ScheduleCommit(handle, shash::Any());

  uploader->WaitForUpload();
  uploader->TearDown();

  EXPECT_EQ(1, chunk_upload_complete_callback_calls);
  EXPECT_EQ(2, buffer_upload_complete_callback_calls);
  EXPECT_EQ(0, UF_MockStreamHandle::instances);

  delete uploader;
}

//------------------------------------------------------------------------------

size_t overall_size_ordering = 0;
bool ordering_test_done = false;
void ChunkUploadCompleteCallback_T_Ordering(const UploaderResults &results) {
  ordering_test_done = true;
}

void BufferUploadCompleteCallback_T_Ordering(const UploaderResults &results) {
  EXPECT_EQ(UploaderResults::kBufferUpload, results.type);
  ASSERT_TRUE(UF_MockUploader::last_buffer.data != NULL);
  EXPECT_LT(size_t(0), UF_MockUploader::last_buffer.size);

  EXPECT_EQ(static_cast<off_t>(overall_size_ordering),
            UF_MockUploader::last_offset);
  overall_size_ordering += UF_MockUploader::last_buffer.size;
}


TEST(T_UploadFacility, DataBlockBasicOrdering) {
  UF_MockUploader *uploader = UF_MockUploader::MockConstruct();

  ASSERT_NE(static_cast<UF_MockUploader *>(NULL), uploader);
  EXPECT_TRUE(uploader->initialize_called);

  UploadStreamHandle *handle = uploader->InitStreamedUpload(
      AbstractUploader::MakeCallback(&ChunkUploadCompleteCallback_T_Ordering));
  ASSERT_NE(static_cast<void *>(NULL), handle);

  std::vector<unsigned> sizes;
  std::vector<unsigned char *> buffers;
  sizes.push_back(384);
  sizes.push_back(1024);
  sizes.push_back(4096);
  sizes.push_back(6172);
  sizes.push_back(128);
  sizes.push_back(1921);
  sizes.push_back(9999);
  sizes.push_back(10);
  sizes.push_back(128);
  sizes.push_back(1950);
  size_t overall_size = 0;
  for (unsigned i = 0; i < 10; ++i) {
    overall_size += sizes[i];
    buffers.push_back(reinterpret_cast<unsigned char *>(smalloc(sizes[i])));
  }
  ASSERT_EQ(size_t(25812), overall_size);

  for (unsigned i = 0; i < 10; ++i) {
    uploader->ScheduleUpload(
        handle,
        AbstractUploader::UploadBuffer(sizes[i], buffers[i]),
        AbstractUploader::MakeCallback(
            &BufferUploadCompleteCallback_T_Ordering));
  }

  uploader->ScheduleCommit(handle, shash::Any());
  uploader->WaitForUpload();
  uploader->TearDown();

  EXPECT_EQ(overall_size, overall_size_ordering);
  EXPECT_TRUE(ordering_test_done);

  for (unsigned i = 0; i < 10; ++i)
    free(buffers[i]);

  delete uploader;
}

TEST(T_UploadFacility, InitDtorRace) {
  UF_MockUploader *uploader = UF_MockUploader::MockConstruct();

  ASSERT_NE(static_cast<UF_MockUploader *>(NULL), uploader);
  EXPECT_TRUE(uploader->initialize_called);

  uploader->WaitForUpload();
  uploader->TearDown();
  delete uploader;
}

}  // namespace upload
