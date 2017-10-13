/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <unistd.h>

#include "file_processing/char_buffer.h"
#include "hash.h"
#include "testutil.h"

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
      : AbstractMockUploader<UF_MockUploader>(spooler_definition),
        initialize_called(false) {}

  virtual std::string name() const { return "UFMock"; }

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
    UF_MockStreamHandle *handle =
        static_cast<UF_MockStreamHandle *>(abstract_handle);
    handle->uploads++;
    last_buffer = buffer;
    Respond(callback, UploaderResults(UploaderResults::kBufferUpload, 0));
    last_offset += buffer.size;
  }

  void FinalizeStreamedUpload(upload::UploadStreamHandle *abstract_handle,
                              const shash::Any &content_hash) {
    UF_MockStreamHandle *handle =
        static_cast<UF_MockStreamHandle *>(abstract_handle);
    handle->commits++;
    const UF_MockStreamHandle::CallbackTN *callback = handle->commit_callback;
    delete handle;
    Respond(callback, UploaderResults(UploaderResults::kChunkCommit, 0));
  }

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

  sleep(1);
  EXPECT_TRUE(uploader->worker_thread_running);

  uploader->TearDown();
  EXPECT_FALSE(uploader->worker_thread_running);

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

  sleep(1);
  EXPECT_TRUE(uploader->worker_thread_running);

  UploadStreamHandle *handle = uploader->InitStreamedUpload(
      AbstractUploader::MakeCallback(&ChunkUploadCompleteCallback_T_Callbacks));
  ASSERT_NE(static_cast<void *>(NULL), handle);

  EXPECT_EQ(1, UF_MockStreamHandle::instances);

  sleep(1);

  EXPECT_EQ(0, chunk_upload_complete_callback_calls);
  EXPECT_EQ(0, buffer_upload_complete_callback_calls);

  CharBuffer b1(1024);
  b1.SetUsedBytes(1000);
  b1.SetBaseOffset(0);
  uploader->ScheduleUpload(
    handle,
    AbstractUploader::UploadBuffer(b1.used_bytes(), b1.ptr()),
    AbstractUploader::MakeCallback(&BufferUploadCompleteCallback_T_Callbacks));

  sleep(1);

  EXPECT_EQ(0, chunk_upload_complete_callback_calls);
  EXPECT_EQ(1, buffer_upload_complete_callback_calls);

  CharBuffer b2(1024);
  b2.SetUsedBytes(1000);
  b2.SetBaseOffset(1000);
  uploader->ScheduleUpload(
    handle,
    AbstractUploader::UploadBuffer(b2.used_bytes(), b2.ptr()),
    AbstractUploader::MakeCallback(&BufferUploadCompleteCallback_T_Callbacks));

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

// Caveat: 'offset' it automatically updated!
static CharBuffer *MakeBuffer(const size_t buffer_size, const size_t used_bytes,
                              size_t *offset) {
  CharBuffer *buffer = new CharBuffer(buffer_size);
  assert(buffer != NULL);
  buffer->SetUsedBytes(used_bytes);
  buffer->SetBaseOffset(static_cast<off_t>(*offset));
  *offset += used_bytes;
  return buffer;
}

TEST(T_UploadFacility, DataBlockBasicOrdering) {
  UF_MockUploader *uploader = UF_MockUploader::MockConstruct();

  ASSERT_NE(static_cast<UF_MockUploader *>(NULL), uploader);
  EXPECT_TRUE(uploader->initialize_called);

  sleep(1);
  EXPECT_TRUE(uploader->worker_thread_running);

  UploadStreamHandle *handle = uploader->InitStreamedUpload(
      AbstractUploader::MakeCallback(&ChunkUploadCompleteCallback_T_Ordering));
  ASSERT_NE(static_cast<void *>(NULL), handle);

  std::vector<CharBuffer *> buffers;
  size_t overall_size = 0;
  buffers.push_back(MakeBuffer(1024, 384, &overall_size));
  buffers.push_back(MakeBuffer(2048, 1024, &overall_size));
  buffers.push_back(MakeBuffer(4096, 4096, &overall_size));
  buffers.push_back(MakeBuffer(8192, 6172, &overall_size));
  buffers.push_back(MakeBuffer(768, 128, &overall_size));
  buffers.push_back(MakeBuffer(2000, 1921, &overall_size));
  buffers.push_back(MakeBuffer(9999, 9999, &overall_size));
  buffers.push_back(MakeBuffer(100, 10, &overall_size));
  buffers.push_back(MakeBuffer(4096, 128, &overall_size));
  buffers.push_back(MakeBuffer(4627, 1950, &overall_size));
  ASSERT_EQ(size_t(25812), overall_size);

  std::vector<CharBuffer *>::const_iterator i = buffers.begin();
  std::vector<CharBuffer *>::const_iterator iend = buffers.end();
  for (; i != iend; ++i) {
    uploader->ScheduleUpload(
      handle,
      AbstractUploader::UploadBuffer((*i)->used_bytes(), (*i)->ptr()),
      AbstractUploader::MakeCallback(&BufferUploadCompleteCallback_T_Ordering));
  }

  uploader->ScheduleCommit(handle, shash::Any());
  uploader->WaitForUpload();
  uploader->TearDown();

  EXPECT_EQ(overall_size, overall_size_ordering);
  EXPECT_TRUE(ordering_test_done);

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
