#include <gtest/gtest.h>
#include <unistd.h>

#include "../../cvmfs/util.h"
#include "../../cvmfs/upload_spooler_definition.h"
#include "../../cvmfs/upload_facility.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/file_processing/char_buffer.h"

#include "testutil.h"


using namespace upload;


struct MockStreamHandle_UF : public upload::UploadStreamHandle {
  MockStreamHandle_UF(const callback_t *commit_callback) :
    UploadStreamHandle(commit_callback),
    commits(0), uploads(0)
  {
    ++instances;
  }

  ~MockStreamHandle_UF() {
    --instances;
  }

  int commits;
  int uploads;

  static int instances;
};

int MockStreamHandle_UF::instances = 0;


/**
 * Mocked uploader that just keeps the processing results in memory for later
 * inspection.
 */
class MockUploader_UF : public AbstractUploader {
 private:
  static const bool not_implemented = false;

 public:
  static const SpoolerDefinition spooler_definition;
  static const std::string       sandbox_path;
  static const std::string       sandbox_tmp_dir;
  static const size_t            min_chunk_size = 512000;
  static const size_t            avg_chunk_size = min_chunk_size * 2;
  static const size_t            max_chunk_size = min_chunk_size * 4;

 public:
  MockUploader_UF(const SpoolerDefinition &spooler_definition) :
    AbstractUploader(spooler_definition),
    initialize_called(false) {}


  static bool WillHandle(const SpoolerDefinition &spooler_definition) {
    return spooler_definition.driver_type == SpoolerDefinition::Unknown;
  }


  void WorkerThread() {
    bool running                = true;
    worker_thread_running       = true;
    const callback_t *callback  = NULL;
    MockStreamHandle_UF* handle = NULL;

    while (running) {
      UploadJob job = AcquireNewJob();
      switch (job.type) {
        case UploadJob::Upload:
          handle = static_cast<MockStreamHandle_UF*>(job.stream_handle);
          handle->uploads++;
          Respond(job.callback, UploaderResults(0, job.buffer));
          break;
        case UploadJob::Commit:
          handle = static_cast<MockStreamHandle_UF*>(job.stream_handle);
          handle->commits++;
          callback = handle->commit_callback;
          delete handle;
          Respond(callback, UploaderResults(0));
          break;
        case UploadJob::Terminate:
          running = false;
          break;
        default:
          FAIL() << "Unrecognized UploadJob type";
          break;
      }
    }

    EXPECT_EQ (0, jobs_in_flight()) << "Unfinished Jobs in the pipeline";

    worker_thread_running = false;
  }

  upload::UploadStreamHandle* InitStreamedUpload(
                                            const callback_t *callback = NULL) {
    return new MockStreamHandle_UF(callback);
  }

  bool Initialize() {
    AbstractUploader::Initialize();
    initialize_called = true;
    return true;
  }

  void FileUpload(const std::string  &local_path,
                  const std::string  &remote_path,
                  const callback_t   *callback = NULL) {
    // untested, since fully implementation dependent!
    assert (MockUploader_UF::not_implemented);
  }

  bool Remove(const std::string &file_to_delete) {
    // untested, since fully implementation dependent!
    assert (MockUploader_UF::not_implemented);
  }

  bool Peek(const std::string &path) const {
    // untested, since fully implementation dependent!
    assert (MockUploader_UF::not_implemented);
  }

  unsigned int GetNumberOfErrors() const {
    // untested, since fully implementation dependent!
    assert (MockUploader_UF::not_implemented);
  }

 public:
  static volatile bool worker_thread_running;
  bool initialize_called;
};

volatile bool MockUploader_UF::worker_thread_running = false;
const std::string MockUploader_UF::sandbox_path    = "/tmp/cvmfs_ut_upload_facility";
const std::string MockUploader_UF::sandbox_tmp_dir = MockUploader_UF::sandbox_path + "/tmp";
const SpoolerDefinition MockUploader_UF::spooler_definition =
  SpoolerDefinition("mock," + sandbox_path + "," +
                              sandbox_tmp_dir,
                    shash::kSha1,
                    true,
                    min_chunk_size,
                    avg_chunk_size,
                    max_chunk_size);


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


class T_UploadFacility : public ::testing::Test {
 protected:
  virtual void SetUp() {
    PolymorphicConstructionUnittestAdapter::RegisterPlugin<upload::AbstractUploader,
                                                           MockUploader_UF>();
  }

  virtual void TearDown() {
    PolymorphicConstructionUnittestAdapter::UnregisterAllPlugins<upload::AbstractUploader>();
  }
};


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_UploadFacility, InitializeAndTearDown) {
  MockUploader_UF *uploader = dynamic_cast<MockUploader_UF*>(
              AbstractUploader::Construct(MockUploader_UF::spooler_definition));

  ASSERT_NE (static_cast<MockUploader_UF*>(NULL), uploader);
  EXPECT_TRUE (uploader->initialize_called);

  sleep(1);
  EXPECT_TRUE (MockUploader_UF::worker_thread_running);

  uploader->TearDown();
  delete uploader;

  EXPECT_FALSE (MockUploader_UF::worker_thread_running);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


volatile int chunk_upload_complete_callback_calls = 0;
void ChunkUploadCompleteCallback_T_Callbacks(const UploaderResults &results) {
  EXPECT_EQ (UploaderResults::kChunkCommit,  results.type);
  EXPECT_EQ (0,                              results.return_code);
  EXPECT_EQ ("",                             results.local_path);
  EXPECT_EQ (static_cast<CharBuffer*>(NULL), results.buffer);
  ++chunk_upload_complete_callback_calls;
}

volatile int buffer_upload_complete_callback_calls = 0;
void BufferUploadCompleteCallback_T_Callbacks(const UploaderResults &results) {
  EXPECT_EQ (UploaderResults::kBufferUpload, results.type);
  EXPECT_EQ (0,                              results.return_code);
  EXPECT_EQ ("",                             results.local_path);
  EXPECT_NE (static_cast<CharBuffer*>(NULL), results.buffer);
  EXPECT_LT (size_t(0),                      results.buffer->used_bytes());
  EXPECT_LE (0,                              results.buffer->base_offset());
  ++buffer_upload_complete_callback_calls;
}


TEST_F(T_UploadFacility, Callbacks) {
  MockUploader_UF *uploader = dynamic_cast<MockUploader_UF*>(
              AbstractUploader::Construct(MockUploader_UF::spooler_definition));

  ASSERT_NE (static_cast<MockUploader_UF*>(NULL), uploader);
  EXPECT_TRUE (uploader->initialize_called);
  EXPECT_EQ (0, chunk_upload_complete_callback_calls);
  EXPECT_EQ (0, buffer_upload_complete_callback_calls);
  EXPECT_EQ (0, MockStreamHandle_UF::instances);

  sleep(1);
  EXPECT_TRUE (MockUploader_UF::worker_thread_running);

  UploadStreamHandle *handle = uploader->InitStreamedUpload(
      AbstractUploader::MakeCallback(&ChunkUploadCompleteCallback_T_Callbacks));
  ASSERT_NE (static_cast<void*>(NULL), handle);

  EXPECT_EQ (1, MockStreamHandle_UF::instances);

  sleep(1);

  EXPECT_EQ (0, chunk_upload_complete_callback_calls);
  EXPECT_EQ (0, buffer_upload_complete_callback_calls);

  CharBuffer b1(1024);
  b1.SetUsedBytes(1000);
  b1.SetBaseOffset(0);
  uploader->ScheduleUpload(handle, &b1,
    AbstractUploader::MakeCallback(&BufferUploadCompleteCallback_T_Callbacks));

  sleep(1);

  EXPECT_EQ (0, chunk_upload_complete_callback_calls);
  EXPECT_EQ (1, buffer_upload_complete_callback_calls);

  CharBuffer b2(1024);
  b2.SetUsedBytes(1000);
  b2.SetBaseOffset(1000);
  uploader->ScheduleUpload(handle, &b2,
    AbstractUploader::MakeCallback(&BufferUploadCompleteCallback_T_Callbacks));

  sleep(1);

  EXPECT_EQ (0, chunk_upload_complete_callback_calls);
  EXPECT_EQ (2, buffer_upload_complete_callback_calls);

  uploader->ScheduleCommit(handle,
                           shash::Any(),
                           "");

  uploader->WaitForUpload();
  uploader->TearDown();

  EXPECT_EQ (1, chunk_upload_complete_callback_calls);
  EXPECT_EQ (2, buffer_upload_complete_callback_calls);
  EXPECT_EQ (0, MockStreamHandle_UF::instances);

  delete uploader;
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//

size_t overall_size_ordering = 0;
bool ordering_test_done = false;
void ChunkUploadCompleteCallback_T_Ordering(const UploaderResults &results) {
  ordering_test_done = true;
}

void BufferUploadCompleteCallback_T_Ordering(const UploaderResults &results) {
  EXPECT_EQ (UploaderResults::kBufferUpload, results.type);
  ASSERT_NE (static_cast<CharBuffer*>(NULL), results.buffer);
  EXPECT_LT (size_t(0), results.buffer->used_bytes());

  EXPECT_EQ (static_cast<off_t>(overall_size_ordering), results.buffer->base_offset());
  overall_size_ordering += results.buffer->used_bytes();
}

// Caveat: 'offset' it automatically updated!
CharBuffer* MakeBuffer(const size_t  buffer_size,
                             size_t &offset,
                       const size_t  used_bytes) {
  CharBuffer *buffer = new CharBuffer(buffer_size);
  assert (buffer != NULL);
  buffer->SetUsedBytes(used_bytes);
  buffer->SetBaseOffset(static_cast<off_t>(offset));
  offset += used_bytes;
  return buffer;
}

TEST_F(T_UploadFacility, DataBlockBasicOrdering) {
  MockUploader_UF *uploader = dynamic_cast<MockUploader_UF*>(
              AbstractUploader::Construct(MockUploader_UF::spooler_definition));

  ASSERT_NE (static_cast<MockUploader_UF*>(NULL), uploader);
  EXPECT_TRUE (uploader->initialize_called);

  sleep(1);
  EXPECT_TRUE (MockUploader_UF::worker_thread_running);

  UploadStreamHandle *handle = uploader->InitStreamedUpload(
      AbstractUploader::MakeCallback(&ChunkUploadCompleteCallback_T_Ordering));
  ASSERT_NE (static_cast<void*>(NULL), handle);

  std::vector<CharBuffer*> buffers;
  size_t overall_size = 0;
  buffers.push_back(MakeBuffer(1024, overall_size,  384));
  buffers.push_back(MakeBuffer(2048, overall_size, 1024));
  buffers.push_back(MakeBuffer(4096, overall_size, 4096));
  buffers.push_back(MakeBuffer(8192, overall_size, 6172));
  buffers.push_back(MakeBuffer( 768, overall_size,  128));
  buffers.push_back(MakeBuffer(2000, overall_size, 1921));
  buffers.push_back(MakeBuffer(9999, overall_size, 9999));
  buffers.push_back(MakeBuffer( 100, overall_size,   10));
  buffers.push_back(MakeBuffer(4096, overall_size,  128));
  buffers.push_back(MakeBuffer(4627, overall_size, 1950));
  ASSERT_EQ (size_t(25812), overall_size);

  std::vector<CharBuffer*>::const_iterator i    = buffers.begin();
  std::vector<CharBuffer*>::const_iterator iend = buffers.end();
  for (; i != iend; ++i) {
    uploader->ScheduleUpload(handle, *i,
      AbstractUploader::MakeCallback(&BufferUploadCompleteCallback_T_Ordering));
  }

  uploader->ScheduleCommit(handle, shash::Any(), "");
  uploader->WaitForUpload();
  uploader->TearDown();

  EXPECT_EQ (overall_size, overall_size_ordering);
  EXPECT_TRUE (ordering_test_done);

  delete uploader;
}

TEST_F(T_UploadFacility, InitDtorRace) {
  MockUploader_UF *uploader = dynamic_cast<MockUploader_UF*>(
              AbstractUploader::Construct(MockUploader_UF::spooler_definition));

  ASSERT_NE (static_cast<MockUploader_UF*>(NULL), uploader);
  EXPECT_TRUE (uploader->initialize_called);

  uploader->WaitForUpload();
  uploader->TearDown();
  delete uploader;
}
