#include <gtest/gtest.h>
#include <unistd.h>

#include "../../cvmfs/util.h"
#include "../../cvmfs/upload_spooler_definition.h"
#include "../../cvmfs/upload_facility.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/file_processing/char_buffer.h"


using namespace upload;


struct MockStreamHandle : public upload::UploadStreamHandle {
  MockStreamHandle(const callback_t *commit_callback) :
    UploadStreamHandle(commit_callback),
    commits(0), uploads(0)
  {
    ++instances;
  }

  ~MockStreamHandle() {
    --instances;
  }

  int commits;
  int uploads;

  static int instances;
};

int MockStreamHandle::instances = 0;


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
    bool running = true;
    worker_thread_running = true;

    while (running) {
      UploadJob job = AcquireNewJob();
      switch (job.type) {
        case UploadJob::Upload:
          static_cast<MockStreamHandle*>(job.stream_handle)->uploads++;
          Respond(job.callback, upload::UploaderResults(0, job.buffer));
          break;
        case UploadJob::Commit:
          static_cast<MockStreamHandle*>(job.stream_handle)->commits++;
          Respond(job.stream_handle->commit_callback, UploaderResults(0));
          delete job.stream_handle;
          break;
        case UploadJob::Terminate:
          running = false;
          break;
        default:
          FAIL() << "Unrecognized UploadJob type";
          break;
      }
    }

    worker_thread_running = false;
  }

  upload::UploadStreamHandle* InitStreamedUpload(
                                            const callback_t *callback = NULL) {
    return new MockStreamHandle(callback);
  }

  bool Initialize() {
    AbstractUploader::Initialize();
    initialize_called = true;
    return true;
  }

  void Upload(const std::string  &local_path,
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
  static bool worker_thread_running;
  bool initialize_called;
};

bool MockUploader_UF::worker_thread_running = false;
const std::string MockUploader_UF::sandbox_path    = "/tmp/cvmfs_ut_upload_facility";
const std::string MockUploader_UF::sandbox_tmp_dir = MockUploader_UF::sandbox_path + "/tmp";
const SpoolerDefinition MockUploader_UF::spooler_definition =
  SpoolerDefinition("mock," + sandbox_path + "," +
                              sandbox_tmp_dir,
                    true,
                    min_chunk_size,
                    avg_chunk_size,
                    max_chunk_size);


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST(T_UploadFacility, InitializeAndTearDown) {
  upload::AbstractUploader::RegisterPlugin<MockUploader_UF>();

  MockUploader_UF *uploader = dynamic_cast<MockUploader_UF*>(
              AbstractUploader::Construct(MockUploader_UF::spooler_definition));

  EXPECT_TRUE (uploader->initialize_called);

  sleep(1);
  EXPECT_TRUE (MockUploader_UF::worker_thread_running);

  delete uploader;
  EXPECT_FALSE (MockUploader_UF::worker_thread_running);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


int chunk_upload_complete_callback_calls = 0;
void ChunkUploadCompleteCallback(const UploaderResults &results) {
  ++chunk_upload_complete_callback_calls;
}

int buffer_upload_complete_callback_calls = 0;
void BufferUploadCompleteCallback(const UploaderResults &results) {
  ++buffer_upload_complete_callback_calls;
}


TEST(T_UploadFacility, Callbacks) {
  MockUploader_UF *uploader =
                       new MockUploader_UF(MockUploader_UF::spooler_definition);

  EXPECT_EQ (0, chunk_upload_complete_callback_calls);
  EXPECT_EQ (0, buffer_upload_complete_callback_calls);
  EXPECT_EQ (0, MockStreamHandle::instances);

  UploadStreamHandle *handle = uploader->InitStreamedUpload(
      AbstractUploader::MakeCallback(&ChunkUploadCompleteCallback));
  ASSERT_NE (static_cast<void*>(NULL), handle);

  EXPECT_EQ (1, MockStreamHandle::instances);

  sleep(1);

  EXPECT_EQ (0, chunk_upload_complete_callback_calls);
  EXPECT_EQ (0, buffer_upload_complete_callback_calls);

  CharBuffer b1(1024);
  b1.SetUsedBytes(1000);
  b1.SetBaseOffset(0);
  uploader->ScheduleUpload(handle, &b1,
    AbstractUploader::MakeCallback(&BufferUploadCompleteCallback));

  sleep(1);

  EXPECT_EQ (0, chunk_upload_complete_callback_calls);
  EXPECT_EQ (1, buffer_upload_complete_callback_calls);

  CharBuffer b2(1024);
  b2.SetUsedBytes(1000);
  b2.SetBaseOffset(1000);
  uploader->ScheduleUpload(handle, &b2,
    AbstractUploader::MakeCallback(&BufferUploadCompleteCallback));

  sleep(1);

  EXPECT_EQ (0, chunk_upload_complete_callback_calls);
  EXPECT_EQ (2, buffer_upload_complete_callback_calls);

  uploader->ScheduleCommit(handle,
                           shash::Any(),
                           "");

  sleep(1);

  EXPECT_EQ (1, chunk_upload_complete_callback_calls);
  EXPECT_EQ (2, buffer_upload_complete_callback_calls);
  EXPECT_EQ (0, MockStreamHandle::instances);
}
