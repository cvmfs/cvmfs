#include <gtest/gtest.h>
#include <unistd.h>
#include <string>
#include <sstream>
#include <tbb/atomic.h>

#include "../../cvmfs/atomic.h"
#include "../../cvmfs/util.h"
#include "../../cvmfs/upload_spooler_definition.h"
#include "../../cvmfs/upload_local.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/file_processing/char_buffer.h"

#include "testutil.h"
#include "c_file_sandbox.h"


using namespace upload;


class UploadCallbacks {
 public:
  UploadCallbacks() {
    simple_upload_invocations            = 0;
    streamed_upload_complete_invocations = 0;
    buffer_upload_complete_invocations   = 0;
  }

  void SimpleUploadClosure(const UploaderResults &results,
                                 UploaderResults  expected) {
    EXPECT_EQ (UploaderResults::kFileUpload,   results.type);
    EXPECT_EQ (static_cast<CharBuffer*>(NULL), results.buffer);
    EXPECT_EQ (expected.return_code,           results.return_code);
    EXPECT_EQ (expected.local_path,            results.local_path);
    ++simple_upload_invocations;
  }

  void StreamedUploadComplete(const UploaderResults &results,
                                    int              return_code) {
    EXPECT_EQ (UploaderResults::kChunkCommit,  results.type);
    EXPECT_EQ ("",                             results.local_path);
    EXPECT_EQ (static_cast<CharBuffer*>(NULL), results.buffer);
    EXPECT_EQ (return_code,                    results.return_code);
    ++streamed_upload_complete_invocations;
  }

  void BufferUploadComplete(const UploaderResults &results,
                                  UploaderResults  expected) {
    EXPECT_EQ (UploaderResults::kBufferUpload, results.type);
    EXPECT_EQ ("",                             results.local_path);
    EXPECT_EQ (expected.buffer,                results.buffer);
    EXPECT_EQ (expected.return_code,           results.return_code);
    ++buffer_upload_complete_invocations;
  }

 public:
  tbb::atomic<unsigned int> simple_upload_invocations;
  tbb::atomic<unsigned int> streamed_upload_complete_invocations;
  tbb::atomic<unsigned int> buffer_upload_complete_invocations;
};


class T_LocalUploader : public FileSandbox {
 private:
  static const std::string sandbox_path;
  static const std::string dest_dir;
  static const std::string tmp_dir;

 public:
  static atomic_int64 gSeed;
  struct StreamHandle {
    StreamHandle() : handle(NULL), content_hash(shash::kMd5) {
      content_hash.Randomize(atomic_xadd64(&gSeed, 1));
    }

    UploadStreamHandle *handle;
    shash::Any          content_hash;
  };

  typedef std::vector<CharBuffer*>                       Buffers;
  typedef std::vector<std::pair<Buffers, StreamHandle> > BufferStreams;

 public:
  T_LocalUploader() :
    FileSandbox(T_LocalUploader::sandbox_path),
    uploader_(NULL) {}

 protected:
  virtual void SetUp() {
    CreateSandbox(T_LocalUploader::tmp_dir);

    const bool success = MkdirDeep(T_LocalUploader::dest_dir, 0700);
    ASSERT_TRUE (success) << "Failed to create uploader destination dir";
    InitializeStorageBackend();

    uploader_ = AbstractUploader::Construct(GetSpoolerDefinition());
    ASSERT_NE (static_cast<AbstractUploader*>(NULL), uploader_);
  }

  virtual void TearDown() {
    if (uploader_ != NULL) {
      uploader_->TearDown();
      delete uploader_;
      uploader_ = NULL;
    }
    RemoveSandbox(T_LocalUploader::tmp_dir);
  }

  void InitializeStorageBackend() {
    const std::string &dir = T_LocalUploader::dest_dir + "/data";
    bool success = MkdirDeep(dir + "/txn", 0700);
    ASSERT_TRUE (success) << "Failed to create transaction tmp dir";
    for (unsigned int i = 0; i <= 255; ++i) {
      success = MkdirDeep(dir + "/" + ByteToHex(static_cast<char>(i)), 0700);
      ASSERT_TRUE (success);
    }
  }

  SpoolerDefinition GetSpoolerDefinition() const {
    const std::string spl_type   = "local";
    const std::string spl_tmp    = T_LocalUploader::tmp_dir;
    const std::string spl_cfg    = T_LocalUploader::dest_dir;
    const std::string definition = spl_type + "," + spl_tmp + "," + spl_cfg;
    const bool use_file_chunking = true;
    const size_t min_chunk_size  = 0;   // chunking does not matter here, we are
    const size_t avg_chunk_size  = 1;   // only testing the upload module.
    const size_t max_chunk_size  = 2;

    return SpoolerDefinition(definition,
                             shash::kSha1,
                             use_file_chunking,
                             min_chunk_size,
                             avg_chunk_size,
                             max_chunk_size);
  }

  Buffers MakeRandomizedBuffers(const unsigned int buffer_count,
                                const          int rng_seed) const {
    Buffers result;

    Prng rng;
    rng.InitSeed(rng_seed);

    for (unsigned int i = 0; i < buffer_count; ++i) {
      const size_t buffer_size  = rng.Next(1024 * 1024) + 10;
      const size_t bytes_to_use = rng.Next(buffer_size) +  5;

      CharBuffer *buffer = new CharBuffer(buffer_size);
      for (unsigned int i = 0; i < bytes_to_use; ++i) {
        *(buffer->ptr() + i) = rng.Next(256);
      }
      buffer->SetUsedBytes(bytes_to_use);

      result.push_back(buffer);
    }

    return result;
  }

  void FreeBuffers(Buffers &buffers) const {
    Buffers::iterator       i    = buffers.begin();
    Buffers::const_iterator iend = buffers.end();
    for (; i != iend; ++i) {
      delete (*i);
    }
    buffers.clear();
  }

  BufferStreams MakeRandomizedBufferStreams(const unsigned int stream_count,
                                            const unsigned int max_buffers_per_stream,
                                            const          int rng_seed) const {
    BufferStreams streams;

    Prng rng;
    rng.InitSeed(rng_seed);

    for (unsigned int i = 0; i < stream_count; ++i) {
      const unsigned int buffers = rng.Next(max_buffers_per_stream) + 1;
      streams.push_back(
        std::make_pair(MakeRandomizedBuffers(buffers, rng.Next(1234567)),
                       StreamHandle()));
    }

    return streams;
  }

  void FreeBufferStreams(BufferStreams &streams) const {
    BufferStreams::iterator       i    = streams.begin();
    BufferStreams::const_iterator iend = streams.end();
    for (; i != iend; ++i) {
      FreeBuffers(i->first);
    }
    streams.clear();
  }

  bool CheckFile(const std::string &remote_path) const {
    const std::string absolute_path = AbsoluteDestinationPath(remote_path);
    return FileExists(absolute_path);
  }

  void CompareFileContents(const std::string &testee_path,
                           const std::string &reference_path) const {
    const size_t testee_size    = GetFileSize(testee_path);
    const size_t reference_size = GetFileSize(reference_path);
    EXPECT_EQ (reference_size, testee_size);

    shash::Any testee_hash    = HashFile(testee_path);
    shash::Any reference_hash = HashFile(reference_path);
    EXPECT_EQ (reference_hash, testee_hash);
  }

  void CompareBuffersAndFileContents(const Buffers     &buffers,
                                     const std::string &file_path) const {
    size_t buffers_size = 0;
    Buffers::const_iterator i    = buffers.begin();
    Buffers::const_iterator iend = buffers.end();
    for (; i != iend; ++i) {
      buffers_size += (*i)->used_bytes();
    }
    const size_t file_size = GetFileSize(file_path);
    EXPECT_EQ (file_size, buffers_size);

    shash::Any buffers_hash = HashBuffers(buffers);
    shash::Any file_hash    = HashFile(file_path);
    EXPECT_EQ (file_hash, buffers_hash);
  }

  std::string AbsoluteDestinationPath(const std::string &remote_path) const {
    return T_LocalUploader::dest_dir + "/" + remote_path;
  }

 private:
  std::string ByteToHex(const unsigned char byte) {
    char hex[3];
    sprintf(hex, "%02x", byte);
    return std::string(hex);
  }

  shash::Any HashFile(const std::string &path) const {
    shash::Any result(shash::kMd5);
    // googletest requires method that use EXPECT_* or ASSERT_* to return void
    HashFileInternal(path, &result);
    return result;
  }

  void HashFileInternal(const std::string &path, shash::Any *hash) const {
    const bool successful = shash::HashFile(path, hash);
    ASSERT_TRUE (successful);
  }

  shash::Any HashBuffers(const Buffers &buffers) const {
    shash::Any buffers_hash(shash::kMd5);
    HashBuffersInternal(buffers, &buffers_hash);
    return buffers_hash;
  }

  void HashBuffersInternal(const Buffers &buffers, shash::Any *hash) const {
    shash::ContextPtr ctx(shash::kMd5);
    ctx.buffer = alloca(ctx.size);
    shash::Init(ctx);

    Buffers::const_iterator i    = buffers.begin();
    Buffers::const_iterator iend = buffers.end();
    for (; i != iend; ++i) {
      CharBuffer *current_buffer = *i;
      shash::Update(current_buffer->ptr(), current_buffer->used_bytes(), ctx);
    }

    shash::Final(ctx, hash);
  }

 protected:
  AbstractUploader *uploader_;
  UploadCallbacks   delegate_;
};
atomic_int64 T_LocalUploader::gSeed = 0;

const std::string T_LocalUploader::sandbox_path = "/tmp/cvmfs_ut_localuploader";
const std::string T_LocalUploader::tmp_dir      = T_LocalUploader::sandbox_path + "/tmp";
const std::string T_LocalUploader::dest_dir     = T_LocalUploader::sandbox_path + "/dest";


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_LocalUploader, Initialize) {
  // nothing to do here... initialization runs completely in the fixture!
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_LocalUploader, SimpleFileUpload) {
  const std::string big_file_path = GetBigFile();
  const std::string dest_name     = "big_file";

  uploader_->Upload(big_file_path, dest_name,
    AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                  &delegate_,
                                  UploaderResults(0, big_file_path)));
  uploader_->WaitForUpload();

  EXPECT_TRUE (CheckFile(dest_name));
  EXPECT_EQ (1u, delegate_.simple_upload_invocations);
  CompareFileContents(big_file_path, AbsoluteDestinationPath(dest_name));
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_LocalUploader, PeekIntoStorage) {
  const std::string small_file_path = GetSmallFile();
  const std::string dest_name       = "small_file";

  uploader_->Upload(small_file_path, dest_name,
    AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                  &delegate_,
                                  UploaderResults(0, small_file_path)));
  uploader_->WaitForUpload();

  EXPECT_TRUE (CheckFile(dest_name));
  EXPECT_EQ (1u, delegate_.simple_upload_invocations);
  CompareFileContents(small_file_path, AbsoluteDestinationPath(dest_name));

  const bool file_exists = uploader_->Peek(dest_name);
  EXPECT_TRUE (file_exists);

  const bool file_doesnt_exist = uploader_->Peek("alien");
  EXPECT_FALSE (file_doesnt_exist);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_LocalUploader, RemoveFromStorage) {
  const std::string small_file_path = GetSmallFile();
  const std::string dest_name       = "also_small_file";

  uploader_->Upload(small_file_path, dest_name,
    AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                  &delegate_,
                                  UploaderResults(0, small_file_path)));
  uploader_->WaitForUpload();

  EXPECT_TRUE (CheckFile(dest_name));
  EXPECT_EQ (1u, delegate_.simple_upload_invocations);
  CompareFileContents(small_file_path, AbsoluteDestinationPath(dest_name));

  const bool file_exists = uploader_->Peek(dest_name);
  EXPECT_TRUE (file_exists);

  const bool removed_successfully = uploader_->Remove(dest_name);
  EXPECT_TRUE (removed_successfully);

  EXPECT_FALSE(CheckFile(dest_name));
  const bool file_still_exists = uploader_->Peek(dest_name);
  EXPECT_FALSE (file_still_exists);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_LocalUploader, UploadEmptyFile) {
  const std::string empty_file_path = GetEmptyFile();
  const std::string dest_name     = "empty_file";

  uploader_->Upload(empty_file_path, dest_name,
    AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                  &delegate_,
                                  UploaderResults(0, empty_file_path)));
  uploader_->WaitForUpload();

  EXPECT_TRUE (CheckFile(dest_name));
  EXPECT_EQ (1u, delegate_.simple_upload_invocations);
  CompareFileContents(empty_file_path, AbsoluteDestinationPath(dest_name));
  EXPECT_EQ (0, GetFileSize(AbsoluteDestinationPath(dest_name)));
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_LocalUploader, UploadHugeFile) {
  const std::string huge_file_path = GetHugeFile();
  const std::string dest_name     = "huge_file";

  uploader_->Upload(huge_file_path, dest_name,
    AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                  &delegate_,
                                  UploaderResults(0, huge_file_path)));
  uploader_->WaitForUpload();

  EXPECT_TRUE (CheckFile(dest_name));
  EXPECT_EQ (1u, delegate_.simple_upload_invocations);
  CompareFileContents(huge_file_path, AbsoluteDestinationPath(dest_name));
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_LocalUploader, UploadManyFiles) {
  const unsigned int number_of_files = 500;
  typedef std::vector<std::pair<std::string, std::string> > Files;

  Files files;
  for (unsigned int i = 0; i < number_of_files; ++i) {
    std::stringstream ss; ss << "file" << i;
    const std::string dest_name = ss.str();
    std::string file;
    switch (i % 3) {
      case 0:
        file = GetEmptyFile();
        break;
      case 1:
        file = GetSmallFile();
        break;
      case 2:
        file = GetBigFile();
        break;
      default:
        FAIL();
        break;
    }
    files.push_back(std::make_pair(file, dest_name));
  }

  Files::const_iterator i    = files.begin();
  Files::const_iterator iend = files.end();
  for (; i != iend; ++i) {
    uploader_->Upload(i->first, i->second,
      AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                    &delegate_,
                                    UploaderResults(0, i->first)));
  }
  uploader_->WaitForUpload();

  EXPECT_EQ (number_of_files, delegate_.simple_upload_invocations);
  for (i = files.begin(); i != iend; ++i) {
    EXPECT_TRUE (CheckFile(i->second));
    CompareFileContents(i->first, AbsoluteDestinationPath(i->second));
  }
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_LocalUploader, SingleStreamedUpload) {
  const unsigned int number_of_buffers = 10;
  Buffers buffers = MakeRandomizedBuffers(number_of_buffers, 1337);

  EXPECT_EQ (0u, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ (0u, delegate_.streamed_upload_complete_invocations);

  UploadStreamHandle *handle = uploader_->InitStreamedUpload(
                                 AbstractUploader::MakeClosure(
                                   &UploadCallbacks::StreamedUploadComplete,
                                   &delegate_,
                                   0));
  ASSERT_NE (static_cast<UploadStreamHandle*>(NULL), handle);

  EXPECT_EQ (0u, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ (0u, delegate_.streamed_upload_complete_invocations);

  Buffers::const_iterator i    = buffers.begin();
  Buffers::const_iterator iend = buffers.end();
  for (; i != iend; ++i) {
    uploader_->ScheduleUpload(handle, *i,
                 AbstractUploader::MakeClosure(
                   &UploadCallbacks::BufferUploadComplete,
                   &delegate_,
                   UploaderResults(0, *i)));
  }
  uploader_->WaitForUpload();

  EXPECT_EQ (number_of_buffers, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ (0u,                delegate_.streamed_upload_complete_invocations);

  shash::Any content_hash(shash::kSha1);
  content_hash.Randomize(42);
  const shash::Suffix hash_suffix = 'A';
  uploader_->ScheduleCommit(handle, content_hash, hash_suffix);
  uploader_->WaitForUpload();

  EXPECT_EQ (number_of_buffers, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ (1u,                delegate_.streamed_upload_complete_invocations);

  const std::string dest = "data" +
                           content_hash.MakePathExplicit(1, 2) + hash_suffix;
  EXPECT_TRUE (CheckFile(dest));
  CompareBuffersAndFileContents(buffers, AbsoluteDestinationPath(dest));

  FreeBuffers(buffers);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_LocalUploader, MultipleStreamedUpload) {
  const unsigned int  number_of_files        = 100;
  const unsigned int  max_buffers_per_stream = 15;
  const shash::Suffix hash_suffix            = 'K';
  BufferStreams streams = MakeRandomizedBufferStreams(number_of_files,
                                                      max_buffers_per_stream,
                                                      42);

  EXPECT_EQ (0u, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ (0u, delegate_.streamed_upload_complete_invocations);

  BufferStreams::iterator       i    = streams.begin();
  BufferStreams::const_iterator iend = streams.end();
  for (; i != iend; ++i) {
    UploadStreamHandle *handle = uploader_->InitStreamedUpload(
                                   AbstractUploader::MakeClosure(
                                     &UploadCallbacks::StreamedUploadComplete,
                                     &delegate_,
                                     0));
    ASSERT_NE (static_cast<UploadStreamHandle*>(NULL), handle);
    i->second.handle = handle;
  }

  EXPECT_EQ (0u, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ (0u, delegate_.streamed_upload_complete_invocations);

  // go through the handles and schedule buffers for them in a round robin
  // fashion --> we want to test concurrent streamed upload behaviour
  BufferStreams active_streams   = streams;
  unsigned int number_of_buffers = 0;
  BufferStreams::iterator j      = active_streams.begin();
  while (! active_streams.empty()) {
          Buffers       &buffers        = j->first;
    const StreamHandle  &current_handle = j->second;

    if (! buffers.empty()) {
      CharBuffer *current_buffer = buffers.front();
      ASSERT_NE (static_cast<CharBuffer*>(NULL), current_buffer);
      ++number_of_buffers;
      uploader_->ScheduleUpload(current_handle.handle, current_buffer,
                   AbstractUploader::MakeClosure(
                     &UploadCallbacks::BufferUploadComplete,
                     &delegate_,
                     UploaderResults(0, buffers.front())));
      buffers.erase(buffers.begin());
    } else {
      uploader_->ScheduleCommit(current_handle.handle,
                                current_handle.content_hash,
                                hash_suffix);
      j = active_streams.erase(j);
    }
  }

  uploader_->WaitForUpload();

  EXPECT_EQ (number_of_buffers, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ (number_of_files,   delegate_.streamed_upload_complete_invocations);

  BufferStreams::const_iterator k    = streams.begin();
  BufferStreams::const_iterator kend = streams.end();
  for (; k != kend; ++k) {
    const shash::Any &content_hash = k->second.content_hash;
    const std::string dest =
                    "data" + content_hash.MakePathWithSuffix(1, 2, hash_suffix);
    EXPECT_TRUE (CheckFile(dest));
    CompareBuffersAndFileContents(k->first, AbsoluteDestinationPath(dest));
  }

  FreeBufferStreams(streams);
}
