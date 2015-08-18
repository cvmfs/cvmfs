/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <openssl/sha.h>
#include <pthread.h>
#include <unistd.h>

#include <cassert>

#include "../../cvmfs/file_processing/async_reader.h"
#include "../../cvmfs/file_processing/char_buffer.h"
#include "../../cvmfs/file_processing/file_scrubbing_task.h"
#include "../../cvmfs/util.h"
#include "c_file_sandbox.h"

namespace upload {

class TestFile : public upload::AbstractFile {
 public:
  TestFile(const std::string path,
           const FileSandbox::ExpectedHashString &expected_hash) :
    AbstractFile(path, GetFileSize(path)),
    expected_hash_(shash::kSha1, shash::HexPtr(expected_hash.first)),
    read_offset_(0)
  {
    const int sha1_retval = SHA1_Init(&sha1_context_);
    assert(sha1_retval == 1);
  }

  unsigned char* sha1_digest()       { return sha1_digest_; }
  SHA_CTX*       sha1_context()      { return &sha1_context_; }
  off_t          read_offset() const { return read_offset_; }

  shash::Any GetHash() const {
    return shash::Any(shash::kSha1, sha1_digest_);
  }

  const shash::Any& GetExpectedHash() const {
    return expected_hash_;
  }

  void CheckHash() const {
    const shash::Any computed_hash = GetHash();
    EXPECT_EQ(expected_hash_, computed_hash)
      << "content hashes do not fit!" << std::endl
      << "expected: " << expected_hash_.ToString() << std::endl
      << "computed: " << computed_hash.ToString();
  }

  void IncreaseReadOffset(const size_t size) { read_offset_ += size; }

 private:
  // sha1 administrative data
  SHA_CTX        sha1_context_;
  unsigned char  sha1_digest_[SHA_DIGEST_LENGTH];
  shash::Any     expected_hash_;

  // file scrubbing bookkeeping (only important for testing purposes)
  off_t          read_offset_;
};



class DummyFileScrubbingTask :
                            public upload::AbstractFileScrubbingTask<TestFile> {
 public:
  DummyFileScrubbingTask(TestFile            *file,
                         upload::CharBuffer  *buffer,
                         const bool           is_last_piece,
                         AbstractReader      *reader) :
    upload::AbstractFileScrubbingTask<TestFile>(file,
                                                buffer,
                                                is_last_piece,
                                                reader) {}

 protected:
  tbb::task* execute() {
    TestFile            *file   = DummyFileScrubbingTask::file();
    upload::CharBuffer  *buffer = DummyFileScrubbingTask::buffer();

    UpdateSha1(file, buffer);
    UpdateMetaData(file, buffer);

    return Finalize();
  }

  void UpdateSha1(TestFile *file, upload::CharBuffer *buffer) {
    int sha_retcode = 0;
    sha_retcode = SHA1_Update(file->sha1_context(),
                              buffer->ptr(),
                              buffer->used_bytes());
    EXPECT_EQ(1, sha_retcode) << "Error during SHA1 update";

    if (IsLast()) {
      sha_retcode = SHA1_Final(file->sha1_digest(),
                               file->sha1_context());
      EXPECT_EQ(1, sha_retcode) << "Error during SHA1 finalization";
    }
  }

  void UpdateMetaData(TestFile *file, upload::CharBuffer *buffer) {
    file->IncreaseReadOffset(buffer->used_bytes());

    if (IsLast()) {
      EXPECT_EQ(file->size(), static_cast<size_t>(file->read_offset()))
        << "File size and bytes read differ";
    }
  }
};



class T_AsyncReader : public FileSandbox {
 public:
  typedef upload::Reader<DummyFileScrubbingTask, TestFile> MyReader;

 public:
  T_AsyncReader() :
    FileSandbox("/tmp/cvmfs_ut_asyncreader") {}

 protected:
  void SetUp() {
    CreateSandbox();
  }

  void TearDown() {
    RemoveSandbox();
  }

  ExpectedHashString GetEmptyFileHash(
                        const shash::Suffix suffix = shash::kSuffixNone) const {
    return std::make_pair("da39a3ee5e6b4b0d3255bfef95601890afd80709", suffix);
  }

  ExpectedHashString GetSmallFileHash(
                        const shash::Suffix suffix = shash::kSuffixNone) const {
    return std::make_pair("e86f148ca3a9a1ad9cf19979548e61c38bfa1384", suffix);
  }

  ExpectedHashString GetBigFileHash(
                        const shash::Suffix suffix = shash::kSuffixNone) const {
    return std::make_pair("59107e4c69e7687499423d3d85154fdba9cd8161", suffix);
  }

  ExpectedHashString GetHugeFileHash(
                        const shash::Suffix suffix = shash::kSuffixNone) const {
    return std::make_pair("e09bdb4354db2ac46309130ee91ad7c4131f29ea", suffix);
  }
};


TEST_F(T_AsyncReader, Initialize) {
  const size_t        max_buffer_size = 4096;
  const unsigned int  max_buffers_in_flight = 10;

  MyReader reader(max_buffer_size, max_buffers_in_flight);
  reader.Initialize();
  reader.TearDown();
}


TEST_F(T_AsyncReader, ReadEmptyFile) {
  const size_t        max_buffer_size = 4096;
  const unsigned int  max_buffers_in_flight = 10;

  MyReader reader(max_buffer_size, max_buffers_in_flight);
  reader.Initialize();

  TestFile *f = new TestFile(GetEmptyFile(), GetEmptyFileHash());
  reader.ScheduleRead(f);
  reader.Wait();

  f->CheckHash();

  reader.TearDown();
}


TEST_F(T_AsyncReader, ReadSmallFile) {
  TestFile *f = new TestFile(GetSmallFile(), GetSmallFileHash());

  const size_t max_buffer_size = f->size() * 3;  // will fit in one buffer
  const unsigned int max_buffers_in_flight = 10;

  MyReader reader(max_buffer_size, max_buffers_in_flight);
  reader.Initialize();

  reader.ScheduleRead(f);
  reader.Wait();

  f->CheckHash();

  reader.TearDown();
}


unsigned int g_FileReadCallback_Callback_calls = 0;
static void FileReadCallback_Callback(TestFile* const& file) {
  file->CheckHash();
  ++g_FileReadCallback_Callback_calls;
}

TEST_F(T_AsyncReader, FileReadCallback) {
  const unsigned int file_count = 5;

  std::vector<TestFile*> files;
  files.push_back(new TestFile(GetBigFile(),   GetBigFileHash()));
  files.push_back(new TestFile(GetEmptyFile(), GetEmptyFileHash()));
  files.push_back(new TestFile(GetSmallFile(), GetSmallFileHash()));
  files.push_back(new TestFile(GetSmallFile(), GetSmallFileHash()));
  files.push_back(new TestFile(GetBigFile(),   GetBigFileHash()));

  const size_t        max_buffer_size = 524288;
  const unsigned int  max_buffers_in_flight = 5;
  MyReader reader(max_buffer_size, max_buffers_in_flight);
  reader.Initialize();
  reader.RegisterListener(FileReadCallback_Callback);

  std::vector<TestFile*>::const_iterator i    = files.begin();
  std::vector<TestFile*>::const_iterator iend = files.end();
  for (; i < iend; ++i) {
    reader.ScheduleRead(*i);
  }

  reader.Wait();

  EXPECT_EQ(file_count, g_FileReadCallback_Callback_calls)
    << "number of callback invocation does not match";

  reader.TearDown();
}


TEST_F(T_AsyncReader, ReadHugeFileSlow) {
  TestFile *f = new TestFile(GetHugeFile(), GetHugeFileHash());

  const size_t max_buffer_size = 524288;  // will NOT fit in one buffer
  const unsigned int max_buffers_in_flight = 10;

  MyReader reader(max_buffer_size, max_buffers_in_flight);
  reader.Initialize();

  reader.ScheduleRead(f);
  reader.Wait();

  f->CheckHash();

  reader.TearDown();
}


TEST_F(T_AsyncReader, ReadManyBigFilesSlow) {
  const int file_count = 5000;

  std::vector<TestFile*> files;
  files.reserve(file_count);
  for (int i = 0; i < file_count; ++i) {
    files.push_back(new TestFile(GetBigFile(), GetBigFileHash()));
  }

  const size_t        max_buffer_size = 524288;
  const unsigned int  max_buffers_in_flight = 5;  // less buffers than files
  MyReader reader(max_buffer_size, max_buffers_in_flight);
  reader.Initialize();

  std::vector<TestFile*>::const_iterator i    = files.begin();
  std::vector<TestFile*>::const_iterator iend = files.end();
  for (; i < iend; ++i) {
    reader.ScheduleRead(*i);
  }

  reader.Wait();

  std::vector<TestFile*>::const_iterator j    = files.begin();
  std::vector<TestFile*>::const_iterator jend = files.end();
  for (; j < jend; ++j) {
    (*j)->CheckHash();
  }

  reader.TearDown();
}


void *deadlock_test(void *v_files) {
  std::vector<TestFile*> &files =
    *static_cast<std::vector<TestFile *> *>(v_files);

  const size_t        max_buffer_size = 524288;
  const unsigned int  max_buffers_in_flight = 5;  // less buffers than files
  T_AsyncReader::MyReader reader(max_buffer_size, max_buffers_in_flight);
  reader.Initialize();

  // do multiple waits (empty queue... should return immediately)
  reader.Wait();
  reader.Wait();
  reader.Wait();
  reader.Wait();
  reader.Wait();

  // process files and wait in between
  std::vector<TestFile*>::const_iterator i    = files.begin();
  std::vector<TestFile*>::const_iterator iend = files.end();
  int k = 0;
  for (; i < iend; ++i, ++k) {
    reader.ScheduleRead(*i);
    if (k % 100 == 0) {
      reader.Wait();
    }
  }

  // wait for everything to finish
  reader.Wait();

  // check that the file reading worked
  std::vector<TestFile*>::const_iterator j    = files.begin();
  std::vector<TestFile*>::const_iterator jend = files.end();
  for (; j < jend; ++j) {
    (*j)->CheckHash();
  }

  reader.TearDown();

  return reinterpret_cast<void *>(1337);
}

TEST_F(T_AsyncReader, MultipleWaitsSlow) {
  const int file_count = 10000;
  std::vector<TestFile*> files;
  files.reserve(file_count);
  for (int i = 0; i < file_count; ++i) {
    files.push_back(new TestFile(GetSmallFile(), GetSmallFileHash()));
  }

  pthread_t thread;
  const int res = pthread_create(&thread,
                                  NULL,
                                 &deadlock_test,
                                  static_cast<void*>(&files));
  ASSERT_EQ(0, res);

  unsigned int timeout = 10;
  while (pthread_kill(thread, 0) != ESRCH && timeout > 0) {
    sleep(1);
    --timeout;
  }
  EXPECT_LT(0u, timeout) << "Timeout expired (possible Deadlock!)";
  if (timeout == 0) {
    pthread_cancel(thread);
  } else {
    void *return_value;
    const int join_res = pthread_join(thread, &return_value);
    ASSERT_EQ(0, join_res);
    EXPECT_EQ((void*)1337, return_value);
  }
}

}  // namespace upload
