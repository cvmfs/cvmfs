/**
 * This file is part of the CernVM File System.
 */

#include <errno.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <string>

#include "c_file_sandbox.h"
#include "c_http_server.h"
#include "crypto/hash.h"
#include "testutil.h"
#include "upload_facility.h"
#include "upload_local.h"
#include "upload_s3.h"
#include "upload_spooler_definition.h"
#include "util/atomic.h"
#include "util/file_guard.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"


/**
 * Port of S3 mockup server
 */
#define CVMFS_S3_TEST_MOCKUP_SERVER_PORT 8082

namespace upload {

class UploadCallbacks {
 public:
  UploadCallbacks() {
    atomic_init32(&simple_upload_invocations);
    atomic_init32(&streamed_upload_complete_invocations);
    atomic_init32(&buffer_upload_complete_invocations);
  }

  void SimpleUploadClosure(const UploaderResults &results,
                           UploaderResults expected) {
    EXPECT_EQ(UploaderResults::kFileUpload, results.type);
    EXPECT_EQ(expected.return_code, results.return_code);
    EXPECT_EQ(expected.local_path, results.local_path);
    atomic_inc32(&simple_upload_invocations);
  }

  void StreamedUploadComplete(const UploaderResults &results, int return_code) {
    EXPECT_EQ(UploaderResults::kChunkCommit, results.type);
    EXPECT_EQ("", results.local_path);
    EXPECT_EQ(return_code, results.return_code);
    atomic_inc32(&streamed_upload_complete_invocations);
  }

  void BufferUploadComplete(const UploaderResults &results,
                            UploaderResults expected) {
    EXPECT_EQ(UploaderResults::kBufferUpload, results.type);
    EXPECT_EQ("", results.local_path);
    EXPECT_EQ(expected.return_code, results.return_code);
    atomic_inc32(&buffer_upload_complete_invocations);
  }

 public:
  atomic_int32 simple_upload_invocations;
  atomic_int32 streamed_upload_complete_invocations;
  atomic_int32 buffer_upload_complete_invocations;
};


template<class UploadersT>
class T_Uploaders : public FileSandbox {
 private:
  static const char sandbox_path[];
  static const std::string dest_dir;
  static const std::string tmp_dir;
  std::string repo_alias;
  std::string s3_conf_path;
  template<typename>
  struct type { };

 public:
  static const unsigned kTotal429Replies;
  static const unsigned k429ThrottleSec;
  static atomic_int64 gSeed;
  struct StreamHandle {
    StreamHandle() : handle(NULL), content_hash(shash::kMd5) {
      content_hash.Randomize(atomic_xadd64(&gSeed, 1));
    }

    UploadStreamHandle *handle;
    shash::Any content_hash;
  };

  typedef std::vector<std::string *> Buffers;
  typedef std::vector<std::pair<Buffers, StreamHandle> > BufferStreams;

  T_Uploaders()
      : FileSandbox(string(T_Uploaders::sandbox_path)), uploader_(NULL) { }

 protected:
  AbstractUploader *uploader_;
  UploadCallbacks delegate_;
  MockHTTPServer *http_server_;

  virtual void SetUp() {
    CreateSandbox(T_Uploaders::tmp_dir);
    const bool success = MkdirDeep(T_Uploaders::dest_dir, 0700);
    ASSERT_TRUE(success) << "Failed to create uploader destination dir";
    repo_alias = "";

    SetUp(type<UploadersT>());

    InitializeStorageBackend();
    uploader_ = AbstractUploader::Construct(GetSpoolerDefinition());
    ASSERT_NE(static_cast<AbstractUploader *>(NULL), uploader_);
  }


  virtual void SetUp(const type<upload::LocalUploader> type_specifier) {
    // Empty, no specific needs
  }


  virtual void SetUp(const type<upload::S3Uploader> type_specifier) {
    repo_alias = "testdata";
    CreateTempS3ConfigFile(10, 10);
    http_server_ = new MockHTTPServer(CVMFS_S3_TEST_MOCKUP_SERVER_PORT);
    // Use custom_handler_data to implement S3 retry logic
    int *custom_handler_data = new int(kTotal429Replies);
    http_server_->SetResponseCallback(S3MockupRequestHandler,
                                      custom_handler_data);
    assert(http_server_->Start());
  }


  virtual void TearDown() {
    TearDown(type<UploadersT>());

    if (uploader_ != NULL) {
      uploader_->TearDown();
      delete uploader_;
      uploader_ = NULL;
    }
    RemoveSandbox(T_Uploaders::tmp_dir);
  }


  virtual void TearDown(const type<upload::LocalUploader> type_specifier) {
    // Empty, no specific needs
  }


  virtual void TearDown(const type<upload::S3Uploader> type_specifier) {
    // Request S3 mockup server to finish
    assert(http_server_->Stop());
  }


  void InitializeStorageBackend() {
    std::string dir_path = T_Uploaders::dest_dir;
    if (repo_alias.size() > 0)
      dir_path += "/" + repo_alias;
    dir_path += "/data";
    const std::string &dir = dir_path;
    bool success = MkdirDeep(dir + "/txn", 0700);
    ASSERT_TRUE(success) << "Failed to create transaction tmp dir";
    for (unsigned int i = 0; i <= 255; ++i) {
      success = MkdirDeep(dir + "/" + ByteToHex(static_cast<char>(i)), 0700);
      ASSERT_TRUE(success);
    }
  }


  SpoolerDefinition GetSpoolerDefinition() const {
    const std::string definition = GetSpoolerDefinition(type<UploadersT>());
    const bool generate_legacy_bulk_chunks = true;
    const bool use_file_chunking = true;
    const size_t min_chunk_size = 0;  // chunking does not matter here, we are
    const size_t avg_chunk_size = 1;  // only testing the upload module.
    const size_t max_chunk_size = 2;

    SpoolerDefinition sd(definition,
                         shash::kSha1,
                         zlib::kZlibDefault,
                         generate_legacy_bulk_chunks,
                         use_file_chunking,
                         min_chunk_size,
                         avg_chunk_size,
                         max_chunk_size);
    sd.num_upload_tasks = 2;
    return sd;
  }


  std::string GetSpoolerDefinition(
      const type<upload::LocalUploader> type_specifier) const {
    const std::string spl_type = "local";
    const std::string spl_tmp = T_Uploaders::tmp_dir;
    const std::string spl_cfg = T_Uploaders::dest_dir;
    const std::string definition = spl_type + "," + spl_tmp + "," + spl_cfg;
    return definition;
  }


  std::string GetSpoolerDefinition(
      const type<upload::S3Uploader> type_specifier) const {
    const std::string spl_type = "S3";
    const std::string spl_tmp = T_Uploaders::tmp_dir;
    const std::string spl_cfg = repo_alias + "@" + s3_conf_path;
    const std::string definition = spl_type + "," + spl_tmp + "," + spl_cfg;
    return definition;
  }


  Buffers MakeRandomizedBuffers(const unsigned int buffer_count,
                                const int rng_seed) const {
    Buffers result;

    Prng rng;
    rng.InitSeed(rng_seed);

    for (unsigned int i = 0; i < buffer_count; ++i) {
      // Was previously used with CharBuffer
      const size_t buffer_size = rng.Next(1024 * 1024) + 10;
      const size_t bytes_to_use = rng.Next(buffer_size) + 5;

      std::string *buffer = new std::string();
      buffer->reserve(bytes_to_use);
      for (unsigned int i = 0; i < bytes_to_use; ++i) {
        buffer->push_back(rng.Next(256));
      }

      result.push_back(buffer);
    }

    return result;
  }


  void FreeBuffers(Buffers *buffers) const {
    for (unsigned i = 0; i < buffers->size(); ++i)
      delete (*buffers)[i];
    buffers->clear();
  }


  BufferStreams MakeRandomizedBufferStreams(
      const unsigned int stream_count,
      const unsigned int max_buffers_per_stream,
      const int rng_seed) const {
    BufferStreams streams;

    Prng rng;
    rng.InitSeed(rng_seed);

    for (unsigned int i = 0; i < stream_count; ++i) {
      const unsigned int buffers = rng.Next(max_buffers_per_stream) + 1;
      streams.push_back(std::make_pair(
          MakeRandomizedBuffers(buffers, rng.Next(1234567)), StreamHandle()));
    }

    return streams;
  }


  void FreeBufferStreams(BufferStreams *streams) const {
    typename BufferStreams::iterator i = streams->begin();
    typename BufferStreams::const_iterator iend = streams->end();
    for (; i != iend; ++i) {
      FreeBuffers(&(i->first));
    }
    streams->clear();
  }


  bool CheckFile(const std::string &remote_path) const {
    const std::string absolute_path = AbsoluteDestinationPath(remote_path);
    return FileExists(absolute_path);
  }


  void CompareFileContents(const std::string &testee_path,
                           const std::string &reference_path) const {
    const size_t testee_size = GetFileSize(testee_path);
    const size_t reference_size = GetFileSize(reference_path);
    EXPECT_EQ(reference_size, testee_size);

    shash::Any testee_hash = HashFile(testee_path);
    shash::Any reference_hash = HashFile(reference_path);
    EXPECT_EQ(reference_hash, testee_hash);
  }


  void CompareBuffersAndFileContents(const Buffers &buffers,
                                     const std::string &file_path) const {
    size_t buffers_size = 0;
    for (unsigned i = 0; i < buffers.size(); ++i) {
      buffers_size += buffers[i]->length();
    }
    const size_t file_size = GetFileSize(file_path);
    EXPECT_EQ(file_size, buffers_size);

    shash::Any buffers_hash = HashBuffers(buffers);
    shash::Any file_hash = HashFile(file_path);
    EXPECT_EQ(file_hash, buffers_hash);
  }


  std::string AbsoluteDestinationPath(const std::string &remote_path) const {
    std::string retme = T_Uploaders::dest_dir;
    if (repo_alias.size() > 0)
      retme += "/" + repo_alias;
    retme += "/" + remote_path;
    return retme;
  }


  bool IsS3() const;

  static HTTPResponse S3MockupRequestHandler(const HTTPRequest &req,
                                             void *data) {
    // Number of 429 retries in a row, should be larger than the number of
    // regular client retries
    // Used in tests testing the retry logic
    int *n429 = static_cast<int *>(data);

    HTTPResponse response;
    // strip bucket name
    std::string req_file = req.path.substr(req.path.find("/", 1) + 1);

    if ((*n429 > 0) && (req.path.size() >= 5)
        && (req.path.compare(req.path.size() - 5, 5, "RETRY") == 0)) {
      (*n429)--;
      response.code = 429;
      response.reason = "Too Many Requests";
      response.AddHeader("Retry-After", "1");
    } else if (req.method == "PUT") {
      std::string path = T_Uploaders::dest_dir + "/" + req_file;
      FILE *file = fopen(path.c_str(), "w");
      assert(file != NULL);
      FileGuard file_guard(file);
      int fid = fileno(file);
      assert(fid >= 0);
      ssize_t bytes_written = write(fid, req.body.c_str(), req.content_length);
      assert(bytes_written >= 0);
      assert(req.content_length == (uint64_t)bytes_written);
      int retval = fsync(fid);
      assert(retval == 0);
    } else if (req.method == "HEAD") {
      if (!FileExists(T_Uploaders::dest_dir + "/" + req_file)) {
        response.code = 404;
        response.reason = "Not Found";
      }

    } else if (req.method == "DELETE") {
      std::string path = T_Uploaders::dest_dir + "/" + req_file;
      if (FileExists(path)) {
        int retval = remove(path.c_str());
        assert(retval == 0);
      }
      response.code = 204;
      response.reason = "No Content";
    }

    return response;
  }


  void CreateTempS3ConfigFile(int accounts, int parallel_connections) {
    ASSERT_GE(accounts, 1);
    ASSERT_GE(parallel_connections, 1);
    FILE *s3_conf = CreateTempFile(T_Uploaders::tmp_dir + "/s3.conf", 0660, "w",
                                   &s3_conf_path);
    ASSERT_TRUE(s3_conf != NULL);
    std::string conf_str = "CVMFS_S3_ACCOUNTS=" + StringifyInt(accounts)
                           + "\n"
                             "CVMFS_S3_ACCESS_KEY=";
    for (int i = 0; accounts - 1 > i; i++)
      conf_str += "ABCDEFGHIJKLMNOPQRST" + StringifyInt(i) + ":";
    conf_str += "ABCDEFGHIJKLMNOPQRST\n"
                "CVMFS_S3_SECRET_KEY=";
    for (int i = 0; accounts - 1 > i; i++)
      conf_str += "ABCDEFGHIJKLMNOPQRSTUABCDEFGHIJKLMNOPQR" + StringifyInt(i)
                  + ":";
    conf_str += "ABCDEFGHIJKLMNOPQRSTUABCDEFGHIJKLMNOPQR1\n"
                "CVMFS_S3_BUCKETS_PER_ACCOUNT=100\n"
                "CVMFS_S3_BUCKET=testbucket\n"
                "CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS="
                + StringifyInt(parallel_connections)
                + "\n"
                  "CVMFS_S3_HOST=127.0.0.1\n"
                  "CVMFS_S3_DNS_BUCKETS=false\n"
                  "CVMFS_S3_PORT="
                + StringifyInt(CVMFS_S3_TEST_MOCKUP_SERVER_PORT);

    fprintf(s3_conf, "%s\n", conf_str.c_str());
    fclose(s3_conf);
  }


  std::string ByteToHex(const unsigned char byte) {
    const unsigned int kLen = 3;
    char hex[kLen];
    snprintf(hex, kLen, "%02x", byte);
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
    ASSERT_TRUE(successful);
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

    for (unsigned i = 0; i < buffers.size(); ++i) {
      shash::Update(reinterpret_cast<const unsigned char *>(buffers[i]->data()),
                    buffers[i]->length(), ctx);
    }

    shash::Final(ctx, hash);
  }
};

template<typename T>
bool T_Uploaders<T>::IsS3() const {
  return false;
}

template<>
bool T_Uploaders<S3Uploader>::IsS3() const {
  return true;
}

template<class UploadersT>
atomic_int64 T_Uploaders<UploadersT>::gSeed = 0;

// Shold be larger than the number of regular retries
template<class UploadersT>
const unsigned T_Uploaders<UploadersT>::kTotal429Replies = 4;

template<class UploadersT>
const unsigned T_Uploaders<UploadersT>::k429ThrottleSec = 1;

template<class UploadersT>
const char T_Uploaders<UploadersT>::sandbox_path[] = "./cvmfs_ut_uploader";

template<class UploadersT>
const std::string
    T_Uploaders<UploadersT>::tmp_dir = string(T_Uploaders::sandbox_path)
                                       + "/tmp";

template<class UploadersT>
const std::string
    T_Uploaders<UploadersT>::dest_dir = string(T_Uploaders::sandbox_path)
                                        + "/dest";

typedef testing::Types<S3Uploader, LocalUploader> UploadTypes;
TYPED_TEST_CASE(T_Uploaders, UploadTypes);


//------------------------------------------------------------------------------

static void LogSupress(const LogSource source, const int mask,
                       const char *msg) { }

TYPED_TEST(T_Uploaders, RetrySlow) {
  if (!TestFixture::IsS3()) {
    SUCCEED();  // Only the S3 uploader has retry logic to test
    return;
  }

  const std::string small_file_path = TestFixture::GetSmallFile();
  const std::string dest_name = "RETRY";

  upload::S3Uploader *s3uploader = static_cast<upload::S3Uploader *>(
      this->uploader_);
  SetAltLogFunc(LogSupress);
  EXPECT_EQ(0U, s3uploader->GetS3FanoutManager()->GetStatistics().num_retries);
  this->uploader_->UploadFile(
      small_file_path, dest_name,
      AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                    &this->delegate_,
                                    UploaderResults(0, small_file_path)));
  this->uploader_->WaitForUpload();
  SetAltLogFunc(NULL);

  EXPECT_TRUE(TestFixture::CheckFile(dest_name));
  EXPECT_EQ(1, atomic_read32(&(this->delegate_.simple_upload_invocations)));
  TestFixture::CompareFileContents(
      small_file_path, TestFixture::AbsoluteDestinationPath(dest_name));

  EXPECT_EQ(this->kTotal429Replies,
            s3uploader->GetS3FanoutManager()->GetStatistics().num_retries);
  EXPECT_EQ(this->kTotal429Replies * this->k429ThrottleSec * 1000,
            s3uploader->GetS3FanoutManager()->GetStatistics().ms_throttled);
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, Initialize) {
  // nothing to do here... initialization runs completely in the fixture!
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, SimpleFileUpload) {
  const std::string big_file_path = TestFixture::GetBigFile();
  const std::string dest_name = "big_file";

  this->uploader_->UploadFile(
      big_file_path,
      dest_name,
      AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                    &this->delegate_,
                                    UploaderResults(0, big_file_path)));

  this->uploader_->WaitForUpload();
  EXPECT_TRUE(TestFixture::CheckFile(dest_name));
  EXPECT_EQ(1, atomic_read32(&(this->delegate_.simple_upload_invocations)));
  TestFixture::CompareFileContents(
      big_file_path, TestFixture::AbsoluteDestinationPath(dest_name));
}

//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, IngestionSource) {
  const std::string small_file_path = TestFixture::GetSmallFile();
  int fd = open(small_file_path.c_str(), O_RDONLY);
  ASSERT_GE(fd, 0);
  std::string content;
  EXPECT_TRUE(SafeReadToString(fd, &content));
  close(fd);
  const std::string dest_name = "string";

  this->uploader_->UploadIngestionSource(
      dest_name,
      new StringIngestionSource(content),
      AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                    &this->delegate_,
                                    UploaderResults(0, "MEM")));
  this->uploader_->WaitForUpload();

  const bool file_exists = this->uploader_->Peek(dest_name);
  EXPECT_TRUE(file_exists);
  EXPECT_TRUE(TestFixture::CheckFile(dest_name));
  TestFixture::CompareFileContents(
      small_file_path, TestFixture::AbsoluteDestinationPath(dest_name));
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, PeekIntoStorage) {
  const std::string small_file_path = TestFixture::GetSmallFile();
  const std::string dest_name = "small_file";

  this->uploader_->UploadFile(
      small_file_path, dest_name,
      AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                    &this->delegate_,
                                    UploaderResults(0, small_file_path)));
  this->uploader_->WaitForUpload();

  EXPECT_TRUE(TestFixture::CheckFile(dest_name));
  EXPECT_EQ(1, atomic_read32(&(this->delegate_.simple_upload_invocations)));
  TestFixture::CompareFileContents(
      small_file_path, TestFixture::AbsoluteDestinationPath(dest_name));

  const bool file_exists = this->uploader_->Peek(dest_name);
  EXPECT_TRUE(file_exists);

  const bool file_doesnt_exist = this->uploader_->Peek("alien");
  EXPECT_FALSE(file_doesnt_exist);
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, RemoveFromStorage) {
  const std::string small_file_path = TestFixture::GetSmallFile();
  const std::string dest_name = "also_small_file";

  this->uploader_->UploadFile(
      small_file_path, dest_name,
      AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                    &this->delegate_,
                                    UploaderResults(0, small_file_path)));
  this->uploader_->WaitForUpload();

  EXPECT_TRUE(TestFixture::CheckFile(dest_name));
  EXPECT_EQ(1, atomic_read32(&(this->delegate_.simple_upload_invocations)));
  TestFixture::CompareFileContents(
      small_file_path, TestFixture::AbsoluteDestinationPath(dest_name));

  const bool file_exists = this->uploader_->Peek(dest_name);
  EXPECT_TRUE(file_exists);

  this->uploader_->RemoveAsync(dest_name);
  this->uploader_->WaitForUpload();
  EXPECT_EQ(0U, this->uploader_->GetNumberOfErrors());

  EXPECT_FALSE(TestFixture::CheckFile(dest_name));
  const bool file_still_exists = this->uploader_->Peek(dest_name);
  EXPECT_FALSE(file_still_exists);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TYPED_TEST(T_Uploaders, UploadEmptyFile) {
  const std::string empty_file_path = TestFixture::GetEmptyFile();
  const std::string dest_name = "empty_file";

  this->uploader_->UploadFile(
      empty_file_path, dest_name,
      AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                    &this->delegate_,
                                    UploaderResults(0, empty_file_path)));
  this->uploader_->WaitForUpload();

  EXPECT_TRUE(TestFixture::CheckFile(dest_name));
  EXPECT_EQ(1, atomic_read32(&(this->delegate_.simple_upload_invocations)));
  TestFixture::CompareFileContents(
      empty_file_path, TestFixture::AbsoluteDestinationPath(dest_name));
  EXPECT_EQ(0, GetFileSize(TestFixture::AbsoluteDestinationPath(dest_name)));
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, UploadHugeFileSlow) {
  const std::string huge_file_path = TestFixture::GetHugeFile();
  const std::string dest_name = "huge_file";

  this->uploader_->UploadFile(
      huge_file_path, dest_name,
      AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                    &this->delegate_,
                                    UploaderResults(0, huge_file_path)));
  this->uploader_->WaitForUpload();

  EXPECT_TRUE(TestFixture::CheckFile(dest_name));
  EXPECT_EQ(1, atomic_read32(&(this->delegate_.simple_upload_invocations)));
  TestFixture::CompareFileContents(
      huge_file_path, TestFixture::AbsoluteDestinationPath(dest_name));
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, UploadManyFilesSlow) {
  const int number_of_files = 500;
  typedef std::vector<std::pair<std::string, std::string> > Files;

  Files files;
  for (int i = 0; i < number_of_files; ++i) {
    const std::string dest_name = "file" + StringifyInt(i);
    std::string file;
    switch (i % 3) {
      case 0:
        file = TestFixture::GetEmptyFile();
        break;
      case 1:
        file = TestFixture::GetSmallFile();
        break;
      case 2:
        file = TestFixture::GetBigFile();
        break;
      default:
        FAIL();
        break;
    }
    files.push_back(std::make_pair(file, dest_name));
  }

  Files::const_iterator i = files.begin();
  Files::const_iterator iend = files.end();
  for (; i != iend; ++i) {
    this->uploader_->UploadFile(
        i->first, i->second,
        AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                      &this->delegate_,
                                      UploaderResults(0, i->first)));
  }
  this->uploader_->WaitForUpload();

  EXPECT_EQ(number_of_files,
            atomic_read32(&(this->delegate_.simple_upload_invocations)));
  for (i = files.begin(); i != iend; ++i) {
    EXPECT_TRUE(TestFixture::CheckFile(i->second));
    TestFixture::CompareFileContents(
        i->first, TestFixture::AbsoluteDestinationPath(i->second));
  }
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, SingleStreamedUpload) {
  const int number_of_buffers = 10;
  typename TestFixture::Buffers buffers = TestFixture::MakeRandomizedBuffers(
      number_of_buffers, 1337);

  EXPECT_EQ(
      0, atomic_read32(&(this->delegate_.buffer_upload_complete_invocations)));
  EXPECT_EQ(
      0,
      atomic_read32(&(this->delegate_.streamed_upload_complete_invocations)));

  UploadStreamHandle *handle = this->uploader_->InitStreamedUpload(
      AbstractUploader::MakeClosure(
          &UploadCallbacks::StreamedUploadComplete, &this->delegate_, 0));
  ASSERT_NE(static_cast<UploadStreamHandle *>(NULL), handle);

  EXPECT_EQ(
      0, atomic_read32(&(this->delegate_.buffer_upload_complete_invocations)));
  EXPECT_EQ(
      0,
      atomic_read32(&(this->delegate_.streamed_upload_complete_invocations)));

  typename TestFixture::Buffers::const_iterator i = buffers.begin();
  typename TestFixture::Buffers::const_iterator iend = buffers.end();
  for (; i != iend; ++i) {
    this->uploader_->ScheduleUpload(
        handle,
        AbstractUploader::UploadBuffer((*i)->length(),
                                       const_cast<char *>((*i)->data())),
        AbstractUploader::MakeClosure(
            &UploadCallbacks::BufferUploadComplete,
            &this->delegate_,
            UploaderResults(UploaderResults::kBufferUpload, 0)));
  }
  this->uploader_->WaitForUpload();

  EXPECT_EQ(
      number_of_buffers,
      atomic_read32(&(this->delegate_.buffer_upload_complete_invocations)));
  EXPECT_EQ(
      0,
      atomic_read32(&(this->delegate_.streamed_upload_complete_invocations)));

  shash::Any content_hash(shash::kSha1, 'A');
  content_hash.Randomize(42);
  this->uploader_->ScheduleCommit(handle, content_hash);
  this->uploader_->WaitForUpload();

  EXPECT_EQ(
      number_of_buffers,
      atomic_read32(&(this->delegate_.buffer_upload_complete_invocations)));
  EXPECT_EQ(
      1,
      atomic_read32(&(this->delegate_.streamed_upload_complete_invocations)));

  const std::string dest = "data/" + content_hash.MakePath();
  EXPECT_TRUE(TestFixture::CheckFile(dest));
  TestFixture::CompareBuffersAndFileContents(
      buffers, TestFixture::AbsoluteDestinationPath(dest));

  TestFixture::FreeBuffers(&buffers);
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, MultipleStreamedUploadSlow) {
  const int number_of_files = 100;
  const int max_buffers_per_stream = 15;
  typename TestFixture::BufferStreams
      streams = TestFixture::MakeRandomizedBufferStreams(
          number_of_files, max_buffers_per_stream, 42);

  EXPECT_EQ(
      0, atomic_read32(&(this->delegate_.buffer_upload_complete_invocations)));
  EXPECT_EQ(
      0,
      atomic_read32(&(this->delegate_.streamed_upload_complete_invocations)));

  typename TestFixture::BufferStreams::iterator i = streams.begin();
  typename TestFixture::BufferStreams::const_iterator iend = streams.end();
  for (; i != iend; ++i) {
    UploadStreamHandle *handle = this->uploader_->InitStreamedUpload(
        AbstractUploader::MakeClosure(
            &UploadCallbacks::StreamedUploadComplete, &this->delegate_, 0));
    ASSERT_NE(static_cast<UploadStreamHandle *>(NULL), handle);
    i->second.handle = handle;
  }

  EXPECT_EQ(
      0, atomic_read32(&(this->delegate_.buffer_upload_complete_invocations)));
  EXPECT_EQ(
      0,
      atomic_read32(&(this->delegate_.streamed_upload_complete_invocations)));

  // go through the handles and schedule buffers for them in a round robin
  // fashion --> we want to test concurrent streamed upload behaviour
  typename TestFixture::BufferStreams active_streams = streams;
  typename TestFixture::BufferStreams::iterator j = active_streams.begin();
  int number_of_buffers = 0;
  while (!active_streams.empty()) {
    typename TestFixture::Buffers &buffers = j->first;
    const typename TestFixture::StreamHandle &current_handle = j->second;

    if (!buffers.empty()) {
      std::string *current_buffer = buffers.front();
      ASSERT_FALSE(current_buffer->empty());
      ++number_of_buffers;
      this->uploader_->ScheduleUpload(
          current_handle.handle,
          AbstractUploader::UploadBuffer(
              current_buffer->length(),
              const_cast<char *>(current_buffer->data())),
          AbstractUploader::MakeClosure(
              &UploadCallbacks::BufferUploadComplete,
              &this->delegate_,
              UploaderResults(UploaderResults::kBufferUpload, 0)));
      buffers.erase(buffers.begin());
    } else {
      this->uploader_->ScheduleCommit(current_handle.handle,
                                      current_handle.content_hash);
      j = active_streams.erase(j);
    }
  }

  this->uploader_->WaitForUpload();

  EXPECT_EQ(
      number_of_buffers,
      atomic_read32(&(this->delegate_.buffer_upload_complete_invocations)));
  EXPECT_EQ(
      number_of_files,
      atomic_read32(&(this->delegate_.streamed_upload_complete_invocations)));

  typename TestFixture::BufferStreams::const_iterator k = streams.begin();
  typename TestFixture::BufferStreams::const_iterator kend = streams.end();
  for (; k != kend; ++k) {
    const shash::Any &content_hash = k->second.content_hash;
    const std::string dest = "data/" + content_hash.MakePath();
    EXPECT_TRUE(TestFixture::CheckFile(dest));
    TestFixture::CompareBuffersAndFileContents(
        k->first, TestFixture::AbsoluteDestinationPath(dest));
  }

  TestFixture::FreeBufferStreams(&streams);
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, PlaceBootstrappingShortcut) {
  if (TestFixture::IsS3()) {
    SUCCEED();  // TODO(rmeusel): enable this as soon as the feature is
    return;     //                implemented for the S3Uploader
  }

  const std::string big_file_path = TestFixture::GetBigFile();

  shash::Any digest(shash::kSha1);
  ASSERT_TRUE(shash::HashFile(big_file_path, &digest));

  const std::string dest_name = "data/" + digest.MakePath();

  this->uploader_->UploadFile(
      big_file_path,
      dest_name,
      AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                    &this->delegate_,
                                    UploaderResults(0, big_file_path)));

  this->uploader_->WaitForUpload();
  EXPECT_TRUE(TestFixture::CheckFile(dest_name));

  EXPECT_EQ(1, atomic_read32(&(this->delegate_.simple_upload_invocations)));
  TestFixture::CompareFileContents(
      big_file_path, TestFixture::AbsoluteDestinationPath(dest_name));

  ASSERT_TRUE(this->uploader_->PlaceBootstrappingShortcut(digest));
  TestFixture::CompareFileContents(
      big_file_path,
      TestFixture::AbsoluteDestinationPath(digest.MakeAlternativePath()));
}

}  // namespace upload
