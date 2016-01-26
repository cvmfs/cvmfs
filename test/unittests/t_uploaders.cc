/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <tbb/atomic.h>
#include <unistd.h>

#include <sstream>  // TODO(jblomer): remove me
#include <string>

#include "../../cvmfs/atomic.h"
#include "../../cvmfs/file_processing/char_buffer.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/upload_facility.h"
#include "../../cvmfs/upload_local.h"
#include "../../cvmfs/upload_s3.h"
#include "../../cvmfs/upload_spooler_definition.h"
#include "../../cvmfs/util.h"
#include "c_file_sandbox.h"
#include "testutil.h"


/**
 * Port of S3 mockup server
 */
#define CVMFS_S3_TEST_MOCKUP_SERVER_PORT 8082

namespace upload {

class UploadCallbacks {
 public:
  UploadCallbacks() {
    simple_upload_invocations            = 0;
    streamed_upload_complete_invocations = 0;
    buffer_upload_complete_invocations   = 0;
  }

  void SimpleUploadClosure(const UploaderResults &results,
                                 UploaderResults  expected) {
    EXPECT_EQ(UploaderResults::kFileUpload,   results.type);
    EXPECT_EQ(static_cast<CharBuffer*>(NULL), results.buffer);
    EXPECT_EQ(expected.return_code,           results.return_code);
    EXPECT_EQ(expected.local_path,            results.local_path);
    ++simple_upload_invocations;
  }

  void StreamedUploadComplete(const UploaderResults &results,
                                    int              return_code) {
    EXPECT_EQ(UploaderResults::kChunkCommit,  results.type);
    EXPECT_EQ("",                             results.local_path);
    EXPECT_EQ(static_cast<CharBuffer*>(NULL), results.buffer);
    EXPECT_EQ(return_code,                    results.return_code);
    ++streamed_upload_complete_invocations;
  }

  void BufferUploadComplete(const UploaderResults &results,
                                  UploaderResults  expected) {
    EXPECT_EQ(UploaderResults::kBufferUpload, results.type);
    EXPECT_EQ("",                             results.local_path);
    EXPECT_EQ(expected.buffer,                results.buffer);
    EXPECT_EQ(expected.return_code,           results.return_code);
    ++buffer_upload_complete_invocations;
  }

 public:
  tbb::atomic<unsigned int> simple_upload_invocations;
  tbb::atomic<unsigned int> streamed_upload_complete_invocations;
  tbb::atomic<unsigned int> buffer_upload_complete_invocations;
};


template <class UploadersT>
class T_Uploaders : public FileSandbox {
 private:
  static const char sandbox_path[];
  static const std::string dest_dir;
  static const std::string tmp_dir;
  std::string repo_alias;
  std::string s3_conf_path;
  template<typename> struct type {};

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

  T_Uploaders() : FileSandbox(string(T_Uploaders::sandbox_path)),
                  uploader_(NULL) {}

 protected:
  AbstractUploader *uploader_;
  UploadCallbacks   delegate_;

  virtual void SetUp() {
    CreateSandbox(T_Uploaders::tmp_dir);
    const bool success = MkdirDeep(T_Uploaders::dest_dir, 0700);
    ASSERT_TRUE(success) << "Failed to create uploader destination dir";
    repo_alias = "";

    SetUp(type<UploadersT>());

    InitializeStorageBackend();
    uploader_ = AbstractUploader::Construct(GetSpoolerDefinition());
    ASSERT_NE(static_cast<AbstractUploader*>(NULL), uploader_);
  }


  virtual void SetUp(const type<upload::LocalUploader> type_specifier) {
    // Empty, no specific needs
  }


  virtual void SetUp(const type<upload::S3Uploader> type_specifier) {
    repo_alias = "testdata";
    CreateTempS3ConfigFile(10, 10);
    CreateS3Mockup();
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
    uploader_->Peek("EXIT");

    // Wait S3 mockup server to finish
    int stat_loc = 0;
    wait(&stat_loc);
    ASSERT_EQ(stat_loc, 0);
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
    const bool use_file_chunking = true;
    const size_t min_chunk_size  = 0;   // chunking does not matter here, we are
    const size_t avg_chunk_size  = 1;   // only testing the upload module.
    const size_t max_chunk_size  = 2;

    return SpoolerDefinition(definition,
                             shash::kSha1,
                             zlib::kZlibDefault,
                             use_file_chunking,
                             min_chunk_size,
                             avg_chunk_size,
                             max_chunk_size);
  }


  std::string GetSpoolerDefinition(
      const type<upload::LocalUploader> type_specifier) const {
    const std::string spl_type   = "local";
    const std::string spl_tmp    = T_Uploaders::tmp_dir;
    const std::string spl_cfg    = T_Uploaders::dest_dir;
    const std::string definition = spl_type + "," + spl_tmp + "," + spl_cfg;
    return definition;
  }


  std::string GetSpoolerDefinition(
      const type<upload::S3Uploader> type_specifier) const {
    const std::string spl_type   = "S3";
    const std::string spl_tmp    = T_Uploaders::tmp_dir;
    const std::string spl_cfg    = repo_alias + "@" + s3_conf_path;
    const std::string definition = spl_type + "," + spl_tmp + "," + spl_cfg;
    return definition;
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


  void FreeBuffers(Buffers *buffers) const {
    Buffers::iterator       i    = buffers->begin();
    Buffers::const_iterator iend = buffers->end();
    for (; i != iend; ++i) {
      delete (*i);
    }
    buffers->clear();
  }


  BufferStreams MakeRandomizedBufferStreams(
      const unsigned int stream_count,
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


  void FreeBufferStreams(BufferStreams *streams) const {
    typename BufferStreams::iterator       i    = streams->begin();
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
    const size_t testee_size    = GetFileSize(testee_path);
    const size_t reference_size = GetFileSize(reference_path);
    EXPECT_EQ(reference_size, testee_size);

    shash::Any testee_hash    = HashFile(testee_path);
    shash::Any reference_hash = HashFile(reference_path);
    EXPECT_EQ(reference_hash, testee_hash);
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
    EXPECT_EQ(file_size, buffers_size);

    shash::Any buffers_hash = HashBuffers(buffers);
    shash::Any file_hash    = HashFile(file_path);
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

 private:
  void CreateS3Mockup() {
    int pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0)  {
      S3MockupServerThread();
      exit(0);
    }
  }


  std::vector<std::string> SplitString(const std::string &str, char delim) {
    std::vector<std::string> elems;
    std::stringstream ss(str);
    std::string item;

    while (std::getline(ss, item, delim)) {
      elems.push_back(item);
    }

    return elems;
  }


  /**
   * Get the field number "idx" (e.g. 2) from a string
   * separated by given delimiter (e.g. ' '). The field numbering
   * starts from zero and minus values refer from the end of the
   * strings.
   */
  std::string GetField(std::string str, char delim, int idx) {
    std::vector<std::string> str_vct = SplitString(str, delim);
    unsigned int idxu = idx % str_vct.size();
    return str_vct.at(idxu);
  }


  /**
   * Get value of the param string from the body. The value of the param
   * string is separated by delim1 and delim2. It starts with delim1 and ends
   * with delim2.
   * @param body Text body from where to search
   * @param param String to be searched from the body
   * @param delim1 param value is separated by this char
   * @param delim2 param value ends with this char
   * @return -1 if not found, otherwise value
   */
  int GetValue(std::string body, std::string param,
               char delim1 = ':', char delim2 = ' ') {
    for (unsigned int i = 0; body.size()-param.size() > i; i++) {
      unsigned int j = 0;
      for (j = 0; param.size() > j; j++) {
        if (body.at(i+j) != param.at(j))
          break;
      }
      if (j == param.size()) {
        std::string l = GetField(body.substr(i), delim1, 1);
        l.erase(l.begin(), std::find_if(l.begin(), l.end(),
                std::not1(std::ptr_fun<int, int>(std::isspace))));
        return atoi(GetField(l, delim2, 0).c_str());
      }
    }
    return -1;
  }


  void S3MockupServerThread() {
    const int kReadBufferSize = 1000;
    int listen_sockfd, accept_sockfd;
    socklen_t clilen;
    struct sockaddr_in serv_addr, cli_addr;
    char buffer[kReadBufferSize];
    int retval = 0;

    // Listen incoming connections
    listen_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(listen_sockfd, 0);
    bzero(reinterpret_cast<char *>(&serv_addr), sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(CVMFS_S3_TEST_MOCKUP_SERVER_PORT);
    int on = 1;
    setsockopt(listen_sockfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    retval = bind(listen_sockfd,
                  (struct sockaddr *) &serv_addr,
                  sizeof(serv_addr));
    ASSERT_GE(retval, 0);
    listen(listen_sockfd, 5);
    clilen = sizeof(cli_addr);

    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    fd_set rfds;
    while (true) {
      // Wait for traffic
      FD_ZERO(&rfds);
      FD_SET(listen_sockfd, &rfds);
      retval = select(listen_sockfd+1, &rfds, NULL, NULL, &tv);
      ASSERT_GE(retval, 0);
      if (retval == 0)  // Timeout
        continue;

      accept_sockfd = accept(listen_sockfd,
                             (struct sockaddr *) &cli_addr,
                             &clilen);
      ASSERT_GE(accept_sockfd, 0);
      bzero(buffer, kReadBufferSize);

      // Get header
      std::string req_header = "";
      char buf[4] = {'0', '0', '0', '0'};
      do {
        int n = read(accept_sockfd, buf, 1);
        ASSERT_EQ(n, 1);
        if (strncmp(buf, "\n\r\n\r", 4) == 0 ||
            strncmp(buf, "\n\n", 2) == 0) {
          break;
        }
        req_header += std::string(buf, 1);
        for (int i = 3; i > 0; i--) {
          buf[i] = buf[i-1];
        }
      } while (true);

      // Parse header
      std::string req_type = "";
      std::string req_file = "";  // target name without bucket prefix
      int content_length = 0;
      req_type = GetField(req_header, ' ', 0);
      req_file = GetField(req_header, ' ', 1);
      req_file = req_file.substr(req_file.find("/", 1) + 1);  // no bucket
      if (req_type.compare("PUT") == 0) {
        content_length = GetValue(req_header, "Content-Length");
        ASSERT_GE(content_length, 0);
      }

      // Get content
      FILE *file = NULL;
      if (req_type.compare("PUT") == 0) {
        std::string path = T_Uploaders::dest_dir + "/" + req_file;
        file = fopen(path.c_str(), "w");
        ASSERT_TRUE(file != NULL);
        int fid = fileno(file);
        ASSERT_GE(fid, 0);
        int left_to_read = content_length;
        while (left_to_read > 0) {
          int n = read(accept_sockfd, buffer, kReadBufferSize-1);
          write(fid, buffer, n);
          left_to_read -= n;
        }
        retval = fsync(fid);
        ASSERT_EQ(retval, 0);
        retval = fclose(file);
        ASSERT_EQ(retval, 0);
      }

      // Reply to client
      std::string reply = "HTTP/1.1 200 OK\r\n";
      if (req_type.compare("HEAD") == 0) {
        if (req_file.size() >= 4 &&
            req_file.compare(req_file.size() - 4, 4, "EXIT") == 0) {
          close(listen_sockfd);
          return;
        }
        if (FileExists(T_Uploaders::dest_dir + "/" + req_file) == false)
          reply = "HTTP/1.1 404 Not Found\r\n";
      } else if (req_type.compare("DELETE") == 0) {
        std::string path = T_Uploaders::dest_dir + "/" + req_file;
        if (FileExists(path)) {
          retval = remove(path.c_str());
          ASSERT_EQ(retval, 0);
        }
        // "No Content"-reply even if file did not exist
        reply = "HTTP/1.1 204 No Content\r\n";
      }
      reply += "Connection: close\r\n\r\n";

      int n = write(accept_sockfd, reply.c_str(), reply.length());
      ASSERT_GE(n, 0);
      close(accept_sockfd);
    }
    close(listen_sockfd);
  }


  void CreateTempS3ConfigFile(int accounts, int parallel_connections) {
    ASSERT_GE(accounts, 1);
    ASSERT_GE(parallel_connections, 1);
    FILE *s3_conf = CreateTempFile(T_Uploaders::tmp_dir + "/s3.conf",
                                   0660, "w", &s3_conf_path);
    ASSERT_TRUE(s3_conf != NULL);
    std::string conf_str =
        "CVMFS_S3_ACCOUNTS=" + StringifyInt(accounts) + "\n"
        "CVMFS_S3_ACCESS_KEY=";
    for (int i = 0; accounts-1 > i; i++)
      conf_str += "ABCDEFGHIJKLMNOPQRST" +
                  StringifyInt(i) + ":";
    conf_str += "ABCDEFGHIJKLMNOPQRST\n"
                "CVMFS_S3_SECRET_KEY=";
    for (int i = 0; accounts-1 > i; i++)
      conf_str += "ABCDEFGHIJKLMNOPQRSTUABCDEFGHIJKLMNOPQR" +
                  StringifyInt(i) + ":";
    conf_str +=
        "ABCDEFGHIJKLMNOPQRSTUABCDEFGHIJKLMNOPQR1\n"
        "CVMFS_S3_BUCKETS_PER_ACCOUNT=100\n"
        "CVMFS_S3_BUCKET=testbucket\n"
        "CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS=" +
        StringifyInt(parallel_connections) + "\n"
        "CVMFS_S3_HOST=127.0.0.1\n"
        "CVMFS_S3_PORT=" + StringifyInt(CVMFS_S3_TEST_MOCKUP_SERVER_PORT);

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

    Buffers::const_iterator i    = buffers.begin();
    Buffers::const_iterator iend = buffers.end();
    for (; i != iend; ++i) {
      CharBuffer *current_buffer = *i;
      shash::Update(current_buffer->ptr(), current_buffer->used_bytes(), ctx);
    }

    shash::Final(ctx, hash);
  }
};

template <typename T>
bool T_Uploaders<T>::IsS3() const {
  return false;
}

template <>
bool T_Uploaders<S3Uploader>::IsS3() const {
  return true;
}

template <class UploadersT>
atomic_int64 T_Uploaders<UploadersT>::gSeed = 0;

template <class UploadersT>
const char T_Uploaders<UploadersT>::sandbox_path[] = "./cvmfs_ut_uploader";

template <class UploadersT>
const std::string T_Uploaders<UploadersT>::tmp_dir =
    string(T_Uploaders::sandbox_path) + "/tmp";

template <class UploadersT>
const std::string T_Uploaders<UploadersT>::dest_dir =
    string(T_Uploaders::sandbox_path) + "/dest";

typedef testing::Types<S3Uploader, LocalUploader> UploadTypes;
TYPED_TEST_CASE(T_Uploaders, UploadTypes);


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, Initialize) {
  // nothing to do here... initialization runs completely in the fixture!
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, SimpleFileUpload) {
  const std::string big_file_path = TestFixture::GetBigFile();
  const std::string dest_name     = "big_file";

  this->uploader_->Upload(big_file_path,
                          dest_name,
                          AbstractUploader::MakeClosure(
                              &UploadCallbacks::SimpleUploadClosure,
                              &this->delegate_,
                              UploaderResults(0, big_file_path)));

  this->uploader_->WaitForUpload();
  EXPECT_TRUE(TestFixture::CheckFile(dest_name));
  EXPECT_EQ(1u, this->delegate_.simple_upload_invocations);
  TestFixture::CompareFileContents(big_file_path,
                                   TestFixture::AbsoluteDestinationPath(
                                       dest_name));
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, PeekIntoStorage) {
  const std::string small_file_path = TestFixture::GetSmallFile();
  const std::string dest_name       = "small_file";

  this->uploader_->Upload(small_file_path, dest_name,
                          AbstractUploader::MakeClosure(
                              &UploadCallbacks::SimpleUploadClosure,
                              &this->delegate_,
                              UploaderResults(0, small_file_path)));
  this->uploader_->WaitForUpload();

  EXPECT_TRUE(TestFixture::CheckFile(dest_name));
  EXPECT_EQ(1u, this->delegate_.simple_upload_invocations);
  TestFixture::CompareFileContents(small_file_path,
                                   TestFixture::AbsoluteDestinationPath(
                                       dest_name));

  const bool file_exists = this->uploader_->Peek(dest_name);
  EXPECT_TRUE(file_exists);

  const bool file_doesnt_exist = this->uploader_->Peek("alien");
  EXPECT_FALSE(file_doesnt_exist);
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, RemoveFromStorage) {
  const std::string small_file_path = TestFixture::GetSmallFile();
  const std::string dest_name       = "also_small_file";

  this->uploader_->Upload(small_file_path, dest_name,
                          AbstractUploader::MakeClosure(
                              &UploadCallbacks::SimpleUploadClosure,
                              &this->delegate_,
                              UploaderResults(0, small_file_path)));
  this->uploader_->WaitForUpload();

  EXPECT_TRUE(TestFixture::CheckFile(dest_name));
  EXPECT_EQ(1u, this->delegate_.simple_upload_invocations);
  TestFixture::CompareFileContents(small_file_path,
                                   TestFixture::AbsoluteDestinationPath(
                                       dest_name));

  const bool file_exists = this->uploader_->Peek(dest_name);
  EXPECT_TRUE(file_exists);

  const bool removed_successfully = this->uploader_->Remove(dest_name);
  EXPECT_TRUE(removed_successfully);

  EXPECT_FALSE(TestFixture::CheckFile(dest_name));
  const bool file_still_exists = this->uploader_->Peek(dest_name);
  EXPECT_FALSE(file_still_exists);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TYPED_TEST(T_Uploaders, UploadEmptyFile) {
  const std::string empty_file_path = TestFixture::GetEmptyFile();
  const std::string dest_name       = "empty_file";

  this->uploader_->Upload(empty_file_path, dest_name,
                          AbstractUploader::MakeClosure(
                              &UploadCallbacks::SimpleUploadClosure,
                              &this->delegate_,
                                  UploaderResults(0, empty_file_path)));
  this->uploader_->WaitForUpload();

  EXPECT_TRUE(TestFixture::CheckFile(dest_name));
  EXPECT_EQ(1u, this->delegate_.simple_upload_invocations);
  TestFixture::CompareFileContents(empty_file_path,
                                   TestFixture::AbsoluteDestinationPath(
                                       dest_name));
  EXPECT_EQ(0, GetFileSize(TestFixture::AbsoluteDestinationPath(dest_name)));
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, UploadHugeFileSlow) {
  const std::string huge_file_path = TestFixture::GetHugeFile();
  const std::string dest_name     = "huge_file";

  this->uploader_->Upload(huge_file_path, dest_name,
                          AbstractUploader::MakeClosure(
                              &UploadCallbacks::SimpleUploadClosure,
                              &this->delegate_,
                              UploaderResults(0, huge_file_path)));
  this->uploader_->WaitForUpload();

  EXPECT_TRUE(TestFixture::CheckFile(dest_name));
  EXPECT_EQ(1u, this->delegate_.simple_upload_invocations);
  TestFixture::CompareFileContents(huge_file_path,
                                   TestFixture::AbsoluteDestinationPath(
                                       dest_name));
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, UploadManyFilesSlow) {
  const unsigned int number_of_files = 500;
  typedef std::vector<std::pair<std::string, std::string> > Files;

  Files files;
  for (unsigned int i = 0; i < number_of_files; ++i) {
    std::stringstream ss;
    ss << "file" << i;
    const std::string dest_name = ss.str();
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

  Files::const_iterator i    = files.begin();
  Files::const_iterator iend = files.end();
  for (; i != iend; ++i) {
    this->uploader_->Upload(i->first, i->second,
                            AbstractUploader::MakeClosure(
                                &UploadCallbacks::SimpleUploadClosure,
                                &this->delegate_,
                                UploaderResults(0, i->first)));
  }
  this->uploader_->WaitForUpload();

  EXPECT_EQ(number_of_files, this->delegate_.simple_upload_invocations);
  for (i = files.begin(); i != iend; ++i) {
    EXPECT_TRUE(TestFixture::CheckFile(i->second));
    TestFixture::CompareFileContents(i->first,
                                     TestFixture::AbsoluteDestinationPath(
                                         i->second));
  }
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, SingleStreamedUpload) {
  const unsigned int number_of_buffers = 10;
  typename TestFixture::Buffers buffers =
      TestFixture::MakeRandomizedBuffers(number_of_buffers, 1337);

  EXPECT_EQ(0u, this->delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(0u, this->delegate_.streamed_upload_complete_invocations);

  UploadStreamHandle *handle = this->uploader_->InitStreamedUpload(
      AbstractUploader::MakeClosure(&UploadCallbacks::StreamedUploadComplete,
                                    &this->delegate_,
                                    0));
  ASSERT_NE(static_cast<UploadStreamHandle*>(NULL), handle);

  EXPECT_EQ(0u, this->delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(0u, this->delegate_.streamed_upload_complete_invocations);

  typename TestFixture::Buffers::const_iterator i    = buffers.begin();
  typename TestFixture::Buffers::const_iterator iend = buffers.end();
  for (; i != iend; ++i) {
    this->uploader_->ScheduleUpload(handle, *i,
                                    AbstractUploader::MakeClosure(
                                        &UploadCallbacks::BufferUploadComplete,
                                        &this->delegate_,
                                        UploaderResults(0, *i)));
  }
  this->uploader_->WaitForUpload();

  EXPECT_EQ(number_of_buffers,
            this->delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(0u,
            this->delegate_.streamed_upload_complete_invocations);

  shash::Any content_hash(shash::kSha1, 'A');
  content_hash.Randomize(42);
  this->uploader_->ScheduleCommit(handle, content_hash);
  this->uploader_->WaitForUpload();

  EXPECT_EQ(number_of_buffers,
            this->delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(1u,
            this->delegate_.streamed_upload_complete_invocations);

  const std::string dest = "data/" + content_hash.MakePath();
  EXPECT_TRUE(TestFixture::CheckFile(dest));
  TestFixture::CompareBuffersAndFileContents(
      buffers,
      TestFixture::AbsoluteDestinationPath(dest));

  TestFixture::FreeBuffers(&buffers);
}


//------------------------------------------------------------------------------


TYPED_TEST(T_Uploaders, MultipleStreamedUploadSlow) {
  const unsigned int  number_of_files        = 100;
  const unsigned int  max_buffers_per_stream = 15;
  typename TestFixture::BufferStreams streams =
      TestFixture::MakeRandomizedBufferStreams(number_of_files,
                                               max_buffers_per_stream,
                                               42);

  EXPECT_EQ(0u, this->delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(0u, this->delegate_.streamed_upload_complete_invocations);

  typename TestFixture::BufferStreams::iterator       i    = streams.begin();
  typename TestFixture::BufferStreams::const_iterator iend = streams.end();
  for (; i != iend; ++i) {
    UploadStreamHandle *handle = this->uploader_->InitStreamedUpload(
        AbstractUploader::MakeClosure(&UploadCallbacks::StreamedUploadComplete,
                                      &this->delegate_,
                                      0));
    ASSERT_NE(static_cast<UploadStreamHandle*>(NULL), handle);
    i->second.handle = handle;
  }

  EXPECT_EQ(0u, this->delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(0u, this->delegate_.streamed_upload_complete_invocations);

  // go through the handles and schedule buffers for them in a round robin
  // fashion --> we want to test concurrent streamed upload behaviour
  typename TestFixture::BufferStreams active_streams = streams;
  typename TestFixture::BufferStreams::iterator j    = active_streams.begin();
  unsigned int number_of_buffers                     = 0;
  while (!active_streams.empty()) {
    typename TestFixture::Buffers             &buffers        = j->first;
    const typename TestFixture::StreamHandle  &current_handle = j->second;

    if (!buffers.empty()) {
      CharBuffer *current_buffer = buffers.front();
      ASSERT_NE(static_cast<CharBuffer*>(NULL), current_buffer);
      ++number_of_buffers;
      this->uploader_->ScheduleUpload(current_handle.handle, current_buffer,
                   AbstractUploader::MakeClosure(
                     &UploadCallbacks::BufferUploadComplete,
                     &this->delegate_,
                     UploaderResults(0, buffers.front())));
      buffers.erase(buffers.begin());
    } else {
      this->uploader_->ScheduleCommit(current_handle.handle,
                                      current_handle.content_hash);
      j = active_streams.erase(j);
    }
  }

  this->uploader_->WaitForUpload();

  EXPECT_EQ(number_of_buffers,
            this->delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(number_of_files,
            this->delegate_.streamed_upload_complete_invocations);

  typename TestFixture::BufferStreams::const_iterator k    = streams.begin();
  typename TestFixture::BufferStreams::const_iterator kend = streams.end();
  for (; k != kend; ++k) {
    const shash::Any &content_hash = k->second.content_hash;
    const std::string dest = "data/" + content_hash.MakePath();
    EXPECT_TRUE(TestFixture::CheckFile(dest));
    TestFixture::CompareBuffersAndFileContents(
        k->first,
        TestFixture::AbsoluteDestinationPath(dest));
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

  this->uploader_->Upload(big_file_path,
                          dest_name,
                          AbstractUploader::MakeClosure(
                              &UploadCallbacks::SimpleUploadClosure,
                              &this->delegate_,
                              UploaderResults(0, big_file_path)));

  this->uploader_->WaitForUpload();
  EXPECT_TRUE(TestFixture::CheckFile(dest_name));

  EXPECT_EQ(1u, this->delegate_.simple_upload_invocations);
  TestFixture::CompareFileContents(big_file_path,
                                   TestFixture::AbsoluteDestinationPath(
                                       dest_name));

  ASSERT_TRUE(this->uploader_->PlaceBootstrappingShortcut(digest));
  TestFixture::CompareFileContents(big_file_path,
                                   TestFixture::AbsoluteDestinationPath(
                                       digest.MakeAlternativePath()));
}

}  // namespace upload
