/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <tbb/atomic.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>  // TODO(jblomer): remove me
#include <iostream>  // TODO(jblomer): remove me
#include <sstream>  // TODO(jblomer): remove me
#include <string>

#include "atomic.h"
#include "c_file_sandbox.h"
#include "file_processing/char_buffer.h"
#include "hash.h"
#include "../common/testutil.h"
#include "upload_s3.h"
#include "upload_spooler_definition.h"
#include "util.h"

namespace upload {

/**
 * Port of S3 mockup server
 */
#define CVMFS_S3_TEST_MOCKUP_SERVER_PORT 8082

/**
 * Read-bufer size of S3 mockup server
 */
#define CVMFS_S3_TEST_READ_BUFFER_SIZE 1000

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


class T_S3Uploader : public FileSandbox {
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
  T_S3Uploader() :
    FileSandbox(T_S3Uploader::sandbox_path),
    uploader_(NULL) {}

 protected:
  std::string s3_conf_path;

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
    int listen_sockfd, accept_sockfd;
    socklen_t clilen;
    struct sockaddr_in serv_addr, cli_addr;
    char buffer[CVMFS_S3_TEST_READ_BUFFER_SIZE];
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
      bzero(buffer, CVMFS_S3_TEST_READ_BUFFER_SIZE);

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
        std::string path = T_S3Uploader::dest_dir + "/" + req_file;
        file = fopen(path.c_str(), "w");
        ASSERT_TRUE(file != NULL);
        int fid = fileno(file);
        ASSERT_GE(fid, 0);
        int left_to_read = content_length;
        while (left_to_read > 0) {
          int n = read(accept_sockfd, buffer, CVMFS_S3_TEST_READ_BUFFER_SIZE-1);
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
        if (FileExists(T_S3Uploader::dest_dir + "/" + req_file) == false)
          reply = "HTTP/1.1 404 Not Found\r\n";
      } else if (req_type.compare("DELETE") == 0) {
        std::string path = T_S3Uploader::dest_dir + "/" + req_file;
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
    FILE *s3_conf = CreateTempFile(T_S3Uploader::tmp_dir + "/s3.conf",
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
        "CVMFS_S3_HOST=localhost\n"
        "CVMFS_S3_PORT=" + StringifyInt(CVMFS_S3_TEST_MOCKUP_SERVER_PORT);

    fprintf(s3_conf, "%s\n", conf_str.c_str());
    fclose(s3_conf);
  }

  virtual void SetUp() {
    CreateSandbox(T_S3Uploader::tmp_dir);

    const bool success = MkdirDeep(T_S3Uploader::dest_dir, 0700);
    ASSERT_TRUE(success) << "Failed to create uploader destination dir";
    InitializeStorageBackend();

    CreateTempS3ConfigFile(10, 10);
    uploader_ = AbstractUploader::Construct(GetSpoolerDefinition());
    ASSERT_NE(static_cast<AbstractUploader*>(NULL), uploader_);

    CreateS3Mockup();
  }

  virtual void TearDown() {
    // Request S3 server to finish
    uploader_->Peek("EXIT");

    // Wait S3 server to finish
    int stat_loc = 0;
    wait(&stat_loc);
    ASSERT_EQ(stat_loc, 0);

    if (uploader_ != NULL) {
      uploader_->TearDown();
      delete uploader_;
      uploader_ = NULL;
    }
    RemoveSandbox(T_S3Uploader::tmp_dir);
  }

  void InitializeStorageBackend() {
    const std::string &dir = T_S3Uploader::dest_dir + "/testdata/data";
    bool success = MkdirDeep(dir + "/txn", 0700);
    ASSERT_TRUE(success) << "Failed to create transaction tmp dir";
    for (unsigned int i = 0; i <= 255; ++i) {
      success = MkdirDeep(dir + "/" + ByteToHex(static_cast<char>(i)), 0700);
      ASSERT_TRUE(success);
    }
  }

  SpoolerDefinition GetSpoolerDefinition() const {
    const std::string spl_type   = "S3";
    const std::string spl_tmp    = T_S3Uploader::tmp_dir;
    const std::string spl_cfg    = "testdata@" + s3_conf_path;
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
        *(buffer->ptr() + i) = rng.Next(CVMFS_S3_TEST_READ_BUFFER_SIZE);
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
    const int rng_seed) const
  {
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
    BufferStreams::iterator       i    = streams->begin();
    BufferStreams::const_iterator iend = streams->end();
    for (; i != iend; ++i) {
      FreeBuffers(i->first);
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
    return T_S3Uploader::dest_dir + "/testdata/" + remote_path;
  }

 private:
  std::string ByteToHex(const unsigned char byte) {
    char hex[3];
    snprintf(hex, sizeof(hex), "%02x", byte);
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

 protected:
  AbstractUploader *uploader_;
  UploadCallbacks   delegate_;
};
atomic_int64 T_S3Uploader::gSeed = 0;

const std::string T_S3Uploader::sandbox_path = "./cvmfs_ut_s3uploader";
const std::string T_S3Uploader::tmp_dir =
  T_S3Uploader::sandbox_path + "/tmp";
const std::string T_S3Uploader::dest_dir = T_S3Uploader::sandbox_path + "/dest";



TEST_F(T_S3Uploader, Initialize) {
  // nothing to do here... initialization runs completely in the fixture!
}


TEST_F(T_S3Uploader, SimpleFileUpload) {
  const std::string big_file_path = GetBigFile();
  const std::string dest_name     = "big_file";

  uploader_->Upload(big_file_path, dest_name,
    AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                  &delegate_,
                                  UploaderResults(0, big_file_path)));
  uploader_->WaitForUpload();
  EXPECT_TRUE(CheckFile(dest_name));
  EXPECT_EQ(1u, delegate_.simple_upload_invocations);
  CompareFileContents(big_file_path, AbsoluteDestinationPath(dest_name));
}


TEST_F(T_S3Uploader, PeekIntoStorage) {
  const std::string small_file_path = GetSmallFile();
  const std::string dest_name       = "small_file";

  uploader_->Upload(small_file_path, dest_name,
    AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                  &delegate_,
                                  UploaderResults(0, small_file_path)));
  uploader_->WaitForUpload();

  EXPECT_TRUE(CheckFile(dest_name));
  EXPECT_EQ(1u, delegate_.simple_upload_invocations);
  CompareFileContents(small_file_path, AbsoluteDestinationPath(dest_name));

  const bool file_exists = uploader_->Peek(dest_name);
  EXPECT_TRUE(file_exists);

  const bool file_doesnt_exist = uploader_->Peek("alien");
  EXPECT_FALSE(file_doesnt_exist);
}


TEST_F(T_S3Uploader, RemoveFromStorage) {
  const std::string small_file_path = GetSmallFile();
  const std::string dest_name       = "also_small_file";

  uploader_->Upload(small_file_path, dest_name,
    AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                  &delegate_,
                                  UploaderResults(0, small_file_path)));
  uploader_->WaitForUpload();

  EXPECT_TRUE(CheckFile(dest_name));
  EXPECT_EQ(1u, delegate_.simple_upload_invocations);
  CompareFileContents(small_file_path, AbsoluteDestinationPath(dest_name));

  const bool file_exists = uploader_->Peek(dest_name);
  EXPECT_TRUE(file_exists);

  const bool removed_successfully = uploader_->Remove(dest_name);
  EXPECT_TRUE(removed_successfully);

  EXPECT_FALSE(CheckFile(dest_name));
  const bool file_still_exists = uploader_->Peek(dest_name);
  EXPECT_FALSE(file_still_exists);
}


TEST_F(T_S3Uploader, UploadEmptyFile) {
  const std::string empty_file_path = GetEmptyFile();
  const std::string dest_name     = "empty_file";

  uploader_->Upload(empty_file_path, dest_name,
    AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                  &delegate_,
                                  UploaderResults(0, empty_file_path)));
  uploader_->WaitForUpload();

  EXPECT_TRUE(CheckFile(dest_name));
  EXPECT_EQ(1u, delegate_.simple_upload_invocations);
  CompareFileContents(empty_file_path, AbsoluteDestinationPath(dest_name));
  EXPECT_EQ(0, GetFileSize(AbsoluteDestinationPath(dest_name)));
}


TEST_F(T_S3Uploader, UploadHugeFile) {
  const std::string huge_file_path = GetHugeFile();
  const std::string dest_name     = "huge_file";

  uploader_->Upload(huge_file_path, dest_name,
    AbstractUploader::MakeClosure(&UploadCallbacks::SimpleUploadClosure,
                                  &delegate_,
                                  UploaderResults(0, huge_file_path)));
  uploader_->WaitForUpload();

  EXPECT_TRUE(CheckFile(dest_name));
  EXPECT_EQ(1u, delegate_.simple_upload_invocations);
  CompareFileContents(huge_file_path, AbsoluteDestinationPath(dest_name));
}


TEST_F(T_S3Uploader, UploadManyFiles) {
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

  EXPECT_EQ(number_of_files, delegate_.simple_upload_invocations);
  for (i = files.begin(); i != iend; ++i) {
    EXPECT_TRUE(CheckFile(i->second));
    CompareFileContents(i->first, AbsoluteDestinationPath(i->second));
  }
}


TEST_F(T_S3Uploader, SingleStreamedUpload) {
  const unsigned int number_of_buffers = 10;
  Buffers buffers = MakeRandomizedBuffers(number_of_buffers, 1337);

  EXPECT_EQ(0u, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(0u, delegate_.streamed_upload_complete_invocations);

  UploadStreamHandle *handle = uploader_->InitStreamedUpload(
                                 AbstractUploader::MakeClosure(
                                   &UploadCallbacks::StreamedUploadComplete,
                                   &delegate_,
                                   0));
  ASSERT_NE(static_cast<UploadStreamHandle*>(NULL), handle);

  EXPECT_EQ(0u, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(0u, delegate_.streamed_upload_complete_invocations);

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

  EXPECT_EQ(number_of_buffers, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(0u,                delegate_.streamed_upload_complete_invocations);

  shash::Any content_hash(shash::kSha1);
  content_hash.Randomize(42);
  const shash::Suffix hash_suffix = 'A';
  uploader_->ScheduleCommit(handle, content_hash, hash_suffix);
  uploader_->WaitForUpload();

  EXPECT_EQ(number_of_buffers, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(1u,                delegate_.streamed_upload_complete_invocations);

  const std::string dest = "data" +
                           content_hash.MakePathExplicit(1, 2) + hash_suffix;
  EXPECT_TRUE(CheckFile(dest));
  CompareBuffersAndFileContents(buffers, AbsoluteDestinationPath(dest));

  FreeBuffers(&buffers);
}


TEST_F(T_S3Uploader, MultipleStreamedUpload) {
  const unsigned int  number_of_files        = 100;
  const unsigned int  max_buffers_per_stream = 15;
  const shash::Suffix hash_suffix            = 'K';
  BufferStreams streams = MakeRandomizedBufferStreams(number_of_files,
                                                      max_buffers_per_stream,
                                                      42);

  EXPECT_EQ(0u, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(0u, delegate_.streamed_upload_complete_invocations);

  BufferStreams::iterator       i    = streams.begin();
  BufferStreams::const_iterator iend = streams.end();
  for (; i != iend; ++i) {
    UploadStreamHandle *handle = uploader_->InitStreamedUpload(
                                   AbstractUploader::MakeClosure(
                                     &UploadCallbacks::StreamedUploadComplete,
                                     &delegate_,
                                     0));
    ASSERT_NE(static_cast<UploadStreamHandle*>(NULL), handle);
    i->second.handle = handle;
  }

  EXPECT_EQ(0u, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(0u, delegate_.streamed_upload_complete_invocations);

  // go through the handles and schedule buffers for them in a round robin
  // fashion --> we want to test concurrent streamed upload behaviour
  BufferStreams active_streams   = streams;
  unsigned int number_of_buffers = 0;
  BufferStreams::iterator j      = active_streams.begin();
  while (!active_streams.empty()) {
          Buffers       &buffers        = j->first;
    const StreamHandle  &current_handle = j->second;

    if (!buffers.empty()) {
      CharBuffer *current_buffer = buffers.front();
      ASSERT_NE(static_cast<CharBuffer*>(NULL), current_buffer);
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

  EXPECT_EQ(number_of_buffers, delegate_.buffer_upload_complete_invocations);
  EXPECT_EQ(number_of_files,   delegate_.streamed_upload_complete_invocations);

  BufferStreams::const_iterator k    = streams.begin();
  BufferStreams::const_iterator kend = streams.end();
  for (; k != kend; ++k) {
    const shash::Any &content_hash = k->second.content_hash;
    const std::string dest =
        "data" + content_hash.MakePathWithSuffix(1, 2, hash_suffix);
    EXPECT_TRUE(CheckFile(dest));
    CompareBuffersAndFileContents(k->first, AbsoluteDestinationPath(dest));
  }

  FreeBufferStreams(&streams);
}

}  // namespace upload
