#include <gtest/gtest.h>
#include <unistd.h>
#include <string>
#include <sstream>
#include <tbb/atomic.h>

#include <iostream> // TODO: remove me!

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
    simple_upload_invocations = 0;
  }

  void SimpleUploadClosure(const UploaderResults &results,
                                 UploaderResults  expected) {
    EXPECT_EQ (expected.return_code, results.return_code);
    EXPECT_EQ (expected.local_path,  results.local_path);
    ++simple_upload_invocations;
  }

 public:
  tbb::atomic<unsigned int> simple_upload_invocations;
};


class T_LocalUploader : public FileSandbox {
 private:
  static const std::string sandbox_path;
  static const std::string dest_dir;
  static const std::string tmp_dir;

 public:
  T_LocalUploader() :
    FileSandbox(T_LocalUploader::sandbox_path) {}

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

    return SpoolerDefinition(definition, use_file_chunking, min_chunk_size,
                                                            avg_chunk_size,
                                                            max_chunk_size);
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

  std::string AbsoluteDestinationPath(const std::string &remote_path) const {
    return T_LocalUploader::dest_dir + "/" + remote_path;
  }

 private:
  std::string ByteToHex(const unsigned char byte) {
    const char pos_1 = byte / 16;
    const char pos_0 = byte % 16;
    const char hex[3] = {
      (pos_1 <= 9) ? '0' + pos_1 : 'a' + (pos_1 - 10),
      (pos_0 <= 9) ? '0' + pos_0 : 'a' + (pos_0 - 10),
      0
    };
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

 protected:
  AbstractUploader *uploader_;
  UploadCallbacks   delegate_;
};

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
