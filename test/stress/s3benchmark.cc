/**
 * This file is part of the CernVM File System.
 */
#include <climits>
#include <cmath>
#include <cstring>
#include <string>

#include "crypto/hash.h"
#include "upload.h"
#include "upload_s3.h"
#include "upload_spooler_definition.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/prng.h"
#include "util/smalloc.h"
#include "util/string.h"

using namespace std;  // NOLINT

struct TestDataChunk {
  void *data;
  int size;
  shash::Any hash;

  TestDataChunk() : data(NULL), size(0) { }
};

class S3TestScenario {
 public:
  S3TestScenario(const string &config_path,
                 const string &temp_path,
                 int num_uploads,
                 int avg_file_size,
                 float duplicate_file_ratio);
  ~S3TestScenario();

  int Run();

 private:
  // variance of file sizes' normal distribution
  const float relative_variance_ = 0.1;
  string config_path_;
  string tmp_path_;
  int num_uploads_;
  int avg_file_size_;
  float duplicate_file_ratio_;
  upload::SpoolerDefinition *spooler_definition_;
  upload::S3Uploader *uploader_;
  vector<TestDataChunk> data_chunks_;
  int unique_files_;  // counter for unique files uploaded
  Prng prng_;

  void GeneratePayload();
  TestDataChunk GenerateChunk();
  TestDataChunk DuplicateChunk(const TestDataChunk &old_chunk);
  void UploadFile(const TestDataChunk &chunk);
  double CalculateDuration(const timespec &start, const timespec &end);
};

S3TestScenario::S3TestScenario(const string &config_path,
                               const string &temp_path,
                               int num_uploads,
                               int avg_file_size,
                               float duplicate_file_ratio)
    : config_path_(config_path)
    , tmp_path_(temp_path)
    , num_uploads_(num_uploads)
    , avg_file_size_(avg_file_size)
    , duplicate_file_ratio_(duplicate_file_ratio)
    , unique_files_(0) {
  prng_.InitLocaltime();

  assert(MkdirDeep(tmp_path_, 0755, true));

  string definition_string = "S3," + tmp_path_ + "/,cvmfs/s3benchmark_cvmfs@"
                             + config_path;
  spooler_definition_ = new upload::SpoolerDefinition(definition_string,
                                                      shash::kSha1);
  uploader_ = new upload::S3Uploader(*spooler_definition_);
}

int S3TestScenario::Run() {
  uint64_t start, payload_generated, files_uploaded, files_reuploaded,
      files_deleted;
  uploader_->Initialize();
  start = platform_monotonic_time_ns();
  GeneratePayload();
  payload_generated = platform_monotonic_time_ns();
  for (vector<TestDataChunk>::iterator chunk = data_chunks_.begin();
       chunk != data_chunks_.end();
       ++chunk) {
    UploadFile(*chunk);
  }
  uploader_->WaitForUpload();
  files_uploaded = platform_monotonic_time_ns();
  for (vector<TestDataChunk>::iterator chunk = data_chunks_.begin();
       chunk != data_chunks_.end();
       ++chunk) {
    UploadFile(*chunk);
  }
  uploader_->WaitForUpload();
  files_reuploaded = platform_monotonic_time_ns();
  for (vector<TestDataChunk>::iterator chunk = data_chunks_.begin();
       chunk != data_chunks_.end();
       ++chunk) {
    string path = "data/" + (*chunk).hash.MakePath();
    uploader_->RemoveAsync(path);
  }
  uploader_->WaitForUpload();
  files_deleted = platform_monotonic_time_ns();

  if (uploader_->GetNumberOfErrors() > 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Benchmark finished with %d errors!\n",
             uploader_->GetNumberOfErrors());
  }

  double duration_payload = (payload_generated - start) * 1e-9;
  double duration_upload = (files_uploaded - payload_generated) * 1e-9;
  double duration_reupload = (files_reuploaded - files_uploaded) * 1e-9;
  double duration_delete = (files_deleted - files_reuploaded) * 1e-9;

  LogCvmfs(kLogCvmfs, kLogStdout,
           "Finished %d uploads, %d unique files "
           "of average size %d bytes\n"
           "Payload generation: %f seconds\n"
           "HEAD(NotFound)+PUT(New): %f seconds\n"
           "HEAD(Found): %f seconds\n"
           "DELETE: %f seconds",
           num_uploads_, unique_files_, avg_file_size_, duration_payload,
           duration_upload, duration_reupload, duration_delete);
  LogCvmfs(kLogCvmfs, kLogStdout,
           "Request rates (reqs/s):\n"
           "HEAD(NotFound)+PUT(New): %f\n"
           "HEAD(Found): %f\n"
           "DELETE: %f",
           num_uploads_ / duration_upload, num_uploads_ / duration_reupload,
           num_uploads_ / duration_delete);
  return 0;
}

void S3TestScenario::GeneratePayload() {
  for (int i = 0; i < num_uploads_; ++i) {
    double duplicate_chance = prng_.NextDouble();
    if (duplicate_chance < duplicate_file_ratio_ && data_chunks_.size() > 0) {
      int duplicate_index = prng_.Next(data_chunks_.size());
      data_chunks_.push_back(DuplicateChunk(data_chunks_[duplicate_index]));
    } else {
      ++unique_files_;
      TestDataChunk generated_chunk = GenerateChunk();
      data_chunks_.push_back(generated_chunk);
    }
  }
}

TestDataChunk S3TestScenario::DuplicateChunk(const TestDataChunk &old_chunk) {
  TestDataChunk duplicate_chunk;
  duplicate_chunk.hash = old_chunk.hash;
  duplicate_chunk.size = old_chunk.size;
  duplicate_chunk.data = smalloc(duplicate_chunk.size);
  memcpy(duplicate_chunk.data, old_chunk.data, duplicate_chunk.size);
  return duplicate_chunk;
}

TestDataChunk S3TestScenario::GenerateChunk() {
  TestDataChunk chunk;

  int chunk_size = 0;
  while (chunk_size <= 0)
    chunk_size = avg_file_size_
                 + prng_.NextNormal() * avg_file_size_ * relative_variance_;
  chunk.size = chunk_size;

  chunk.data = smalloc(chunk.size);
  char *data = reinterpret_cast<char *>(chunk.data);
  for (int i = 0; i < chunk_size; ++i) {
    data[i] = prng_.Next(CHAR_MAX - CHAR_MIN) + CHAR_MIN;
  }

  chunk.hash = shash::Any(shash::kSha1);
  shash::HashMem((unsigned char *)chunk.data, chunk.size, &chunk.hash);

  return chunk;
}

void S3TestScenario::UploadFile(const TestDataChunk &chunk) {
  upload::UploadStreamHandle *handle = uploader_->InitStreamedUpload(NULL);
  upload::S3Uploader::UploadBuffer buffer = upload::S3Uploader::UploadBuffer(
      chunk.size, chunk.data);
  uploader_->ScheduleUpload(handle, buffer, NULL);
  uploader_->ScheduleCommit(handle, chunk.hash);
}

S3TestScenario::~S3TestScenario() {
  uploader_->TearDown();
  delete uploader_;
  assert(RemoveTree(tmp_path_));
}

void Usage() {
  LogCvmfs(kLogCvmfs, kLogStderr,
           "CVMFS S3Uploader benchmark.\n"
           "Generates random files, uploads them to specified S3 storage,\n"
           "reuploads the same files and finally deletes them.\n"
           "Outputs the time duration of each step.\n\n"
           "Usage: s3benchmark [-n num-files] [-s average-file-size] "
           "[-t tmp-path] [-d duplicate-ratio] [-h] -c path/to/s3.cfg\n"
           "Options:\n"
           "  -c path to cvmfs-format s3 config file\n"
           "  -n number of files to be uploaded\n"
           "  -s average file size (file sizes are normally distributed)\n"
           "  -t temporary path used by S3Uploader\n"
           "  -d ratio of duplicate files (upload some files multiple times)\n"
           "  -h print this usage message\n");
}

int main(int argc, char *argv[]) {
  string s3_config, tmp_path = "/tmp/s3benchmark";
  int num_files = 1000, file_size = 4096;
  float duplicate_ratio = 0;

  int c;
  while ((c = getopt(argc, argv, "c:n:s:t:d:h")) != -1) {
    switch (c) {
      case 'c':
        s3_config = string(optarg);
        break;
      case 'n':
        num_files = atoi(optarg);
        break;
      case 's':
        file_size = atoi(optarg);
        break;
      case 't':
        tmp_path = string(optarg);
        break;
      case 'd':
        duplicate_ratio = atof(optarg);
        break;
      case 'h':
        Usage();
        return 0;
        break;
      case '?':
      default:
        Usage();
        return 1;
    }
  }
  if (s3_config.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Path to s3 config file not specified");
    return 1;
  }
  S3TestScenario *scenario = new S3TestScenario(
      s3_config, tmp_path, num_files, file_size, duplicate_ratio);

  int err = scenario->Run();
  delete scenario;
  if (err) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Error when running the benchmark");
    return err;
  }
  return 0;
}
