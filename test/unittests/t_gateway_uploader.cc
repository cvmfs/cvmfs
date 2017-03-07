/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdlib>

#include "upload.h"
#include "upload_gateway.h"
#include "upload_spooler_definition.h"
#include "util/pointer.h"
#include "util/posix.h"

class HttpUploaderMocked : public upload::HttpUploader {
 public:
  explicit HttpUploaderMocked(const upload::SpoolerDefinition& definition)
      : upload::HttpUploader(definition) {}

 protected:
  virtual bool ReadSessionTokenFile(const std::string& /*token_file_name*/,
                                    std::string* token) {
    if (!token) {
      return false;
    }

    *token = "ThisIsAFakeToken";
    return true;
  }
};

class T_HttpUploaderConfig : public ::testing::Test {
 protected:
  T_HttpUploaderConfig() : config() {}

  upload::HttpUploader::Config config;
  std::string token_file_name;
};

class T_HttpUploader : public ::testing::Test {
 protected:
  T_HttpUploader() {}
};

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionBadArgs) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.address:8080/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, 0, 0, 0,
      "/var/spool/cvmfs/test.cern.ch/session_token_some_path");
  EXPECT_FALSE(upload::HttpUploader::ParseSpoolerDefinition(definition, NULL));
}

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionBadConfigString) {
  upload::SpoolerDefinition definition(
      "local,/local/temp/dir,/local/repo/dir", shash::kSha1, zlib::kZlibDefault,
      false, 0, 0, 0, "/var/spool/cvmfs/test.cern.ch/session_token_some_path");

  EXPECT_FALSE(
      upload::HttpUploader::ParseSpoolerDefinition(definition, &config));
}

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionGoodConfigString) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.address:8080/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, 0, 0, 0,
      "/var/spool/cvmfs/test.cern.ch/session_token_some_path");

  EXPECT_TRUE(
      upload::HttpUploader::ParseSpoolerDefinition(definition, &config));

  EXPECT_EQ(config.api_url, "http://my.repo.address:8080/api/v1");
  EXPECT_EQ(config.session_token_file,
            "/var/spool/cvmfs/test.cern.ch/session_token_some_path");
}

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionNoSessionTokenFile) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.address:8080/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, 0, 0, 0);

  EXPECT_FALSE(
      upload::HttpUploader::ParseSpoolerDefinition(definition, &config));
}

TEST_F(T_HttpUploader, Construct) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.address:8080/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, 0, 0, 0,
      "/var/spool/cvmfs/test.cern.ch/session_token_some_path");
  HttpUploaderMocked uploader(definition);
  EXPECT_TRUE(uploader.Initialize());
  uploader.TearDown();
}

/*
// This test needs to be disabled, since the Spooler class isn't aware of
// HttpUploaderMocked at this point
TEST_F(T_HttpUploader, ConstructThroughSpooler) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.address:8080/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, 0, 0, 0,
      "/var/spool/cvmfs/test.cern.ch/session_token_some_path");
  UniquePtr<upload::Spooler> spooler(upload::Spooler::Construct(definition));
  EXPECT_TRUE(spooler.IsValid());
  EXPECT_EQ(spooler->backend_name(), "HTTP");
}
*/
