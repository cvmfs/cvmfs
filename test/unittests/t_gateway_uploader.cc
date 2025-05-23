/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdlib>

#include "repository_tag.h"
#include "upload.h"
#include "upload_gateway.h"
#include "upload_spooler_definition.h"
#include "util/pointer.h"
#include "util/posix.h"

class GatewayUploaderMocked : public upload::GatewayUploader {
 public:
  explicit GatewayUploaderMocked(const upload::SpoolerDefinition &definition)
      : upload::GatewayUploader(definition) { }

 protected:
  virtual void ReadSessionTokenFile(const std::string & /*token_file_name*/,
                                    std::string *token) {
    *token = "ThisIsAFakeToken";
  }
  virtual bool ReadKey(const std::string & /*key_file_name*/,
                       std::string *key_id, std::string *secret) {
    if (!(key_id && secret)) {
      return false;
    }

    *key_id = "fake_id";
    *secret = "fake_secret";
    return true;
  }
};

class T_GatewayUploaderConfig : public ::testing::Test {
 protected:
  T_GatewayUploaderConfig() : config() { }

  upload::GatewayUploader::Config config;
  std::string token_file_name;
};

class T_GatewayUploader : public ::testing::Test {
 protected:
  T_GatewayUploader() { }
};

TEST_F(T_GatewayUploaderConfig, ParseSpoolerDefinitionBadArgs) {
  upload::SpoolerDefinition definition(
      "gw,/local/temp/dir,http://my.repo.address:4929/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, false, 0, 0, 0,
      "/var/spool/cvmfs/test.cern.ch/session_token_some_path", "some_key_file");
  EXPECT_FALSE(
      upload::GatewayUploader::ParseSpoolerDefinition(definition, NULL));
}

TEST_F(T_GatewayUploaderConfig, ParseSpoolerDefinitionGoodConfigString) {
  upload::SpoolerDefinition definition(
      "gw,/local/temp/dir,http://my.repo.address:4929/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, false, 0, 0, 0,
      "/var/spool/cvmfs/test.cern.ch/session_token_some_path", "some_key_file");

  EXPECT_TRUE(
      upload::GatewayUploader::ParseSpoolerDefinition(definition, &config));

  EXPECT_EQ(config.api_url, "http://my.repo.address:4929/api/v1");
  EXPECT_EQ(config.session_token_file,
            "/var/spool/cvmfs/test.cern.ch/session_token_some_path");
}

TEST_F(T_GatewayUploaderConfig, ParseSpoolerDefinitionNoSessionTokenFile) {
  upload::SpoolerDefinition definition(
      "gw,/local/temp/dir,http://my.repo.address:4929/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, 0, 0, 0);

  EXPECT_FALSE(
      upload::GatewayUploader::ParseSpoolerDefinition(definition, &config));
}

TEST_F(T_GatewayUploader, Construct) {
  upload::SpoolerDefinition definition(
      "gw,/local/temp/dir,http://my.repo.address:4929/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, false, 0, 0, 0,
      "/var/spool/cvmfs/test.cern.ch/session_token_some_path", "some_key_file");
  GatewayUploaderMocked uploader(definition);
  EXPECT_TRUE(uploader.Initialize());
  EXPECT_TRUE(
      uploader.FinalizeSession(false, "", "", RepositoryTag("new_tag", "")));
  uploader.TearDown();
}

/*
// This test needs to be disabled, since the Spooler class isn't aware of
// GatewayUploaderMocked at this point
TEST_F(T_GatewayUploader, ConstructThroughSpooler) {
  upload::SpoolerDefinition definition(
      "gw,/local/temp/dir,http://my.repo.address:4929/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, 0, 0, 0,
      "/var/spool/cvmfs/test.cern.ch/session_token_some_path");
  UniquePtr<upload::Spooler> spooler(upload::Spooler::Construct(definition));
  EXPECT_TRUE(spooler.IsValid());
  EXPECT_EQ(spooler->backend_name(), "HTTP");
}
*/
