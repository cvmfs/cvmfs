/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <upload.h>
#include <upload_http.h>
#include <upload_spooler_definition.h>
#include <util/pointer.h>

class T_HttpUploaderConfig : public ::testing::Test {
 protected:
  T_HttpUploaderConfig() : config() {}

  upload::HttpUploader::Config config;
};

class T_HttpUploader : public ::testing::Test {
 protected:
  T_HttpUploader() {}
};

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionBadArgs) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.address:8080/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, 0, 0, 0, "root", "/some/subpath");
  EXPECT_FALSE(upload::HttpUploader::ParseSpoolerDefinition(definition, NULL));
}

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionBadConfigString) {
  upload::SpoolerDefinition definition("local,/local/temp/dir,/local/repo/dir",
                                       shash::kSha1, zlib::kZlibDefault, false,
                                       0, 0, 0, "root", "/some/subpath");

  EXPECT_FALSE(
      upload::HttpUploader::ParseSpoolerDefinition(definition, &config));
}

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionGoodConfigString) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.address:8080/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, 0, 0, 0, "root", "/some/subpath");

  EXPECT_TRUE(
      upload::HttpUploader::ParseSpoolerDefinition(definition, &config));

  EXPECT_EQ(config.user_name, "root");
  EXPECT_EQ(config.repository_subpath, "/some/subpath");
  EXPECT_EQ(config.repository_address, "http://my.repo.address");
  EXPECT_EQ(config.port, 8080);
  EXPECT_EQ(config.api_path, "api/v1");
}

TEST_F(T_HttpUploaderConfig,
       ParseSpoolerDefinitionGoodConfigStringMissingUserName) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.address:8080/api/v1", shash::kSha1);
  EXPECT_FALSE(
      upload::HttpUploader::ParseSpoolerDefinition(definition, &config));
}

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionInvalidPortNumber) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.address:1000000/api/v1",
      shash::kSha1, zlib::kZlibDefault, false, 0, 0, 0, "root",
      "/some/subpath");
  EXPECT_FALSE(
      upload::HttpUploader::ParseSpoolerDefinition(definition, &config));
}

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionEmptyApiPath) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.address:8080", shash::kSha1,
      zlib::kZlibDefault, false, 0, 0, 0, "root", "/some/subpath");
  EXPECT_TRUE(
      upload::HttpUploader::ParseSpoolerDefinition(definition, &config));
}

TEST_F(T_HttpUploader, Construct) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.address:8080/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, 0, 0, 0, "root", "/some/subpath");
  upload::HttpUploader uploader(definition);
  uploader.Initialize();
  uploader.TearDown();
}

TEST_F(T_HttpUploader, ConstructThroughSpooler) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.address:8080/api/v1", shash::kSha1,
      zlib::kZlibDefault, false, 0, 0, 0, "root", "/some/subpath");
  UniquePtr<upload::Spooler> spooler(upload::Spooler::Construct(definition));
  EXPECT_TRUE(spooler.IsValid());
  EXPECT_EQ(spooler->backend_name(), "HTTP");
}
