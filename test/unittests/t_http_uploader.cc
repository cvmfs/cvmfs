/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <upload_http.h>
#include <upload_spooler_definition.h>

class T_HttpUploaderConfig : public ::testing::Test {
 protected:
  T_HttpUploaderConfig() : config() {}
  virtual void SetUp() {}

  upload::HttpUploader::Config config;
};

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionBadArgs) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.addres:8080/api/v1", shash::kSha1);
  EXPECT_FALSE(upload::HttpUploader::ParseSpoolerDefinition(definition, NULL));
}

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionBadConfigString) {
  upload::SpoolerDefinition definition("local,/local/temp/dir,/local/repo/dir",
                                       shash::kSha1);
  EXPECT_FALSE(
      upload::HttpUploader::ParseSpoolerDefinition(definition, &config));
}

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionGoodConfigString) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.addres:8080/api/v1", shash::kSha1);
  EXPECT_TRUE(
      upload::HttpUploader::ParseSpoolerDefinition(definition, &config));
}

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionInvalidPortNumber) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.addres:1000000/api/v1",
      shash::kSha1);
  EXPECT_FALSE(
      upload::HttpUploader::ParseSpoolerDefinition(definition, &config));
}

TEST_F(T_HttpUploaderConfig, ParseSpoolerDefinitionEmptyApiPath) {
  upload::SpoolerDefinition definition(
      "http,/local/temp/dir,http://my.repo.addres:8080", shash::kSha1);
  EXPECT_TRUE(
      upload::HttpUploader::ParseSpoolerDefinition(definition, &config));
}
