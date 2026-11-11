/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <memory>

#include "c_repository.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace publish {

class T_Repository : public ::testing::Test {
 protected:
  virtual void SetUp() { }

 protected:
};


TEST_F(T_Repository, MasterReplica) {
  Publisher *publisher = GetTestPublisher();

  EXPECT_FALSE(publisher->IsMasterReplica());
  publisher->MarkReplicatible(true);
  EXPECT_TRUE(publisher->IsMasterReplica());
  publisher->MarkReplicatible(false);
  EXPECT_FALSE(publisher->IsMasterReplica());

  delete publisher;
}

TEST_F(T_Repository, ReadOnlyPublicKeyPaths) {
  const std::unique_ptr<Publisher> publisher(GetTestPublisher());
  SettingsRepository settings(publisher->settings());
  const std::string dir = settings.keychain().keychain_dir();
  const std::string key = settings.keychain().master_public_key_path();

  settings.SetPublicKeyPaths(dir);
  EXPECT_NO_THROW(Repository from_directory(settings));
  EXPECT_EQ(key, settings.GetClientPublicKeyPaths());

  settings.SetPublicKeyPaths(key);
  EXPECT_NO_THROW(Repository from_file(settings));
  EXPECT_EQ(key, settings.GetClientPublicKeyPaths());

  settings.SetPublicKeyPaths(key + ":" + dir);
  EXPECT_NO_THROW(Repository from_list(settings));
  EXPECT_EQ(key + ":" + key, settings.GetClientPublicKeyPaths());
  EXPECT_EQ(dir, settings.keychain().keychain_dir());
}

TEST_F(T_Repository, ReadOnlySymlinkKeyDirectory) {
  const std::unique_ptr<Publisher> publisher(GetTestPublisher());
  SettingsRepository settings(publisher->settings());
  const std::string dir = settings.keychain().keychain_dir();
  const std::string key = settings.keychain().master_public_key_path();
  const std::string link = dir + "-link";
  ASSERT_TRUE(SymlinkForced(GetFileName(dir), link));
  const std::string client_key = link + "/" + settings.fqrn() + ".pub";

  settings.GetKeychain()->SetKeychainDir(link);
  EXPECT_NO_THROW(Repository from_default_link(settings));
  EXPECT_EQ(client_key, settings.GetClientPublicKeyPaths());

  ASSERT_TRUE(SymlinkForced(key, dir + "/additional.pub"));
  settings.SetPublicKeyPaths(link);
  EXPECT_NO_THROW(Repository from_configured_link(settings));
  EXPECT_EQ(client_key, settings.GetClientPublicKeyPaths());
  EXPECT_EQ(0, unlink(link.c_str()));
}

}  // namespace publish
