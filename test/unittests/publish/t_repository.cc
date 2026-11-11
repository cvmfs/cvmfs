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


TEST_F(T_Repository, ConfiguredPublicKeyPathsPreservePublisherKeychain) {
  const std::string config_dir = CreateTempDir("/tmp/cvmfs-settings");
  ASSERT_FALSE(config_dir.empty());
  const std::string alias = "test.cvmfs.io";
  ASSERT_TRUE(MkdirDeep(config_dir + "/" + alias, 0700));
  const std::string server_conf = config_dir + "/" + alias + "/server.conf";
  const std::unique_ptr<Publisher> source_publisher(GetTestPublisher());
  const std::string existing_dir =
      source_publisher->settings().keychain().keychain_dir();
  const std::string existing_key = existing_dir + "/" + alias + ".pub";
  const std::string link = config_dir + "/keys-link";
  ASSERT_TRUE(SymlinkForced(existing_dir, link));
  // CVMFS_PUBLIC_KEY previously also supplied the keychain path for a
  // publisher. Preserve that behavior for paths that need not exist yet.
  const std::string paths[] = {existing_dir, link, existing_key,
                               existing_key + ":" + existing_dir,
                               config_dir + "/missing",
                               config_dir + "/key.pem:" + config_dir + "/keys"};
  const std::string resolved[] = {
      existing_key, link + "/" + alias + ".pub", existing_key,
      existing_key + ":" + existing_key, paths[4], paths[5]};
  for (unsigned i = 0; i < sizeof(paths) / sizeof(paths[0]); ++i) {
    ASSERT_TRUE(SafeWriteToFile("CVMFS_PUBLIC_KEY='" + paths[i] + "'\n",
                                server_conf, 0600));
    SettingsBuilder builder;
    builder.SetConfigPath(config_dir);
    const SettingsRepository repository = builder.CreateSettingsRepository(
        alias);
    EXPECT_EQ(paths[i], repository.keychain().keychain_dir());
    EXPECT_EQ(resolved[i], repository.ResolvePublicKeyPaths());
    const SettingsPublisher publisher(repository);
    EXPECT_EQ(paths[i], publisher.keychain().keychain_dir());
    EXPECT_EQ(paths[i] + "/" + alias + ".pub",
              publisher.keychain().master_public_key_path());
    EXPECT_EQ(paths[i] + "/" + alias + ".key",
              publisher.keychain().private_key_path());
  }
  EXPECT_TRUE(RemoveTree(config_dir));
}

TEST_F(T_Repository, ConfiguredSigningDirectoryOverridesPublicKeyPaths) {
  const std::string config_dir = CreateTempDir("/tmp/cvmfs-settings");
  ASSERT_FALSE(config_dir.empty());
  const std::string alias = "test.cvmfs.io";
  ASSERT_TRUE(MkdirDeep(config_dir + "/" + alias, 0700));
  const std::string server_conf = config_dir + "/" + alias + "/server.conf";
  const std::string public_keys = config_dir + "/public-keys";
  const std::string signing_keys = config_dir + "/signing-keys";
  ASSERT_TRUE(SafeWriteToFile("CVMFS_PUBLIC_KEY='" + public_keys + "'\n"
                                 "CVMFS_KEYS_DIR='" + signing_keys + "'\n",
                             server_conf, 0600));
  SettingsBuilder builder;
  builder.SetConfigPath(config_dir);
  const SettingsRepository repository = builder.CreateSettingsRepository(alias);
  SettingsPublisher publisher(repository);
  builder.ApplyOptionsFromServerPath(*builder.options_mgr(), &publisher);
  EXPECT_EQ(public_keys, repository.ResolvePublicKeyPaths());
  EXPECT_EQ(signing_keys, publisher.keychain().keychain_dir());
  EXPECT_EQ(signing_keys + "/" + alias + ".key",
            publisher.keychain().private_key_path());
  EXPECT_TRUE(RemoveTree(config_dir));
}

}  // namespace publish
