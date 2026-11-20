/**
 * This file is part of the CernVM File System.
 */

#include <cstdlib>
#include <string>
#include <vector>

#include "config_repo_fetch.h"
#include "crypto/signature.h"
#include "gtest/gtest.h"
#include "options.h"
#include "util/file_guard.h"
#include "util/posix.h"

using namespace std;  // NOLINT

class T_ConfigRepo : public ::testing::Test {
 protected:
  virtual void SetUp() {
    const char *keys[] = {
        "CVMFS_HTTP_PROXY",     "CVMFS_FALLBACK_PROXY",    "CVMFS_SERVER_URL",
        "CVMFS_CLIENT_PROFILE", "CVMFS_USE_CDN",           "CVMFS_KEYS_DIR",
        "CVMFS_MOUNT_DIR",      "CVMFS_CONFIG_REPOSITORY", "LEVEL"};
    for (unsigned i = 0; i < sizeof(keys) / sizeof(keys[0]); ++i)
      unsetenv(keys[i]);

    tree_.fqrn = "cvmfs-config.cern.ch";
    tree_.revision = 42;
    tree_.root_hash = "abc";
    tree_.files["etc/cvmfs/common.conf"] =
        "if [ \"$CVMFS_HTTP_PROXY\" = \"\" ]; then\n"
        "  CVMFS_HTTP_PROXY=\"auto;DIRECT\"\n"
        "fi\n"
        "if [ \"$CVMFS_HTTP_PROXY\" = \"auto;DIRECT\" ]; then\n"
        "  CVMFS_USE_CDN=yes\n"
        "fi\n";
    tree_.files["etc/cvmfs/domain.d/cern.ch.conf"] =
        "CVMFS_HTTP_PROXY=\"$CVMFS_HTTP_PROXY\"\n"
        "CVMFS_USE_CDN=\"$CVMFS_USE_CDN\"\n"
        ". ../common.conf\n"
        "if [ \"$CVMFS_USE_CDN\" = \"yes\" ]; then\n"
        "  CVMFS_SERVER_URL=\"http://cdn/cvmfs/@fqrn@\"\n"
        "else\n"
        "  CVMFS_SERVER_URL=\"http://s1/cvmfs/@fqrn@\"\n"
        "fi\n"
        "CVMFS_KEYS_DIR=$CVMFS_MOUNT_DIR/$CVMFS_CONFIG_REPOSITORY/etc/cvmfs/"
        "keys/cern.ch\n";
    tree_.files
        ["etc/cvmfs/keys/opensciencegrid.org/opensciencegrid.org.pub"] = "osg";
    tree_.files["etc/cvmfs/keys/cern.ch/cern.ch.pub"] = "cern";
    tree_.files["etc/cvmfs/keys/cern.ch/README"] = "readme";
    tree_.symlinks
        ["etc/cvmfs/domain.d/igwn.org.conf"] = "opensciencegrid.org.conf";
    tree_.files["etc/cvmfs/domain.d/opensciencegrid.org.conf"] = "OSG=1\n";
    tree_.symlinks["etc/cvmfs/keys/osgstorage.org/opensciencegrid.org.pub"] =
        "../opensciencegrid.org/opensciencegrid.org.pub";
    tree_.symlinks["etc/cvmfs/escape"] = "../../../etc/passwd";
    tree_.symlinks["etc/cvmfs/absolute"] = "/etc/passwd";
    tree_.symlinks["etc/cvmfs/loop1"] = "loop2";
    tree_.symlinks["etc/cvmfs/loop2"] = "loop1";
  }

  ConfigRepoTree tree_;
};


TEST_F(T_ConfigRepo, Lookup) {
  string content;
  EXPECT_TRUE(tree_.Lookup("etc/cvmfs/common.conf", &content));
  EXPECT_TRUE(tree_.Lookup("etc/cvmfs/domain.d/../common.conf", &content));
  EXPECT_TRUE(tree_.Lookup("/etc//cvmfs/./common.conf", &content));
  EXPECT_FALSE(tree_.Lookup("etc/cvmfs/missing.conf", &content));
  EXPECT_FALSE(tree_.Lookup("etc/cvmfs", &content));
  EXPECT_FALSE(tree_.Lookup("../etc/cvmfs/common.conf", &content));

  EXPECT_TRUE(tree_.Lookup("etc/cvmfs/domain.d/igwn.org.conf", &content));
  EXPECT_EQ("OSG=1\n", content);
  EXPECT_TRUE(tree_.Lookup(
      "etc/cvmfs/keys/osgstorage.org/opensciencegrid.org.pub", &content));
  EXPECT_EQ("osg", content);

  EXPECT_FALSE(tree_.Lookup("etc/cvmfs/escape", &content));
  EXPECT_FALSE(tree_.Lookup("etc/cvmfs/absolute", &content));
  EXPECT_FALSE(tree_.Lookup("etc/cvmfs/loop1", &content));
}


TEST_F(T_ConfigRepo, DirectoryAndList) {
  EXPECT_TRUE(tree_.HasDirectory("etc/cvmfs"));
  EXPECT_TRUE(tree_.HasDirectory("etc"));
  EXPECT_FALSE(tree_.HasDirectory("etc/cvmfs/config.d"));
  EXPECT_FALSE(tree_.HasDirectory("etc/cvmfs/common.conf"));
  EXPECT_FALSE(ConfigRepoTree().HasDirectory("etc/cvmfs"));

  vector<string> names = tree_.List("etc/cvmfs/keys/cern.ch");
  ASSERT_EQ(2U, names.size());
  EXPECT_EQ("README", names[0]);
  EXPECT_EQ("cern.ch.pub", names[1]);
  names = tree_.List("etc/cvmfs/keys/osgstorage.org");
  ASSERT_EQ(1U, names.size());
  EXPECT_EQ("opensciencegrid.org.pub", names[0]);
  EXPECT_TRUE(tree_.List("etc/cvmfs/nothing").empty());
}


TEST_F(T_ConfigRepo, MapMountPath) {
  string rel;
  EXPECT_TRUE(tree_.MapMountPath(
      "/cvmfs", "/cvmfs/cvmfs-config.cern.ch/etc/cvmfs/keys/cern.ch", &rel));
  EXPECT_EQ("etc/cvmfs/keys/cern.ch", rel);
  EXPECT_TRUE(tree_.MapMountPath(
      "/cvmfs/", "/cvmfs/cvmfs-config.cern.ch/etc/cvmfs/keys/", &rel));
  EXPECT_EQ("etc/cvmfs/keys", rel);
  EXPECT_TRUE(
      tree_.MapMountPath("/cvmfs", "/cvmfs/cvmfs-config.cern.ch", &rel));
  EXPECT_EQ("", rel);
  EXPECT_FALSE(tree_.MapMountPath("/cvmfs", "/etc/cvmfs/keys", &rel));
  EXPECT_FALSE(tree_.MapMountPath(
      "/cvmfs", "/cvmfs/cvmfs-config.cern.chX/etc/cvmfs", &rel));
  EXPECT_FALSE(tree_.MapMountPath("", "/cvmfs/cvmfs-config.cern.ch", &rel));
}


TEST_F(T_ConfigRepo, Serialization) {
  const string buf = tree_.Serialize();
  ConfigRepoTree copy;
  ASSERT_TRUE(ConfigRepoTree::Deserialize(buf, &copy));
  EXPECT_EQ(tree_.fqrn, copy.fqrn);
  EXPECT_EQ(tree_.revision, copy.revision);
  EXPECT_EQ(tree_.root_hash, copy.root_hash);
  EXPECT_EQ(tree_.files, copy.files);
  EXPECT_EQ(tree_.symlinks, copy.symlinks);

  ConfigRepoTree broken;
  EXPECT_FALSE(ConfigRepoTree::Deserialize("", &broken));
  EXPECT_FALSE(
      ConfigRepoTree::Deserialize(buf.substr(0, buf.size() - 1), &broken));
  EXPECT_FALSE(ConfigRepoTree::Deserialize(buf + "x", &broken));
  string bad_magic = buf;
  bad_magic[0] = 'X';
  EXPECT_FALSE(ConfigRepoTree::Deserialize(bad_magic, &broken));

  ConfigRepoTree big;
  big.files["etc/cvmfs/big"] = string(ConfigRepoTree::kMaxFileSize + 1, 'x');
  EXPECT_FALSE(ConfigRepoTree::Deserialize(big.Serialize(), &broken));
}


TEST_F(T_ConfigRepo, InlineIncludes) {
  const string path = "etc/cvmfs/domain.d/cern.ch.conf";
  string content;
  ASSERT_TRUE(tree_.Lookup(path, &content));
  const string inlined = OptionsManager::InlineIncludes(tree_, "/cvmfs", path,
                                                        content);
  EXPECT_EQ(string::npos, inlined.find(". ../common.conf"));
  EXPECT_NE(string::npos, inlined.find("CVMFS_USE_CDN=yes"));

  // Prefix forms and absolute paths under the mount point
  const char *includes[] = {
      ". $CVMFS_MOUNT_DIR/$CVMFS_CONFIG_REPOSITORY/etc/cvmfs/common.conf",
      "source ${CVMFS_MOUNT_DIR}/${CVMFS_CONFIG_REPOSITORY}/etc/cvmfs/"
      "common.conf",
      ". /cvmfs/cvmfs-config.cern.ch/etc/cvmfs/common.conf",
      ". \"../common.conf\"  # comment"};
  for (unsigned i = 0; i < sizeof(includes) / sizeof(includes[0]); ++i) {
    const string result = OptionsManager::InlineIncludes(tree_, "/cvmfs", path,
                                                         includes[i]);
    EXPECT_NE(string::npos, result.find("CVMFS_USE_CDN=yes")) << includes[i];
  }

  // Left for the shell
  const char *passthrough[] = {". ../missing.conf", ". $SOMEWHERE/common.conf",
                               ". /etc/cvmfs/common.conf",
                               ". ../../../../common.conf",
                               "echo . ../common.conf"};
  for (unsigned i = 0; i < sizeof(passthrough) / sizeof(passthrough[0]); ++i) {
    EXPECT_EQ(passthrough[i], OptionsManager::InlineIncludes(
                                  tree_, "/cvmfs", path, passthrough[i]));
  }

  ConfigRepoTree loop;
  loop.files["etc/cvmfs/a.conf"] = ". b.conf\n";
  loop.files["etc/cvmfs/b.conf"] = ". a.conf\n";
  EXPECT_EQ("", OptionsManager::InlineIncludes(loop, "/cvmfs",
                                               "etc/cvmfs/a.conf", ". b.conf"));

  ConfigRepoTree deep;
  deep.files["etc/cvmfs/1.conf"] = ". 2.conf";
  deep.files["etc/cvmfs/2.conf"] = ". 3.conf";
  deep.files["etc/cvmfs/3.conf"] = ". 4.conf";
  deep.files["etc/cvmfs/4.conf"] = ". 5.conf";
  deep.files["etc/cvmfs/5.conf"] = "LEVEL=5";
  EXPECT_EQ("", OptionsManager::InlineIncludes(deep, "/cvmfs",
                                               "etc/cvmfs/0.conf", ". 1.conf"));
  EXPECT_EQ("LEVEL=5", OptionsManager::InlineIncludes(
                           deep, "/cvmfs", "etc/cvmfs/2.conf", ". 3.conf"));
}


// Parsing a config repository file from the tree must give the same result as
// parsing the same files from disk.
TEST_F(T_ConfigRepo, ParseBufferMatchesParsePath) {
  const string dir = CreateTempDir("./cvmfs_ut_config_repo");
  ASSERT_FALSE(dir.empty());
  const string abs_dir = GetAbsolutePath(dir);
  ASSERT_TRUE(MkdirDeep(abs_dir + "/etc/cvmfs/domain.d", 0700));
  for (map<string, string>::const_iterator i = tree_.files.begin();
       i != tree_.files.end();
       ++i) {
    if (i->first.find("keys") != string::npos)
      continue;
    ASSERT_TRUE(SafeWriteToFile(i->second, abs_dir + "/" + i->first, 0600));
  }
  const string path = "etc/cvmfs/domain.d/cern.ch.conf";

  const char *profiles[] = {"", "http://squid:3128"};
  for (unsigned p = 0; p < 2; ++p) {
    BashOptionsManager from_disk;
    BashOptionsManager from_tree;
    from_disk.set_taint_environment(false);
    from_tree.set_taint_environment(false);
    if (profiles[p][0] != '\0')
      setenv("CVMFS_HTTP_PROXY", profiles[p], 1);
    else
      unsetenv("CVMFS_HTTP_PROXY");
    setenv("CVMFS_MOUNT_DIR", "/cvmfs", 1);
    setenv("CVMFS_CONFIG_REPOSITORY", "cvmfs-config.cern.ch", 1);

    from_disk.ParsePath(abs_dir + "/" + path, false);
    string content;
    ASSERT_TRUE(tree_.Lookup(path, &content));
    from_tree.ParseBuffer(
        OptionsManager::InlineIncludes(tree_, "/cvmfs", path, content), content,
        "label");

    const char *params[] = {"CVMFS_HTTP_PROXY", "CVMFS_USE_CDN",
                            "CVMFS_SERVER_URL", "CVMFS_KEYS_DIR"};
    for (unsigned i = 0; i < sizeof(params) / sizeof(params[0]); ++i) {
      string disk_value;
      string tree_value;
      EXPECT_EQ(from_disk.GetValue(params[i], &disk_value),
                from_tree.GetValue(params[i], &tree_value))
          << params[i];
      EXPECT_EQ(disk_value, tree_value) << params[i];
    }
    string source;
    EXPECT_TRUE(from_tree.GetSource("CVMFS_SERVER_URL", &source));
    EXPECT_EQ("label", source);
  }
  string server_url;
  unsetenv("CVMFS_HTTP_PROXY");
  unsetenv("CVMFS_MOUNT_DIR");
  unsetenv("CVMFS_CONFIG_REPOSITORY");
}


TEST_F(T_ConfigRepo, SimpleParserBuffer) {
  SimpleOptionsParser options;
  options.set_taint_environment(false);
  options.ParseBuffer("IGNORED=1", "A=1\nB=\"2\"\n. ../common.conf\n", "lbl");
  string value;
  EXPECT_TRUE(options.GetValue("A", &value));
  EXPECT_EQ("1", value);
  EXPECT_TRUE(options.GetValue("B", &value));
  EXPECT_EQ("2", value);
  EXPECT_FALSE(options.GetValue("IGNORED", &value));
}


TEST_F(T_ConfigRepo, RewriteProxies) {
  struct {
    const char *proxies;
    const char *fallback;
    const char *exp_proxies;
    const char *exp_fallback;
  } cases[] = {
      {"auto;DIRECT", "", "DIRECT", ""},
      {"auto", "", "DIRECT", ""},
      {"", "", "DIRECT", ""},
      {"http://a:3128|http://b:3128;DIRECT", "",
       "http://a:3128|http://b:3128;DIRECT", ""},
      {"http://a:3128|auto", "", "http://a:3128;DIRECT", ""},
      {"http://a:3128", "http://fb:3126", "http://a:3128",
       "http://fb:3126;DIRECT"},
      {"AUTO;http://a:3128", "", "http://a:3128;DIRECT", ""},
      {"http://a:3128", "http://fb:3126;DIRECT", "http://a:3128",
       "http://fb:3126;DIRECT"},
  };
  for (unsigned i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
    string proxies = cases[i].proxies;
    string fallback = cases[i].fallback;
    config_repo::RewriteProxies(&proxies, &fallback);
    EXPECT_EQ(cases[i].exp_proxies, proxies) << i;
    EXPECT_EQ(cases[i].exp_fallback, fallback) << i;
  }
}


TEST_F(T_ConfigRepo, ParseFetchMode) {
  EXPECT_EQ(config_repo::kFetchMount, config_repo::ParseFetchMode("mount"));
  EXPECT_EQ(config_repo::kFetchDirect, config_repo::ParseFetchMode("Direct"));
  EXPECT_EQ(config_repo::kFetchDirectFallback,
            config_repo::ParseFetchMode("direct-fallback"));
  EXPECT_EQ(config_repo::kFetchDirectFallback,
            config_repo::ParseFetchMode("garbage"));
}


TEST_F(T_ConfigRepo, LoadPublicRsaKeysMem) {
  signature::SignatureManager generator;
  generator.Init();
  generator.GenerateMasterKeyPair();
  const string pem = generator.GetActivePubkeys();
  ASSERT_FALSE(pem.empty());

  signature::SignatureManager loader;
  loader.Init();
  vector<string> pems;
  pems.push_back(pem);
  pems.push_back(pem);
  EXPECT_TRUE(loader.LoadPublicRsaKeysMem(pems));
  EXPECT_EQ(pem + pem, loader.GetActivePubkeys());

  pems.push_back("garbage");
  EXPECT_FALSE(loader.LoadPublicRsaKeysMem(pems));
  EXPECT_TRUE(loader.LoadPublicRsaKeysMem(vector<string>()));
  EXPECT_EQ("", loader.GetActivePubkeys());
  loader.Fini();
  generator.Fini();
}


TEST_F(T_ConfigRepo, LoadBlacklistMem) {
  signature::SignatureManager sig;
  sig.Init();
  EXPECT_TRUE(sig.LoadBlacklistMem("AA:BB\n<repo 3\n", false));
  vector<string> blacklist = sig.GetBlacklist();
  ASSERT_EQ(2U, blacklist.size());
  EXPECT_EQ("AA:BB", blacklist[0]);
  EXPECT_EQ("<repo 3", blacklist[1]);

  EXPECT_TRUE(sig.LoadBlacklistMem("CC:DD", true));
  EXPECT_EQ(3U, sig.GetBlacklist().size());
  EXPECT_TRUE(sig.LoadBlacklistMem("", false));
  EXPECT_TRUE(sig.GetBlacklist().empty());
  sig.Fini();
}
