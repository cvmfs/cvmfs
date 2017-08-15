/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/param.h>
#include <sys/wait.h>
#include <unistd.h>

#include <string>

#include "cache_posix.h"
#include "cache_tiered.h"
#include "catalog_mgr_client.h"
#include "catalog_mgr_rw.h"
#include "compression.h"
#include "history_sqlite.h"
#include "manifest.h"
#include "mountpoint.h"
#include "options.h"
#include "signature.h"
#include "testutil.h"
#include "upload.h"
#include "upload_spooler_definition.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "uuid.h"

using namespace std;  // NOLINT

class T_MountPoint : public ::testing::Test {
 protected:
  virtual void SetUp() {
    uuid_dummy_ = cvmfs::Uuid::Create("");
    used_fds_ = GetNoUsedFds();
    fd_cwd_ = open(".", O_RDONLY);
    ASSERT_GE(fd_cwd_, 0);
    tmp_path_ = CreateTempDir("./cvmfs_ut_cache");
    options_mgr_.SetValue("CVMFS_CACHE_BASE", tmp_path_);
    options_mgr_.SetValue("CVMFS_SHARED_CACHE", "no");
    options_mgr_.SetValue("CVMFS_MAX_RETRIES", "0");
    fs_info_.name = "unit-test";
    fs_info_.options_mgr = &options_mgr_;
    // Silence syslog error
    options_mgr_.SetValue("CVMFS_MOUNT_DIR", "/no/such/dir");
  }

  virtual void TearDown() {
    delete uuid_dummy_;
    int retval = fchdir(fd_cwd_);
    ASSERT_EQ(0, retval);
    close(fd_cwd_);
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
    if (repo_path_ != "")
      RemoveTree(repo_path_);
    EXPECT_EQ(used_fds_, GetNoUsedFds()) << ShowOpenFiles();
  }

  void CreateMiniRepository() {
    char abs_path[MAXPATHLEN];
    ASSERT_TRUE(getcwd(abs_path, MAXPATHLEN) != NULL);
    repo_path_ =  string(abs_path) + "/repo";
    MakeCacheDirectories(repo_path_ + "/data", 0700);

    shash::Any hash_cert(shash::kSha1);
    CreateKeys(&hash_cert);
    CreateWhitelist();

    upload::SpoolerDefinition sd(
      "local," + repo_path_ + "/data/txn," + repo_path_,
      shash::kSha1);
    ASSERT_TRUE(sd.IsValid());
    UniquePtr<upload::Spooler> spooler(upload::Spooler::Construct(sd));
    ASSERT_TRUE(spooler.IsValid());

    const bool volatile_content = false;
    const string voms_authz;
    UniquePtr<manifest::Manifest> manifest(
      catalog::WritableCatalogManager::CreateRepository(
        repo_path_ + "/data/txn",
        volatile_content,
        voms_authz,
        spooler.weak_ref()));
    ASSERT_TRUE(manifest.IsValid());

    shash::Any history_hash(shash::kSha1);
    CreateHistory(manifest, &history_hash);

    manifest->set_certificate(hash_cert);
    manifest->set_history(history_hash);
    manifest->set_repository_name("keys.cern.ch");
    manifest->set_publish_timestamp(time(NULL));
    CreateManifest(manifest.weak_ref());

    options_mgr_.SetValue("CVMFS_ROOT_HASH",
                          manifest->catalog_hash().ToString());
    options_mgr_.SetValue("CVMFS_SERVER_URL", "file://" + repo_path_);
    options_mgr_.SetValue("CVMFS_HTTP_PROXY", "DIRECT");
  }


  void CreateHistory(manifest::Manifest *manifest, shash::Any *history_hash) {
    {
      UniquePtr<history::SqliteHistory> history(
        history::SqliteHistory::Create(repo_path_ + "/history",
                                       "keys.cern.ch"));
      ASSERT_TRUE(history.IsValid());
      history::History::Tag tag;
      tag.name = "snapshot";
      tag.root_hash = manifest->catalog_hash();
      tag.timestamp = time(NULL);
      ASSERT_TRUE(history->Insert(tag));
    }
    history_hash->suffix = shash::kSuffixHistory;
    ASSERT_TRUE(zlib::CompressPath2Null(repo_path_ + "/history", history_hash));
    ASSERT_TRUE(
      zlib::CompressPath2Path(
        repo_path_ + "/history",
        repo_path_ + "/data/" + history_hash->MakePath()));
  }


  void CreateManifest(manifest::Manifest *manifest) {
    UniquePtr<signature::SignatureManager> signature_mgr(
      new signature::SignatureManager());
    signature_mgr->Init();
    ASSERT_TRUE(
      signature_mgr->LoadCertificatePath(repo_path_ + "/testrepo.crt"));
    ASSERT_TRUE(
      signature_mgr->LoadPrivateKeyPath(repo_path_ + "/testrepo.key", ""));
    ASSERT_TRUE(signature_mgr->KeysMatch());

    string signed_manifest = manifest->ExportString();
    shash::Any published_hash(manifest->GetHashAlgorithm());
    shash::HashMem(
      reinterpret_cast<const unsigned char *>(signed_manifest.data()),
      signed_manifest.length(), &published_hash);
    signed_manifest += "--\n" + published_hash.ToString() + "\n";
    unsigned char *sig;
    unsigned sig_size;
    ASSERT_TRUE(
      signature_mgr->Sign(reinterpret_cast<const unsigned char *>(
                            published_hash.ToString().data()),
                          published_hash.GetHexSize(),
                          &sig, &sig_size));
    signed_manifest += string(reinterpret_cast<char *>(sig), sig_size);
    free(sig);
    ASSERT_TRUE(
      SafeWriteToFile(signed_manifest, repo_path_ + "/.cvmfspublished", 0600));
    signature_mgr->Fini();
  }


  void CreateWhitelist() {
    // valid for 128 years as of 2016
    string whitelist_b64 =
      "MjAxNjA2MTcxNjA1MzkKRTIxNDQwNjE3MTYwNTM5Ck5rZXlzLmNlcm4uY2gK"
      "MDA6N0M6RkE6RUU6MUE6MkI6OTg6NzQ6NUQ6MTQ6QTY6MjU6NEU6QzQ6NDA6"
      "QkM6QkQ6NDQ6NDc6QTMKLS0KNWJlYWQ2MzBjMzAxZjdjZTc3MmY2NGJlOWVm"
      "MDFlMjhkNmFhY2E2NgpkM7pmYusuNTNC7XBhQzlAy4wMo1sqrv9EnjcCI6xg"
      "B8YHEvILtHMpB4qK1NyI2We9zuXgRe5MVwDkIEGMvRedCgiStPMD3aCFT730"
      "yv/b5qltYuwlwnjdezOwSAvj6BJ9ITSaW6wT1IA5BtqhBv0I8cloWvV0CfyI"
      "m+pnebb/yyu8hIsOH0SdRhZsFx23Eml50FrzvwaavbDVQHtU46YbqlqgGwFy"
      "QJE0X7lljrbtAjJHOAxurnDyhENnna6tWxwedpMOYEwwEoqF20plHqawSZbL"
      "oDjuHCEu2TrGkj+CguUT/XPSbTLMVjg+yMsi23e+a+P9ipOhOaEL4mk/LqPx";
    string wl;
    ASSERT_TRUE(Debase64(whitelist_b64, &wl));
    EXPECT_TRUE(SafeWriteToFile(wl, repo_path_ + "/.cvmfswhitelist", 0600));
  }


  void CreateKeys(shash::Any *hash_cert) {
    // Key material for a repo named "keys.cern.ch"
    string pubkey = "-----BEGIN PUBLIC KEY-----\n"
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6eJmVLlzDanGoZjqDf/M\n"
      "tcrds7mrHhRSBWLHzqucsPVLi8+zl7WRfjtb+SEe4xvSkd3mdKKPzew4s7tOic5m\n"
      "D9sl9wKpU6AfMpTfuOEZvcWDFh5lsAeNldE+LViHCibHoj2WIEAI+HZkoNAFlg+c\n"
      "ZDyKXxg+Xk2ZPwjLKGX6rwWEvlDebj0q57mI8nZ8tXogu51FFy3fcTndm+DWt+D5\n"
      "7GN1fvFEHbvncrqKSzbgnTVgTwMueoRK5H3I6HuW+DfAUADsQgLbm5PpcgZZNga5\n"
      "qNJQyg+ozaX/3SfWaE2z5sweU1wMNll3fjs3CwIRQPLY0d5g/187z6T1mpiuz6hm\n"
      "IQIDAQAB\n"
      "-----END PUBLIC KEY-----\n";
    string masterkey = "-----BEGIN RSA PRIVATE KEY-----\n"
      "MIIEogIBAAKCAQEA6eJmVLlzDanGoZjqDf/Mtcrds7mrHhRSBWLHzqucsPVLi8+z\n"
      "l7WRfjtb+SEe4xvSkd3mdKKPzew4s7tOic5mD9sl9wKpU6AfMpTfuOEZvcWDFh5l\n"
      "sAeNldE+LViHCibHoj2WIEAI+HZkoNAFlg+cZDyKXxg+Xk2ZPwjLKGX6rwWEvlDe\n"
      "bj0q57mI8nZ8tXogu51FFy3fcTndm+DWt+D57GN1fvFEHbvncrqKSzbgnTVgTwMu\n"
      "eoRK5H3I6HuW+DfAUADsQgLbm5PpcgZZNga5qNJQyg+ozaX/3SfWaE2z5sweU1wM\n"
      "Nll3fjs3CwIRQPLY0d5g/187z6T1mpiuz6hmIQIDAQABAoIBADnFTnmHBUBOu12X\n"
      "I9kpYitVXMXUCsx3QHtMFwaZpS6gqHR0bWv/0VxY1TMIV1TJvo2BPjd5IARBYRAk\n"
      "KBYqAVPRUeNdqO2bE5mu5EQKdg1GCEciYwPEGdjzwmP5BgIf6hfNFpQIvS6CMAD4\n"
      "4Shb2sl3msY6es1YZY4IYgYsimtIfMmVPv0awRX9xJ0cQWZP5Feo09jguY02xPim\n"
      "7JzkGBKazaKFKw7tHoNwfWt312oSdXjmWicUbdDljyrM8olLYwgpoz3ngwYSdKDZ\n"
      "Lcw8b1BXbMj2UERZQMCzIR7j340mp/cUeNSEDqErKwSm+LjPUfTj6XKO1hIvO1kv\n"
      "u1ZS7rECgYEA93BOxUcCa03dYzK+wLrnbVB9SeMXLWOGxJrxF7wid9Ju4WSilWsi\n"
      "BLUzAOSkTVZ7PKIBf2HTnTRYt+B+KuVkb+mnU6/6I/zgp/XsDXY+7sH4bAvBZZ3M\n"
      "Ry/O05lk3sFOKu8rWE1yn3k2XDI1CcDNK5dQ4X0AOvKJynW2c6bIRbsCgYEA8foI\n"
      "y1+gfm9OaUL2MTdXo+16gmtdscUTZzK4oSEN8YRe+PnwV3cS34FjVxy7ZMCX3IWp\n"
      "5gs9KFJof0gxlnu86oAVux2pT7oxiWsvQVpIBWbnGKvJJp20EPPhfX6Ox5fjdcye\n"
      "xVpeyseReL0QH3p0Ej8T4bU5m9OY1d4M+/83d9MCgYBTtyafbjfuUAjQEBIjqNi1\n"
      "zl6lSfTEgYDOMdHSAu/ydDrZfS/Yt8dpqliYO8Mu+0x0pic1jsaG0HgXthdZsgS6\n"
      "LGZVVRufY2YqzXRQ1anTI8NF4vBKzgmYKB+kzagoCWTF9+dFV+ao99yhcscpBpcj\n"
      "4W0W7TDPwNFHs23IUSw/EwKBgCBYA4Trq1A7IIgBY1cAxr4qqA12vHdemFFa/kLL\n"
      "YEnAH9G31uBaEjO938FtHb9B3wqi8yrEpdAV89HPnJE4yO+vXzg7pr35bVWo9hAO\n"
      "OUI/lvQ9Qg3fVopNjv5vRDZ5nvXH/BD1G2aPdmplGxqaC5nExKuOxbyGdA9iNuoY\n"
      "GxnxAoGAPuh+WTXKGsEychIHlP1XlFdG7TB+18WbsS3RRIlAJ3VKka57NVt6xwhb\n"
      "G4CaGObEcJYETvbx+H7qpQIPE9ERIZj1PDTSMN6a8q7JEO8+ECA+RpWqgk43hbQK\n"
      "v1d1Bc3aLfXayoHjTSFYZ9v3X0ZNLDNH9nIosAY388c5C0lIsco=\n"
      "-----END RSA PRIVATE KEY-----\n";
    string certificate = "-----BEGIN CERTIFICATE-----\n"
      "MIIC4DCCAcgCCQCbl8VGkUwDtDANBgkqhkiG9w0BAQsFADAyMTAwLgYDVQQDDCdr\n"
      "ZXlzLmNlcm4uY2ggQ2VyblZNLUZTIFJlbGVhc2UgTWFuYWdlcnMwHhcNMTYwNjE3\n"
      "MTUyMzQwWhcNMTcwNjE3MTUyMzQwWjAyMTAwLgYDVQQDDCdrZXlzLmNlcm4uY2gg\n"
      "Q2VyblZNLUZTIFJlbGVhc2UgTWFuYWdlcnMwggEiMA0GCSqGSIb3DQEBAQUAA4IB\n"
      "DwAwggEKAoIBAQCuSd7oeR4t8KbWCBzGysKks324Dap/gyQKE8KHSrDnnFo8WBTh\n"
      "sIiVZmZbW5pKZE/qpW7Muz0DVrgjRdwhdJUO5DuUGLH7eX7n2a1rNoC76RSd0SR1\n"
      "vejzFwO2+9laQuXFPWbzL1Ja4FHDZmLNrHntqPHKiLUw/7q8fpSMYTHA6kJC98fk\n"
      "Ck6riiTjEA/Gob22tXNqozyM8uKAZ4hSbN85odQb/Zsn5vgj0ZFcXKYV8wtMc4He\n"
      "5Onz0sSTqUbgnRMqIkdA3l67aPAICiAvLCwxfZD1sgwe0dKm/1ou9pakSWbLntZa\n"
      "8YsTE4un3aWZGqJGCp2+b+QAZuQb/5MdzWw9AgMBAAEwDQYJKoZIhvcNAQELBQAD\n"
      "ggEBAFZL6rdEp/89Th68KQgdVVx9USiSjIpJzUYEMN0psBqoLmcF35bd784K8iPg\n"
      "dRwfHKU0LvKAABl1od8nKNSuFuQ70kL2nY0fZQ0cTt14MPcot1PVxARmyL9delzk\n"
      "VVbGVkBn7u3nptIKm4CU8aAft4KBBhbGuPhfXLkRGDGrZv0IG0KCYXPfGnYzl3rF\n"
      "ugCiRjoqZcvUVQg1l2J2yuHhZ12iGaLHPpccmWZvpRVzpaS+XbDjPCAn75DCOaqR\n"
      "dtXFO0AqtWj+4jXvKQ6RoZAU0opX3K7h5qrYeh2lkI9XlyxKD7lBmIZmNf7brXpW\n"
      "nFvQm3OjNT9ZRG9T712hiQ/chdc=\n"
      "-----END CERTIFICATE-----\n";
    string key = "-----BEGIN RSA PRIVATE KEY-----\n"
      "MIIEpgIBAAKCAQEArkne6HkeLfCm1ggcxsrCpLN9uA2qf4MkChPCh0qw55xaPFgU\n"
      "4bCIlWZmW1uaSmRP6qVuzLs9A1a4I0XcIXSVDuQ7lBix+3l+59mtazaAu+kUndEk\n"
      "db3o8xcDtvvZWkLlxT1m8y9SWuBRw2Zizax57ajxyoi1MP+6vH6UjGExwOpCQvfH\n"
      "5ApOq4ok4xAPxqG9trVzaqM8jPLigGeIUmzfOaHUG/2bJ+b4I9GRXFymFfMLTHOB\n"
      "3uTp89LEk6lG4J0TKiJHQN5eu2jwCAogLywsMX2Q9bIMHtHSpv9aLvaWpElmy57W\n"
      "WvGLExOLp92lmRqiRgqdvm/kAGbkG/+THc1sPQIDAQABAoIBAQCQRlBC6vgjmWHS\n"
      "LVb87J2hz3+Tm6R2960etmrCqf61S8WazGNEzGjUG7dBixu210EcgaOt0JVaLTAy\n"
      "6sKl4yb888up9aNoA5QdAyG+bZi1dOV/GsDuwq2ShYuqruKnCFfCJekSCCtJVQX6\n"
      "FchWb59jMAYv3Wj4Tclb/gCkEFUqVqORcEXzwOvqcFXqBagenTKXEqotGcjsSTSY\n"
      "VAbQJFlJdJ7CayOpG9uJ33AxnaCSgbYr2bTWUJ1FqtqqCqO0PZgpg0emEyxGZqE/\n"
      "UlTytFNianHhbGwrzsoXCN1Q3RK5ExN3/RznH8zw7rCXOdfmUNaUiNm9nmUX4sTh\n"
      "3ieO+/7hAoGBANZMwh5lNf3FZluF68g5al3x36gRs/PZDEQfDhPqIyYXV9j8uIuq\n"
      "/Uiv/5xJQIbPFqF25ffroCYqO/mDJpU1FY/hWnW/3cCi1H41xiG9DUOqOx2gqE+6\n"
      "kzBlehTiunNIhmSglK9Ev5LTMXXm7Z/fCE0s1WOTLpO1d/OU1yi/hp4JAoGBANAz\n"
      "+FlKLDvRgNXG/vs7q0pmnkRWtlsvm8Y0WROYdPDSTj7oWk8VBp43RL3zBKGNkPYR\n"
      "3Bz6j89XrSsRsYnwMPkEnYN1OGunaylTmI7zG6gZVyoog8CDxW1RBHITafbjQPl2\n"
      "LKqi8JJL16PJEh17Bl2y99zQiRG155a4qab1BCmVAoGBAMt0HGfXFydTHhaOUofJ\n"
      "Wt7OH9Tk2cAMtMSH50mo5K3pQ5HSfTK8p7M2xKqQMR7LxWSOCU8S+PzC5CXDCgJm\n"
      "X442GTfpbJLTBIK+ctjdL5aqK225dZIcRFmSPhFOIE4K8OzgN8ker/KpZy/Uio1Z\n"
      "pfv/MKhUt8esZbFwAcXB8ABhAoGBALWkIY8EvwKRDK11Jw9YR2BplrpYTE/RgT2y\n"
      "feQyphNT5x/K5r8HwPZXkYmGcwvezhFgE4DUuJJUE6f3j8Sf4JngBOujYM3LChrL\n"
      "69ULE53cPcdyAT/7tkpg3FgJx/C04wLArsdP0EJSGJez3DIMGsm0Ubo71Nm2sY01\n"
      "Hg2ixTbhAoGBAJ6dUGDa5d+CtsQ7CwEgwcaB7+gk8qn7iKCCmeYe7Ja7nYqT97gC\n"
      "WSTDqjRX/AE7vQ3UysvJS9yzRakZiINv03rZXmZv3ft5uDzbqPhBjsX9Nnw3Emua\n"
      "OO+VjREFgdl5q7TLvm1ERXRTHdIWXv9zwC1ybtZUbuGQ42WKWfGwsNHN\n"
      "-----END RSA PRIVATE KEY-----\n";
    ASSERT_TRUE(SafeWriteToFile(pubkey, repo_path_ + "/testrepo.pub", 0600));
    ASSERT_TRUE(SafeWriteToFile(masterkey,
                                repo_path_ + "/testrepo.masterkey", 0600));
    ASSERT_TRUE(SafeWriteToFile(certificate,
                                repo_path_ + "/testrepo.crt", 0600));
    ASSERT_TRUE(SafeWriteToFile(key, repo_path_ + "/testrepo.key", 0600));

    hash_cert->suffix = shash::kSuffixCertificate;
    ASSERT_TRUE(
      zlib::CompressPath2Null(repo_path_ + "/testrepo.crt", hash_cert));
    ASSERT_TRUE(
      zlib::CompressPath2Path(repo_path_ + "/testrepo.crt",
                              repo_path_ + "/data/" + hash_cert->MakePath()));

    options_mgr_.SetValue("CVMFS_PUBLIC_KEY",
                          repo_path_ + string("/testrepo.pub"));
  }

 protected:
  FileSystem::FileSystemInfo fs_info_;
  SimpleOptionsParser options_mgr_;
  string tmp_path_;
  string repo_path_;
  int fd_cwd_;
  unsigned used_fds_;
  /**
   * Initialize libuuid / open file descriptor on /dev/urandom
   */
  cvmfs::Uuid *uuid_dummy_;
};


TEST_F(T_MountPoint, CreateBasic) {
  FileSystem *file_system = FileSystem::Create(fs_info_);
  ASSERT_TRUE(file_system != NULL);
  EXPECT_TRUE(file_system->IsValid());
  delete file_system;

  // Cached
  file_system = FileSystem::Create(fs_info_);
  ASSERT_TRUE(file_system != NULL);
  EXPECT_TRUE(file_system->IsValid());
  delete file_system;
}


TEST_F(T_MountPoint, MkCacheParm) {
  FileSystem *file_system = FileSystem::Create(fs_info_);
  ASSERT_TRUE(file_system != NULL);

  file_system->cache_mgr_instance_ = "ceph";
  EXPECT_EQ("CVMFS_CACHE_ceph_TYPE",
            file_system->MkCacheParm("CVMFS_CACHE_TYPE", "ceph"));
  file_system->cache_mgr_instance_ = file_system->kDefaultCacheMgrInstance;
  EXPECT_EQ("CVMFS_CACHE_TYPE",
            file_system->MkCacheParm("CVMFS_CACHE_TYPE", "default"));
  delete file_system;
}


TEST_F(T_MountPoint, CheckInstanceName) {
  FileSystem *fs = FileSystem::Create(fs_info_);
  ASSERT_TRUE(fs != NULL);

  EXPECT_TRUE(fs->CheckInstanceName("ceph"));
  EXPECT_TRUE(fs->CheckInstanceName("cephCache_01"));
  EXPECT_FALSE(fs->CheckInstanceName("ceph cache 01"));
  EXPECT_FALSE(fs->CheckInstanceName("aNameThatIsLongerThanItShouldBe"));
  delete fs;
}


TEST_F(T_MountPoint, CheckPosixCacheSettings) {
  FileSystem *fs = FileSystem::Create(fs_info_);
  ASSERT_TRUE(fs != NULL);

  FileSystem::PosixCacheSettings settings;
  EXPECT_TRUE(fs->CheckPosixCacheSettings(settings));
  settings.is_alien = true;
  EXPECT_TRUE(fs->CheckPosixCacheSettings(settings));
  settings.is_shared = true;
  EXPECT_FALSE(fs->CheckPosixCacheSettings(settings));
  settings.is_shared = false;
  settings.is_managed = true;
  EXPECT_FALSE(fs->CheckPosixCacheSettings(settings));
  settings.is_alien = false;
  settings.is_shared = true;
  EXPECT_TRUE(fs->CheckPosixCacheSettings(settings));
  fs->type_ = FileSystem::kFsLibrary;
  EXPECT_FALSE(fs->CheckPosixCacheSettings(settings));
  fs->type_ = FileSystem::kFsFuse;
  settings.cache_base_defined = true;
  EXPECT_TRUE(fs->CheckPosixCacheSettings(settings));
  settings.cache_dir_defined = true;
  EXPECT_FALSE(fs->CheckPosixCacheSettings(settings));
  delete fs;
}


TEST_F(T_MountPoint, TriageCacheMgr) {
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ("default", fs->cache_mgr_instance());
  }
  options_mgr_.SetValue("CVMFS_CACHE_PRIMARY", "string with spaces");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailCacheDir, fs->boot_status());
  }
  options_mgr_.SetValue("CVMFS_CACHE_PRIMARY", "foo");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailCacheDir, fs->boot_status());
  }
  options_mgr_.SetValue("CVMFS_CACHE_PRIMARY", "default");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ("default", fs->cache_mgr_instance());
  }
}


TEST_F(T_MountPoint, RamCacheMgr) {
  options_mgr_.SetValue("CVMFS_CACHE_PRIMARY", "ram");
  options_mgr_.SetValue("CVMFS_CACHE_ram_TYPE", "ram");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ("ram", fs->cache_mgr_instance());
    EXPECT_EQ(kRamCacheManager, fs->cache_mgr()->id());
  }
  options_mgr_.SetValue("CVMFS_CACHE_ram_MALLOC", "unknown");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }
  options_mgr_.SetValue("CVMFS_CACHE_ram_MALLOC", "libc");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
  }
}


TEST_F(T_MountPoint, TieredCacheMgr) {
  options_mgr_.SetValue("CVMFS_CACHE_PRIMARY", "tiered");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_TYPE", "tiered");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }
  options_mgr_.SetValue("CVMFS_CACHE_tiered_UPPER", "ram_upper");
  options_mgr_.SetValue("CVMFS_CACHE_ram_upper_TYPE", "ram");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }
  options_mgr_.SetValue("CVMFS_CACHE_tiered_LOWER", "posix_lower");
  options_mgr_.SetValue("CVMFS_CACHE_posix_lower_TYPE", "posix");
  options_mgr_.SetValue("CVMFS_CACHE_posix_lower_BASE", tmp_path_);
  options_mgr_.SetValue("CVMFS_CACHE_posix_lower_SHARED", "false");
  options_mgr_.SetValue("CVMFS_CACHE_posix_lower_QUOTA_LIMIT", "0");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ("tiered", fs->cache_mgr_instance());
    EXPECT_EQ(kTieredCacheManager, fs->cache_mgr()->id());
    EXPECT_EQ(kRamCacheManager, reinterpret_cast<TieredCacheManager *>(
      fs->cache_mgr())->upper_->id());
    EXPECT_EQ(kPosixCacheManager, reinterpret_cast<TieredCacheManager *>(
      fs->cache_mgr())->lower_->id());
  }

  options_mgr_.SetValue("CVMFS_CACHE_tiered_LOWER", "ram_lower");
  options_mgr_.SetValue("CVMFS_CACHE_ram_lower_TYPE", "ram");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ("tiered", fs->cache_mgr_instance());
    EXPECT_EQ(kTieredCacheManager, fs->cache_mgr()->id());
  }

  options_mgr_.SetValue("CVMFS_CACHE_tiered_LOWER", "tiered");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailCacheDir, fs->boot_status());
  }
}


TEST_F(T_MountPoint, TieredComplex) {
  options_mgr_.SetValue("CVMFS_CACHE_PRIMARY", "tiered");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_TYPE", "tiered");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_UPPER", "tiered_upper");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_upper_TYPE", "tiered");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_upper_UPPER", "uu_ram");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_upper_LOWER", "ul_ram");
  options_mgr_.SetValue("CVMFS_CACHE_uu_ram_TYPE", "ram");
  options_mgr_.SetValue("CVMFS_CACHE_ul_ram_TYPE", "ram");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_LOWER", "tiered_lower");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_lower_TYPE", "tiered");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_lower_UPPER", "lu_posix");
  options_mgr_.SetValue("CVMFS_CACHE_tiered_lower_LOWER", "ll_posix");
  options_mgr_.SetValue("CVMFS_CACHE_lu_posix_TYPE", "posix");
  options_mgr_.SetValue("CVMFS_CACHE_lu_posix_BASE", tmp_path_ + "/lu");
  options_mgr_.SetValue("CVMFS_CACHE_lu_posix_SHARED", "false");
  options_mgr_.SetValue("CVMFS_CACHE_lu_posix_QUOTA_LIMIT", "0");
  options_mgr_.SetValue("CVMFS_CACHE_ll_posix_TYPE", "posix");
  options_mgr_.SetValue("CVMFS_CACHE_ll_posix_BASE", tmp_path_ + "/ll");
  options_mgr_.SetValue("CVMFS_CACHE_ll_posix_SHARED", "false");
  options_mgr_.SetValue("CVMFS_CACHE_ll_posix_QUOTA_LIMIT", "0");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status()) << fs->boot_error();
    EXPECT_EQ("tiered", fs->cache_mgr_instance());
    EXPECT_EQ(kTieredCacheManager, fs->cache_mgr()->id());
    TieredCacheManager *upper = reinterpret_cast<TieredCacheManager *>(
      reinterpret_cast<TieredCacheManager *>(fs->cache_mgr())->upper_);
    TieredCacheManager *lower = reinterpret_cast<TieredCacheManager *>(
      reinterpret_cast<TieredCacheManager *>(fs->cache_mgr())->lower_);
    EXPECT_EQ(kRamCacheManager, upper->upper_->id());
    EXPECT_EQ(kRamCacheManager, upper->lower_->id());
    EXPECT_EQ(kPosixCacheManager, lower->upper_->id());
    EXPECT_EQ(kPosixCacheManager, lower->lower_->id());
  }
}


TEST_F(T_MountPoint, CacheSettings) {
  options_mgr_.SetValue("CVMFS_ALIEN_CACHE", tmp_path_ + "/alien");
  options_mgr_.SetValue("CVMFS_QUOTA_LIMIT", "-1");
  options_mgr_.SetValue("CVMFS_SHARED_CACHE", "yes");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }

  options_mgr_.UnsetValue("CVMFS_SHARED_CACHE");
  options_mgr_.SetValue("CVMFS_QUOTA_LIMIT", "10");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }

  options_mgr_.SetValue("CVMFS_QUOTA_LIMIT", "-1");
  options_mgr_.SetValue("CVMFS_NFS_SOURCE", "yes");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
  }
  options_mgr_.UnsetValue("CVMFS_NFS_SOURCE");

  fs_info_.type = FileSystem::kFsLibrary;
  options_mgr_.SetValue("CVMFS_SHARED_CACHE", "yes");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }

  options_mgr_.UnsetValue("CVMFS_SHARED_CACHE");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ(tmp_path_ + "/unit-test", fs->workspace());
    EXPECT_EQ(tmp_path_ + "/alien",
              reinterpret_cast<PosixCacheManager *>(
                fs->cache_mgr())->cache_path());
  }

  fs_info_.type = FileSystem::kFsFuse;
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_EQ(".", fs->workspace());
    EXPECT_EQ(tmp_path_ + "/alien",
              reinterpret_cast<PosixCacheManager *>(
                fs->cache_mgr())->cache_path());
  }

  RemoveTree(tmp_path_ + "/unit-test");
  options_mgr_.UnsetValue("CVMFS_ALIEN_CACHE");
  options_mgr_.SetValue("CVMFS_NFS_SOURCE", "yes");
  options_mgr_.SetValue("CVMFS_NFS_SHARED", tmp_path_ + "/nfs");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_TRUE(fs->IsNfsSource());
    EXPECT_TRUE(fs->IsHaNfsSource());
    EXPECT_EQ(".", reinterpret_cast<PosixCacheManager *>(
      fs->cache_mgr())->cache_path());
    EXPECT_EQ(".", fs->workspace());
    EXPECT_EQ(tmp_path_ + "/nfs", fs->nfs_maps_dir_);
  }

  options_mgr_.SetValue("CVMFS_CACHE_DIR", tmp_path_ + "/cachedir_direct");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOptions, fs->boot_status());
  }
  options_mgr_.UnsetValue("CVMFS_CACHE_BASE");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
  }
}


TEST_F(T_MountPoint, SharedDirectory) {
  // Same directory, different file system name
  pid_t pid;
  switch (pid = fork()) {
    case -1:
      abort();
    case 0:
      fs_info_.name = "other";
      UniquePtr<FileSystem> fs01(FileSystem::Create(fs_info_));
      int retval = fs01->IsValid() ? 0 : 1;
      exit(retval);
  }

  UniquePtr<FileSystem> fs01(FileSystem::Create(fs_info_));
  EXPECT_TRUE(fs01->IsValid());
  int stat_loc;
  int retval = waitpid(pid, &stat_loc, 0);
  EXPECT_NE(retval, -1);
  EXPECT_TRUE(WIFEXITED(stat_loc));
  EXPECT_EQ(0, WEXITSTATUS(stat_loc));
}


TEST_F(T_MountPoint, CrashGuard) {
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailOk, fs->boot_status());
    EXPECT_FALSE(fs->found_previous_crash());
  }

  MkdirDeep(tmp_path_ + "/unit-test", 0700, false);
  int fd = open((tmp_path_ + string("/unit-test/running.unit-test")).c_str(),
                O_RDWR | O_CREAT, 0600);
  ASSERT_GE(fd, 0);
  close(fd);

  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  EXPECT_EQ(loader::kFailOk, fs->boot_status());
  EXPECT_TRUE(fs->found_previous_crash());
}


TEST_F(T_MountPoint, LockWorkspace) {
  pid_t pid;
  switch (pid = fork()) {
    case -1:
      abort();
    case 0:
      UniquePtr<FileSystem> fs01(FileSystem::Create(fs_info_));
      switch (fs01->boot_status()) {
        case loader::kFailOk:
          exit(0);
        case loader::kFailLockWorkspace:
          exit(1);
        default:
          exit(2);
      }
  }

  UniquePtr<FileSystem> fs01(FileSystem::Create(fs_info_));
  int stat_loc;
  int retval = waitpid(pid, &stat_loc, 0);
  EXPECT_NE(retval, -1);
  EXPECT_TRUE(WIFEXITED(stat_loc));
  EXPECT_TRUE(((fs01->boot_status() == 0) && (WEXITSTATUS(stat_loc) == 1)) ||
              ((fs01->boot_status() == loader::kFailLockWorkspace) &&
                  (WEXITSTATUS(stat_loc) == 0)));
}


TEST_F(T_MountPoint, UuidCache) {
  string cached_uuid;
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    ASSERT_EQ(loader::kFailOk, fs->boot_status());
    cached_uuid = fs->uuid_cache()->uuid();
  }
  ASSERT_EQ(fchdir(fd_cwd_), 0);

  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());
  EXPECT_EQ(cached_uuid, fs->uuid_cache()->uuid());
}


TEST_F(T_MountPoint, QuotaMgr) {
  // Fails because the unit test binary cannot become a quota manager process
  options_mgr_.SetValue("CVMFS_SHARED_CACHE", "yes");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    EXPECT_EQ(loader::kFailQuota, fs->boot_status());
  }
}


TEST_F(T_MountPoint, MountLatest) {
  CreateMiniRepository();
  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());
  string root_hash;
  EXPECT_TRUE(options_mgr_.GetValue("CVMFS_ROOT_HASH", &root_hash));
  options_mgr_.UnsetValue("CVMFS_ROOT_HASH");

  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
    EXPECT_EQ(root_hash, mp->catalog_mgr()->GetRootHash().ToString());
  }

  // Again to check proper cleanup
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
  }
}


TEST_F(T_MountPoint, MountMulti) {
  CreateMiniRepository();
  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());

  UniquePtr<MountPoint> mp01(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
  EXPECT_EQ(loader::kFailOk, mp01->boot_status());

  UniquePtr<MountPoint> mp02(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
  EXPECT_EQ(loader::kFailOk, mp02->boot_status());
}


TEST_F(T_MountPoint, MountErrors) {
  CreateMiniRepository();
  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("test", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
    string root_hash;
    EXPECT_TRUE(options_mgr_.GetValue("CVMFS_ROOT_HASH", &root_hash));
    EXPECT_EQ(root_hash, mp->catalog_mgr()->GetRootHash().ToString());
  }

  options_mgr_.UnsetValue("CVMFS_ROOT_HASH");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("wrong.name", fs.weak_ref()));
    EXPECT_EQ(loader::kFailCatalog, mp->boot_status());
  }

  options_mgr_.SetValue("CVMFS_UID_MAP", "/no/such/file");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("test", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOptions, mp->boot_status());
  }

  options_mgr_.UnsetValue("CVMFS_HTTP_PROXY");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("test", fs.weak_ref()));
    EXPECT_EQ(loader::kFailWpad, mp->boot_status());
  }

  options_mgr_.SetValue("CVMFS_PUBLIC_KEY", "/no/such/key");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("test", fs.weak_ref()));
    EXPECT_EQ(loader::kFailSignature, mp->boot_status());
  }
}


TEST_F(T_MountPoint, Blacklist) {
  CreateMiniRepository();
  EXPECT_TRUE(MkdirDeep(repo_path_ + "/config.test/etc/cvmfs", 0700, true));
  options_mgr_.SetValue("CVMFS_MOUNT_DIR", repo_path_);
  options_mgr_.SetValue("CVMFS_CONFIG_REPOSITORY", "config.test");
  options_mgr_.UnsetValue("CVMFS_ROOT_HASH");

  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    ASSERT_EQ(loader::kFailOk, fs->boot_status());
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
    EXPECT_TRUE(mp->ReloadBlacklists());
  }
  RemoveTree("cvmfs_ut_cache");

  string bad_revision = "<keys.cern.ch 1000";
  EXPECT_TRUE(SafeWriteToFile(bad_revision,
                              repo_path_ + "/config.test/etc/cvmfs/blacklist",
                              0600));
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    ASSERT_EQ(loader::kFailOk, fs->boot_status());
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailRevisionBlacklisted, mp->boot_status());
  }

  string bad_fingerprint =
    "00:7C:FA:EE:1A:2B:98:74:5D:14:A6:25:4E:C4:40:BC:BD:44:47:A3\n";
  EXPECT_TRUE(SafeWriteToFile(bad_fingerprint,
                              repo_path_ + "/config.test/etc/cvmfs/blacklist",
                              0600));
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    ASSERT_EQ(loader::kFailOk, fs->boot_status());
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailCatalog, mp->boot_status());
  }

  options_mgr_.UnsetValue("CVMFS_CONFIG_REPOSITORY");
  options_mgr_.SetValue("CVMFS_BLACKLIST", "/no/such/file");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    ASSERT_EQ(loader::kFailOk, fs->boot_status());
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
    EXPECT_TRUE(mp->ReloadBlacklists());
  }
  RemoveTree("cvmfs_ut_cache");

  options_mgr_.SetValue("CVMFS_BLACKLIST",
                        repo_path_ + "/config.test/etc/cvmfs/blacklist");
  {
    UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
    ASSERT_EQ(loader::kFailOk, fs->boot_status());
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailCatalog, mp->boot_status());
  }
}


TEST_F(T_MountPoint, History) {
  CreateMiniRepository();
  options_mgr_.UnsetValue("CVMFS_ROOT_HASH");
  UniquePtr<FileSystem> fs(FileSystem::Create(fs_info_));
  ASSERT_EQ(loader::kFailOk, fs->boot_status());

  options_mgr_.SetValue("CVMFS_REPOSITORY_DATE", "1984-03-04T00:00:00Z");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailHistory, mp->boot_status());
  }

  if (sizeof(time_t) > 32) {
    options_mgr_.SetValue("CVMFS_REPOSITORY_DATE", "2424-01-01T00:00:00Z");
  } else {
    options_mgr_.SetValue("CVMFS_REPOSITORY_DATE", "2038-01-01T00:00:00Z");
  }
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
  }

  options_mgr_.SetValue("CVMFS_REPOSITORY_TAG", "no-such-tag");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailHistory, mp->boot_status());
  }

  options_mgr_.SetValue("CVMFS_REPOSITORY_TAG", "snapshot");
  {
    UniquePtr<MountPoint> mp(MountPoint::Create("keys.cern.ch", fs.weak_ref()));
    EXPECT_EQ(loader::kFailOk, mp->boot_status());
  }
}
