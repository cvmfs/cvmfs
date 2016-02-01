/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cerrno>
#include <cstdio>
#include <ctime>
#include <string>

#include "../../cvmfs/catalog_sql.h"
#include "../../cvmfs/compression.h"
#include "../../cvmfs/history_sqlite.h"
#include "../../cvmfs/shortstring.h"
#include "../../cvmfs/statistics.h"
#include "../../cvmfs/util.h"
#include "testutil.h"

using namespace std;  // NOLINT

template <class ObjectFetcherT>
class T_ObjectFetcher : public ::testing::Test {
 public:
  T_ObjectFetcher()
  : sandbox(GetCurrentWorkingDirectory() + "/cvmfs_ut_object_fetcher"),
    fqrn("test.cern.ch"),
    backend_storage(sandbox + "/backend"),
    backend_storage_dir(sandbox + "/backend/data"),
    manifest_path(backend_storage + "/.cvmfspublished"),
    whitelist_path(backend_storage + "/.cvmfswhitelist"),
    temp_directory(sandbox + "/tmp"),
    public_key_path(sandbox + "/" + fqrn + ".pub"),
    private_key_path(sandbox + "/" + fqrn + ".key"),
    certificate_path(sandbox + "/" + fqrn + ".crt"),
    master_key_path(sandbox + "/" + fqrn + ".masterkey") { }

 protected:
  const std::string  sandbox;
  const std::string  fqrn;
  const std::string  backend_storage;
  const std::string  backend_storage_dir;
  const std::string  manifest_path;
  const std::string  whitelist_path;
  const std::string  temp_directory;
  const std::string  public_key_path;
  const std::string  private_key_path;
  const std::string  certificate_path;
  const std::string  master_key_path;
  static const unsigned int catalog_revision;

  static const shash::Any broken_history_hash;
  static const shash::Any broken_catalog_hash;

  // determined during setup
  static shash::Any root_hash;
  static shash::Any history_hash;
  static shash::Any previous_history_hash;
  static shash::Any certificate_hash;

 protected:
  virtual void SetUp() {
    if (NeedsFilesystemSandbox()) {
      const bool retval = MkdirDeep(sandbox,                        0700) &&
                          MkdirDeep(backend_storage,                0700) &&
                          MkdirDeep(backend_storage_dir,            0700) &&
                          MkdirDeep(temp_directory,                 0700) &&
                          MakeCacheDirectories(backend_storage_dir, 0700);
      ASSERT_TRUE(retval) << "failed to create sandbox";
    }

    InitializeSandbox();
  }

  virtual void TearDown() {
    if (NeedsFilesystemSandbox()) {
      const bool retval = RemoveTree(sandbox);
      ASSERT_TRUE(retval) << "failed to remove sandbox";

      FinalizeExternalManagers();
    }

    MockHistory::Reset();
    MockCatalog::Reset();

    root_hash             = shash::Any();
    history_hash          = shash::Any();
    previous_history_hash = shash::Any();
    certificate_hash      = shash::Any();
  }

  void InitializeSandbox() {
    // create some history objects
    CreateHistory(&previous_history_hash);
    CreateHistory(&history_hash, previous_history_hash);

    // create a catalog
    CreateCatalog(&root_hash, "");

    // create some 'broken' database objects
    CreateBrokenDatabases();

    if (NeedsFilesystemSandbox()) {
      WriteKeychain();
      WriteManifest();
      WriteWhitelist();
      InitializeExternalManagers();
    }
  }

  void WriteKeychain() {
    // plant public key
    const std::string public_key =
      "-----BEGIN PUBLIC KEY-----\n"
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA+zf/4RhfAQp0xIFEZWFq\n"
      "g3AAR2F81BNVrejauTZbQ2YcyKe8XM4tH2Z9TqgkZdm3Uv+9pmOjLCvJijZU3h2C\n"
      "J1HqBbsYuRAp0GOx4Bw5fSYjAaZ11bsTwgRFdtEt50P8vN6BXE0jaicR6TMwvo00\n"
      "nCm/4rZ2Dyw96B91kZR/KIhNaM5VvoIaMD/+iU78NhvK5370bB2rFCPd3M+GVVTD\n"
      "zWueJx2qdWmjgvjbP5wwMUnIAa5r/OfSVaQOSGDsJPdTlfVObeCYepOn5zNKL0vm\n"
      "/tdttayRCBwhrnf6eED4QOvCbUS/127BLpg69EfZPm1je53/kKzKmleoPOAdiI3L\n"
      "wQIDAQAB\n"
      "-----END PUBLIC KEY-----\n";

    const std::string private_key =
      "-----BEGIN RSA PRIVATE KEY-----\n"
      "MIIEowIBAAKCAQEAprEBl90v8wYDEWykTu5DtqdDJbarfbK3mLQ+ACnc89Rv0/+n\n"
      "VAMs+Pxsp3f04Ksn/d8CdxhHnP0eEHGL1dFp4NBb0aQsp2hS3A0j+q6OAVGHhwGa\n"
      "D7jRwuZTvxdABlb+mzwA3SgxaxImeaAzLiCyEzgn8g8D3PuK2307y05ffuSW69D7\n"
      "dwTSWGxBUwN3mtmhJObDUlKqrxONkwJjwweHOjA44/rUahgGwE5HBxdYnIwxKTXf\n"
      "JiNXuM0ifNbAdxFkYI5RMMG4ACRVlMBDznAMWN4gX31ODgvcqAPS1BCFvLwUp0Uv\n"
      "Fdpkx1eSS2j35cy1S2U9IJRJRmHcpMsCx18qqQIDAQABAoIBAFWlcuUdj8p1V1Q3\n"
      "lwC7KW2K6VRSVDDEh9LdIVIiMFXT3BV4MPhYnV3dDNLLLrXVGzo0wGaCwiDmaPEX\n"
      "9jpJW/ZX+CVyDkOBtbk8wzTFRU9mHZV/fDIByz0x7OutPYdEYZNPND5trV6PN9ec\n"
      "OU/FGtoHK3cSwfBkCkeWRAfE4AopAqXiW6oQJmCeTw4iBiwlWz8LyszpAAocGnkw\n"
      "1pv9vJdzaqNI8gIZwSR856QkkLLkx/PsfVC7P8m3cW+wR1aev7gy5h3OOQtmmPZr\n"
      "oihiaUjFgLHjrtXTEDDvSJHID2i5CnrBA6pckkakVsYjgrIsmRt086EWhm/d/MnT\n"
      "bYk+MCUCgYEA3NLFby6bohsOLcGoZRxdzhX4x3cnrv4tHtJAhpWkyStDEEAEsoEo\n"
      "n5lL3NXaHOFaIsVMsrxAdexsy28SObb0VXCUq25AkO9d3osZuvUqhcqBGaSRubSW\n"
      "+/o6kKTH/frxNCsZff9/2DW+h0HlWC0J9hz9SYxHPMvGMpuXLLZc/MsCgYEAwT65\n"
      "I9YKMvaTOmvEq0fkvIOFkHE2hD8wIIsiXzwtd5Dz0WGCytChtfk79wlU+ivO49Vj\n"
      "89lfgaBovVwqya0hG6BdSKtGtJrNSg+dLda0iF028wOJZwrHW3154G5NstWni+DH\n"
      "X4g4/Jckm3GPaqx1JUhoAQkVD0n0zIPMmzXMm9sCgYA8+8xgpxt+CkDBLDFIMyxI\n"
      "xNKeq25/KlzlnSUKxfrrP5JWw4dIkNjaMqi7xSdmQGn3HkvPNKQMrQ1ipTsAS2Fw\n"
      "0xWLvngATsq6semaMyjaKBG9NjC0e4YS8okH9ynwH0RLpvd0T4JMAJOsoOsojc+p\n"
      "c1oT3LSzuoby5Ps78uT9PwKBgG2YVQet7Dra/MS1OeSp8V+4d5djnbeC3piWT/gS\n"
      "+PZGjwDAfJzplczOfOOttzPqhHtSGk3BihjKEJzUGLAoMA6q9DyyZncnFCZclJfa\n"
      "nKh5tSA9cT+vLUEF+IkABbDW2x7JbEkRyL/4OBqwXNXy2L08Qz/TFs6E8wDJ/tBH\n"
      "wrLhAoGBAL6c/jREYgIMgvYkS3oeGc3vs5zwgFbVqm9lk3aQsjrTiAJa11NXMfDo\n"
      "Ri3K5ZYMUoXzRePScyMEvLNOPc+A850wI2o4tyRQ7wIz35pN6cWx13LlOwXFKvFb\n"
      "m6ULNXSp8DrhtWLdxCH8DY9M+KxSXCb7JNSgmDU6EAJ9UriSQKlX\n"
      "-----END RSA PRIVATE KEY-----";

    const std::string master_key =
      "-----BEGIN RSA PRIVATE KEY-----\n"
      "MIIEowIBAAKCAQEA+zf/4RhfAQp0xIFEZWFqg3AAR2F81BNVrejauTZbQ2YcyKe8\n"
      "XM4tH2Z9TqgkZdm3Uv+9pmOjLCvJijZU3h2CJ1HqBbsYuRAp0GOx4Bw5fSYjAaZ1\n"
      "1bsTwgRFdtEt50P8vN6BXE0jaicR6TMwvo00nCm/4rZ2Dyw96B91kZR/KIhNaM5V\n"
      "voIaMD/+iU78NhvK5370bB2rFCPd3M+GVVTDzWueJx2qdWmjgvjbP5wwMUnIAa5r\n"
      "/OfSVaQOSGDsJPdTlfVObeCYepOn5zNKL0vm/tdttayRCBwhrnf6eED4QOvCbUS/\n"
      "127BLpg69EfZPm1je53/kKzKmleoPOAdiI3LwQIDAQABAoIBAEuaV8RbPEQo7Gky\n"
      "6e2EurRhoYPZ3+JHC1LyL9jrdd27vk/YwwQ+/C9l/bINQh7wvY4Z7u5DMBkb+GRC\n"
      "45mQ0dmZek9NNiyDo8HWLvLeK6LxNjnJ6c5vpYuPE4SlgSYHPOluIQoxIMZSib5f\n"
      "rHy8LCgPHHNTLAZ27w6LYSt1wCrm+ZhOq8tOjqQn1A7pogSC8rmJYuSI4m64C0zt\n"
      "03JCy62T5OMd2+bYNsrEVKHj8GdOS/kTX5ssj9aArVxK1uG/fY6OE1kMFKAgt1Z/\n"
      "OKzORvLjN3880YLyghgqtUgD64NeSj3Oi45xWuCw0s9GnVrolOfPEyggqja527ha\n"
      "JcI0IJECgYEA/xUmzPVTdYcTr2okBECQ08OBQJntsDLHuQEI/oFvceKG7f9tYJ/6\n"
      "0iMcx8iXoYDBRQBwW8XGlx5c/+8alhy2y3J7qiHH3BCcMkWiJjwSrPCk0Tyd5xha\n"
      "3Hh+rDA1/U1MJxsdZv0js50MHSU69+0FWHw0XYje3IkWZmzlDvP5tn0CgYEA/B9K\n"
      "Y+J5p2qQyF6ACfXQ2786RgAPkeeza3Fdn4pmtAA2LpqTa9GQO4X0JfigWYjEYkCG\n"
      "/OGhQCkO2vz5F1hltwIH9AfHTh9DfStq9vY8R7vnYB/Am1MJdppjTkWjlu0Bnz1S\n"
      "7a3Mt3xyM8uURuQQzu9r+5NGPCle0bdmEt9D+ZUCgYEAl9Gg/E/vUn4Iy1ijAxzi\n"
      "lgdAgJCdFUfD82qYTdH/4IpwwGpMUTwmbreTQ50yEl+tqEHwnc6CuiLKO1G2Qy3n\n"
      "5gLHc6UTbPk93fXv4k3S17eKgTZQzOCEA8B7tEQlfhNphcTvpQJ5I0gPk7E6/aDG\n"
      "k7mo+RqjeiLlgCTD2DiBoYkCgYBG4p1NvA0sLuAKFde19TEFt5wwti+qfBSL7tG9\n"
      "23HIxg51x+wO8lq1AZKFYoPi6HsejLnnO6DUozaUB3AZSjc+3wlRaSZ2JoAZHy3x\n"
      "xYVpPcFt2z+R7CTK/dlR1m6KLpS4Ksu4G5dlN038lg5YaCL5q4MWtm+W1qLcH85J\n"
      "HBDGiQKBgDWobRgk+i6BN2jnJssAP191MPntfbAaiOfOo9doYTnRrwdBA3Qwutjb\n"
      "ufbvxdCBFSZeGbzNWc3ge/6WA6XeRHistH33sYdsCgNO7L30Uyv3HSf1OsxdTDLZ\n"
      "nkvXn+sFFNsWmuNihi+hhlz+md7naSgE51l4gvdQa22sLmgvls6C\n"
      "-----END RSA PRIVATE KEY-----";

    const std::string certificate =
      "-----BEGIN CERTIFICATE-----\n"
      "MIIC4DCCAcgCCQClsBOairrIxTANBgkqhkiG9w0BAQsFADAyMTAwLgYDVQQDDCd0\n"
      "ZXN0LmNlcm4uY2ggQ2VyblZNLUZTIFJlbGVhc2UgTWFuYWdlcnMwHhcNMTQxMjE4\n"
      "MTAxOTMzWhcNMTUxMjE4MTAxOTMzWjAyMTAwLgYDVQQDDCd0ZXN0LmNlcm4uY2gg\n"
      "Q2VyblZNLUZTIFJlbGVhc2UgTWFuYWdlcnMwggEiMA0GCSqGSIb3DQEBAQUAA4IB\n"
      "DwAwggEKAoIBAQCmsQGX3S/zBgMRbKRO7kO2p0Mltqt9sreYtD4AKdzz1G/T/6dU\n"
      "Ayz4/Gynd/Tgqyf93wJ3GEec/R4QcYvV0Wng0FvRpCynaFLcDSP6ro4BUYeHAZoP\n"
      "uNHC5lO/F0AGVv6bPADdKDFrEiZ5oDMuILITOCfyDwPc+4rbfTvLTl9+5Jbr0Pt3\n"
      "BNJYbEFTA3ea2aEk5sNSUqqvE42TAmPDB4c6MDjj+tRqGAbATkcHF1icjDEpNd8m\n"
      "I1e4zSJ81sB3EWRgjlEwwbgAJFWUwEPOcAxY3iBffU4OC9yoA9LUEIW8vBSnRS8V\n"
      "2mTHV5JLaPflzLVLZT0glElGYdykywLHXyqpAgMBAAEwDQYJKoZIhvcNAQELBQAD\n"
      "ggEBAHNYHnKfWWGpw8s3LqRIY2+wq494Yt1Nrh7ZZASQZoNjcYuXJjieDtYqIKSC\n"
      "JvhOm24NlqCb1QNE/QBzHh9pn/MXboC+IWyYLlFpkJkrVfhlnwaK/JwdoE5hIgU5\n"
      "dtt/einHND5OD7cXMrrUfPKfzjPhxNHoJGPVGldY0OlchHyQJRU7eMFfvcwEhqaM\n"
      "ZDPpLCFW4/H7bJ0oL377F8nOEq29Wu8KbqcJs2GyJ99PlWMTw0p/tvkXgdlai1oM\n"
      "ioFmZ8bkVCMa5vqZgymS83TTHHkXEjJ9UZ++KfBJonez7BfCOSkh9ryLN3kad/sO\n"
      "QxuquL7I2T9us60G7yqD95inHsw=\n"
      "-----END CERTIFICATE-----";

    WriteFile(public_key_path,  public_key);
    WriteFile(private_key_path, private_key);
    WriteFile(master_key_path,  master_key);
    WriteFile(certificate_path, certificate);

    certificate_hash = h("0000000000000000000000000000000000000000",
                         shash::kSuffixCertificate);
    CommitIntoStorage(certificate_path, &certificate_hash);
  }

  void WriteFile(const std::string &path, const std::string &content) {
    FILE *f = fopen(path.c_str(), "w+");
    ASSERT_NE(static_cast<FILE*>(NULL), f)
      << "failed to open. errno: " << errno;
    const size_t bytes_written = fwrite(content.data(), 1, content.length(), f);
    ASSERT_EQ(bytes_written, content.length())
      << "failed to write. errno: " << errno;

    const int retval = fclose(f);
    ASSERT_EQ(0, retval) << "failed to close. errno: " << errno;
  }

  void Sign(const shash::Any              hash,
            signature::SignatureManager  *signature_manager,
            std::string                  *signature) const {
    unsigned char *sig;
    unsigned sig_size;
    ASSERT_TRUE(signature_manager->Sign(
                  reinterpret_cast<const unsigned char *>(
                    hash.ToString().data()),
                    hash.GetHexSize(),
                  &sig, &sig_size));
    *signature = std::string(reinterpret_cast<char*>(sig), sig_size);
  }

  void SignRsa(const shash::Any  hash,
               std::string      *signature) const {
    std::string hash_string = hash.ToString();
    FILE *f_rsa_pkey = fopen(master_key_path.c_str(), "r");
    ASSERT_NE(static_cast<FILE*>(NULL), f_rsa_pkey);
    RSA *rsa = PEM_read_RSAPrivateKey(f_rsa_pkey, NULL, NULL, NULL);
    ASSERT_NE(static_cast<RSA*>(NULL), rsa);
    fclose(f_rsa_pkey);

    unsigned char *sig = (unsigned char*)malloc(RSA_size(rsa));
    const int res =
      RSA_private_encrypt(hash_string.length(),
                          reinterpret_cast<const unsigned char *>(
                            hash_string.data()),
                          sig, rsa, RSA_PKCS1_PADDING);
    ASSERT_NE(-1, res) << "RSA error code: " << ERR_get_error();
    *signature = std::string(reinterpret_cast<char*>(sig), res);

    free(sig);
    RSA_free(rsa);
  }

  void SignString(std::string                  *str,
                  signature::SignatureManager  *signature_manager,
                  const bool                    rsa = false) const {
    shash::Any hash = h("0000000000000000000000000000000000000000");
    shash::HashMem(
      reinterpret_cast<const unsigned char *>(str->data()),
      str->length(), &hash);

    std::string sig;
    if (rsa) {
      SignRsa(hash, &sig);
    } else {
      Sign(hash, signature_manager, &sig);
    }

    *str += "--\n";
    *str += hash.ToString() + "\n";
    *str += sig;
  }

  void WriteManifest() {
    // create manifest
    const uint64_t    catalog_size = 0;
    const std::string root_path    = "";
    UniquePtr<manifest::Manifest> manifest(new manifest::Manifest(
                                                 root_hash,
                                                 catalog_size,
                                                 root_path));
    manifest->set_history(history_hash);
    manifest->set_certificate(certificate_hash);
    manifest->set_repository_name(fqrn);

    signature::SignatureManager signature_manager;
    signature_manager.Init();

    ASSERT_TRUE(signature_manager.LoadCertificatePath(certificate_path));
    ASSERT_TRUE(signature_manager.LoadPrivateKeyPath(private_key_path, ""));

    std::string manifest_string = manifest->ExportString();
    SignString(&manifest_string, &signature_manager);
    WriteFile(manifest_path, manifest_string);

    signature_manager.Fini();
  }

  void WriteWhitelist() {
    string whitelist;
    whitelist += MakeWhitelistTimestamp(time(0)) + "\n";
    whitelist += "E" + MakeWhitelistTimestamp(time(0) + 30 /*days*/ *24*60*60)
              + "\n";
    whitelist += "N" + fqrn + "\n";

    signature::SignatureManager signature_manager;
    signature_manager.Init();

    ASSERT_TRUE(signature_manager.LoadCertificatePath(certificate_path));
    ASSERT_TRUE(signature_manager.LoadPrivateKeyPath(master_key_path, ""));

    whitelist += signature_manager.FingerprintCertificate(shash::kSha1)
              + "\n";
    SignString(&whitelist, &signature_manager, true);
    WriteFile(whitelist_path, whitelist);

    signature_manager.Fini();
  }

  std::string MakeWhitelistTimestamp(const time_t timestamp) const {
    struct tm ti;
    localtime_r(&timestamp, &ti);

    char result[15];
    snprintf(result, sizeof(result), "%04d%02d%02d%02d%02d%02d",
             ti.tm_year + 1900,
             ti.tm_mon + 1,
             ti.tm_mday,
             ti.tm_hour,
             ti.tm_min,
             ti.tm_sec);

    return string(result);
  }

  void InitializeExternalManagers() {
    download_manager_.Init(1, true, &statistics_);
    signature_manager_.Init();
    ASSERT_TRUE(signature_manager_.LoadPublicRsaKeys(public_key_path));
  }

  void FinalizeExternalManagers() {
    download_manager_.Fini();
    signature_manager_.Fini();
  }

  ObjectFetcherT* GetObjectFetcher() {
    return GetObjectFetcher(type<ObjectFetcherT>());
  }

  void CreateHistory(
    shash::Any *content_hash,
    const shash::Any &previous_revision = shash::Any()
  ) {
    return CreateHistory(type<ObjectFetcherT>(),
                         content_hash,
                         previous_revision);
  }

  void CreateCatalog(
    shash::Any *content_hash,
    const std::string &root_path
  ) {
    return CreateCatalog(type<ObjectFetcherT>(),
                         content_hash,
                         root_path);
  }

  bool NeedsFilesystemSandbox() {
    return NeedsFilesystemSandbox(type<ObjectFetcherT>());
  }

  bool IsHttpObjectFetcher() {
    return GetObjectFetcherType(type<ObjectFetcherT>()) == "http";
  }

  bool IsLocalObjectFetcher() {
    return GetObjectFetcherType(type<ObjectFetcherT>()) == "local";
  }

  bool IsMockObjectFetcher() {
    return GetObjectFetcherType(type<ObjectFetcherT>()) == "mock";
  }

  typedef std::vector<std::string> DirectoryListing;
  void ListDirectory(const std::string &path, DirectoryListing *listing) const {
    ASSERT_TRUE(listing != NULL);
    listing->clear();

    DIR *dp;
    struct dirent *ep;
    dp = opendir(path.c_str());
    ASSERT_NE(static_cast<DIR*>(NULL), dp);

    while ( (ep = readdir(dp)) ) {
      const std::string name = ep->d_name;
      if (name != "." && name != "..") {
        listing->push_back(name);
      }
    }

    closedir(dp);
  }

  size_t CountTemporaryFiles() {
    if (!NeedsFilesystemSandbox()) {
      return 0;
    }

    DirectoryListing listing;
    ListDirectory(temp_directory, &listing);
    return listing.size();
  }

 private:
  // type-based overloading helper struct
  // Inspired from here:
  //   http://stackoverflow.com/questions/5512910/
  //          explicit-specialization-of-template-class-member-function
  template <typename T> struct type {};

  ObjectFetcherT* GetObjectFetcher(const type<LocalObjectFetcher<> > type_spec)
  {
    return new LocalObjectFetcher<>(backend_storage, temp_directory);
  }

  ObjectFetcherT* GetObjectFetcher(const type<HttpObjectFetcher<> > type_spec) {
    return new HttpObjectFetcher<>(fqrn,
                                   "file://" + backend_storage,
                                   temp_directory,
                                   &download_manager_,
                                   &signature_manager_);
  }

  ObjectFetcherT* GetObjectFetcher(const type<MockObjectFetcher> type_spec) {
    return new MockObjectFetcher();
  }

  void CreateHistory(const type<LocalObjectFetcher<> >  type_spec,
                           shash::Any                  *content_hash,
                     const shash::Any                  &previous_revision) {
    CreateSandboxHistory(content_hash, previous_revision);
  }

  void CreateHistory(const type<HttpObjectFetcher<> >  type_spec,
                           shash::Any                 *content_hash,
                     const shash::Any                 &previous_revision) {
    CreateSandboxHistory(content_hash, previous_revision);
  }

  void CreateBrokenDatabases() {
    MockHistory::RegisterObject(broken_history_hash, NULL);
    MockCatalog::RegisterObject(broken_catalog_hash, NULL);

    if (NeedsFilesystemSandbox()) {
      const std::string history = CreateTempPath(sandbox + "/history", 0700);
      ASSERT_FALSE(history.empty()) << "failed to create tmp in: " << sandbox;
      InsertIntoStorage(history, broken_history_hash);

      const std::string catalog = CreateTempPath(sandbox + "/catalog", 0700);
      ASSERT_FALSE(catalog.empty()) << "failed to create tmp in: " << sandbox;
      InsertIntoStorage(catalog, broken_catalog_hash);
    }
  }

  void CreateSandboxHistory(
    shash::Any *content_hash,
    const shash::Any &previous_revision
  ) {
    const std::string tmp_path = CreateTempPath(sandbox + "/history", 0700);
    ASSERT_FALSE(tmp_path.empty()) << "failed to create tmp in: " << sandbox;

    history::SqliteHistory *history =
                              ObjectFetcherT::HistoryTN::Create(tmp_path, fqrn);
    ASSERT_NE(static_cast<history::SqliteHistory*>(NULL), history) <<
      "failed to create new history database in: " << tmp_path;
    history->SetPreviousRevision(previous_revision);
    delete history;

    *content_hash = h("0000000000000000000000000000000000000000",
                      shash::kSuffixHistory);
    CommitIntoStorage(tmp_path, content_hash);
  }

  void CreateHistory(const type<MockObjectFetcher>  type_spec,
                           shash::Any              *content_hash,
                     const shash::Any              &previous_revision) {
    const bool writable = true;
    MockHistory *history = new MockHistory(writable, fqrn);

    // Note: this doesn't work with more than one legacy level!
    if (!previous_revision.IsNull()) {
      history->SetPreviousRevision(previous_revision);
      *content_hash = MockHistory::root_hash;
    } else {
      *content_hash = h("0000000000000000000000000000000000000000",
                        shash::kSuffixHistory);
      content_hash->Randomize();
    }

    MockHistory::RegisterObject(*content_hash, history);
  }

  void CreateCatalog(const type<LocalObjectFetcher<> >  type_spec,
                           shash::Any                  *content_hash,
                     const std::string                 &root_path) {
    CreateSandboxCatalog(content_hash, root_path);
  }

  void CreateCatalog(const type<HttpObjectFetcher<> >  type_spec,
                           shash::Any                 *content_hash,
                     const std::string                &root_path) {
    CreateSandboxCatalog(content_hash, root_path);
  }

  void CreateSandboxCatalog(
    shash::Any *content_hash,
    const std::string &root_path
  ) {
    const std::string tmp_path = CreateTempPath(sandbox + "/catalog", 0700);
    ASSERT_FALSE(tmp_path.empty()) << "failed to create tmp in: " << sandbox;

    // TODO(rmeusel): WritableCatalog::Create()
    const bool volatile_content = false;
    catalog::CatalogDatabase *catalog_db =
                                     catalog::CatalogDatabase::Create(tmp_path);
    ASSERT_NE(static_cast<catalog::CatalogDatabase*>(NULL), catalog_db) <<
      "failed to create new catalog database in: " << tmp_path;

    catalog::DirectoryEntry root_entry;  // mocked root entry...
    const bool success = catalog_db->InsertInitialValues(root_path,
                                                         volatile_content,
                                                         "",
                                                         root_entry) &&
                         catalog_db->SetProperty("revision", catalog_revision);
    ASSERT_TRUE(success) << "failed to initialise catalog in: " << tmp_path;
    delete catalog_db;

    *content_hash = h("0000000000000000000000000000000000000000",
                      shash::kSuffixCatalog);
    CommitIntoStorage(tmp_path, content_hash);
  }

  void CreateCatalog(const type<MockObjectFetcher >  type_spec,
                           shash::Any               *content_hash,
                     const std::string              &root_path) {
    *content_hash = MockCatalog::root_hash;
    MockCatalog *catalog = new MockCatalog(root_path,
                                           *content_hash,
                                           1024,
                                           catalog_revision,
                                           t(27, 11, 1987),
                                           true);
    // register the new catalog in the data structures
    MockCatalog::RegisterObject(catalog->hash(), catalog);
  }

  void CommitIntoStorage(const std::string  &tmp_path,
                               shash::Any   *content_hash) {
    const std::string txn_path = CreateTempPath(temp_directory + "/blob", 0600);
    ASSERT_TRUE(zlib::CompressPath2Path(tmp_path, txn_path, content_hash)) <<
      "failed to compress file " << tmp_path << " to " << txn_path;
    InsertIntoStorage(txn_path, *content_hash);
  }

  void InsertIntoStorage(const std::string &txn_path,
                         const shash::Any  &content_hash) {
    const std::string res_path = backend_storage + "/data/" +
                                 content_hash.MakePath();
    ASSERT_EQ(0, rename(txn_path.c_str(), res_path.c_str())) <<
      "failed to rename() compressed file " << txn_path << " to " << res_path <<
      " with content hash " << content_hash.ToString() <<
      " (errno: " << errno << ")";
  }

  bool NeedsFilesystemSandbox(const type<LocalObjectFetcher<> > type_spec) {
    return true;
  }
  bool NeedsFilesystemSandbox(const type<HttpObjectFetcher<> > type_spec) {
    return true;
  }
  bool NeedsFilesystemSandbox(const type<MockObjectFetcher> type_spec) {
    return false;
  }

  std::string GetObjectFetcherType(const type<LocalObjectFetcher<> > spec) {
    return "local";
  }
  std::string GetObjectFetcherType(const type<MockObjectFetcher> spec) {
    return "mock";
  }
  std::string GetObjectFetcherType(const type<HttpObjectFetcher<> > spec) {
    return "http";
  }

 private:
  perf::Statistics             statistics_;
  download::DownloadManager    download_manager_;
  signature::SignatureManager  signature_manager_;
};

template <class ObjectFetcherT>
const unsigned int T_ObjectFetcher<ObjectFetcherT>::catalog_revision = 1;

template <class ObjectFetcherT>
const shash::Any T_ObjectFetcher<ObjectFetcherT>::broken_history_hash =
                                  h("b904b56ffd47ba89f26d5a887063b4a1fbfb9307",
                                    shash::kSuffixHistory);

template <class ObjectFetcherT>
const shash::Any T_ObjectFetcher<ObjectFetcherT>::broken_catalog_hash =
                                  h("c4818243550ae6a46f55602043eb23f8c4f97910",
                                    shash::kSuffixCatalog);

template <class ObjectFetcherT>
shash::Any T_ObjectFetcher<ObjectFetcherT>::root_hash;

template <class ObjectFetcherT>
shash::Any T_ObjectFetcher<ObjectFetcherT>::history_hash;

template <class ObjectFetcherT>
shash::Any T_ObjectFetcher<ObjectFetcherT>::previous_history_hash;

template <class ObjectFetcherT>
shash::Any T_ObjectFetcher<ObjectFetcherT>::certificate_hash;

typedef ::testing::Types<
  MockObjectFetcher,
  LocalObjectFetcher<>,
  HttpObjectFetcher<> > ObjectFetcherTypes;
TYPED_TEST_CASE(T_ObjectFetcher, ObjectFetcherTypes);


TYPED_TEST(T_ObjectFetcher, InitializeSlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  EXPECT_TRUE(object_fetcher.IsValid());
  if (TestFixture::NeedsFilesystemSandbox()) {
    EXPECT_EQ(0u, TestFixture::CountTemporaryFiles());
  }
}


TYPED_TEST(T_ObjectFetcher, FetchManifestSlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  manifest::Manifest *manifest = NULL;
  typename TypeParam::Failures retval =
    object_fetcher->FetchManifest(&manifest);
  EXPECT_EQ(TypeParam::kFailOk, retval);
  ASSERT_NE(static_cast<manifest::Manifest*>(NULL), manifest);

  EXPECT_EQ(TestFixture::root_hash,    manifest->catalog_hash());
  EXPECT_EQ(TestFixture::history_hash, manifest->history());
  delete manifest;

  UniquePtr<manifest::Manifest> manifest_ptr;
  EXPECT_FALSE(manifest_ptr.IsValid());
  typename TypeParam::Failures retval2 =
    object_fetcher->FetchManifest(&manifest_ptr);
  EXPECT_EQ(TypeParam::kFailOk, retval2);
  ASSERT_TRUE(manifest_ptr.IsValid());

  EXPECT_EQ(TestFixture::root_hash,    manifest_ptr->catalog_hash());
  EXPECT_EQ(TestFixture::history_hash, manifest_ptr->history());

  if (TestFixture::NeedsFilesystemSandbox()) {
    EXPECT_EQ(0u, TestFixture::CountTemporaryFiles());
  }
}


TYPED_TEST(T_ObjectFetcher, FetchHistorySlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  EXPECT_TRUE(object_fetcher->HasHistory());

  typename TypeParam::HistoryTN *history = NULL;
  typename TypeParam::Failures retval = object_fetcher->FetchHistory(&history);
  EXPECT_EQ(TypeParam::kFailOk, retval);
  ASSERT_NE(static_cast<typename TypeParam::HistoryTN*>(NULL), history);
  EXPECT_EQ(TestFixture::previous_history_hash, history->previous_revision());
  delete history;

  if (TestFixture::NeedsFilesystemSandbox()) {
    EXPECT_EQ(0u, TestFixture::CountTemporaryFiles());
  }

  UniquePtr<typename TypeParam::HistoryTN> history_ptr;
  EXPECT_FALSE(history_ptr.IsValid());
  typename TypeParam::Failures retval2 =
    object_fetcher->FetchHistory(&history_ptr);
  EXPECT_EQ(TypeParam::kFailOk, retval2);
  ASSERT_TRUE(history_ptr.IsValid());

  if (TestFixture::NeedsFilesystemSandbox()) {
    EXPECT_LE(1u, TestFixture::CountTemporaryFiles());
  }
}


TYPED_TEST(T_ObjectFetcher, FetchLegacyHistorySlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  typename TypeParam::HistoryTN *history = NULL;
  typename TypeParam::Failures retval =
    object_fetcher->FetchHistory(&history, TestFixture::previous_history_hash);
  EXPECT_EQ(TypeParam::kFailOk, retval);
  ASSERT_NE(static_cast<typename TypeParam::HistoryTN*>(NULL), history)
    << "didn't find: "
    << TestFixture::previous_history_hash.ToStringWithSuffix();
  EXPECT_TRUE(history->previous_revision().IsNull());
  if (TestFixture::NeedsFilesystemSandbox()) {
    EXPECT_LE(1u, TestFixture::CountTemporaryFiles());
  }
  delete history;
  if (TestFixture::NeedsFilesystemSandbox()) {
    EXPECT_EQ(0u, TestFixture::CountTemporaryFiles());
  }

  UniquePtr<typename TypeParam::HistoryTN> history_ptr;
  EXPECT_FALSE(history_ptr.IsValid());
  typename TypeParam::Failures retval2 =
    object_fetcher->FetchHistory(&history_ptr,
                                 TestFixture::previous_history_hash);
  EXPECT_EQ(TypeParam::kFailOk, retval2);
  ASSERT_TRUE(history_ptr.IsValid());
  EXPECT_TRUE(history_ptr->previous_revision().IsNull());
  if (TestFixture::NeedsFilesystemSandbox()) {
    EXPECT_LE(1u, TestFixture::CountTemporaryFiles());
  }
}


TYPED_TEST(T_ObjectFetcher, FetchInvalidHistorySlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  UniquePtr<typename TypeParam::HistoryTN> history;
  EXPECT_FALSE(history.IsValid());
  typename TypeParam::Failures retval =
    object_fetcher->FetchHistory(&history,
                                 h("400d35465f179a4acacb5fe749e6ce20a0bbdb84",
                                   shash::kSuffixHistory));
  EXPECT_FALSE(history.IsValid());
  typename TypeParam::Failures expected_failure =
    (TestFixture::IsHttpObjectFetcher())
      ? TypeParam::kFailUnknown    // HttpObjectFetcher is used via file://
      : TypeParam::kFailNotFound;  // which doesn't support proper errors
                                   // TODO(rmeusel): fix me
  EXPECT_EQ(expected_failure, retval) << "code: " << Code2Ascii(retval);

  if (TestFixture::NeedsFilesystemSandbox()) {
    EXPECT_EQ(0u, TestFixture::CountTemporaryFiles());
  }
}


TYPED_TEST(T_ObjectFetcher, FetchCatalogSlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  typename TypeParam::CatalogTN *catalog = NULL;
  typename TypeParam::Failures retval =
    object_fetcher->FetchCatalog(TestFixture::root_hash, "", &catalog);
  EXPECT_EQ(TypeParam::kFailOk, retval);
  ASSERT_NE(static_cast<typename TypeParam::CatalogTN*>(NULL), catalog);

  EXPECT_EQ("",                            catalog->path().ToString());
  EXPECT_EQ(TestFixture::catalog_revision, catalog->revision());

  if (TestFixture::NeedsFilesystemSandbox()) {
    EXPECT_LE(1u, TestFixture::CountTemporaryFiles());
  }
  delete catalog;
  if (TestFixture::NeedsFilesystemSandbox()) {
    EXPECT_EQ(0u, TestFixture::CountTemporaryFiles());
  }

  UniquePtr<typename TypeParam::CatalogTN> catalog_ptr;
  EXPECT_FALSE(catalog_ptr.IsValid());
  typename TypeParam::Failures retval2 =
    object_fetcher->FetchCatalog(TestFixture::root_hash, "", &catalog_ptr);
  EXPECT_EQ(TypeParam::kFailOk, retval2);
  ASSERT_TRUE(catalog_ptr.IsValid());

  EXPECT_EQ("",                            catalog_ptr->path().ToString());
  EXPECT_EQ(TestFixture::catalog_revision, catalog_ptr->revision());

  if (TestFixture::NeedsFilesystemSandbox()) {
    EXPECT_LE(0u, TestFixture::CountTemporaryFiles());
  }
}


TYPED_TEST(T_ObjectFetcher, FetchInvalidCatalogSlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  shash::Any invalid_clg = h("5739dc30f42525a261b2f4b383b220df3e36f04d",
                             shash::kSuffixCatalog);

  typename TypeParam::Failures expected_failure =
    (TestFixture::IsHttpObjectFetcher())
      ? TypeParam::kFailUnknown    // HttpObjectFetcher is used via file://
      : TypeParam::kFailNotFound;  // which doesn't support proper errors
                                   // TODO(rmeusel): fix me

  typename TypeParam::CatalogTN *catalog = NULL;
  typename TypeParam::Failures retval =
    object_fetcher->FetchCatalog(invalid_clg, "", &catalog);
  EXPECT_EQ(expected_failure, retval) << "code: " << Code2Ascii(retval);
  ASSERT_EQ(static_cast<typename TypeParam::CatalogTN*>(NULL), catalog);

  if (TestFixture::NeedsFilesystemSandbox()) {
    EXPECT_EQ(0u, TestFixture::CountTemporaryFiles());
  }

  UniquePtr<typename TypeParam::CatalogTN> catalog_ptr;
  EXPECT_FALSE(catalog_ptr.IsValid());
  typename TypeParam::Failures retval2 =
    object_fetcher->FetchCatalog(invalid_clg, "", &catalog_ptr);
  EXPECT_EQ(expected_failure, retval2) << "code: " << Code2Ascii(retval2);
  EXPECT_FALSE(catalog_ptr.IsValid());

  if (TestFixture::NeedsFilesystemSandbox()) {
    EXPECT_LE(0u, TestFixture::CountTemporaryFiles());
  }
}


TYPED_TEST(T_ObjectFetcher, AutoCleanupFetchedFilesSlow) {
  if (!TestFixture::NeedsFilesystemSandbox()) {
    // this is only valid if we are actually creating files...
    return;
  }

  size_t files = 0;
  EXPECT_EQ(files, TestFixture::CountTemporaryFiles());

  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  UniquePtr<manifest::Manifest>            manifest;
  UniquePtr<typename TypeParam::CatalogTN> catalog;
  UniquePtr<typename TypeParam::HistoryTN> history;
  typename TypeParam::Failures retval;

  retval = object_fetcher->FetchManifest(&manifest);
  EXPECT_EQ(TypeParam::kFailOk, retval);
  ASSERT_TRUE(manifest.IsValid());
  EXPECT_EQ(files, TestFixture::CountTemporaryFiles());

  retval = object_fetcher->FetchCatalog(TestFixture::root_hash, "", &catalog);
  EXPECT_EQ(TypeParam::kFailOk, retval);
  ASSERT_TRUE(catalog.IsValid());

  EXPECT_LT(files, TestFixture::CountTemporaryFiles());
  files = TestFixture::CountTemporaryFiles();

  retval = object_fetcher->FetchHistory(&history);
  EXPECT_EQ(TypeParam::kFailOk, retval);
  ASSERT_TRUE(history.IsValid());

  EXPECT_LT(files, TestFixture::CountTemporaryFiles());
  files = TestFixture::CountTemporaryFiles();

  delete object_fetcher.Release();
  EXPECT_EQ(files, TestFixture::CountTemporaryFiles());

  delete history.Release();
  EXPECT_GT(files, TestFixture::CountTemporaryFiles());
  files = TestFixture::CountTemporaryFiles();

  delete catalog.Release();
  EXPECT_GT(files, TestFixture::CountTemporaryFiles());
  EXPECT_EQ(0u, TestFixture::CountTemporaryFiles());
}


TYPED_TEST(T_ObjectFetcher, LoadBrokenDatabaseObjectsSlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  UniquePtr<typename TypeParam::CatalogTN> catalog;
  UniquePtr<typename TypeParam::HistoryTN> history;
  typename TypeParam::Failures retval;
  const typename TypeParam::Failures expected_error =
    TestFixture::IsHttpObjectFetcher()
      ? TypeParam::kFailBadData
      : TestFixture::IsLocalObjectFetcher()
          ? TypeParam::kFailDecompression
          : TestFixture::IsMockObjectFetcher()
              ? TypeParam::kFailLocalIO
              : TypeParam::kFailUnknown;

  retval = object_fetcher->FetchCatalog(TestFixture::broken_catalog_hash, "",
                                        &catalog);
  EXPECT_EQ(expected_error, retval)
    << "expected: " << Code2Ascii(expected_error) << std::endl
    << "actual:   " << Code2Ascii(retval);
  EXPECT_FALSE(catalog.IsValid());
  EXPECT_EQ(0u, TestFixture::CountTemporaryFiles());

  retval = object_fetcher->FetchHistory(&history,
                                        TestFixture::broken_history_hash);
  EXPECT_EQ(expected_error, retval)
    << "expected: " << Code2Ascii(expected_error) << std::endl
    << "actual:   " << Code2Ascii(retval);
  EXPECT_FALSE(history.IsValid());
  EXPECT_EQ(0u, TestFixture::CountTemporaryFiles());
}
