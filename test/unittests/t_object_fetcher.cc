/**
 * This file is part of the CernVM File System.
 */

#include <cerrno>
#include <cstdio>
#include <ctime>
#include <sstream>

#include <gtest/gtest.h>

#include "../../cvmfs/catalog_sql.h"
#include "../../cvmfs/compression.h"
#include "../../cvmfs/history_sqlite.h"
#include "../../cvmfs/util.h"

#include "testutil.h"

using namespace history;

template <class ObjectFetcherT>
class T_ObjectFetcher : public ::testing::Test {
 protected:
  static const std::string  sandbox;
  static const std::string  fqrn;
  static const std::string  backend_storage;
  static const std::string  backend_storage_dir;
  static const std::string  manifest_path;
  static const std::string  whitelist_path;
  static const std::string  temp_directory;
  static const std::string  public_key_path;
  static const std::string  private_key_path;
  static const std::string  certificate_path;
  static const std::string  master_key_path;
  static const unsigned int catalog_revision;

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
    InsertIntoStorage(certificate_path, &certificate_hash);
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
            signature::SignatureManager  &signature_manager,
            std::string                  *signature) const {
    unsigned char *sig;
    unsigned sig_size;
    ASSERT_TRUE(signature_manager.Sign(
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
                  signature::SignatureManager  &signature_manager,
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
    SignString(&manifest_string, signature_manager);
    WriteFile(manifest_path, manifest_string);

    signature_manager.Fini();
  }

  void WriteWhitelist() {
    std::stringstream whitelist;
    whitelist << MakeWhitelistTimestamp(time(0)) << std::endl;
    whitelist << "E" << MakeWhitelistTimestamp(time(0) + 30 /*days*/ *24*60*60)
              << std::endl;
    whitelist << "N" << fqrn << std::endl;

    signature::SignatureManager signature_manager;
    signature_manager.Init();

    ASSERT_TRUE(signature_manager.LoadCertificatePath(certificate_path));
    ASSERT_TRUE(signature_manager.LoadPrivateKeyPath(master_key_path, ""));

    whitelist << signature_manager.FingerprintCertificate(shash::kSha1)
              << std::endl;
    std::string whitelist_string = whitelist.str();

    SignString(&whitelist_string, signature_manager, true);
    WriteFile(whitelist_path, whitelist_string);

    signature_manager.Fini();
  }

  std::string MakeWhitelistTimestamp(const time_t timestamp) const {
    struct tm* ti = localtime(&timestamp);

    std::stringstream ss;
    ss << std::setfill('0')
       << std::setw(4) << ti->tm_year + 1900
       << std::setw(2) << ti->tm_mon  + 1
       << std::setw(2) << ti->tm_mday
       << std::setw(2) << ti->tm_hour
       << std::setw(2) << ti->tm_min
       << std::setw(2) << ti->tm_sec;

    return ss.str();
  }

  void InitializeExternalManagers() {
    download_manager_.Init(1, true);
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

  void CreateHistory(      shash::Any  *content_hash,
                     const shash::Any  &previous_revision = shash::Any()) {
    return CreateHistory(type<ObjectFetcherT>(),
                         content_hash,
                         previous_revision);
  }

  void CreateCatalog(      shash::Any   *content_hash,
                     const std::string  &root_path) {
    return CreateCatalog(type<ObjectFetcherT>(),
                         content_hash,
                         root_path);
  }

  bool NeedsFilesystemSandbox() {
    return NeedsFilesystemSandbox(type<ObjectFetcherT>());
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

 private:
  // type-based overloading helper struct
  // Inspired from here:
  //   http://stackoverflow.com/questions/5512910/
  //          explicit-specialization-of-template-class-member-function
  template <typename T> struct type {};

  ObjectFetcherT* GetObjectFetcher(const type<LocalObjectFetcher<> > type_spec) {
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

  void CreateSandboxHistory(      shash::Any  *content_hash,
                            const shash::Any  &previous_revision) {
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
    InsertIntoStorage(tmp_path, content_hash);
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

  void CreateSandboxCatalog(      shash::Any   *content_hash,
                            const std::string  &root_path) {
    const std::string tmp_path = CreateTempPath(sandbox + "/catalog", 0700);
    ASSERT_FALSE(tmp_path.empty()) << "failed to create tmp in: " << sandbox;

    // TODO: WritableCatalog::Create()
    const bool volatile_content = false;
    catalog::CatalogDatabase *catalog_db =
                                     catalog::CatalogDatabase::Create(tmp_path);
    ASSERT_NE(static_cast<catalog::CatalogDatabase*>(NULL), catalog_db) <<
      "failed to create new catalog database in: " << tmp_path;

    catalog::DirectoryEntry root_entry; // mocked root entry...
    const bool success = catalog_db->InsertInitialValues(root_path,
                                                         volatile_content,
                                                         root_entry) &&
                         catalog_db->SetProperty("revision", catalog_revision);
    ASSERT_TRUE(success) << "failed to initialise catalog in: " << tmp_path;
    delete catalog_db;

    *content_hash = h("0000000000000000000000000000000000000000",
                      shash::kSuffixCatalog);
    InsertIntoStorage(tmp_path, content_hash);
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

  void InsertIntoStorage(const std::string  &tmp_path,
                               shash::Any   *content_hash) {
    const std::string txn_path = CreateTempPath(temp_directory + "/blob", 0600);
    ASSERT_TRUE(zlib::CompressPath2Path(tmp_path, txn_path, content_hash)) <<
      "failed to compress file " << tmp_path << " to " << tmp_path;
    const std::string res_path = backend_storage + "/" +
                                 content_hash->MakePathWithSuffix();
    ASSERT_EQ(0, rename(txn_path.c_str(), res_path.c_str())) <<
      "failed to rename() compressed file " << txn_path << " to " << res_path <<
      " with content hash " << content_hash->ToString() <<
      " (errno: " << errno << ")";
  }

  bool NeedsFilesystemSandbox(const type<LocalObjectFetcher<> > type_spec) { return true;  }
  bool NeedsFilesystemSandbox(const type<HttpObjectFetcher<> >  type_spec) { return true;  }
  bool NeedsFilesystemSandbox(const type<MockObjectFetcher>     type_spec) { return false; }

 private:
  download::DownloadManager    download_manager_;
  signature::SignatureManager  signature_manager_;
};

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::sandbox =
  "/tmp/cvmfs_ut_object_fetcher";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::fqrn = "test.cern.ch";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::backend_storage =
  T_ObjectFetcher<ObjectFetcherT>::sandbox + "/backend";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::backend_storage_dir =
  T_ObjectFetcher<ObjectFetcherT>::sandbox + "/backend/data";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::manifest_path =
  T_ObjectFetcher<ObjectFetcherT>::backend_storage + "/.cvmfspublished";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::whitelist_path =
  T_ObjectFetcher<ObjectFetcherT>::backend_storage + "/.cvmfswhitelist";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::temp_directory =
  T_ObjectFetcher<ObjectFetcherT>::sandbox + "/tmp";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::public_key_path =
  T_ObjectFetcher<ObjectFetcherT>::sandbox + "/" +
  T_ObjectFetcher<ObjectFetcherT>::fqrn + ".pub";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::private_key_path =
  T_ObjectFetcher<ObjectFetcherT>::sandbox + "/" +
  T_ObjectFetcher<ObjectFetcherT>::fqrn + ".key";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::certificate_path =
  T_ObjectFetcher<ObjectFetcherT>::sandbox + "/" +
  T_ObjectFetcher<ObjectFetcherT>::fqrn + ".crt";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::master_key_path =
  T_ObjectFetcher<ObjectFetcherT>::sandbox + "/" +
  T_ObjectFetcher<ObjectFetcherT>::fqrn + ".masterkey";

template <class ObjectFetcherT>
const unsigned int T_ObjectFetcher<ObjectFetcherT>::catalog_revision = 1;

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
}


TYPED_TEST(T_ObjectFetcher, FetchManifestSlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  UniquePtr<manifest::Manifest> manifest(object_fetcher->FetchManifest());
  ASSERT_TRUE(manifest.IsValid());

  EXPECT_EQ(TestFixture::root_hash,    manifest->catalog_hash());
  EXPECT_EQ(TestFixture::history_hash, manifest->history());
}


TYPED_TEST(T_ObjectFetcher, FetchHistorySlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  EXPECT_TRUE(object_fetcher->HasHistory());

  UniquePtr<typename TypeParam::HistoryTN> history(object_fetcher->FetchHistory());
  ASSERT_TRUE(history.IsValid());
  EXPECT_EQ(TestFixture::previous_history_hash, history->previous_revision());
}


TYPED_TEST(T_ObjectFetcher, FetchLegacyHistorySlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  UniquePtr<typename TypeParam::HistoryTN> history(
              object_fetcher->FetchHistory(TestFixture::previous_history_hash));
  ASSERT_TRUE(history.IsValid())
    << "didn't find: " << TestFixture::previous_history_hash.ToStringWithSuffix();
  EXPECT_TRUE(history->previous_revision().IsNull());
}


TYPED_TEST(T_ObjectFetcher, FetchInvalidHistorySlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  UniquePtr<typename TypeParam::HistoryTN> history(
      object_fetcher->FetchHistory(h("400d35465f179a4acacb5fe749e6ce20a0bbdb84",
                                     shash::kSuffixHistory)));
  ASSERT_FALSE(history.IsValid());
}


TYPED_TEST(T_ObjectFetcher, FetchCatalogSlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  UniquePtr<typename TypeParam::CatalogTN> catalog(
    object_fetcher->FetchCatalog(TestFixture::root_hash, ""));

  ASSERT_TRUE(catalog.IsValid());
  EXPECT_EQ("",                            catalog->path().ToString());
  EXPECT_EQ(TestFixture::catalog_revision, catalog->revision());
}


TYPED_TEST(T_ObjectFetcher, FetchInvalidCatalogSlow) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  UniquePtr<typename TypeParam::CatalogTN> catalog(
    object_fetcher->FetchCatalog(h("5739dc30f42525a261b2f4b383b220df3e36f04d",
                                   shash::kSuffixCatalog), ""));
  ASSERT_FALSE(catalog.IsValid());
}


TYPED_TEST(T_ObjectFetcher, AutoCleanupFetchedFilesSlow) {
  if (!TestFixture::NeedsFilesystemSandbox()) {
    // this is only valid if we are actually creating files...
    return;
  }

  typedef typename TestFixture::DirectoryListing DirectoryListing;
  DirectoryListing listing;
  TestFixture::ListDirectory(TestFixture::temp_directory, &listing);

  size_t files = 0;

  EXPECT_EQ(files, listing.size());

  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE(object_fetcher.IsValid());

  UniquePtr<manifest::Manifest> manifest(object_fetcher->FetchManifest());
  ASSERT_TRUE(manifest.IsValid());

  EXPECT_EQ(files, listing.size());

  UniquePtr<typename TypeParam::CatalogTN> catalog(
    object_fetcher->FetchCatalog(TestFixture::root_hash, ""));
  ASSERT_TRUE(catalog.IsValid());

  TestFixture::ListDirectory(TestFixture::temp_directory, &listing);
  EXPECT_LT(files, listing.size());
  files = listing.size();

  UniquePtr<typename TypeParam::HistoryTN> history(object_fetcher->FetchHistory());
  ASSERT_TRUE(history.IsValid());

  TestFixture::ListDirectory(TestFixture::temp_directory, &listing);
  EXPECT_LT(files, listing.size());
  files = listing.size();

  delete object_fetcher.Release();

  TestFixture::ListDirectory(TestFixture::temp_directory, &listing);
  EXPECT_EQ(files, listing.size());

  delete history.Release();

  TestFixture::ListDirectory(TestFixture::temp_directory, &listing);
  EXPECT_GT(files, listing.size());
  files = listing.size();

  delete catalog.Release();

  TestFixture::ListDirectory(TestFixture::temp_directory, &listing);
  EXPECT_GT(files, listing.size());
  files = listing.size();

  EXPECT_EQ(0u, listing.size());
}
