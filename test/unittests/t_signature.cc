/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include "crypto/signature.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace signature {

class T_Signature : public ::testing::Test {
 protected:
  virtual void SetUp() { sign_mgr_.Init(); }

  virtual void TearDown() { sign_mgr_.Fini(); }

  SignatureManager sign_mgr_;
};

TEST_F(T_Signature, Rsa) {
  sign_mgr_.GenerateMasterKeyPair();
  const unsigned char *buffer = reinterpret_cast<const unsigned char *>("abc");
  unsigned char *signature;
  unsigned signature_size;
  ASSERT_TRUE(sign_mgr_.SignRsa(buffer, 3, &signature, &signature_size));
  EXPECT_TRUE(sign_mgr_.VerifyRsa(buffer, 3, signature, signature_size));
  free(signature);
}

TEST_F(T_Signature, Certificate) {
  sign_mgr_.GenerateCertificate("test.cvmfs.io");
  const unsigned char *buffer = reinterpret_cast<const unsigned char *>("abc");
  unsigned char *signature;
  unsigned signature_size;
  ASSERT_TRUE(sign_mgr_.Sign(buffer, 3, &signature, &signature_size));
  EXPECT_TRUE(sign_mgr_.Verify(buffer, 3, signature, signature_size));
  free(signature);

  std::string cert = sign_mgr_.GetCertificate();
  EXPECT_FALSE(cert.empty());
  EXPECT_TRUE(sign_mgr_.LoadCertificateMem(
      reinterpret_cast<const unsigned char *>(cert.data()), cert.length()));
  EXPECT_TRUE(sign_mgr_.KeysMatch());
}

TEST_F(T_Signature, Export) {
  sign_mgr_.GenerateCertificate("test.cvmfs.io");
  EXPECT_TRUE(sign_mgr_.KeysMatch());

  std::string cert = sign_mgr_.GetCertificate();
  std::string privkey = sign_mgr_.GetPrivateKey();
  EXPECT_TRUE(sign_mgr_.LoadCertificateMem(
      reinterpret_cast<const unsigned char *>(cert.data()), cert.length()));
  SafeWriteToFile(privkey, "test.crt", 0600);
  EXPECT_TRUE(sign_mgr_.LoadPrivateKeyPath("test.crt", ""));
  unlink("test.crt");
  EXPECT_TRUE(sign_mgr_.KeysMatch());

  sign_mgr_.GenerateMasterKeyPair();
  std::string pubkeys = sign_mgr_.GetActivePubkeys();
  std::string master_key = sign_mgr_.GetPrivateMasterKey();
  SafeWriteToFile(pubkeys, "key.pub", 0600);
  SafeWriteToFile(master_key, "masterkey.pem", 0600);
  EXPECT_TRUE(sign_mgr_.LoadPublicRsaKeys("key.pub"));
  EXPECT_TRUE(sign_mgr_.LoadPrivateMasterKeyPath("masterkey.pem"));
  unlink("key.pub");
  unlink("masterkey.pem");

  const unsigned char *buffer = reinterpret_cast<const unsigned char *>("abc");
  unsigned char *signature;
  unsigned signature_size;
  ASSERT_TRUE(sign_mgr_.SignRsa(buffer, 3, &signature, &signature_size));
  EXPECT_TRUE(sign_mgr_.VerifyRsa(buffer, 3, signature, signature_size));
  free(signature);
}

TEST_F(T_Signature, GetSetKeys) {
  sign_mgr_.GenerateMasterKeyPair();
  sign_mgr_.GenerateCertificate("test.cvmfs.io");
  EXPECT_TRUE(sign_mgr_.KeysMatch());

  const unsigned char *buffer = reinterpret_cast<const unsigned char *>("abc");
  unsigned char *signature;
  unsigned signature_size;
  EXPECT_TRUE(sign_mgr_.SignRsa(buffer, 3, &signature, &signature_size));
  EXPECT_TRUE(sign_mgr_.VerifyRsa(buffer, 3, signature, signature_size));
  free(signature);
  EXPECT_TRUE(sign_mgr_.Sign(buffer, 3, &signature, &signature_size));
  EXPECT_TRUE(sign_mgr_.Verify(buffer, 3, signature, signature_size));
  free(signature);

  EXPECT_TRUE(
      sign_mgr_.LoadPrivateMasterKeyMem(sign_mgr_.GetPrivateMasterKey()));
  EXPECT_TRUE(sign_mgr_.LoadPrivateKeyMem(sign_mgr_.GetPrivateKey()));
  EXPECT_TRUE(sign_mgr_.SignRsa(buffer, 3, &signature, &signature_size));
  EXPECT_TRUE(sign_mgr_.VerifyRsa(buffer, 3, signature, signature_size));
  free(signature);
  EXPECT_TRUE(sign_mgr_.Sign(buffer, 3, &signature, &signature_size));
  EXPECT_TRUE(sign_mgr_.Verify(buffer, 3, signature, signature_size));
  free(signature);
}

}  // namespace signature
