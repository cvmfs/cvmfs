/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include "signature.h"

using namespace std;  // NOLINT

namespace signature {

class T_Signature : public ::testing::Test {
 protected:
  virtual void SetUp() {
    sign_mgr_.Init();
  }

  virtual void TearDown() {
    sign_mgr_.Fini();
  }

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

}  // namespace signature
