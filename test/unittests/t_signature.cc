/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdlib>

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
  sign_mgr_.GenerateRsaKeys();
  const unsigned char *buffer = reinterpret_cast<const unsigned char *>("abc");
  unsigned char *signature;
  unsigned signature_size;
  ASSERT_TRUE(sign_mgr_.SignRsa(buffer, 3, &signature, &signature_size));
  EXPECT_TRUE(sign_mgr_.VerifyRsa(buffer, 3, signature, signature_size));
  free(signature);
}

}  // namespace signature
