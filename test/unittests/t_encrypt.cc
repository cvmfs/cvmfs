/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "../../cvmfs/encrypt.h"
#include "../../cvmfs/util.h"

using namespace std;  // NOLINT

namespace cipher {

TEST(T_Encrypt, None) {
  CipherNone cipher;
  UniquePtr<Key>k(Key::CreateRandomly(cipher.key_size()));
  ASSERT_TRUE(k.IsValid());

  string empty;
  string dummy = "Hello, World!";
  string ciphertext;
  string plaintext;
  bool retval;

  retval = cipher.Encrypt(empty, *k, &ciphertext);
  EXPECT_TRUE(retval);
  retval = Cipher::Decrypt(ciphertext, *k, &plaintext);
  EXPECT_TRUE(retval);
  EXPECT_EQ(empty, plaintext);

  retval = cipher.Encrypt(dummy, *k, &ciphertext);
  EXPECT_TRUE(retval);
  retval = Cipher::Decrypt(ciphertext, *k, &plaintext);
  EXPECT_TRUE(retval);
  EXPECT_EQ(dummy, plaintext);
}


TEST(T_Encrypt, DecryptWrongEnvelope) {
  CipherNone cipher;
  UniquePtr<Key>k(Key::CreateRandomly(cipher.key_size()));
  ASSERT_TRUE(k.IsValid());
  UniquePtr<Key>k_bad(Key::CreateRandomly(1));
  ASSERT_TRUE(k_bad.IsValid());

  string ciphertext;
  string plaintext;
  int retval = Cipher::Decrypt(ciphertext, *k, &plaintext);
  EXPECT_FALSE(retval);

  ciphertext = "X";
  ciphertext[0] = 0xF0;
  retval = Cipher::Decrypt(ciphertext, *k, &plaintext);
  EXPECT_FALSE(retval);
  ciphertext[0] = 0x0F;
  retval = Cipher::Decrypt(ciphertext, *k, &plaintext);
  EXPECT_FALSE(retval);

  ciphertext[0] = cipher.algorithm() << 4;
  retval = Cipher::Decrypt(ciphertext, *k_bad, &plaintext);
  EXPECT_FALSE(retval);
  retval = Cipher::Decrypt(ciphertext, *k, &plaintext);
  EXPECT_TRUE(retval);
}

}  // namespace cipher
