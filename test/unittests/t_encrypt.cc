/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>

#include "../../cvmfs/encrypt.h"
#include "../../cvmfs/util.h"
#include "testutil.h"

using namespace std;  // NOLINT

namespace cipher {

TEST(T_Encrypt, KeyFiles) {
  CipherNone cipher;
  UniquePtr<Key> k(Key::CreateRandomly(cipher.key_size()));
  ASSERT_TRUE(k.IsValid());

  string tmp_path;
  FILE *f = CreateTempFile("./key", 0600, "w+", &tmp_path);
  ASSERT_TRUE(f != NULL);
  fclose(f);
  EXPECT_FALSE(k->SaveToFile("/no/such/file"));
  EXPECT_TRUE(k->SaveToFile(tmp_path));

  UniquePtr<Key> k_restore1(Key::CreateFromFile(tmp_path));
  ASSERT_TRUE(k_restore1.IsValid());
  EXPECT_EQ(k->size(), k_restore1->size());
  EXPECT_EQ( 0, memcmp(k->data(), k_restore1->data(),
                       std::min(k->size(), k_restore1->size())) );

  EXPECT_EQ(0, truncate(tmp_path.c_str(), 0));
  UniquePtr<Key> k_restore2(Key::CreateFromFile(tmp_path));
  EXPECT_FALSE(k_restore2.IsValid());

  unlink(tmp_path.c_str());
  UniquePtr<Key> k_restore3(Key::CreateFromFile(tmp_path));
  EXPECT_FALSE(k_restore3.IsValid());
}


TEST(T_Encrypt, None) {
  CipherNone cipher;
  UniquePtr<Key> k(Key::CreateRandomly(cipher.key_size()));
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


TEST(T_Encrypt, Aes_256_Cbc) {
  CipherAes256Cbc cipher;
  UniquePtr<Key> k(Key::CreateRandomly(cipher.key_size()));
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

  retval = Cipher::Decrypt(ciphertext.substr(0, 1), *k, &plaintext);
  EXPECT_EQ("", plaintext);
  retval = Cipher::Decrypt(ciphertext.substr(0, 1 + cipher.block_size()),
                           *k, &plaintext);
  EXPECT_EQ("", plaintext);
  retval = Cipher::Decrypt(ciphertext.substr(0, ciphertext.length()-1),
                           *k, &plaintext);
  EXPECT_EQ("", plaintext);
}


TEST(T_Encrypt, DecryptWrongEnvelope) {
  CipherNone cipher;
  UniquePtr<Key> k(Key::CreateRandomly(cipher.key_size()));
  ASSERT_TRUE(k.IsValid());
  UniquePtr<Key> k_bad(Key::CreateRandomly(1));
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
