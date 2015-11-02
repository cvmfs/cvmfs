/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <string>

#include "../../cvmfs/hash.h"
#include "../../cvmfs/prng.h"
#include "../../cvmfs/smalloc.h"

using namespace std;  // NOLINT

TEST(T_Shash, TestVectors) {
  shash::Any md5(shash::kMd5);
  shash::Any sha1(shash::kSha1);
  shash::Any rmd160(shash::kRmd160);
  shash::Any sha256(shash::kSha256);
  shash::Any sha3(shash::kSha3);

  HashString("", &md5);
  HashString("", &sha1);
  HashString("", &rmd160);
  HashString("", &sha256);
  HashString("", &sha3);
  EXPECT_EQ("d41d8cd98f00b204e9800998ecf8427e", md5.ToString());
  EXPECT_EQ("da39a3ee5e6b4b0d3255bfef95601890afd80709", sha1.ToString());
  EXPECT_EQ(
    "9c1185a5c5e9fc54612808977ee8f548b2258d31-rmd160", rmd160.ToString());
  EXPECT_EQ(
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-sha256",
    sha256.ToString());
  EXPECT_EQ(
    "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a-sha3",
    sha3.ToString());

  HashString("abc", &md5);
  HashString("abc", &sha1);
  HashString("abc", &rmd160);
  HashString("abc", &sha256);
  HashString("abc", &sha3);
  EXPECT_EQ("900150983cd24fb0d6963f7d28e17f72", md5.ToString());
  EXPECT_EQ("a9993e364706816aba3e25717850c26c9cd0d89d", sha1.ToString());
  EXPECT_EQ(
    "8eb208f7e05d987a9b044a8e98c6b087f15a0bfc-rmd160", rmd160.ToString());
  EXPECT_EQ(
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad-sha256",
    sha256.ToString());
  EXPECT_EQ(
    "3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532-sha3",
    sha3.ToString());

  HashString("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", &md5);
  HashString("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", &sha1);
  HashString(
    "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", &rmd160);
  HashString(
    "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", &sha256);
  HashString("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", &sha3);
  EXPECT_EQ("8215ef0796a20bcaaae116d3876c664a", md5.ToString());
  EXPECT_EQ("84983e441c3bd26ebaae4aa1f95129e5e54670f1", sha1.ToString());
  EXPECT_EQ(
    "12a053384a9c0c88e405a06c27dcf49ada62eb2b-rmd160", rmd160.ToString());
  EXPECT_EQ(
    "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1-sha256",
    sha256.ToString());
  EXPECT_EQ(
    "41c0dba2a9d6240849100376a8235e2c82e1b9998a999e21db32dd97496d3376-sha3",
    sha3.ToString());

  HashString("abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoi"
             "jklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu", &md5);
  HashString("abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoi"
             "jklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu", &sha1);
  HashString("abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoi"
             "jklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu", &rmd160);
  HashString("abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoi"
             "jklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu", &sha256);
  HashString("abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoi"
             "jklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu", &sha3);
  EXPECT_EQ("03dd8807a93175fb062dfb55dc7d359c", md5.ToString());
  EXPECT_EQ("a49b2446a02c645bf419f995b67091253a04a259", sha1.ToString());
  EXPECT_EQ(
    "6f3fa39b6b503c384f919a49a7aa5c2c08bdfb45-rmd160", rmd160.ToString());
  EXPECT_EQ(
    "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1-sha256",
    sha256.ToString());
  EXPECT_EQ(
    "916f6061fe879741ca6469b43971dfdb28b1a32dc36cb3254e812be27aad1d18-sha3",
    sha3.ToString());

  HashString("The quick brown fox jumps over the lazy dog", &md5);
  HashString("The quick brown fox jumps over the lazy dog", &sha1);
  HashString("The quick brown fox jumps over the lazy dog", &rmd160);
  HashString("The quick brown fox jumps over the lazy dog", &sha256);
  HashString("The quick brown fox jumps over the lazy dog", &sha3);
  EXPECT_EQ("9e107d9d372bb6826bd81d3542a419d6", md5.ToString());
  EXPECT_EQ("2fd4e1c67a2d28fced849ee1bb76e7391b93eb12", sha1.ToString());
  EXPECT_EQ(
    "37f332f68db77bd9d7edd4969571ad671cf9dd3b-rmd160", rmd160.ToString());
  EXPECT_EQ(
    "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592-sha256",
    sha256.ToString());
  EXPECT_EQ(
    "69070dda01975c8c120c3aada1b282394e7f032fa9cf32f4cb2259a0897dfc04-sha3",
    sha3.ToString());

  void *a_1m = smalloc(1000000);
  memset(a_1m, 'a', 1000000);

  HashMem(reinterpret_cast<const unsigned char *>(a_1m), 1000000, &md5);
  EXPECT_EQ("7707d6ae4e027c70eea2a935c2296f21", md5.ToString());

  HashMem(reinterpret_cast<const unsigned char *>(a_1m), 1000000, &sha1);
  EXPECT_EQ("34aa973cd4c4daa4f61eeb2bdbad27316534016f", sha1.ToString());

  HashMem(reinterpret_cast<const unsigned char *>(a_1m), 1000000, &rmd160);
  EXPECT_EQ(
    "52783243c1697bdbe16d37f97f68f08325dc1528-rmd160", rmd160.ToString());

  HashMem(reinterpret_cast<const unsigned char *>(a_1m), 1000000, &sha256);
  EXPECT_EQ(
    "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0-sha256",
    sha256.ToString());

  HashMem(reinterpret_cast<const unsigned char *>(a_1m), 1000000, &sha3);
  EXPECT_EQ(
    "5c8875ae474a3634ba4fd55ec85bffd661f32aca75c6d699d0cdcb6c115891c1-sha3",
    sha3.ToString());

  free(a_1m);
}


TEST(T_Shash, LongTestVectorsSlow) {
  string s = "abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmno";
  unsigned rep_s = 16777216;
  unsigned char zeros[1024];
  memset(zeros, 0, 1024);
  unsigned rep_zeros = 1024*1024*6;  // 6GB of zeros

  shash::Any md5(shash::kMd5);
  shash::Any sha1(shash::kSha1);
  shash::Any rmd160(shash::kRmd160);
  shash::Any sha256(shash::kSha256);
  shash::Any sha3(shash::kSha3);
  shash::ContextPtr context_ptr_md5(shash::kMd5);
  shash::ContextPtr context_ptr_sha1(shash::kSha1);
  shash::ContextPtr context_ptr_rmd160(shash::kRmd160);
  shash::ContextPtr context_ptr_sha256(shash::kSha256);
  shash::ContextPtr context_ptr_sha3(shash::kSha3);
  context_ptr_md5.buffer = smalloc(context_ptr_md5.size);
  context_ptr_sha1.buffer = smalloc(context_ptr_sha1.size);
  context_ptr_rmd160.buffer = smalloc(context_ptr_rmd160.size);
  context_ptr_sha256.buffer = smalloc(context_ptr_sha256.size);
  context_ptr_sha3.buffer = smalloc(context_ptr_sha3.size);

  shash::Init(context_ptr_md5);
  for (unsigned i = 0; i < rep_s; ++i) {
    shash::Update(reinterpret_cast<const unsigned char *>(s.data()), s.length(),
                  context_ptr_md5);
  }
  shash::Final(context_ptr_md5, &md5);
  EXPECT_EQ("d338139169d50f55526194c790ec0448", md5.ToString());

  shash::Init(context_ptr_sha1);
  for (unsigned i = 0; i < rep_s; ++i) {
    shash::Update(reinterpret_cast<const unsigned char *>(s.data()), s.length(),
                  context_ptr_sha1);
  }
  shash::Final(context_ptr_sha1, &sha1);
  EXPECT_EQ("7789f0c9ef7bfc40d93311143dfbe69e2017f592", sha1.ToString());

  shash::Init(context_ptr_rmd160);
  for (unsigned i = 0; i < rep_s; ++i) {
    shash::Update(reinterpret_cast<const unsigned char *>(s.data()), s.length(),
                  context_ptr_rmd160);
  }
  shash::Final(context_ptr_rmd160, &rmd160);
  EXPECT_EQ(
    "29b6df855772aa9a95442bf83b282b495f9f6541-rmd160", rmd160.ToString());

  shash::Init(context_ptr_sha256);
  for (unsigned i = 0; i < rep_s; ++i) {
    shash::Update(reinterpret_cast<const unsigned char *>(s.data()), s.length(),
                  context_ptr_sha256);
  }
  shash::Final(context_ptr_sha256, &sha256);
  EXPECT_EQ(
    "50e72a0e26442fe2552dc3938ac58658228c0cbfb1d2ca872ae435266fcd055e-sha256",
    sha256.ToString());

  shash::Init(context_ptr_sha3);
  for (unsigned i = 0; i < rep_s; ++i) {
    shash::Update(reinterpret_cast<const unsigned char *>(s.data()), s.length(),
                  context_ptr_sha3);
  }
  shash::Final(context_ptr_sha3, &sha3);
  EXPECT_EQ(
    "ecbbc42cbf296603acb2c6bc0410ef4378bafb24b710357f12df607758b33e2b-sha3",
    sha3.ToString());

  shash::Init(context_ptr_md5);
  for (unsigned i = 0; i < rep_zeros; ++i) {
    shash::Update(zeros, 1024, context_ptr_md5);
  }
  shash::Final(context_ptr_md5, &md5);
  EXPECT_EQ("58cf638a733f919007b4287cf5396d0c", md5.ToString());

  shash::Init(context_ptr_sha1);
  for (unsigned i = 0; i < rep_zeros; ++i) {
    shash::Update(zeros, 1024, context_ptr_sha1);
  }
  shash::Final(context_ptr_sha1, &sha1);
  EXPECT_EQ("d5e3c4896b59b3e5f59e8ad658a65f6253a75ce9", sha1.ToString());

  shash::Init(context_ptr_rmd160);
  for (unsigned i = 0; i < rep_zeros; ++i) {
    shash::Update(zeros, 1024, context_ptr_rmd160);
  }
  shash::Final(context_ptr_rmd160, &rmd160);
  EXPECT_EQ(
    "c10dc655d66e1eddd503c4540579d3aa163123a0-rmd160", rmd160.ToString());

  shash::Init(context_ptr_sha256);
  for (unsigned i = 0; i < rep_zeros; ++i) {
    shash::Update(zeros, 1024, context_ptr_sha256);
  }
  shash::Final(context_ptr_sha256, &sha256);
  EXPECT_EQ(
    "5c32c2b28999325bc5ad39d6530bcb46fbdf1f86375a991b7269764c50b0d109-sha256",
    sha256.ToString());

  shash::Init(context_ptr_sha3);
  for (unsigned i = 0; i < rep_zeros; ++i) {
    shash::Update(zeros, 1024, context_ptr_sha3);
  }
  shash::Final(context_ptr_sha3, &sha3);
  EXPECT_EQ(
    "12fe27ad6f3f1869bfff612e87888630345995d773d5365a1870ceb9f8eb7beb-sha3",
    sha3.ToString());

  free(context_ptr_md5.buffer);
  free(context_ptr_sha1.buffer);
  free(context_ptr_rmd160.buffer);
  free(context_ptr_sha256.buffer);
  free(context_ptr_sha3.buffer);
}


TEST(T_Shash, VerifyHex) {
  EXPECT_EQ(shash::HexPtr("").IsValid(), false);
  EXPECT_EQ(shash::HexPtr("012abc").IsValid(), false);
  EXPECT_EQ(shash::HexPtr("A68b329da9893e34099c7d8ad5cb9c94").IsValid(), false);
  EXPECT_EQ(shash::HexPtr("68b329da9893e34099c7d8ad5cb9c9400").IsValid(),
            false);
  EXPECT_EQ(shash::HexPtr("8b329da9893e34099c7d8ad5cb9c940-").IsValid(), false);
  EXPECT_EQ(shash::HexPtr("8b329da9893e34099c7d8ad5cb9c940-rmd160").IsValid(),
            false);
  EXPECT_EQ(shash::HexPtr("68b329da9893e34099c7d8ad5cb9c940").IsValid(),
            true);

  EXPECT_EQ(
    shash::HexPtr("adc83b19e793491b1c6ea0fd8b46cd9f32e592fcX").IsValid(),
    false);
  EXPECT_EQ(
    shash::HexPtr("adc83b19e793491b1c6ea0fd8b46cd9f32e592fc-NO").IsValid(),
    false);
  EXPECT_EQ(shash::HexPtr(
    "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc-longsuffix").IsValid(),
    false);
  EXPECT_EQ(
    shash::HexPtr("adc83b19e793491b1c6ea0fd8b46cd9f32e592fc-rmd161").IsValid(),
    false);

  EXPECT_EQ(shash::HexPtr("adc83b19e793491b1c6ea0fd8b46cd9f32e592fc").IsValid(),
            true);
  EXPECT_EQ(shash::HexPtr(
    "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc-rmd160").IsValid(),
    true);
  EXPECT_EQ(shash::HexPtr(
    "adc83b19e793491b1c6ea0fd8b46cd9f32e592f-rmd160").IsValid(),
    false);
  EXPECT_EQ(shash::HexPtr(
    "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc-rmd1600").IsValid(),
    false);

  EXPECT_EQ(shash::HexPtr(
    "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc-sha256").IsValid(),
    false);
  EXPECT_EQ(shash::HexPtr(
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855").
      IsValid(), false);
  EXPECT_EQ(shash::HexPtr(
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-sha224").
      IsValid(), false);
  EXPECT_EQ(shash::HexPtr(
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-rmd160").
      IsValid(), false);
  EXPECT_EQ(shash::HexPtr(
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-sha256").
      IsValid(), true);

  EXPECT_EQ(shash::HexPtr(
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-sha3").
      IsValid(), true);
  EXPECT_EQ(shash::HexPtr(
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-sha3256").
      IsValid(), false);
}


TEST(T_Shash, IsNull) {
  const shash::Any hash_md5(shash::kMd5);
  ASSERT_TRUE(hash_md5.IsNull());
  EXPECT_EQ("00000000000000000000000000000000", hash_md5.ToString());

  const shash::Any hash_sha1(shash::kSha1);
  ASSERT_TRUE(hash_sha1.IsNull());
  EXPECT_EQ("0000000000000000000000000000000000000000", hash_sha1.ToString());

  const shash::Any hash_rmd160(shash::kRmd160);
  ASSERT_TRUE(hash_rmd160.IsNull());
  EXPECT_EQ("0000000000000000000000000000000000000000-rmd160",
            hash_rmd160.ToString());

  const shash::Any hash_sha256(shash::kSha256);
  ASSERT_TRUE(hash_sha256.IsNull());
  EXPECT_EQ(
    "0000000000000000000000000000000000000000000000000000000000000000-sha256",
    hash_sha256.ToString());

  const shash::Any hash_sha3(shash::kSha3);
  ASSERT_TRUE(hash_sha3.IsNull());
  EXPECT_EQ(
    "0000000000000000000000000000000000000000000000000000000000000000-sha3",
    hash_sha3.ToString());
}


TEST(T_Shash, ToString) {
  Prng prng;
  prng.InitSeed(1337);

  shash::Any hash_md5(shash::kMd5);
  hash_md5.Randomize(&prng);
  ASSERT_FALSE(hash_md5.IsNull());
  EXPECT_EQ("583525ddfde0ebe0b3afff68cde4d983", hash_md5.ToString());

  shash::Any hash_sha1(shash::kSha1);
  hash_sha1.Randomize(&prng);
  ASSERT_FALSE(hash_sha1.IsNull());
  EXPECT_EQ("efc0075d82e876211b66b4b0b91ce2ec217ee60a", hash_sha1.ToString());

  shash::Any hash_rmd160(shash::kRmd160);
  hash_rmd160.Randomize(&prng);
  ASSERT_FALSE(hash_rmd160.IsNull());
  EXPECT_EQ("850b90946048b2760f4d50ce83249dad6317ef10-rmd160",
            hash_rmd160.ToString());

  shash::Any hash_sha256(shash::kSha256);
  hash_sha256.Randomize(&prng);
  ASSERT_FALSE(hash_sha256.IsNull());
  EXPECT_EQ(
    "ea99bef923dd717df9309639b9480bbdf14f1d2a595d878162130f7486f8a5aa-sha256",
    hash_sha256.ToString());

  shash::Any hash_sha3(shash::kSha3);
  hash_sha3.Randomize(&prng);
  ASSERT_FALSE(hash_sha3.IsNull());
  EXPECT_EQ(
    "dbed6dbf462fadde7f397678476394165477bd83adb910027812ac5f0d81ecd6-sha3",
    hash_sha3.ToString());
}


TEST(T_Shash, ToStringWithSuffix) {
  Prng prng;
  prng.InitSeed(1337);

  shash::Any hash_md5(shash::kMd5);
  hash_md5.Randomize(&prng);
  hash_md5.suffix = 'C';
  ASSERT_FALSE(hash_md5.IsNull());
  EXPECT_EQ("583525ddfde0ebe0b3afff68cde4d983C", hash_md5.ToStringWithSuffix());
  EXPECT_EQ("583525ddfde0ebe0b3afff68cde4d983", hash_md5.ToString());

  shash::Any hash_sha1(shash::kSha1);
  hash_sha1.Randomize(&prng);
  hash_sha1.suffix = 'A';
  ASSERT_FALSE(hash_sha1.IsNull());
  EXPECT_EQ("efc0075d82e876211b66b4b0b91ce2ec217ee60aA",
            hash_sha1.ToStringWithSuffix());
  EXPECT_EQ("efc0075d82e876211b66b4b0b91ce2ec217ee60a", hash_sha1.ToString());

  shash::Any hash_rmd160(shash::kRmd160);
  hash_rmd160.Randomize(&prng);
  hash_rmd160.suffix = 'Q';
  ASSERT_FALSE(hash_rmd160.IsNull());
  EXPECT_EQ("850b90946048b2760f4d50ce83249dad6317ef10-rmd160Q",
            hash_rmd160.ToStringWithSuffix());
  EXPECT_EQ("850b90946048b2760f4d50ce83249dad6317ef10-rmd160",
            hash_rmd160.ToString());

  shash::Any hash_sha256(shash::kSha256);
  hash_sha256.Randomize(&prng);
  hash_sha256.suffix = 'L';
  ASSERT_FALSE(hash_sha256.IsNull());
  EXPECT_EQ(
    "ea99bef923dd717df9309639b9480bbdf14f1d2a595d878162130f7486f8a5aa-sha256L",
    hash_sha256.ToStringWithSuffix());
  EXPECT_EQ(
    "ea99bef923dd717df9309639b9480bbdf14f1d2a595d878162130f7486f8a5aa-sha256",
    hash_sha256.ToString());

  shash::Any hash_sha3(shash::kSha3);
  hash_sha3.Randomize(&prng);
  hash_sha3.suffix = 'H';
  ASSERT_FALSE(hash_sha3.IsNull());
  EXPECT_EQ(
    "dbed6dbf462fadde7f397678476394165477bd83adb910027812ac5f0d81ecd6-sha3H",
    hash_sha3.ToStringWithSuffix());
  EXPECT_EQ(
    "dbed6dbf462fadde7f397678476394165477bd83adb910027812ac5f0d81ecd6-sha3",
    hash_sha3.ToString());
}


TEST(T_Shash, ToFingerprint) {
  shash::Any md5(
    shash::kMd5, shash::HexPtr("9fd52a9f04d1ac6735403d16d755c94a"), 'H');
  EXPECT_EQ("9F:D5:2A:9F:04:D1:AC:67:35:40:3D:16:D7:55:C9:4A",
            md5.ToFingerprint(false));
  EXPECT_EQ("9F:D5:2A:9F:04:D1:AC:67:35:40:3D:16:D7:55:C9:4AH",
            md5.ToFingerprint(true));

  shash::Any
    sha1(shash::kSha1,
         shash::HexPtr("cf95c182bb9214bcb9a23fed6658c60d061b45b5"), 'F');
  EXPECT_EQ("CF:95:C1:82:BB:92:14:BC:B9:A2:3F:ED:66:58:C6:0D:06:1B:45:B5",
            sha1.ToFingerprint(false));
  EXPECT_EQ("CF:95:C1:82:BB:92:14:BC:B9:A2:3F:ED:66:58:C6:0D:06:1B:45:B5F",
            sha1.ToFingerprint(true));

  shash::Any
    rmd160(shash::kRmd160,
           shash::HexPtr("5a6e43fe25f5988160a07ff1fb200b29e6c10ad0"), 'M');
  EXPECT_EQ("5A:6E:43:FE:25:F5:98:81:60:A0:7F:F1:FB:20:0B:29:E6:C1:0A:D0"
            "-RMD160", rmd160.ToFingerprint(false));
  EXPECT_EQ("5A:6E:43:FE:25:F5:98:81:60:A0:7F:F1:FB:20:0B:29:E6:C1:0A:D0"
            "-RMD160M", rmd160.ToFingerprint(true));

  shash::Any sha256(shash::kSha256, shash::HexPtr(
    "ea99bef923dd717df9309639b9480bbdf14f1d2a595d878162130f7486f8a5aa"), 'Q');
  EXPECT_EQ("EA:99:BE:F9:23:DD:71:7D:F9:30:96:39:B9:48:0B:BD:F1:4F:1D:2A:59:5D:"
            "87:81:62:13:0F:74:86:F8:A5:AA-SHA256",
            sha256.ToFingerprint(false));
  EXPECT_EQ("EA:99:BE:F9:23:DD:71:7D:F9:30:96:39:B9:48:0B:BD:F1:4F:1D:2A:59:5D:"
            "87:81:62:13:0F:74:86:F8:A5:AA-SHA256Q",
            sha256.ToFingerprint(true));

  shash::Any sha3(shash::kSha3, shash::HexPtr(
    "ea99bef923dd717df9309639b9480bbdf14f1d2a595d878162130f7486f8a5aa"), 'C');
  EXPECT_EQ("EA:99:BE:F9:23:DD:71:7D:F9:30:96:39:B9:48:0B:BD:F1:4F:1D:2A:59:5D:"
            "87:81:62:13:0F:74:86:F8:A5:AA-SHA3",
            sha3.ToFingerprint(false));
  EXPECT_EQ("EA:99:BE:F9:23:DD:71:7D:F9:30:96:39:B9:48:0B:BD:F1:4F:1D:2A:59:5D:"
            "87:81:62:13:0F:74:86:F8:A5:AA-SHA3C",
            sha3.ToFingerprint(true));
}


TEST(T_Shash, InitializeAnyWithSuffix) {
  shash::Any hash_md5(
    shash::kMd5, shash::HexPtr("9fd52a9f04d1ac6735403d16d755c94a"), 'H');
  ASSERT_FALSE(hash_md5.IsNull());
  EXPECT_EQ("9fd52a9f04d1ac6735403d16d755c94aH", hash_md5.ToStringWithSuffix());
  EXPECT_EQ("9fd52a9f04d1ac6735403d16d755c94a", hash_md5.ToString());

  shash::Any
    hash_sha1(shash::kSha1,
              shash::HexPtr("cf95c182bb9214bcb9a23fed6658c60d061b45b5"), 'F');
  ASSERT_FALSE(hash_sha1.IsNull());
  EXPECT_EQ("cf95c182bb9214bcb9a23fed6658c60d061b45b5F",
            hash_sha1.ToStringWithSuffix());
  EXPECT_EQ("cf95c182bb9214bcb9a23fed6658c60d061b45b5",
            hash_sha1.ToString());

  shash::Any
    hash_rmd160(shash::kRmd160,
                shash::HexPtr("5a6e43fe25f5988160a07ff1fb200b29e6c10ad0"), 'M');
  ASSERT_FALSE(hash_rmd160.IsNull());
  EXPECT_EQ("5a6e43fe25f5988160a07ff1fb200b29e6c10ad0-rmd160M",
            hash_rmd160.ToStringWithSuffix());
  EXPECT_EQ("5a6e43fe25f5988160a07ff1fb200b29e6c10ad0-rmd160",
            hash_rmd160.ToString());

  shash::Any hash_sha256(shash::kSha256, shash::HexPtr(
    "ea99bef923dd717df9309639b9480bbdf14f1d2a595d878162130f7486f8a5aa"), 'Q');
  ASSERT_FALSE(hash_sha256.IsNull());
  EXPECT_EQ(hash_sha256.ToStringWithSuffix(),
    "ea99bef923dd717df9309639b9480bbdf14f1d2a595d878162130f7486f8a5aa-sha256Q");
  EXPECT_EQ(hash_sha256.ToString(),
    "ea99bef923dd717df9309639b9480bbdf14f1d2a595d878162130f7486f8a5aa-sha256");

  shash::Any hash_sha3(shash::kSha3, shash::HexPtr(
    "ea99bef923dd717df9309639b9480bbdf14f1d2a595d878162130f7486f8a5aa"), 'C');
  ASSERT_FALSE(hash_sha3.IsNull());
  EXPECT_EQ(hash_sha3.ToStringWithSuffix(),
    "ea99bef923dd717df9309639b9480bbdf14f1d2a595d878162130f7486f8a5aa-sha3C");
  EXPECT_EQ(hash_sha3.ToString(),
    "ea99bef923dd717df9309639b9480bbdf14f1d2a595d878162130f7486f8a5aa-sha3");
}


TEST(T_Shash, MakePathExplicit) {
  Prng prng;
  prng.InitSeed(42);

  shash::Any hash_md5(shash::kMd5);
  hash_md5.Randomize(&prng);
  ASSERT_FALSE(hash_md5.IsNull());
  EXPECT_EQ("91/3969a1ae06052779065b87eb53cb64",
            hash_md5.MakePathExplicit(1, 2));
  EXPECT_EQ("913/969/a1ae06052779065b87eb53cb64",
            hash_md5.MakePathExplicit(2, 3));
  EXPECT_EQ("913969a1ae06052779065b87eb53cb64",
            hash_md5.MakePathExplicit(0, 3));
  EXPECT_EQ("9/1/3/969a1ae06052779065b87eb53cb64",
            hash_md5.MakePathExplicit(3, 1));

  shash::Any hash_sha1(shash::kSha1);
  hash_sha1.Randomize(&prng);
  ASSERT_FALSE(hash_sha1.IsNull());
  EXPECT_EQ("c9/116bb5576d69586386887c6a9eeeb569a406a1",
            hash_sha1.MakePathExplicit(1, 2));
  EXPECT_EQ("c91/16b/b5576d69586386887c6a9eeeb569a406a1",
            hash_sha1.MakePathExplicit(2, 3));
  EXPECT_EQ("c9116bb5576d69586386887c6a9eeeb569a406a1",
            hash_sha1.MakePathExplicit(0, 3));
  EXPECT_EQ("c/9/1/16bb5576d69586386887c6a9eeeb569a406a1",
            hash_sha1.MakePathExplicit(3, 1));

  shash::Any hash_rmd160(shash::kRmd160);
  hash_rmd160.Randomize(&prng);
  ASSERT_FALSE(hash_rmd160.IsNull());
  EXPECT_EQ("2e/5beea626f6ddef63d56405371f80732782086f-rmd160",
            hash_rmd160.MakePathExplicit(1, 2));
  EXPECT_EQ("2e5/bee/a626f6ddef63d56405371f80732782086f-rmd160",
            hash_rmd160.MakePathExplicit(2, 3));
  EXPECT_EQ("2e5beea626f6ddef63d56405371f80732782086f-rmd160",
            hash_rmd160.MakePathExplicit(0, 3));
  EXPECT_EQ("2/e/5/beea626f6ddef63d56405371f80732782086f-rmd160",
            hash_rmd160.MakePathExplicit(3, 1));

  shash::Any hash_sha256(shash::kSha256);
  hash_sha256.Randomize(&prng);
  ASSERT_FALSE(hash_sha256.IsNull());
  EXPECT_EQ(hash_sha256.MakePathExplicit(1, 2), "51/"
    "c00c437200dac16a2efcf04234ddde05e1665519a85df98572985bbd9881e3-sha256");
  EXPECT_EQ(hash_sha256.MakePathExplicit(1, 3), "51c/"
    "00c437200dac16a2efcf04234ddde05e1665519a85df98572985bbd9881e3-sha256");

  shash::Any hash_sha3(shash::kSha3);
  hash_sha3.Randomize(&prng);
  ASSERT_FALSE(hash_sha3.IsNull());
  EXPECT_EQ(hash_sha3.MakePathExplicit(1, 2), "b4/"
    "a4e6a81ae7031eda54bb3b00d5df5d924e7d6ba50cea9455aba9c0f469edce-sha3");
  EXPECT_EQ(hash_sha3.MakePathExplicit(1, 3), "b4a/"
    "4e6a81ae7031eda54bb3b00d5df5d924e7d6ba50cea9455aba9c0f469edce-sha3");
}


TEST(T_Shash, MakePathDefault) {
  Prng prng;
  prng.InitSeed(27111987);

  shash::Any hash_md5(shash::kMd5);
  hash_md5.Randomize(&prng);
  ASSERT_FALSE(hash_md5.IsNull());
  EXPECT_EQ("95/9a032bcfdd999742a321eb0daeddd5", hash_md5.MakePath());
  hash_md5.suffix = 'Q';
  EXPECT_EQ("95/9a032bcfdd999742a321eb0daeddd5Q", hash_md5.MakePath());

  shash::Any hash_sha1(shash::kSha1);
  hash_sha1.Randomize(&prng);
  ASSERT_FALSE(hash_sha1.IsNull());
  EXPECT_EQ("cf/3e56cf3da37ad17cf1f2c2ff5d86497fc29068", hash_sha1.MakePath());
  hash_sha1.suffix = 'V';
  EXPECT_EQ("cf/3e56cf3da37ad17cf1f2c2ff5d86497fc29068V", hash_sha1.MakePath());

  shash::Any hash_rmd160(shash::kRmd160);
  hash_rmd160.Randomize(&prng);
  ASSERT_FALSE(hash_rmd160.IsNull());
  EXPECT_EQ("aa/1deda59d5329553580d78fcd0b393157a5d28e-rmd160",
            hash_rmd160.MakePath());
  hash_rmd160.suffix = 'C';
  EXPECT_EQ("aa/1deda59d5329553580d78fcd0b393157a5d28e-rmd160C",
            hash_rmd160.MakePath());

  shash::Any hash_sha256(shash::kSha256);
  hash_sha256.Randomize(&prng);
  ASSERT_FALSE(hash_sha256.IsNull());
  EXPECT_EQ("01/52bb2ee41313d8d63b9274230f90379adf92017d479810ac4f796a28925527"
            "-sha256", hash_sha256.MakePath());
  hash_sha256.suffix = 'D';
  EXPECT_EQ("01/52bb2ee41313d8d63b9274230f90379adf92017d479810ac4f796a28925527"
            "-sha256D", hash_sha256.MakePath());

  shash::Any hash_sha3(shash::kSha3);
  hash_sha3.Randomize(&prng);
  ASSERT_FALSE(hash_sha3.IsNull());
  EXPECT_EQ("49/cadab9d5427330e8e14aea6f430872fd1c9cfdbbd147f49d8cf2ac6a576199"
            "-sha3", hash_sha3.MakePath());
  hash_sha3.suffix = 'H';
  EXPECT_EQ("49/cadab9d5427330e8e14aea6f430872fd1c9cfdbbd147f49d8cf2ac6a576199"
            "-sha3H", hash_sha3.MakePath());
}


TEST(T_Shash, MakePathWithoutSuffix) {
  Prng prng;
  prng.InitSeed(27111987);

  shash::Any hash_md5(shash::kMd5);
  hash_md5.Randomize(&prng);
  ASSERT_FALSE(hash_md5.IsNull());
  EXPECT_EQ("95/9a032bcfdd999742a321eb0daeddd5",
            hash_md5.MakePathWithoutSuffix());
  hash_md5.suffix = 'Q';
  EXPECT_EQ("95/9a032bcfdd999742a321eb0daeddd5",
            hash_md5.MakePathWithoutSuffix());

  shash::Any hash_sha1(shash::kSha1);
  hash_sha1.Randomize(&prng);
  ASSERT_FALSE(hash_sha1.IsNull());
  EXPECT_EQ("cf/3e56cf3da37ad17cf1f2c2ff5d86497fc29068",
            hash_sha1.MakePathWithoutSuffix());
  hash_sha1.suffix = 'V';
  EXPECT_EQ("cf/3e56cf3da37ad17cf1f2c2ff5d86497fc29068",
            hash_sha1.MakePathWithoutSuffix());

  shash::Any hash_rmd160(shash::kRmd160);
  hash_rmd160.Randomize(&prng);
  ASSERT_FALSE(hash_rmd160.IsNull());
  EXPECT_EQ("aa/1deda59d5329553580d78fcd0b393157a5d28e-rmd160",
            hash_rmd160.MakePathWithoutSuffix());
  hash_rmd160.suffix = 'C';
  EXPECT_EQ("aa/1deda59d5329553580d78fcd0b393157a5d28e-rmd160",
            hash_rmd160.MakePathWithoutSuffix());

  shash::Any hash_sha256(shash::kSha256);
  hash_sha256.Randomize(&prng);
  ASSERT_FALSE(hash_sha256.IsNull());
  EXPECT_EQ("01/52bb2ee41313d8d63b9274230f90379adf92017d479810ac4f796a28925527"
            "-sha256", hash_sha256.MakePathWithoutSuffix());
  hash_sha256.suffix = 'D';
  EXPECT_EQ("01/52bb2ee41313d8d63b9274230f90379adf92017d479810ac4f796a28925527"
            "-sha256", hash_sha256.MakePathWithoutSuffix());

  shash::Any hash_sha3(shash::kSha3);
  hash_sha3.Randomize(&prng);
  ASSERT_FALSE(hash_sha3.IsNull());
  EXPECT_EQ("49/cadab9d5427330e8e14aea6f430872fd1c9cfdbbd147f49d8cf2ac6a576199"
            "-sha3", hash_sha3.MakePathWithoutSuffix());
  hash_sha3.suffix = 'H';
  EXPECT_EQ("49/cadab9d5427330e8e14aea6f430872fd1c9cfdbbd147f49d8cf2ac6a576199"
            "-sha3", hash_sha3.MakePathWithoutSuffix());
}


TEST(T_Shash, HashSuffix) {
  Prng prng;
  prng.InitSeed(9);

  shash::Any hash_md5(shash::kMd5);
  hash_md5.Randomize(&prng);
  hash_md5.suffix = 'A';
  ASSERT_FALSE(hash_md5.IsNull());
  ASSERT_TRUE(hash_md5.HasSuffix());
  EXPECT_EQ("2ec5fe3c17045abdb136a5e6a913e32a",
            hash_md5.ToString());
  EXPECT_EQ("2ec5fe3c17045abdb136a5e6a913e32aA",
            hash_md5.ToStringWithSuffix());
  EXPECT_EQ("2e/c5fe3c17045abdb136a5e6a913e32a",
            hash_md5.MakePathWithoutSuffix());
  EXPECT_EQ("2e/c5fe3c17045abdb136a5e6a913e32aA",
            hash_md5.MakePath());

  shash::Any hash_sha1(shash::kSha1);
  hash_sha1.Randomize(&prng);
  hash_sha1.suffix = 'B';
  ASSERT_FALSE(hash_sha1.IsNull());
  ASSERT_TRUE(hash_sha1.HasSuffix());
  EXPECT_EQ("b75ae68b53d2fc149b77e504132d37569b7e766b",
            hash_sha1.ToString());
  EXPECT_EQ("b75ae68b53d2fc149b77e504132d37569b7e766bB",
            hash_sha1.ToStringWithSuffix());
  EXPECT_EQ("b7/5ae68b53d2fc149b77e504132d37569b7e766b",
            hash_sha1.MakePathWithoutSuffix());
  EXPECT_EQ("b7/5ae68b53d2fc149b77e504132d37569b7e766bB",
            hash_sha1.MakePath());

  shash::Any hash_rmd160(shash::kRmd160);
  hash_rmd160.Randomize(&prng);
  hash_rmd160.suffix = 'C';
  ASSERT_FALSE(hash_rmd160.IsNull());
  ASSERT_TRUE(hash_rmd160.HasSuffix());
  EXPECT_EQ("a74a19bd6162343a21c8590aa9cebca9014c636d-rmd160",
            hash_rmd160.ToString());
  EXPECT_EQ("a74a19bd6162343a21c8590aa9cebca9014c636d-rmd160C",
            hash_rmd160.ToStringWithSuffix());
  EXPECT_EQ("a7/4a19bd6162343a21c8590aa9cebca9014c636d-rmd160",
            hash_rmd160.MakePathWithoutSuffix());
  EXPECT_EQ("a7/4a19bd6162343a21c8590aa9cebca9014c636d-rmd160C",
            hash_rmd160.MakePath());
}


TEST(T_Shash, Equality) {
  shash::Any hash_md5_1(shash::kMd5); ASSERT_TRUE(hash_md5_1.IsNull());
  shash::Any hash_md5_2(shash::kMd5); ASSERT_TRUE(hash_md5_2.IsNull());
  shash::Any hash_md5_3(shash::kMd5); hash_md5_3.Randomize(1337);
  shash::Any hash_md5_4(shash::kMd5); hash_md5_4.Randomize(1337);
  shash::Any hash_md5_5(shash::kMd5); hash_md5_5.Randomize(42);
  shash::Any hash_md5_6(shash::kMd5); hash_md5_6.Randomize(42);
  shash::Any hash_md5_7(shash::kMd5); ASSERT_TRUE(hash_md5_7.IsNull());
  hash_md5_7.suffix = 'A';
  shash::Any hash_md5_8(shash::kMd5); ASSERT_TRUE(hash_md5_8.IsNull());
  hash_md5_8.suffix = 'A';
  shash::Any hash_md5_9(shash::kMd5); hash_md5_9.Randomize(7);
  hash_md5_9.suffix = 'A';
  shash::Any hash_md5_0(shash::kMd5); hash_md5_0.Randomize(7);
  hash_md5_0.suffix = 'A';

  EXPECT_EQ(hash_md5_1, hash_md5_2); EXPECT_EQ(hash_md5_1, hash_md5_1);
  EXPECT_EQ(hash_md5_3, hash_md5_4); EXPECT_EQ(hash_md5_3, hash_md5_3);
  EXPECT_EQ(hash_md5_5, hash_md5_6); EXPECT_EQ(hash_md5_5, hash_md5_5);
  EXPECT_EQ(hash_md5_7, hash_md5_8); EXPECT_EQ(hash_md5_7, hash_md5_7);
  EXPECT_EQ(hash_md5_9, hash_md5_0); EXPECT_EQ(hash_md5_9, hash_md5_9);

  EXPECT_EQ(hash_md5_1, hash_md5_7); EXPECT_EQ(hash_md5_1, hash_md5_8);
  EXPECT_EQ(hash_md5_7, hash_md5_1); EXPECT_EQ(hash_md5_7, hash_md5_2);

  EXPECT_NE(hash_md5_1, hash_md5_3); EXPECT_NE(hash_md5_1, hash_md5_4);
  EXPECT_NE(hash_md5_1, hash_md5_5); EXPECT_NE(hash_md5_1, hash_md5_6);
  EXPECT_NE(hash_md5_1, hash_md5_9); EXPECT_NE(hash_md5_1, hash_md5_0);

  EXPECT_NE(hash_md5_3, hash_md5_1); EXPECT_NE(hash_md5_3, hash_md5_2);
  EXPECT_NE(hash_md5_3, hash_md5_5); EXPECT_NE(hash_md5_3, hash_md5_6);
  EXPECT_NE(hash_md5_3, hash_md5_7); EXPECT_NE(hash_md5_3, hash_md5_8);
  EXPECT_NE(hash_md5_3, hash_md5_9); EXPECT_NE(hash_md5_3, hash_md5_0);

  EXPECT_NE(hash_md5_5, hash_md5_1); EXPECT_NE(hash_md5_5, hash_md5_2);
  EXPECT_NE(hash_md5_5, hash_md5_3); EXPECT_NE(hash_md5_5, hash_md5_4);
  EXPECT_NE(hash_md5_5, hash_md5_7); EXPECT_NE(hash_md5_5, hash_md5_8);
  EXPECT_NE(hash_md5_5, hash_md5_9); EXPECT_NE(hash_md5_5, hash_md5_0);

  EXPECT_NE(hash_md5_7, hash_md5_3); EXPECT_NE(hash_md5_7, hash_md5_4);
  EXPECT_NE(hash_md5_7, hash_md5_5); EXPECT_NE(hash_md5_7, hash_md5_6);
  EXPECT_NE(hash_md5_7, hash_md5_9); EXPECT_NE(hash_md5_7, hash_md5_0);

  EXPECT_NE(hash_md5_9, hash_md5_1); EXPECT_NE(hash_md5_9, hash_md5_2);
  EXPECT_NE(hash_md5_9, hash_md5_3); EXPECT_NE(hash_md5_9, hash_md5_4);
  EXPECT_NE(hash_md5_9, hash_md5_5); EXPECT_NE(hash_md5_9, hash_md5_6);
  EXPECT_NE(hash_md5_9, hash_md5_7); EXPECT_NE(hash_md5_9, hash_md5_8);

  shash::Any hash_sha1_1(shash::kSha1); ASSERT_TRUE(hash_sha1_1.IsNull());
  shash::Any hash_sha1_2(shash::kSha1); ASSERT_TRUE(hash_sha1_2.IsNull());
  shash::Any hash_sha1_3(shash::kSha1); hash_sha1_3.Randomize(153);
  shash::Any hash_sha1_4(shash::kSha1); hash_sha1_4.Randomize(153);
  shash::Any hash_sha1_5(shash::kSha1); hash_sha1_5.Randomize(8761);
  shash::Any hash_sha1_6(shash::kSha1); hash_sha1_6.Randomize(8761);
  shash::Any hash_sha1_7(shash::kSha1);
  ASSERT_TRUE(hash_sha1_7.IsNull()); hash_sha1_7.suffix = 'B';
  shash::Any hash_sha1_8(shash::kSha1);
  ASSERT_TRUE(hash_sha1_8.IsNull()); hash_sha1_8.suffix = 'B';
  shash::Any hash_sha1_9(shash::kSha1); hash_sha1_9.Randomize(1);
  hash_sha1_9.suffix = 'B';
  shash::Any hash_sha1_0(shash::kSha1); hash_sha1_0.Randomize(1);
  hash_sha1_0.suffix = 'B';

  EXPECT_EQ(hash_sha1_1, hash_sha1_2); EXPECT_EQ(hash_sha1_1, hash_sha1_1);
  EXPECT_EQ(hash_sha1_3, hash_sha1_4); EXPECT_EQ(hash_sha1_3, hash_sha1_3);
  EXPECT_EQ(hash_sha1_5, hash_sha1_6); EXPECT_EQ(hash_sha1_5, hash_sha1_5);
  EXPECT_EQ(hash_sha1_7, hash_sha1_8); EXPECT_EQ(hash_sha1_7, hash_sha1_7);
  EXPECT_EQ(hash_sha1_9, hash_sha1_0); EXPECT_EQ(hash_sha1_9, hash_sha1_9);

  EXPECT_EQ(hash_sha1_1, hash_sha1_7); EXPECT_EQ(hash_sha1_1, hash_sha1_8);
  EXPECT_EQ(hash_sha1_7, hash_sha1_1); EXPECT_EQ(hash_sha1_7, hash_sha1_2);

  EXPECT_NE(hash_sha1_1, hash_sha1_3); EXPECT_NE(hash_sha1_1, hash_sha1_4);
  EXPECT_NE(hash_sha1_1, hash_sha1_5); EXPECT_NE(hash_sha1_1, hash_sha1_6);
  EXPECT_NE(hash_sha1_1, hash_sha1_9); EXPECT_NE(hash_sha1_1, hash_sha1_0);

  EXPECT_NE(hash_sha1_3, hash_sha1_1); EXPECT_NE(hash_sha1_3, hash_sha1_2);
  EXPECT_NE(hash_sha1_3, hash_sha1_5); EXPECT_NE(hash_sha1_3, hash_sha1_6);
  EXPECT_NE(hash_sha1_3, hash_sha1_7); EXPECT_NE(hash_sha1_3, hash_sha1_8);
  EXPECT_NE(hash_sha1_3, hash_sha1_9); EXPECT_NE(hash_sha1_3, hash_sha1_0);

  EXPECT_NE(hash_sha1_5, hash_sha1_1); EXPECT_NE(hash_sha1_5, hash_sha1_2);
  EXPECT_NE(hash_sha1_5, hash_sha1_3); EXPECT_NE(hash_sha1_5, hash_sha1_4);
  EXPECT_NE(hash_sha1_5, hash_sha1_7); EXPECT_NE(hash_sha1_5, hash_sha1_8);
  EXPECT_NE(hash_sha1_5, hash_sha1_9); EXPECT_NE(hash_sha1_5, hash_sha1_0);

  EXPECT_NE(hash_sha1_7, hash_sha1_3); EXPECT_NE(hash_sha1_7, hash_sha1_4);
  EXPECT_NE(hash_sha1_7, hash_sha1_5); EXPECT_NE(hash_sha1_7, hash_sha1_6);
  EXPECT_NE(hash_sha1_7, hash_sha1_9); EXPECT_NE(hash_sha1_7, hash_sha1_0);

  EXPECT_NE(hash_sha1_9, hash_sha1_1); EXPECT_NE(hash_sha1_9, hash_sha1_2);
  EXPECT_NE(hash_sha1_9, hash_sha1_3); EXPECT_NE(hash_sha1_9, hash_sha1_4);
  EXPECT_NE(hash_sha1_9, hash_sha1_5); EXPECT_NE(hash_sha1_9, hash_sha1_6);
  EXPECT_NE(hash_sha1_9, hash_sha1_7); EXPECT_NE(hash_sha1_9, hash_sha1_8);

  shash::Any hash_rmd_1(shash::kRmd160); ASSERT_TRUE(hash_rmd_1.IsNull());
  shash::Any hash_rmd_2(shash::kRmd160); ASSERT_TRUE(hash_rmd_2.IsNull());
  shash::Any hash_rmd_3(shash::kRmd160); hash_rmd_3.Randomize(234);
  shash::Any hash_rmd_4(shash::kRmd160); hash_rmd_4.Randomize(234);
  shash::Any hash_rmd_5(shash::kRmd160); hash_rmd_5.Randomize(883);
  shash::Any hash_rmd_6(shash::kRmd160); hash_rmd_6.Randomize(883);
  shash::Any hash_rmd_7(shash::kRmd160); ASSERT_TRUE(hash_rmd_7.IsNull());
  hash_rmd_7.suffix = 'C';
  shash::Any hash_rmd_8(shash::kRmd160); ASSERT_TRUE(hash_rmd_8.IsNull());
  hash_rmd_8.suffix = 'C';
  shash::Any hash_rmd_9(shash::kRmd160); hash_rmd_9.Randomize(8);
  hash_rmd_9.suffix = 'C';
  shash::Any hash_rmd_0(shash::kRmd160); hash_rmd_0.Randomize(8);
  hash_rmd_0.suffix = 'C';

  EXPECT_EQ(hash_rmd_1, hash_rmd_2); EXPECT_EQ(hash_rmd_1, hash_rmd_1);
  EXPECT_EQ(hash_rmd_3, hash_rmd_4); EXPECT_EQ(hash_rmd_3, hash_rmd_3);
  EXPECT_EQ(hash_rmd_5, hash_rmd_6); EXPECT_EQ(hash_rmd_5, hash_rmd_5);
  EXPECT_EQ(hash_rmd_7, hash_rmd_8); EXPECT_EQ(hash_rmd_7, hash_rmd_7);
  EXPECT_EQ(hash_rmd_9, hash_rmd_0); EXPECT_EQ(hash_rmd_9, hash_rmd_9);

  EXPECT_EQ(hash_rmd_1, hash_rmd_7); EXPECT_EQ(hash_rmd_1, hash_rmd_8);
  EXPECT_EQ(hash_rmd_7, hash_rmd_1); EXPECT_EQ(hash_rmd_7, hash_rmd_2);

  EXPECT_NE(hash_rmd_1, hash_rmd_3); EXPECT_NE(hash_rmd_1, hash_rmd_4);
  EXPECT_NE(hash_rmd_1, hash_rmd_5); EXPECT_NE(hash_rmd_1, hash_rmd_6);
  EXPECT_NE(hash_rmd_1, hash_rmd_9); EXPECT_NE(hash_rmd_1, hash_rmd_0);

  EXPECT_NE(hash_rmd_3, hash_rmd_1); EXPECT_NE(hash_rmd_3, hash_rmd_2);
  EXPECT_NE(hash_rmd_3, hash_rmd_5); EXPECT_NE(hash_rmd_3, hash_rmd_6);
  EXPECT_NE(hash_rmd_3, hash_rmd_7); EXPECT_NE(hash_rmd_3, hash_rmd_8);
  EXPECT_NE(hash_rmd_3, hash_rmd_9); EXPECT_NE(hash_rmd_3, hash_rmd_0);

  EXPECT_NE(hash_rmd_5, hash_rmd_1); EXPECT_NE(hash_rmd_5, hash_rmd_2);
  EXPECT_NE(hash_rmd_5, hash_rmd_3); EXPECT_NE(hash_rmd_5, hash_rmd_4);
  EXPECT_NE(hash_rmd_5, hash_rmd_7); EXPECT_NE(hash_rmd_5, hash_rmd_8);
  EXPECT_NE(hash_rmd_5, hash_rmd_9); EXPECT_NE(hash_rmd_5, hash_rmd_0);

  EXPECT_NE(hash_rmd_7, hash_rmd_3); EXPECT_NE(hash_rmd_7, hash_rmd_4);
  EXPECT_NE(hash_rmd_7, hash_rmd_5); EXPECT_NE(hash_rmd_7, hash_rmd_6);
  EXPECT_NE(hash_rmd_7, hash_rmd_9); EXPECT_NE(hash_rmd_7, hash_rmd_0);

  EXPECT_NE(hash_rmd_9, hash_rmd_1); EXPECT_NE(hash_rmd_9, hash_rmd_2);
  EXPECT_NE(hash_rmd_9, hash_rmd_3); EXPECT_NE(hash_rmd_9, hash_rmd_4);
  EXPECT_NE(hash_rmd_9, hash_rmd_5); EXPECT_NE(hash_rmd_9, hash_rmd_6);
  EXPECT_NE(hash_rmd_9, hash_rmd_7); EXPECT_NE(hash_rmd_9, hash_rmd_8);

  shash::Sha256 sha256_null;
  shash::Sha256 sha256_random;  sha256_random.Randomize(42);
  EXPECT_EQ(sha256_random, sha256_random);
  EXPECT_NE(sha256_random, sha256_null);
  shash::Any hash_sha256_1(shash::kSha256);
  shash::Any hash_sha256_2(shash::kSha256);
  EXPECT_EQ(hash_sha256_1, hash_sha256_2);

  shash::Sha3 sha3_null;
  shash::Sha3 sha3_random;  sha3_random.Randomize(42);
  EXPECT_EQ(sha3_random, sha3_random);
  EXPECT_NE(sha3_random, sha3_null);
  shash::Any hash_sha3_1(shash::kSha3);
  shash::Any hash_sha3_2(shash::kSha3);
  EXPECT_EQ(hash_sha3_1, hash_sha3_2);
}


template <shash::Algorithms algo_>
static shash::Any make_hash(const std::string &hash, const char suffix) {
  shash::Any any_hash(algo_, shash::HexPtr(hash));
  any_hash.suffix = suffix;
  return any_hash;
}

static shash::Any md5(const std::string &hash, const char suffix = 0) {
  return make_hash<shash::kMd5>(hash, suffix);
}
static shash::Any sha1(const std::string &hash, const char suffix = 0) {
  return make_hash<shash::kSha1>(hash, suffix);
}
static shash::Any rmd160(const std::string &hash, const char suffix = 0) {
  return make_hash<shash::kRmd160>(hash, suffix);
}
static shash::Any sha256(const std::string &hash, const char suffix = 0) {
  return make_hash<shash::kSha256>(hash, suffix);
}

static shash::Any sha3(const std::string &hash, const char suffix = 0) {
  return make_hash<shash::kSha3>(hash, suffix);
}


TEST(T_Shash, LowerThan) {
  EXPECT_LT(md5("00000000000000000000000000000000"),
            md5("3a5ebf256415b7f4cce58505026c95f9"));
  EXPECT_LT(md5("1de4351771a2485cf9f529c696404eaa"),
            md5("3a5ebf256415b7f4cce58505026c95f9"));
  EXPECT_LT(md5("3a5ebf256415b7f4cce58505026c95f9"),
            md5("ae1332b41f2ec2e455eae5908fa8eca1"));
  EXPECT_LT(md5("ae1332b41f2ec2e455eae5908fa8eca1"),
            md5("ffffffffffffffffffffffffffffffff"));
  EXPECT_LT(md5("00000000000000000000000000000000"),
            md5("ffffffffffffffffffffffffffffffff"));

  EXPECT_EQ(md5("9facbf452def2d7efc5b5c48cdb837fa", 'A'),
            md5("9facbf452def2d7efc5b5c48cdb837fa", 'B'));
  EXPECT_EQ(md5("9facbf452def2d7efc5b5c48cdb837fa", 'B'),
            md5("9facbf452def2d7efc5b5c48cdb837fa", 'C'));
  EXPECT_EQ(md5("9facbf452def2d7efc5b5c48cdb837fa", 'C'),
            md5("9facbf452def2d7efc5b5c48cdb837fa", 'D'));
  EXPECT_EQ(md5("9facbf452def2d7efc5b5c48cdb837fa", 'A'),
            md5("9facbf452def2d7efc5b5c48cdb837fa", 'D'));


  EXPECT_LT(sha1("0000000000000000000000000000000000000000"),
            sha1("3cc2a7bc3db3ce79ebcb75ca3f01680ec74e9fbd"));
  EXPECT_LT(sha1("3cc2a7bc3db3ce79ebcb75ca3f01680ec74e9fbd"),
            sha1("813cdb0163dcff260f1a0ca2647184a2877f1c7f"));
  EXPECT_LT(sha1("813cdb0163dcff260f1a0ca2647184a2877f1c7f"),
            sha1("da39a3ee5e6b4b0d3255bfef95601890afd80709"));
  EXPECT_LT(sha1("da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            sha1("ffffffffffffffffffffffffffffffffffffffff"));
  EXPECT_LT(sha1("0000000000000000000000000000000000000000"),
            sha1("ffffffffffffffffffffffffffffffffffffffff"));

  EXPECT_EQ(sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'A'),
            sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'B'));
  EXPECT_EQ(sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'B'),
            sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'C'));
  EXPECT_EQ(sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'C'),
            sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'D'));
  EXPECT_EQ(sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'A'),
            sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'D'));


  EXPECT_LT(rmd160("0000000000000000000000000000000000000000"),
            rmd160("7e1363cebb1e1f4584111343da5a2d84858a2349"));
  EXPECT_LT(rmd160("7e1363cebb1e1f4584111343da5a2d84858a2349"),
            rmd160("a7fcc792608978d3e6e01352e45a3a66e1488fcc"));
  EXPECT_LT(rmd160("a7fcc792608978d3e6e01352e45a3a66e1488fcc"),
            rmd160("d61c9a94f632a51b5920a91f5ad6ff6f0b36e968"));
  EXPECT_LT(rmd160("d61c9a94f632a51b5920a91f5ad6ff6f0b36e968"),
            rmd160("ffffffffffffffffffffffffffffffffffffffff"));
  EXPECT_LT(rmd160("0000000000000000000000000000000000000000"),
            rmd160("ffffffffffffffffffffffffffffffffffffffff"));

  EXPECT_EQ(rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'A'),
            rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'B'));
  EXPECT_EQ(rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'B'),
            rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'C'));
  EXPECT_EQ(rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'C'),
            rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'D'));
  EXPECT_EQ(rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'A'),
            rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'D'));

  EXPECT_LT(
    sha256("0000000000000000000000000000000000000000000000000000000000000000"),
    sha256("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"));
  EXPECT_EQ(sha256(
    "0000000000000000000000000000000000000000000000000000000000000000", 'A'),
    sha256(
    "0000000000000000000000000000000000000000000000000000000000000000", 'B'));

  EXPECT_LT(
    sha3("0000000000000000000000000000000000000000000000000000000000000000"),
    sha3("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"));
  EXPECT_EQ(sha3(
    "0000000000000000000000000000000000000000000000000000000000000000", 'A'),
    sha3(
    "0000000000000000000000000000000000000000000000000000000000000000", 'B'));
}


TEST(T_Shash, GreaterThan) {
  EXPECT_GT(md5("3a5ebf256415b7f4cce58505026c95f9"),
            md5("00000000000000000000000000000000"));
  EXPECT_GT(md5("3a5ebf256415b7f4cce58505026c95f9"),
            md5("1de4351771a2485cf9f529c696404eaa"));
  EXPECT_GT(md5("ae1332b41f2ec2e455eae5908fa8eca1"),
            md5("3a5ebf256415b7f4cce58505026c95f9"));
  EXPECT_GT(md5("ffffffffffffffffffffffffffffffff"),
            md5("ae1332b41f2ec2e455eae5908fa8eca1"));
  EXPECT_GT(md5("ffffffffffffffffffffffffffffffff"),
            md5("00000000000000000000000000000000"));

  EXPECT_EQ(md5("9facbf452def2d7efc5b5c48cdb837fa", 'B'),
            md5("9facbf452def2d7efc5b5c48cdb837fa", 'A'));
  EXPECT_EQ(md5("9facbf452def2d7efc5b5c48cdb837fa", 'C'),
            md5("9facbf452def2d7efc5b5c48cdb837fa", 'B'));
  EXPECT_EQ(md5("9facbf452def2d7efc5b5c48cdb837fa", 'D'),
            md5("9facbf452def2d7efc5b5c48cdb837fa", 'C'));
  EXPECT_EQ(md5("9facbf452def2d7efc5b5c48cdb837fa", 'D'),
            md5("9facbf452def2d7efc5b5c48cdb837fa", 'A'));

  EXPECT_GT(sha1("3cc2a7bc3db3ce79ebcb75ca3f01680ec74e9fbd"),
            sha1("0000000000000000000000000000000000000000"));
  EXPECT_GT(sha1("813cdb0163dcff260f1a0ca2647184a2877f1c7f"),
            sha1("3cc2a7bc3db3ce79ebcb75ca3f01680ec74e9fbd"));
  EXPECT_GT(sha1("da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            sha1("813cdb0163dcff260f1a0ca2647184a2877f1c7f"));
  EXPECT_GT(sha1("ffffffffffffffffffffffffffffffffffffffff"),
            sha1("da39a3ee5e6b4b0d3255bfef95601890afd80709"));
  EXPECT_GT(sha1("ffffffffffffffffffffffffffffffffffffffff"),
            sha1("0000000000000000000000000000000000000000"));

  EXPECT_EQ(sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'B'),
            sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'A'));
  EXPECT_EQ(sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'C'),
            sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'B'));
  EXPECT_EQ(sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'D'),
            sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'C'));
  EXPECT_EQ(sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'D'),
            sha1("6c973e8803b3fbaabfb09dd916e295ed24da1d43", 'A'));


  EXPECT_GT(rmd160("7e1363cebb1e1f4584111343da5a2d84858a2349"),
            rmd160("0000000000000000000000000000000000000000"));
  EXPECT_GT(rmd160("a7fcc792608978d3e6e01352e45a3a66e1488fcc"),
            rmd160("7e1363cebb1e1f4584111343da5a2d84858a2349"));
  EXPECT_GT(rmd160("d61c9a94f632a51b5920a91f5ad6ff6f0b36e968"),
            rmd160("a7fcc792608978d3e6e01352e45a3a66e1488fcc"));
  EXPECT_GT(rmd160("ffffffffffffffffffffffffffffffffffffffff"),
            rmd160("d61c9a94f632a51b5920a91f5ad6ff6f0b36e968"));
  EXPECT_GT(rmd160("ffffffffffffffffffffffffffffffffffffffff"),
            rmd160("0000000000000000000000000000000000000000"));

  EXPECT_EQ(rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'B'),
            rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'A'));
  EXPECT_EQ(rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'C'),
            rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'B'));
  EXPECT_EQ(rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'D'),
            rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'C'));
  EXPECT_EQ(rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'D'),
            rmd160("980b67db08d3b02d87de6ac05bad34e725fe00f5", 'A'));

  EXPECT_GT(
    sha256("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
    sha256("0000000000000000000000000000000000000000000000000000000000000000"));

  EXPECT_GT(
    sha3("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
    sha3("0000000000000000000000000000000000000000000000000000000000000000"));
}
