/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/hash.h"
#include "../../cvmfs/prng.h"


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
}


TEST(T_Shash, MakePathExplicit) {
  Prng prng;
  prng.InitSeed(42);

  shash::Any hash_md5(shash::kMd5);
  hash_md5.Randomize(&prng);
  ASSERT_FALSE(hash_md5.IsNull());
  EXPECT_EQ("/91/3969a1ae06052779065b87eb53cb64",
            hash_md5.MakePathExplicit(1, 2));
  EXPECT_EQ("/913/969/a1ae06052779065b87eb53cb64",
            hash_md5.MakePathExplicit(2, 3));
  EXPECT_EQ("/913969a1ae06052779065b87eb53cb64",
            hash_md5.MakePathExplicit(0, 3));
  EXPECT_EQ("/9/1/3/969a1ae06052779065b87eb53cb64",
            hash_md5.MakePathExplicit(3, 1));

  shash::Any hash_sha1(shash::kSha1);
  hash_sha1.Randomize(&prng);
  ASSERT_FALSE(hash_sha1.IsNull());
  EXPECT_EQ("/c9/116bb5576d69586386887c6a9eeeb569a406a1",
            hash_sha1.MakePathExplicit(1, 2));
  EXPECT_EQ("/c91/16b/b5576d69586386887c6a9eeeb569a406a1",
            hash_sha1.MakePathExplicit(2, 3));
  EXPECT_EQ("/c9116bb5576d69586386887c6a9eeeb569a406a1",
            hash_sha1.MakePathExplicit(0, 3));
  EXPECT_EQ("/c/9/1/16bb5576d69586386887c6a9eeeb569a406a1",
            hash_sha1.MakePathExplicit(3, 1));

  shash::Any hash_rmd160(shash::kRmd160);
  hash_rmd160.Randomize(&prng);
  ASSERT_FALSE(hash_rmd160.IsNull());
  EXPECT_EQ("/2e/5beea626f6ddef63d56405371f80732782086f-rmd160",
            hash_rmd160.MakePathExplicit(1, 2));
  EXPECT_EQ("/2e5/bee/a626f6ddef63d56405371f80732782086f-rmd160",
            hash_rmd160.MakePathExplicit(2, 3));
  EXPECT_EQ("/2e5beea626f6ddef63d56405371f80732782086f-rmd160",
            hash_rmd160.MakePathExplicit(0, 3));
  EXPECT_EQ("/2/e/5/beea626f6ddef63d56405371f80732782086f-rmd160",
            hash_rmd160.MakePathExplicit(3, 1));
}


TEST(T_Shash, MakePathExplicitWithPrefix) {
  Prng prng;
  prng.InitSeed(42);

  shash::Any hash_md5(shash::kMd5);
  hash_md5.Randomize(&prng);
  ASSERT_FALSE(hash_md5.IsNull());
  EXPECT_EQ("dataA/91/3969a1ae06052779065b87eb53cb64",
            hash_md5.MakePathExplicit(1, 2, "dataA"));
  EXPECT_EQ("dataB/913/969/a1ae06052779065b87eb53cb64",
            hash_md5.MakePathExplicit(2, 3, "dataB"));
  EXPECT_EQ("dataC/913969a1ae06052779065b87eb53cb64",
            hash_md5.MakePathExplicit(0, 3, "dataC"));
  EXPECT_EQ("dataD/9/1/3/969a1ae06052779065b87eb53cb64",
            hash_md5.MakePathExplicit(3, 1, "dataD"));

  shash::Any hash_sha1(shash::kSha1);
  hash_sha1.Randomize(&prng);
  ASSERT_FALSE(hash_sha1.IsNull());
  EXPECT_EQ("dataA/c9/116bb5576d69586386887c6a9eeeb569a406a1",
            hash_sha1.MakePathExplicit(1, 2, "dataA"));
  EXPECT_EQ("dataB/c91/16b/b5576d69586386887c6a9eeeb569a406a1",
            hash_sha1.MakePathExplicit(2, 3, "dataB"));
  EXPECT_EQ("dataC/c9116bb5576d69586386887c6a9eeeb569a406a1",
            hash_sha1.MakePathExplicit(0, 3, "dataC"));
  EXPECT_EQ("dataD/c/9/1/16bb5576d69586386887c6a9eeeb569a406a1",
            hash_sha1.MakePathExplicit(3, 1, "dataD"));

  shash::Any hash_rmd160(shash::kRmd160);
  hash_rmd160.Randomize(&prng);
  ASSERT_FALSE(hash_rmd160.IsNull());
  EXPECT_EQ("dataA/2e/5beea626f6ddef63d56405371f80732782086f-rmd160",
            hash_rmd160.MakePathExplicit(1, 2, "dataA"));
  EXPECT_EQ("dataB/2e5/bee/a626f6ddef63d56405371f80732782086f-rmd160",
            hash_rmd160.MakePathExplicit(2, 3, "dataB"));
  EXPECT_EQ("dataC/2e5beea626f6ddef63d56405371f80732782086f-rmd160",
            hash_rmd160.MakePathExplicit(0, 3, "dataC"));
  EXPECT_EQ("dataD/2/e/5/beea626f6ddef63d56405371f80732782086f-rmd160",
            hash_rmd160.MakePathExplicit(3, 1, "dataD"));
}


TEST(T_Shash, MakePathDefault) {
  Prng prng;
  prng.InitSeed(27111987);

  shash::Any hash_md5(shash::kMd5);
  hash_md5.Randomize(&prng);
  ASSERT_FALSE(hash_md5.IsNull());
  EXPECT_EQ("data/95/9a032bcfdd999742a321eb0daeddd5",  hash_md5.MakePath());
  EXPECT_EQ("dataX/95/9a032bcfdd999742a321eb0daeddd5",
            hash_md5.MakePath("dataX"));
  EXPECT_EQ("dataY/95/9a032bcfdd999742a321eb0daeddd5",
            hash_md5.MakePath("dataY"));
  EXPECT_EQ("dataZ/95/9a032bcfdd999742a321eb0daeddd5",
            hash_md5.MakePath("dataZ"));

  shash::Any hash_sha1(shash::kSha1);
  hash_sha1.Randomize(&prng);
  ASSERT_FALSE(hash_sha1.IsNull());
  EXPECT_EQ("data/cf/3e56cf3da37ad17cf1f2c2ff5d86497fc29068",
            hash_sha1.MakePath());
  EXPECT_EQ("dataX/cf/3e56cf3da37ad17cf1f2c2ff5d86497fc29068",
            hash_sha1.MakePath("dataX"));
  EXPECT_EQ("dataY/cf/3e56cf3da37ad17cf1f2c2ff5d86497fc29068",
            hash_sha1.MakePath("dataY"));
  EXPECT_EQ("dataZ/cf/3e56cf3da37ad17cf1f2c2ff5d86497fc29068",
            hash_sha1.MakePath("dataZ"));

  shash::Any hash_rmd160(shash::kRmd160);
  hash_rmd160.Randomize(&prng);
  ASSERT_FALSE(hash_rmd160.IsNull());
  EXPECT_EQ("data/aa/1deda59d5329553580d78fcd0b393157a5d28e-rmd160",
            hash_rmd160.MakePath());
  EXPECT_EQ("dataX/aa/1deda59d5329553580d78fcd0b393157a5d28e-rmd160",
            hash_rmd160.MakePath("dataX"));
  EXPECT_EQ("dataY/aa/1deda59d5329553580d78fcd0b393157a5d28e-rmd160",
            hash_rmd160.MakePath("dataY"));
  EXPECT_EQ("dataZ/aa/1deda59d5329553580d78fcd0b393157a5d28e-rmd160",
            hash_rmd160.MakePath("dataZ"));
}


TEST(T_Shash, HashSuffix) {
  Prng prng;
  prng.InitSeed(9);

  shash::Any hash_md5(shash::kMd5);
  hash_md5.Randomize(&prng);
  hash_md5.suffix = 'A';
  ASSERT_FALSE(hash_md5.IsNull());
  ASSERT_TRUE(hash_md5.HasSuffix());
  EXPECT_EQ("2ec5fe3c17045abdb136a5e6a913e32aA",
            hash_md5.ToStringWithSuffix());
  EXPECT_EQ("data/2e/c5fe3c17045abdb136a5e6a913e32aA",
            hash_md5.MakePathWithSuffix());
  EXPECT_EQ("dataX/2e/c5fe3c17045abdb136a5e6a913e32aA",
            hash_md5.MakePathWithSuffix("dataX"));
  EXPECT_EQ("dataY/2e/c5fe3c17045abdb136a5e6a913e32aA",
            hash_md5.MakePathWithSuffix("dataY"));
  EXPECT_EQ("dataZ/2e/c5fe3c17045abdb136a5e6a913e32aA",
            hash_md5.MakePathWithSuffix("dataZ"));
  EXPECT_EQ("dataX/2e/c5fe3c17045abdb136a5e6a913e32a",
            hash_md5.MakePath("dataX"));
  EXPECT_EQ("dataY/2e/c5fe3c17045abdb136a5e6a913e32a",
            hash_md5.MakePath("dataY"));
  EXPECT_EQ("dataZ/2e/c5fe3c17045abdb136a5e6a913e32a",
            hash_md5.MakePath("dataZ"));
  EXPECT_EQ("/2e/c5fe3c17045abdb136a5e6a913e32aP",
            hash_md5.MakePathWithSuffix(1, 2, shash::kSuffixPartial));
  EXPECT_EQ("/2ec/5fe3c17045abdb136a5e6a913e32aC",
            hash_md5.MakePathWithSuffix(1, 3, shash::kSuffixCatalog));

  shash::Any hash_sha1(shash::kSha1);
  hash_sha1.Randomize(&prng);
  hash_sha1.suffix = 'B';
  ASSERT_FALSE(hash_sha1.IsNull());
  ASSERT_TRUE(hash_sha1.HasSuffix());
  EXPECT_EQ("b75ae68b53d2fc149b77e504132d37569b7e766bB",
            hash_sha1.ToStringWithSuffix());
  EXPECT_EQ("data/b7/5ae68b53d2fc149b77e504132d37569b7e766bB",
            hash_sha1.MakePathWithSuffix());
  EXPECT_EQ("dataX/b7/5ae68b53d2fc149b77e504132d37569b7e766bB",
            hash_sha1.MakePathWithSuffix("dataX"));
  EXPECT_EQ("dataY/b7/5ae68b53d2fc149b77e504132d37569b7e766bB",
            hash_sha1.MakePathWithSuffix("dataY"));
  EXPECT_EQ("dataZ/b7/5ae68b53d2fc149b77e504132d37569b7e766bB",
            hash_sha1.MakePathWithSuffix("dataZ"));
  EXPECT_EQ("dataX/b7/5ae68b53d2fc149b77e504132d37569b7e766b",
            hash_sha1.MakePath("dataX"));
  EXPECT_EQ("dataY/b7/5ae68b53d2fc149b77e504132d37569b7e766b",
            hash_sha1.MakePath("dataY"));
  EXPECT_EQ("dataZ/b7/5ae68b53d2fc149b77e504132d37569b7e766b",
            hash_sha1.MakePath("dataZ"));
  EXPECT_EQ("/b7/5ae68b53d2fc149b77e504132d37569b7e766bP",
            hash_sha1.MakePathWithSuffix(1, 2, shash::kSuffixPartial));
  EXPECT_EQ("/b75/ae68b53d2fc149b77e504132d37569b7e766bC",
            hash_sha1.MakePathWithSuffix(1, 3, shash::kSuffixCatalog));

  shash::Any hash_rmd160(shash::kRmd160);
  hash_rmd160.Randomize(&prng);
  hash_rmd160.suffix = 'C';
  ASSERT_FALSE(hash_rmd160.IsNull());
  ASSERT_TRUE(hash_rmd160.HasSuffix());
  EXPECT_EQ("a74a19bd6162343a21c8590aa9cebca9014c636d-rmd160C",
            hash_rmd160.ToStringWithSuffix());
  EXPECT_EQ("data/a7/4a19bd6162343a21c8590aa9cebca9014c636d-rmd160C",
            hash_rmd160.MakePathWithSuffix());
  EXPECT_EQ("dataX/a7/4a19bd6162343a21c8590aa9cebca9014c636d-rmd160C",
            hash_rmd160.MakePathWithSuffix("dataX"));
  EXPECT_EQ("dataY/a7/4a19bd6162343a21c8590aa9cebca9014c636d-rmd160C",
            hash_rmd160.MakePathWithSuffix("dataY"));
  EXPECT_EQ("dataZ/a7/4a19bd6162343a21c8590aa9cebca9014c636d-rmd160C",
            hash_rmd160.MakePathWithSuffix("dataZ"));
  EXPECT_EQ("dataX/a7/4a19bd6162343a21c8590aa9cebca9014c636d-rmd160",
            hash_rmd160.MakePath("dataX"));
  EXPECT_EQ("dataY/a7/4a19bd6162343a21c8590aa9cebca9014c636d-rmd160",
            hash_rmd160.MakePath("dataY"));
  EXPECT_EQ("dataZ/a7/4a19bd6162343a21c8590aa9cebca9014c636d-rmd160",
            hash_rmd160.MakePath("dataZ"));
  EXPECT_EQ("/a7/4a19bd6162343a21c8590aa9cebca9014c636d-rmd160P",
            hash_rmd160.MakePathWithSuffix(1, 2, shash::kSuffixPartial));
  EXPECT_EQ("/a74a/19bd6162343a21c8590aa9cebca9014c636d-rmd160C",
            hash_rmd160.MakePathWithSuffix(1, 4, shash::kSuffixCatalog));
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
}
