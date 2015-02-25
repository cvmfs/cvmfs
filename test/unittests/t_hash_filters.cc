/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <algorithm>

#include "../../cvmfs/garbage_collection/hash_filter.h"

static shash::Any sha(const std::string &hash,
                      const char suffix = shash::kSuffixNone) {
  return shash::Any(shash::kSha1, shash::HexPtr(hash), suffix);
}

static shash::Any rmd(const std::string &hash,
                      const char suffix = shash::kSuffixNone) {
  return shash::Any(shash::kRmd160, shash::HexPtr(hash), suffix);
}

static shash::Any md5(const std::string &hash,
                      const char suffix = shash::kSuffixNone) {
  return shash::Any(shash::kMd5, shash::HexPtr(hash), suffix);
}

class RandomHashGenerator {
 public:
  RandomHashGenerator(Prng &rng) : rng_(rng) {}

  shash::Any operator()() {
    const shash::Algorithms type = static_cast<shash::Algorithms>(rng_.Next(3));
    shash::Any result_hash(type);
    result_hash.Randomize(&rng_);
    return result_hash;
  }

 private:
  Prng &rng_;
};


template <class HashFilterT>
class T_HashFilter : public ::testing::Test {
 public:
  // TODO: C++11 replace this custom classes by std::bind()

  class FillWrapper {
   public:
    FillWrapper(HashFilterT *filter) : filter_(filter) {}
    void operator ()(const shash::Any &hash) {
      filter_->Fill(hash);
    }

   private:
    HashFilterT *filter_;
  };

  class ContainsWrapper {
   public:
    ContainsWrapper(HashFilterT *filter) : filter_(filter) {}
    void operator ()(const shash::Any &hash) {
      EXPECT_TRUE(filter_->Contains(hash));
    }

   private:
    HashFilterT *filter_;
  };
};

typedef ::testing::Types<SimpleHashFilter, SmallhashFilter> HashFilterTypes;
TYPED_TEST_CASE(T_HashFilter, HashFilterTypes);


TYPED_TEST(T_HashFilter, Initialize) {
  TypeParam filter;
}


TYPED_TEST(T_HashFilter, EmptyFilter) {
  TypeParam filter;
  filter.Freeze();
  EXPECT_EQ(0u, filter.Count());

  EXPECT_FALSE(filter.Contains(sha("ac1ddc9c4283f5bb8db64c2e5771eeb44803399f")));
  EXPECT_FALSE(filter.Contains(rmd("da39a3ee5e6b4b0d3255bfef95601890afd80709")));
  EXPECT_FALSE(filter.Contains(md5("eb8a956c0a1164b84262505a629e8a1f")));

  EXPECT_FALSE(filter.Contains(sha("ed0e0b2c3590bb9e72f849ed6bebb97f06371abf", shash::kSuffixCatalog)));
  EXPECT_FALSE(filter.Contains(rmd("f66853b7a7731719f3babb59623b00b3e02a4a4f", shash::kSuffixHistory)));
  EXPECT_FALSE(filter.Contains(md5("c70ac2b76b5c25b07ae80ea572bb8265",         shash::kSuffixCatalog)));
}


TYPED_TEST(T_HashFilter, FillSha1) {
  TypeParam filter;
  filter.Fill(sha("451afd372792933f4dbad535413346bfe7e7cc08"));
  filter.Fill(sha("2579075d95e9c7abfbedb78de5307a7f27aa7109"));
  filter.Fill(sha("40e938a032915acb48da226792d394905321fb9e"));
  filter.Fill(sha("ed737e36b7b881184cddb78d73e99d11ea2e948a"));
  filter.Fill(sha("8773f6f313e91c7727fe9502dc3e926b4b70f065"));
  filter.Fill(sha("c5aba768b5d33a7875e70a05e72e87bfdfb98933"));
  filter.Fill(sha("b2fa258840f9d665922e09e3825caa5783fdb6a4"));
  filter.Fill(sha("b9ca3169cbeb32229097bdd8d42849b6144e2e92"));
  filter.Freeze();

  EXPECT_EQ(8u, filter.Count());

  EXPECT_TRUE(filter.Contains(sha("451afd372792933f4dbad535413346bfe7e7cc08")));
  EXPECT_TRUE(filter.Contains(sha("2579075d95e9c7abfbedb78de5307a7f27aa7109")));
  EXPECT_TRUE(filter.Contains(sha("40e938a032915acb48da226792d394905321fb9e")));
  EXPECT_TRUE(filter.Contains(sha("8773f6f313e91c7727fe9502dc3e926b4b70f065")));
  EXPECT_TRUE(filter.Contains(sha("c5aba768b5d33a7875e70a05e72e87bfdfb98933")));
  EXPECT_TRUE(filter.Contains(sha("ed737e36b7b881184cddb78d73e99d11ea2e948a")));
  EXPECT_TRUE(filter.Contains(sha("b2fa258840f9d665922e09e3825caa5783fdb6a4")));
  EXPECT_TRUE(filter.Contains(sha("b9ca3169cbeb32229097bdd8d42849b6144e2e92")));
  EXPECT_FALSE(filter.Contains(sha("8271640616eb764f5b87dcb5a4c3d9a2aadcd816")));
  EXPECT_FALSE(filter.Contains(sha("7d988a2f0539bcd5c9cce8ba349f0e15ae2da92e")));
  EXPECT_FALSE(filter.Contains(sha("50e2e699ca7e5edfb81de5a92a6555ba9852c3a7")));
}


TYPED_TEST(T_HashFilter, FillRmd160) {
  TypeParam filter;
  filter.Fill(rmd("451afd372792933f4dbad535413346bfe7e7cc08"));
  filter.Fill(rmd("2579075d95e9c7abfbedb78de5307a7f27aa7109"));
  filter.Fill(rmd("40e938a032915acb48da226792d394905321fb9e"));
  filter.Fill(rmd("ed737e36b7b881184cddb78d73e99d11ea2e948a"));
  filter.Fill(rmd("8773f6f313e91c7727fe9502dc3e926b4b70f065"));
  filter.Fill(rmd("c5aba768b5d33a7875e70a05e72e87bfdfb98933"));
  filter.Fill(rmd("b2fa258840f9d665922e09e3825caa5783fdb6a4"));
  filter.Fill(rmd("b9ca3169cbeb32229097bdd8d42849b6144e2e92"));
  filter.Freeze();

  EXPECT_EQ(8u, filter.Count());

  EXPECT_TRUE(filter.Contains(rmd("451afd372792933f4dbad535413346bfe7e7cc08")));
  EXPECT_TRUE(filter.Contains(rmd("2579075d95e9c7abfbedb78de5307a7f27aa7109")));
  EXPECT_TRUE(filter.Contains(rmd("40e938a032915acb48da226792d394905321fb9e")));
  EXPECT_TRUE(filter.Contains(rmd("8773f6f313e91c7727fe9502dc3e926b4b70f065")));
  EXPECT_TRUE(filter.Contains(rmd("c5aba768b5d33a7875e70a05e72e87bfdfb98933")));
  EXPECT_TRUE(filter.Contains(rmd("ed737e36b7b881184cddb78d73e99d11ea2e948a")));
  EXPECT_TRUE(filter.Contains(rmd("b2fa258840f9d665922e09e3825caa5783fdb6a4")));
  EXPECT_TRUE(filter.Contains(rmd("b9ca3169cbeb32229097bdd8d42849b6144e2e92")));
  EXPECT_FALSE(filter.Contains(rmd("8271640616eb764f5b87dcb5a4c3d9a2aadcd816")));
  EXPECT_FALSE(filter.Contains(rmd("7d988a2f0539bcd5c9cce8ba349f0e15ae2da92e")));
  EXPECT_FALSE(filter.Contains(rmd("50e2e699ca7e5edfb81de5a92a6555ba9852c3a7")));
}


TYPED_TEST(T_HashFilter, FillMd5) {
  TypeParam filter;
  filter.Fill(md5("e57c8f2414e88dc0818a4aa1525bad80"));
  filter.Fill(md5("0707c6df7f700f1f7c8fcdbdc5442ab9"));
  filter.Fill(md5("890c770aa0c0047e196e7425cf47b3b0"));
  filter.Fill(md5("a66fe32270201b9ef8af419a36bb56d3"));
  filter.Fill(md5("78f14c618130d70e3a80828450cf4365"));
  filter.Fill(md5("03ac3abaa667a3f2727864eedaf47559"));
  filter.Fill(md5("bd14ae8288fad1b24c3bcec7e91296cf"));
  filter.Fill(md5("b67539c4ef2a4f8b5251a9472a613b1b"));
  filter.Freeze();

  EXPECT_EQ(8u, filter.Count());

  EXPECT_TRUE(filter.Contains(md5("e57c8f2414e88dc0818a4aa1525bad80")));
  EXPECT_TRUE(filter.Contains(md5("0707c6df7f700f1f7c8fcdbdc5442ab9")));
  EXPECT_TRUE(filter.Contains(md5("890c770aa0c0047e196e7425cf47b3b0")));
  EXPECT_TRUE(filter.Contains(md5("a66fe32270201b9ef8af419a36bb56d3")));
  EXPECT_TRUE(filter.Contains(md5("78f14c618130d70e3a80828450cf4365")));
  EXPECT_TRUE(filter.Contains(md5("03ac3abaa667a3f2727864eedaf47559")));
  EXPECT_TRUE(filter.Contains(md5("bd14ae8288fad1b24c3bcec7e91296cf")));
  EXPECT_TRUE(filter.Contains(md5("b67539c4ef2a4f8b5251a9472a613b1b")));
  EXPECT_FALSE(filter.Contains(md5("9731b9b024f890dd1ce81e54a5f9bfe7")));
  EXPECT_FALSE(filter.Contains(md5("eb8a956c0a1164b84262505a629e8a1f")));
  EXPECT_FALSE(filter.Contains(md5("b35a73d2b0cfe44018e9068f81abbb99")));
}


TYPED_TEST(T_HashFilter, FillHeterogeneous) {
  TypeParam filter;
  filter.Fill(sha("451afd372792933f4dbad535413346bfe7e7cc08"));
  filter.Fill(sha("2579075d95e9c7abfbedb78de5307a7f27aa7109"));
  filter.Fill(sha("40e938a032915acb48da226792d394905321fb9e"));
  filter.Fill(rmd("8773f6f313e91c7727fe9502dc3e926b4b70f065"));
  filter.Fill(rmd("c5aba768b5d33a7875e70a05e72e87bfdfb98933"));
  filter.Fill(rmd("ed737e36b7b881184cddb78d73e99d11ea2e948a"));
  filter.Fill(md5("9731b9b024f890dd1ce81e54a5f9bfe7"));
  filter.Fill(md5("eb8a956c0a1164b84262505a629e8a1f"));
  filter.Fill(md5("b35a73d2b0cfe44018e9068f81abbb99"));
  filter.Freeze();

  EXPECT_EQ(9u, filter.Count());

  EXPECT_TRUE(filter.Contains(sha("451afd372792933f4dbad535413346bfe7e7cc08")));
  EXPECT_TRUE(filter.Contains(sha("2579075d95e9c7abfbedb78de5307a7f27aa7109")));
  EXPECT_TRUE(filter.Contains(sha("40e938a032915acb48da226792d394905321fb9e")));
  EXPECT_TRUE(filter.Contains(rmd("8773f6f313e91c7727fe9502dc3e926b4b70f065")));
  EXPECT_TRUE(filter.Contains(rmd("c5aba768b5d33a7875e70a05e72e87bfdfb98933")));
  EXPECT_TRUE(filter.Contains(rmd("ed737e36b7b881184cddb78d73e99d11ea2e948a")));
  EXPECT_TRUE(filter.Contains(md5("9731b9b024f890dd1ce81e54a5f9bfe7")));
  EXPECT_TRUE(filter.Contains(md5("eb8a956c0a1164b84262505a629e8a1f")));
  EXPECT_TRUE(filter.Contains(md5("b35a73d2b0cfe44018e9068f81abbb99")));
  EXPECT_FALSE(filter.Contains(rmd("451afd372792933f4dbad535413346bfe7e7cc08")));
  EXPECT_FALSE(filter.Contains(rmd("2579075d95e9c7abfbedb78de5307a7f27aa7109")));
  EXPECT_FALSE(filter.Contains(rmd("40e938a032915acb48da226792d394905321fb9e")));
  EXPECT_FALSE(filter.Contains(sha("8773f6f313e91c7727fe9502dc3e926b4b70f065")));
  EXPECT_FALSE(filter.Contains(sha("c5aba768b5d33a7875e70a05e72e87bfdfb98933")));
  EXPECT_FALSE(filter.Contains(sha("ed737e36b7b881184cddb78d73e99d11ea2e948a")));
  EXPECT_FALSE(filter.Contains(md5("148dd83c24e8de4377d1ecfa17d10617")));
  EXPECT_FALSE(filter.Contains(md5("ed70ca83dc1aac21ce26355fd1bb9325")));
  EXPECT_FALSE(filter.Contains(md5("f60447901653b1ddcc698d644b9675c8")));
}


TYPED_TEST(T_HashFilter, DoubleInsert) {
  TypeParam filter;
  filter.Fill(sha("2e19e3667a617f9381357e580f935ee783a98025"));
  EXPECT_EQ(1u, filter.Count());

  filter.Fill(sha("2e19e3667a617f9381357e580f935ee783a98025"));
  EXPECT_EQ(1u, filter.Count());

  filter.Fill(rmd("dd240ad846f9610d577778caed9410fdc3e385fa"));
  EXPECT_EQ(2u, filter.Count());

  filter.Fill(rmd("dd240ad846f9610d577778caed9410fdc3e385fa"));
  EXPECT_EQ(2u, filter.Count());

  filter.Fill(md5("d985d0ea551c1253c2305140c583d11f"));
  EXPECT_EQ(3u, filter.Count());

  filter.Fill(md5("d985d0ea551c1253c2305140c583d11f"));
  EXPECT_EQ(3u, filter.Count());

  filter.Fill(sha("2e19e3667a617f9381357e580f935ee783a98025", shash::kSuffixPartial));
  filter.Fill(rmd("dd240ad846f9610d577778caed9410fdc3e385fa", shash::kSuffixPartial));
  filter.Fill(md5("d985d0ea551c1253c2305140c583d11f",         shash::kSuffixPartial));
  EXPECT_EQ(3u, filter.Count()) << "hash suffix is not ignored";

  filter.Fill(rmd("2e19e3667a617f9381357e580f935ee783a98025"));
  filter.Fill(sha("dd240ad846f9610d577778caed9410fdc3e385fa"));
  EXPECT_EQ(5u, filter.Count()) << "hash type is mixed up";

  filter.Freeze();
  EXPECT_TRUE(filter.Contains(sha("2e19e3667a617f9381357e580f935ee783a98025")));
  EXPECT_TRUE(filter.Contains(rmd("2e19e3667a617f9381357e580f935ee783a98025")));
  EXPECT_TRUE(filter.Contains(sha("dd240ad846f9610d577778caed9410fdc3e385fa")));
  EXPECT_TRUE(filter.Contains(rmd("dd240ad846f9610d577778caed9410fdc3e385fa")));
  EXPECT_TRUE(filter.Contains(md5("d985d0ea551c1253c2305140c583d11f")));

  EXPECT_TRUE(filter.Contains(sha("2e19e3667a617f9381357e580f935ee783a98025", shash::kSuffixPartial)));
  EXPECT_TRUE(filter.Contains(rmd("2e19e3667a617f9381357e580f935ee783a98025", shash::kSuffixPartial)));
  EXPECT_TRUE(filter.Contains(sha("dd240ad846f9610d577778caed9410fdc3e385fa", shash::kSuffixPartial)));
  EXPECT_TRUE(filter.Contains(rmd("dd240ad846f9610d577778caed9410fdc3e385fa", shash::kSuffixPartial)));
  EXPECT_TRUE(filter.Contains(md5("d985d0ea551c1253c2305140c583d11f",         shash::kSuffixPartial)));

  EXPECT_TRUE(filter.Contains(sha("2e19e3667a617f9381357e580f935ee783a98025", shash::kSuffixCatalog)));
  EXPECT_TRUE(filter.Contains(rmd("2e19e3667a617f9381357e580f935ee783a98025", shash::kSuffixCatalog)));
  EXPECT_TRUE(filter.Contains(sha("dd240ad846f9610d577778caed9410fdc3e385fa", shash::kSuffixCatalog)));
  EXPECT_TRUE(filter.Contains(rmd("dd240ad846f9610d577778caed9410fdc3e385fa", shash::kSuffixCatalog)));
  EXPECT_TRUE(filter.Contains(md5("d985d0ea551c1253c2305140c583d11f",         shash::kSuffixCatalog)));
}


TYPED_TEST(T_HashFilter, FillHeterogeneousWithSuffixes) {
  TypeParam filter;
  filter.Fill(sha("451afd372792933f4dbad535413346bfe7e7cc08"));
  filter.Fill(sha("451afd372792933f4dbad535413346bfe7e7cc08", shash::kSuffixPartial));
  filter.Fill(sha("2579075d95e9c7abfbedb78de5307a7f27aa7109"));
  filter.Fill(sha("2579075d95e9c7abfbedb78de5307a7f27aa7109", shash::kSuffixPartial));
  filter.Fill(sha("40e938a032915acb48da226792d394905321fb9e"));
  filter.Fill(sha("40e938a032915acb48da226792d394905321fb9e", shash::kSuffixPartial));
  filter.Fill(rmd("8773f6f313e91c7727fe9502dc3e926b4b70f065"));
  filter.Fill(rmd("8773f6f313e91c7727fe9502dc3e926b4b70f065", shash::kSuffixPartial));
  filter.Fill(rmd("c5aba768b5d33a7875e70a05e72e87bfdfb98933"));
  filter.Fill(rmd("c5aba768b5d33a7875e70a05e72e87bfdfb98933", shash::kSuffixPartial));
  filter.Fill(rmd("ed737e36b7b881184cddb78d73e99d11ea2e948a"));
  filter.Fill(rmd("ed737e36b7b881184cddb78d73e99d11ea2e948a", shash::kSuffixPartial));
  filter.Fill(md5("9731b9b024f890dd1ce81e54a5f9bfe7"));
  filter.Fill(md5("9731b9b024f890dd1ce81e54a5f9bfe7", shash::kSuffixPartial));
  filter.Fill(md5("eb8a956c0a1164b84262505a629e8a1f"));
  filter.Fill(md5("eb8a956c0a1164b84262505a629e8a1f", shash::kSuffixPartial));
  filter.Fill(md5("b35a73d2b0cfe44018e9068f81abbb99"));
  filter.Fill(md5("b35a73d2b0cfe44018e9068f81abbb99", shash::kSuffixPartial));
  filter.Freeze();

  // suffixes are not checked in hash equality!
  EXPECT_EQ(9u, filter.Count());

  EXPECT_TRUE(filter.Contains(sha("451afd372792933f4dbad535413346bfe7e7cc08")));
  EXPECT_TRUE(filter.Contains(sha("2579075d95e9c7abfbedb78de5307a7f27aa7109")));
  EXPECT_TRUE(filter.Contains(sha("40e938a032915acb48da226792d394905321fb9e")));
  EXPECT_TRUE(filter.Contains(rmd("8773f6f313e91c7727fe9502dc3e926b4b70f065")));
  EXPECT_TRUE(filter.Contains(rmd("c5aba768b5d33a7875e70a05e72e87bfdfb98933")));
  EXPECT_TRUE(filter.Contains(rmd("ed737e36b7b881184cddb78d73e99d11ea2e948a")));
  EXPECT_TRUE(filter.Contains(md5("9731b9b024f890dd1ce81e54a5f9bfe7")));
  EXPECT_TRUE(filter.Contains(md5("eb8a956c0a1164b84262505a629e8a1f")));
  EXPECT_TRUE(filter.Contains(md5("b35a73d2b0cfe44018e9068f81abbb99")));

  EXPECT_TRUE(filter.Contains(sha("451afd372792933f4dbad535413346bfe7e7cc08", shash::kSuffixCatalog)));
  EXPECT_TRUE(filter.Contains(sha("2579075d95e9c7abfbedb78de5307a7f27aa7109", shash::kSuffixCatalog)));
  EXPECT_TRUE(filter.Contains(sha("40e938a032915acb48da226792d394905321fb9e", shash::kSuffixCatalog)));
  EXPECT_TRUE(filter.Contains(rmd("8773f6f313e91c7727fe9502dc3e926b4b70f065", shash::kSuffixCatalog)));
  EXPECT_TRUE(filter.Contains(rmd("c5aba768b5d33a7875e70a05e72e87bfdfb98933", shash::kSuffixCatalog)));
  EXPECT_TRUE(filter.Contains(rmd("ed737e36b7b881184cddb78d73e99d11ea2e948a", shash::kSuffixCatalog)));
  EXPECT_TRUE(filter.Contains(md5("9731b9b024f890dd1ce81e54a5f9bfe7",         shash::kSuffixCatalog)));
  EXPECT_TRUE(filter.Contains(md5("eb8a956c0a1164b84262505a629e8a1f",         shash::kSuffixCatalog)));
  EXPECT_TRUE(filter.Contains(md5("b35a73d2b0cfe44018e9068f81abbb99",         shash::kSuffixCatalog)));

  EXPECT_FALSE(filter.Contains(rmd("451afd372792933f4dbad535413346bfe7e7cc08")));
  EXPECT_FALSE(filter.Contains(rmd("2579075d95e9c7abfbedb78de5307a7f27aa7109")));
  EXPECT_FALSE(filter.Contains(rmd("40e938a032915acb48da226792d394905321fb9e")));
  EXPECT_FALSE(filter.Contains(sha("8773f6f313e91c7727fe9502dc3e926b4b70f065")));
  EXPECT_FALSE(filter.Contains(sha("c5aba768b5d33a7875e70a05e72e87bfdfb98933")));
  EXPECT_FALSE(filter.Contains(sha("ed737e36b7b881184cddb78d73e99d11ea2e948a")));
  EXPECT_FALSE(filter.Contains(md5("148dd83c24e8de4377d1ecfa17d10617")));
  EXPECT_FALSE(filter.Contains(md5("ed70ca83dc1aac21ce26355fd1bb9325")));
  EXPECT_FALSE(filter.Contains(md5("f60447901653b1ddcc698d644b9675c8")));

  EXPECT_FALSE(filter.Contains(rmd("451afd372792933f4dbad535413346bfe7e7cc08", shash::kSuffixCertificate)));
  EXPECT_FALSE(filter.Contains(rmd("2579075d95e9c7abfbedb78de5307a7f27aa7109", shash::kSuffixCertificate)));
  EXPECT_FALSE(filter.Contains(rmd("40e938a032915acb48da226792d394905321fb9e", shash::kSuffixCertificate)));
  EXPECT_FALSE(filter.Contains(sha("8773f6f313e91c7727fe9502dc3e926b4b70f065", shash::kSuffixCertificate)));
  EXPECT_FALSE(filter.Contains(sha("c5aba768b5d33a7875e70a05e72e87bfdfb98933", shash::kSuffixCertificate)));
  EXPECT_FALSE(filter.Contains(sha("ed737e36b7b881184cddb78d73e99d11ea2e948a", shash::kSuffixCertificate)));
  EXPECT_FALSE(filter.Contains(md5("148dd83c24e8de4377d1ecfa17d10617",         shash::kSuffixCertificate)));
  EXPECT_FALSE(filter.Contains(md5("ed70ca83dc1aac21ce26355fd1bb9325",         shash::kSuffixCertificate)));
  EXPECT_FALSE(filter.Contains(md5("f60447901653b1ddcc698d644b9675c8",         shash::kSuffixCertificate)));
}


TYPED_TEST(T_HashFilter, FillManyRandomHashesSlow) {
  TypeParam filter;

  Prng rng;
  rng.InitSeed(78475);
  RandomHashGenerator                    random_hash_generator(rng);
  typename TestFixture::FillWrapper      fill(&filter);
  typename TestFixture::ContainsWrapper  check_contains(&filter);

  const unsigned int hash_count = 1000000;
  std::vector<shash::Any> random_hashes(hash_count, shash::Any());
  std::generate(random_hashes.begin(), random_hashes.end(), random_hash_generator);
  std::for_each(random_hashes.begin(), random_hashes.end(), fill);
  filter.Freeze();

  EXPECT_EQ(hash_count, filter.Count());
  EXPECT_FALSE(filter.Contains(rmd("451afd372792933f4dbad535413346bfe7e7cc08")));
  EXPECT_FALSE(filter.Contains(rmd("2579075d95e9c7abfbedb78de5307a7f27aa7109")));
  EXPECT_FALSE(filter.Contains(rmd("40e938a032915acb48da226792d394905321fb9e")));
  EXPECT_FALSE(filter.Contains(sha("8773f6f313e91c7727fe9502dc3e926b4b70f065")));
  EXPECT_FALSE(filter.Contains(sha("c5aba768b5d33a7875e70a05e72e87bfdfb98933")));
  EXPECT_FALSE(filter.Contains(sha("ed737e36b7b881184cddb78d73e99d11ea2e948a")));
  EXPECT_FALSE(filter.Contains(md5("148dd83c24e8de4377d1ecfa17d10617")));
  EXPECT_FALSE(filter.Contains(md5("ed70ca83dc1aac21ce26355fd1bb9325")));
  EXPECT_FALSE(filter.Contains(md5("f60447901653b1ddcc698d644b9675c8")));

  std::for_each(random_hashes.begin(), random_hashes.end(), check_contains);
}
