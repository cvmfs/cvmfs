#include <gtest/gtest.h>

#include "../../cvmfs/garbage_collection/hash_filter.h"

static shash::Any h(const std::string &hash) {
  return shash::Any(shash::kSha1, shash::HexPtr(hash));
}

TEST(T_HashFilters, SimpleHashFilter) {
  SimpleHashFilter filter;
  filter.Fill(h("451afd372792933f4dbad535413346bfe7e7cc08"));
  filter.Fill(h("2579075d95e9c7abfbedb78de5307a7f27aa7109"));
  filter.Fill(h("40e938a032915acb48da226792d394905321fb9e"));
  filter.Fill(h("ed737e36b7b881184cddb78d73e99d11ea2e948a"));
  filter.Fill(h("8773f6f313e91c7727fe9502dc3e926b4b70f065"));
  filter.Fill(h("c5aba768b5d33a7875e70a05e72e87bfdfb98933"));
  filter.Fill(h("b2fa258840f9d665922e09e3825caa5783fdb6a4"));
  filter.Fill(h("b9ca3169cbeb32229097bdd8d42849b6144e2e92"));
  filter.Freeze();

  EXPECT_EQ (8u, filter.Count());

  EXPECT_TRUE (filter.Contains(h("451afd372792933f4dbad535413346bfe7e7cc08")));
  EXPECT_TRUE (filter.Contains(h("2579075d95e9c7abfbedb78de5307a7f27aa7109")));
  EXPECT_TRUE (filter.Contains(h("40e938a032915acb48da226792d394905321fb9e")));
  EXPECT_TRUE (filter.Contains(h("8773f6f313e91c7727fe9502dc3e926b4b70f065")));
  EXPECT_TRUE (filter.Contains(h("c5aba768b5d33a7875e70a05e72e87bfdfb98933")));
  EXPECT_TRUE (filter.Contains(h("ed737e36b7b881184cddb78d73e99d11ea2e948a")));
  EXPECT_TRUE (filter.Contains(h("b2fa258840f9d665922e09e3825caa5783fdb6a4")));
  EXPECT_TRUE (filter.Contains(h("b9ca3169cbeb32229097bdd8d42849b6144e2e92")));
  EXPECT_FALSE(filter.Contains(h("8271640616eb764f5b87dcb5a4c3d9a2aadcd816")));
  EXPECT_FALSE(filter.Contains(h("7d988a2f0539bcd5c9cce8ba349f0e15ae2da92e")));
  EXPECT_FALSE(filter.Contains(h("50e2e699ca7e5edfb81de5a92a6555ba9852c3a7")));
}
