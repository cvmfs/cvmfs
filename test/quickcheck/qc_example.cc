/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "fuzztest/fuzztest.h"

#include <algorithm>
#include <map>
#include <vector>

// A basic property (round-trip / involution): reversing a vector twice must
// yield the original. FuzzTest infers an Arbitrary<std::vector<int>> domain for
// the single parameter, so no explicit .WithDomains() is required.
void ReverseVectorTwice(const std::vector<int> &l0) {
  auto l1 = l0;
  std::reverse(begin(l1), end(l1));
  std::reverse(begin(l1), end(l1));
  EXPECT_EQ(l0, l1);
}
FUZZ_TEST(QC, ReverseVectorTwice);

// A model/oracle property: the smallest/largest key of a std::map built from a
// sequence must equal the min/max of that sequence. Unlike RapidCheck (which
// sampled a generator imperatively inside the body), FuzzTest declares the
// randomness as an input domain -- here a non-empty vector of ints.
void MapIsOrderedByKey(const std::vector<int> &non_empty_seq) {
  std::map<int, int> m0;
  for (const auto &val : non_empty_seq) {
    m0.insert(std::make_pair(val, val));
  }
  auto sorted_seq = non_empty_seq;
  std::sort(std::begin(sorted_seq), std::end(sorted_seq));

  EXPECT_EQ(sorted_seq.front(), m0.begin()->first);
  EXPECT_EQ(sorted_seq.back(), m0.rbegin()->first);
}
FUZZ_TEST(QC, MapIsOrderedByKey)
    .WithDomains(fuzztest::NonEmpty(fuzztest::Arbitrary<std::vector<int> >()));
