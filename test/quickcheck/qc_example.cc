/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <vector>

class T_QcExample : public ::testing::Test { };

RC_GTEST_PROP(QC, ReverseVectorTwice, (const std::vector<int> &l0)) {
  auto l1 = l0;
  std::reverse(begin(l1), end(l1));
  std::reverse(begin(l1), end(l1));
  RC_ASSERT(l0 == l1);
}

RC_GTEST_FIXTURE_PROP(T_QcExample, MapIsOrderedByKey, ()) {
  const auto non_empty_seq = *rc::gen::nonEmpty<std::vector<int> >();
  auto m0 = std::map<int, int>{};
  for (const auto &val : non_empty_seq) {
    m0.insert(std::make_pair(val, val));
  }
  auto sorted_seq = non_empty_seq;
  std::sort(std::begin(sorted_seq), std::end(sorted_seq));
  const auto min_val = sorted_seq.front();
  const auto max_val = sorted_seq.back();
  const auto smallest_key = m0.begin()->first;
  const auto largest_key = m0.rbegin()->first;

  RC_ASSERT(min_val == smallest_key);
  RC_ASSERT(max_val == largest_key);
}
