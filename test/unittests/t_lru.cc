/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "lru.h"
#include "statistics.h"
#include "util/string.h"

using lru::LruCache;

static inline uint32_t hasher_int(const int &value) { return value; }

static const unsigned cache_size = 1024;
const std::string name = "lru_cache";


TEST(T_LruCache, Initialize) {
  perf::Statistics statistics;
  LruCache<int, std::string> cache(cache_size, -1, hasher_int,
                                   perf::StatisticsTemplate(name, &statistics));
  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());
}


TEST(T_LruCache, Insert) {
  perf::Statistics statistics;
  LruCache<int, std::string> cache(cache_size, -1, hasher_int,
                                   perf::StatisticsTemplate(name, &statistics));
  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  cache.Insert(1, "eins");
  cache.Insert(2, "zwei");
  cache.Insert(3, "drei");
  cache.Insert(4, "vier");
  cache.Insert(5, "fünf");
  cache.Insert(6, "sechs");
  cache.Insert(7, "sieben");
  cache.Insert(8, "acht");
  cache.Insert(9, "neun");

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());
}


TEST(T_LruCache, UpdateValue) {
  perf::Statistics statistics;
  LruCache<int, std::string> cache(cache_size, -1, hasher_int,
                                   perf::StatisticsTemplate(name, &statistics));

  EXPECT_TRUE(cache.Insert(1, "one"));
  EXPECT_TRUE(cache.Insert(2, "two"));
  EXPECT_FALSE(cache.IsFull());
  unsigned i = 3;
  for (; !cache.IsFull(); ++i) {
    cache.Insert(i, "");
  }
  EXPECT_EQ(0U, statistics.Lookup(name + ".n_replace")->Get());

  EXPECT_FALSE(cache.UpdateValue(i, "BIGNUMBER"));
  EXPECT_TRUE(cache.UpdateValue(1, "ONE"));

  std::string value;
  EXPECT_TRUE(cache.Lookup(1, &value));
  EXPECT_EQ("ONE", value);

  EXPECT_TRUE(cache.UpdateValue(2, "TWO"));
  // Overflow, 2 should be removed, and not be the "newest" entry after
  // UpdateValue
  cache.Insert(i, "");
  EXPECT_EQ(1U, statistics.Lookup(name + ".n_replace")->Get());
  EXPECT_FALSE(cache.Lookup(2, &value));
}


TEST(T_LruCache, UpdateOnly) {
  perf::Statistics statistics;
  LruCache<int, std::string> cache(cache_size, -1, hasher_int,
                                   perf::StatisticsTemplate(name, &statistics));

  EXPECT_TRUE(cache.Insert(1, "one"));
  EXPECT_TRUE(cache.Insert(2, "two"));

  int key;
  std::string value;
  cache.FilterBegin();
  EXPECT_TRUE(cache.FilterNext());
  cache.FilterGet(&key, &value);
  EXPECT_EQ(1, key);
  cache.FilterEnd();

  cache.Update(1);
  cache.FilterBegin();
  EXPECT_TRUE(cache.FilterNext());
  cache.FilterGet(&key, &value);
  EXPECT_EQ(2, key);
  cache.FilterEnd();

  EXPECT_DEATH(cache.Update(3), ".*");
  cache.Pause();
  EXPECT_DEATH(cache.Update(1), ".*");
}


TEST(T_LruCache, Drop) {
  perf::Statistics statistics;
  LruCache<int, std::string> cache(cache_size, -1, hasher_int,
                                   perf::StatisticsTemplate(name, &statistics));
  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  cache.Insert(1, "eins");
  cache.Insert(2, "zwei");
  cache.Insert(3, "drei");
  cache.Insert(4, "vier");
  cache.Insert(5, "fünf");
  cache.Insert(6, "sechs");
  cache.Insert(7, "sieben");
  cache.Insert(8, "acht");
  cache.Insert(9, "neun");

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  cache.Drop();

  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  const std::string neg = "notfound";
  std::string v(neg);
  bool found;
  found = cache.Lookup(5, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("fünf", v);
  found = cache.Lookup(4, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("vier", v);
  found = cache.Lookup(7, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("sieben", v);
  found = cache.Lookup(3, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("drei", v);
  found = cache.Lookup(5, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("fünf", v);
  found = cache.Lookup(9, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("neun", v);
  found = cache.Lookup(6, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("sechs", v);
  found = cache.Lookup(8, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("acht", v);
  found = cache.Lookup(1, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("eins", v);
  found = cache.Lookup(4, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("vier", v);
  found = cache.Lookup(2, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("zwei", v);
  found = cache.Lookup(5, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("fünf", v);
  found = cache.Lookup(9, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("neun", v);
  found = cache.Lookup(6, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("sechs", v);
  found = cache.Lookup(4, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("vier", v);
  found = cache.Lookup(5, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("fünf", v);
  found = cache.Lookup(9, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("neun", v);
  found = cache.Lookup(6, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("sechs", v);

  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());
}


TEST(T_LruCache, Lookup) {
  perf::Statistics statistics;
  LruCache<int, std::string> cache(cache_size, -1, hasher_int,
                                   perf::StatisticsTemplate(name, &statistics));
  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  cache.Insert(1, "eins");
  cache.Insert(2, "zwei");
  cache.Insert(3, "drei");
  cache.Insert(4, "vier");
  cache.Insert(5, "fünf");
  cache.Insert(6, "sechs");
  cache.Insert(7, "sieben");
  cache.Insert(8, "acht");
  cache.Insert(9, "neun");

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  std::string v;
  bool found;
  found = cache.Lookup(5, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("fünf", v);
  found = cache.Lookup(4, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("vier", v);
  found = cache.Lookup(7, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("sieben", v);
  found = cache.Lookup(3, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("drei", v);
  found = cache.Lookup(5, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("fünf", v);
  found = cache.Lookup(9, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("neun", v);
  found = cache.Lookup(6, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("sechs", v);
  found = cache.Lookup(8, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("acht", v);
  found = cache.Lookup(1, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("eins", v);
  found = cache.Lookup(4, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("vier", v);
  found = cache.Lookup(2, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("zwei", v);
  found = cache.Lookup(5, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("fünf", v);
  found = cache.Lookup(9, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("neun", v);
  found = cache.Lookup(6, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("sechs", v);
  found = cache.Lookup(4, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("vier", v);
  found = cache.Lookup(5, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("fünf", v);
  found = cache.Lookup(9, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("neun", v);
  found = cache.Lookup(6, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("sechs", v);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  const std::string neg = "notfound";
  v = neg;
  found = cache.Lookup(0, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(-1, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(11, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(123, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(14232, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(10, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(23, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(41, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(0, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(16, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(93, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());
}


TEST(T_LruCache, Update) {
  perf::Statistics statistics;
  LruCache<int, std::string> cache(cache_size, -1, hasher_int,
                                   perf::StatisticsTemplate(name, &statistics));
  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  bool inserted;
  inserted = cache.Insert(1, "eins");
  EXPECT_TRUE(inserted);
  inserted = cache.Insert(2, "zwei");
  EXPECT_TRUE(inserted);
  inserted = cache.Insert(3, "drei");
  EXPECT_TRUE(inserted);
  inserted = cache.Insert(4, "vier");
  EXPECT_TRUE(inserted);
  inserted = cache.Insert(5, "fünf");
  EXPECT_TRUE(inserted);
  inserted = cache.Insert(6, "sechs");
  EXPECT_TRUE(inserted);
  inserted = cache.Insert(7, "sieben");
  EXPECT_TRUE(inserted);
  inserted = cache.Insert(8, "acht");
  EXPECT_TRUE(inserted);
  inserted = cache.Insert(9, "neun");
  EXPECT_TRUE(inserted);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  bool found;
  std::string v;

  found = cache.Lookup(1, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("eins", v);
  found = cache.Lookup(2, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("zwei", v);
  found = cache.Lookup(3, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("drei", v);
  found = cache.Lookup(4, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("vier", v);
  found = cache.Lookup(5, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("fünf", v);
  found = cache.Lookup(6, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("sechs", v);
  found = cache.Lookup(7, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("sieben", v);
  found = cache.Lookup(8, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("acht", v);
  found = cache.Lookup(9, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("neun", v);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  inserted = cache.Insert(1, "one");
  EXPECT_FALSE(inserted);
  inserted = cache.Insert(2, "two");
  EXPECT_FALSE(inserted);
  inserted = cache.Insert(3, "three");
  EXPECT_FALSE(inserted);
  inserted = cache.Insert(4, "four");
  EXPECT_FALSE(inserted);
  inserted = cache.Insert(5, "five");
  EXPECT_FALSE(inserted);
  inserted = cache.Insert(6, "six");
  EXPECT_FALSE(inserted);
  inserted = cache.Insert(7, "seven");
  EXPECT_FALSE(inserted);
  inserted = cache.Insert(8, "eight");
  EXPECT_FALSE(inserted);
  inserted = cache.Insert(9, "nine");
  EXPECT_FALSE(inserted);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  found = cache.Lookup(1, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("one", v);
  found = cache.Lookup(2, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("two", v);
  found = cache.Lookup(3, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("three", v);
  found = cache.Lookup(4, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("four", v);
  found = cache.Lookup(5, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("five", v);
  found = cache.Lookup(6, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("six", v);
  found = cache.Lookup(7, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("seven", v);
  found = cache.Lookup(8, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("eight", v);
  found = cache.Lookup(9, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("nine", v);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());
}


TEST(T_LruCache, Forget) {
  perf::Statistics statistics;
  LruCache<int, std::string> cache(cache_size, -1, hasher_int,
                                   perf::StatisticsTemplate(name, &statistics));
  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  cache.Insert(1, "eins");
  cache.Insert(2, "zwei");
  cache.Insert(3, "drei");
  cache.Insert(4, "vier");
  cache.Insert(5, "fünf");
  cache.Insert(6, "sechs");
  cache.Insert(7, "sieben");
  cache.Insert(8, "acht");
  cache.Insert(9, "neun");

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  const std::string neg = "notfound";
  std::string v(neg);
  bool found;
  found = cache.Forget(3);
  EXPECT_TRUE(found);
  found = cache.Forget(8);
  EXPECT_TRUE(found);
  found = cache.Forget(1);
  EXPECT_TRUE(found);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  found = cache.Forget(-1);
  EXPECT_FALSE(found);
  found = cache.Forget(0);
  EXPECT_FALSE(found);
  found = cache.Forget(10);
  EXPECT_FALSE(found);
  found = cache.Forget(2000);
  EXPECT_FALSE(found);
  found = cache.Forget(8);
  EXPECT_FALSE(found);
  found = cache.Forget(3);
  EXPECT_FALSE(found);
  found = cache.Forget(1);
  EXPECT_FALSE(found);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  found = cache.Lookup(8, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(3, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(1, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);

  found = cache.Lookup(2, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("zwei", v);
  found = cache.Lookup(4, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("vier", v);
  found = cache.Lookup(5, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("fünf", v);
  found = cache.Lookup(6, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("sechs", v);
  found = cache.Lookup(7, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("sieben", v);
  found = cache.Lookup(9, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("neun", v);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  found = cache.Forget(2);
  EXPECT_TRUE(found);
  found = cache.Forget(4);
  EXPECT_TRUE(found);
  found = cache.Forget(5);
  EXPECT_TRUE(found);
  found = cache.Forget(6);
  EXPECT_TRUE(found);
  found = cache.Forget(7);
  EXPECT_TRUE(found);
  found = cache.Forget(9);
  EXPECT_TRUE(found);

  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());
}


TEST(T_LruCache, FillCompletely) {
  perf::Statistics statistics;
  LruCache<int, std::string> cache(cache_size, -1, hasher_int,
                                   perf::StatisticsTemplate(name, &statistics));
  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  for (unsigned i = 1; i <= cache_size; ++i) {
    EXPECT_FALSE(cache.IsFull());
    cache.Insert(i, StringifyInt(i));
  }

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_TRUE(cache.IsFull());

  bool found;
  std::string v;
  for (unsigned i = 1; i <= cache_size; ++i) {
    EXPECT_TRUE(cache.IsFull());
    found = cache.Lookup(i, &v);
    EXPECT_TRUE(found);
    EXPECT_EQ(StringifyInt(i), v);
  }

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_TRUE(cache.IsFull());
}


TEST(T_LruCache, LeastRecentlyUsedReplacementSlow) {
  perf::Statistics statistics;
  LruCache<int, std::string> cache(cache_size, -1, hasher_int,
                                   perf::StatisticsTemplate(name, &statistics));
  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  for (unsigned i = 1; i <= cache_size; ++i) {
    EXPECT_FALSE(cache.IsFull());
    cache.Insert(i, StringifyInt(i));
  }

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_TRUE(cache.IsFull());

  bool found;
  std::string v;
  const std::string neg = "notfound";

  // lookup a couple of entries to make then "Least Recently Used" except for
  // the last one
  found = cache.Lookup(2, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("2", v);
  found = cache.Lookup(4, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("4", v);
  found = cache.Lookup(5, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("5", v);
  found = cache.Lookup(6, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("6", v);
  found = cache.Lookup(7, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("7", v);
  found = cache.Lookup(9, &v, false);
  EXPECT_TRUE(found);
  EXPECT_EQ("9", v);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_TRUE(cache.IsFull());

  // insert many new entries to kick out "Non-Recently Used" ones
  for (unsigned i = 1; i <= cache_size - 6; ++i) {
    cache.Insert(i + cache_size, StringifyInt(i + cache_size));
  }

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_TRUE(cache.IsFull());

  // check if the "Least Recently Used" ones are still there
  found = cache.Lookup(2, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("2", v);
  found = cache.Lookup(4, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("4", v);
  found = cache.Lookup(5, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("5", v);
  found = cache.Lookup(6, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("6", v);
  found = cache.Lookup(7, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("7", v);
  found = cache.Lookup(9, &v);
  EXPECT_FALSE(found);

  v = neg;
  found = cache.Lookup(8, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(3, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);
  found = cache.Lookup(1, &v);
  EXPECT_FALSE(found);
  EXPECT_EQ(neg, v);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_TRUE(cache.IsFull());

  for (unsigned i = 1; i <= cache_size - 6; ++i) {
    EXPECT_TRUE(cache.IsFull());
    found = cache.Lookup(i + cache_size, &v);
    EXPECT_TRUE(found);
    EXPECT_EQ(StringifyInt(i + cache_size), v);
  }

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_TRUE(cache.IsFull());
}


TEST(T_LruCache, PauseAndResume) {
  perf::Statistics statistics;
  LruCache<int, std::string> cache(cache_size, -1, hasher_int,
                                   perf::StatisticsTemplate(name, &statistics));
  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  bool inserted;
  inserted = cache.Insert(1, "eins");
  EXPECT_TRUE(inserted);
  inserted = cache.Insert(2, "zwei");
  EXPECT_TRUE(inserted);
  inserted = cache.Insert(3, "drei");
  EXPECT_TRUE(inserted);
  inserted = cache.Insert(4, "vier");
  EXPECT_TRUE(inserted);
  inserted = cache.Insert(5, "fünf");
  EXPECT_TRUE(inserted);

  cache.Pause();

  inserted = cache.Insert(6, "sechs");
  EXPECT_FALSE(inserted);
  inserted = cache.Insert(7, "sieben");
  EXPECT_FALSE(inserted);

  cache.Resume();

  inserted = cache.Insert(8, "acht");
  EXPECT_TRUE(inserted);
  inserted = cache.Insert(9, "neun");
  EXPECT_TRUE(inserted);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  const std::string neg = "notfound";
  std::string v;
  bool found;
  found = cache.Lookup(5, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("fünf", v);
  found = cache.Lookup(4, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("vier", v);
  found = cache.Lookup(7, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("sieben", v);
  found = cache.Lookup(3, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("drei", v);
  found = cache.Lookup(5, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("fünf", v);
  found = cache.Lookup(9, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("neun", v);
  found = cache.Lookup(6, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("sechs", v);
  found = cache.Lookup(8, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("acht", v);
  found = cache.Lookup(1, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("eins", v);
  found = cache.Lookup(4, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("vier", v);
  found = cache.Lookup(2, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("zwei", v);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  cache.Pause();

  v = neg;
  found = cache.Lookup(5, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("fünf", v);
  found = cache.Lookup(4, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("vier", v);
  found = cache.Lookup(7, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("sieben", v);
  found = cache.Lookup(3, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("drei", v);
  found = cache.Lookup(5, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("fünf", v);
  found = cache.Lookup(9, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("neun", v);
  found = cache.Lookup(6, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("sechs", v);
  found = cache.Lookup(8, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("acht", v);
  found = cache.Lookup(1, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("eins", v);
  found = cache.Lookup(4, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("vier", v);
  found = cache.Lookup(2, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("zwei", v);

  cache.Resume();

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  found = cache.Forget(1);
  EXPECT_TRUE(found);
  found = cache.Forget(8);
  EXPECT_TRUE(found);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  cache.Pause();

  found = cache.Forget(9);
  EXPECT_FALSE(found);
  found = cache.Forget(2);
  EXPECT_FALSE(found);

  cache.Resume();

  found = cache.Lookup(1, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("eins", v);
  found = cache.Lookup(2, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("zwei", v);
  found = cache.Lookup(3, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("drei", v);
  found = cache.Lookup(4, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("vier", v);
  found = cache.Lookup(5, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("fünf", v);
  found = cache.Lookup(6, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("sechs", v);
  found = cache.Lookup(7, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("sieben", v);
  found = cache.Lookup(8, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("acht", v);
  found = cache.Lookup(9, &v);
  EXPECT_TRUE(found);
  EXPECT_EQ("neun", v);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  cache.Pause();

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  v = neg;
  found = cache.Lookup(1, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("eins", v);
  found = cache.Lookup(2, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("zwei", v);
  found = cache.Lookup(3, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("drei", v);
  found = cache.Lookup(4, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("vier", v);
  found = cache.Lookup(5, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("fünf", v);
  found = cache.Lookup(6, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("sechs", v);
  found = cache.Lookup(7, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("sieben", v);
  found = cache.Lookup(8, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("acht", v);
  found = cache.Lookup(9, &v);
  EXPECT_FALSE(found);
  EXPECT_NE("neun", v);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());

  cache.Pause();
  cache.Drop();

  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_FALSE(cache.IsFull());
}
