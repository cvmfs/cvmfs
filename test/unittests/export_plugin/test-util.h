/**
 * This file is part of the CernVM File System.
 */
#ifndef TEST_UNITTESTS_EXPORT_PLUGIN_TEST_UTIL_H_
#define TEST_UNITTESTS_EXPORT_PLUGIN_TEST_UTIL_H_

#include <unistd.h>

void AssertListHas(const char *query, char **dirList, size_t listLen,
  bool hasNot = false);

#endif  // TEST_UNITTESTS_EXPORT_PLUGIN_TEST_UTIL_H_
