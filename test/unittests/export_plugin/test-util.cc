/**
 * This file is part of the CernVM File System.
 */
#include "test-util.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstring>

void AssertListHas(const char *query, char **dirList, size_t listLen,
  bool hasNot) {
  char **curEl = dirList;
  while ((*curEl) != NULL) {
    if (strcmp(*curEl, query) == 0) {
      return;
    }
    curEl = (curEl+1);
  }
  ASSERT_TRUE(hasNot) << "Could not find element " << query << " in list";
}
