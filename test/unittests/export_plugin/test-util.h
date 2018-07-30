/**
 * This file is part of the CernVM File System.
 */
#ifndef TEST_UNITTESTS_EXPORT_PLUGIN_TEST_UTIL_H_
#define TEST_UNITTESTS_EXPORT_PLUGIN_TEST_UTIL_H_

#include <unistd.h>

#include <string>

#include "hash.h"
#include "xattr.h"

void AssertListHas(const char *query, char **dirList, size_t listLen,
  bool hasNot = false);

XattrList *create_sample_xattrlist(std::string var);

struct cvmfs_attr *create_sample_stat(const char *name,
  ino_t st_ino, mode_t st_mode, off_t st_size,
  XattrList *xlist, shash::Any *content_hash = NULL,
  const char *link = NULL);

#endif  // TEST_UNITTESTS_EXPORT_PLUGIN_TEST_UTIL_H_
