/**
 * This file is part of the CernVM File System.
 */
#ifndef TEST_UNITTESTS_SHRINKWRAP_TEST_UTIL_H_
#define TEST_UNITTESTS_SHRINKWRAP_TEST_UTIL_H_

#include <sys/types.h>
#include <unistd.h>

#include <string>

#include "hash.h"
#include "xattr.h"

void ExpectListHas(const char *query, char **dir_list, bool has_not = false);

void FreeList(char **list, size_t len);

XattrList *create_sample_xattrlist(std::string var);

struct cvmfs_attr *create_sample_stat(const char *name,
  ino_t st_ino, mode_t st_mode, off_t st_size,
  XattrList *xlist, shash::Any *content_hash = NULL,
  const char *link = NULL);

bool SupportsXattrs(const std::string &directory);

#endif  // TEST_UNITTESTS_SHRINKWRAP_TEST_UTIL_H_
