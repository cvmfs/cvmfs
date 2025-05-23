/**
 * This file is part of the CernVM File System.
 */
#ifndef TEST_UNITTESTS_SHRINKWRAP_TESTUTIL_SHRINKWRAP_H_
#define TEST_UNITTESTS_SHRINKWRAP_TESTUTIL_SHRINKWRAP_H_

#include <sys/types.h>
#include <unistd.h>

#include <string>

#include "crypto/hash.h"
#include "xattr.h"

void ExpectListHas(const char *query, char **dir_list, bool has_not = false);

void FreeList(char **list, size_t len);

XattrList *CreateSampleXattrlist(std::string var);

struct cvmfs_attr *CreateSampleStat(const char *name,
                                    ino_t st_ino,
                                    mode_t st_mode,
                                    off_t st_size,
                                    XattrList *xlist,
                                    shash::Any *content_hash = NULL,
                                    const char *link = NULL);

bool SupportsXattrs(const std::string &directory);

#endif  // TEST_UNITTESTS_SHRINKWRAP_TESTUTIL_SHRINKWRAP_H_
