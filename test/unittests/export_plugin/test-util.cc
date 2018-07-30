/**
 * This file is part of the CernVM File System.
 */
#include "test-util.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstring>

#include "libcvmfs.h"
#include "xattr.h"

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

XattrList *create_sample_xattrlist(std::string var) {
  XattrList *result = new XattrList();
  result->Set("user.foo", var);
  result->Set("user.bar", std::string(255, 'a'));
  return result;
}
struct cvmfs_attr *create_sample_stat(const char *name,
  ino_t st_ino, mode_t st_mode, off_t st_size,
  XattrList *xlist, shash::Any *content_hash/* = NULL*/,
  const char *link/* = NULL*/) {
  char *hash_result = NULL;
  if (content_hash != NULL) {
    std::string hash = content_hash->ToString();
    hash_result = strdup(hash.c_str());
  }
  char *link_result = NULL;
  if (link != NULL) {
    link_result = strdup(link);
  }
  struct cvmfs_attr *result = cvmfs_attr_init();
  result->st_ino = st_ino;
  result->st_mode = st_mode;
  result->st_nlink = 1;
  result->st_uid = getuid();
  result->st_gid = getgid();
  result->st_size = st_size;
  result->mtime = time(NULL);

  result->cvm_checksum = hash_result;
  result->cvm_symlink = link_result;
  result->cvm_name = strdup(name);
  result->cvm_parent = strdup("/");
  result->cvm_xattrs = xlist;

  return result;
}