/**
 * This file is part of the CernVM File System.
 */
#include "testutil_shrinkwrap.h"

#include <errno.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include "libcvmfs.h"
#include "util/platform.h"
#include "util/posix.h"
#include "xattr.h"

void ExpectListHas(const char *query, char **dir_list, bool has_not) {
  char **cur_el = dir_list;
  while ((*cur_el) != NULL) {
    if (strcmp(*cur_el, query) == 0) {
      return;
    }
    cur_el = (cur_el + 1);
  }
  EXPECT_TRUE(has_not) << "Could not find element " << query << " in list";
}


void FreeList(char **list, size_t len) {
  for (size_t i = 0; i < len; i++) {
    free(*(list + i));
  }
  free(list);
}


XattrList *CreateSampleXattrlist(std::string var) {
  XattrList *result = new XattrList();
  result->Set("user.foo", std::string(var));
  result->Set("user.bar", std::string(255, 'a'));
  return result;
}


struct cvmfs_attr *CreateSampleStat(const char *name,
                                    ino_t st_ino,
                                    mode_t st_mode,
                                    off_t st_size,
                                    XattrList *xlist,
                                    shash::Any *content_hash /* = NULL*/,
                                    const char *link /* = NULL*/) {
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


bool SupportsXattrs(const std::string &directory) {
  bool result = true;
  std::string path = directory + "/testxattr";
  CreateFile(path, 0600, false /*ignore_failure*/);
  bool retval = platform_setxattr(path, "user.foo", "bar");
  // On errors other than EOPNOTSUPP, we do want the test cases to fail
  if (!retval && (errno = EOPNOTSUPP))
    result = false;
  unlink(path.c_str());
  return result;
}
