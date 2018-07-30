/**
 * This file is part of the CernVM File System.
 */
#include <gtest/gtest.h>

#include "libcvmfs.h"
#include "util/posix.h"

#include "export_plugin/fs_traversal_interface.h"
#include "export_plugin/posix/interface.h"
#include "export_plugin/util.h"
#include "test-util.h"

TEST(T_Fs_Traversal_POSIX, TestGarbageCollection) {
  struct fs_traversal *dest = posix_get_interface();
  struct fs_traversal_context *context
    = dest->initialize("./", "posix", "./data", 4, NULL);

  std::string content1 = "a";
  shash::Any content1_hash(shash::kSha1);
  shash::HashString(content1, &content1_hash);
  std::string content2 = "b";
  shash::Any content2_hash(shash::kSha1);
  shash::HashString(content2, &content2_hash);
  XattrList *xlist1 = create_sample_xattrlist("TestGarbageCollection1");
  XattrList *xlist2 = create_sample_xattrlist("TestGarbageCollection2");
  struct cvmfs_attr *stat1 = create_sample_stat("foo", 0, 0777, 0, xlist1,
    &content1_hash);
  struct cvmfs_attr *stat2 = create_sample_stat("foo", 0, 0777, 0, xlist1,
    &content2_hash);
  struct cvmfs_attr *stat3 = create_sample_stat("foo", 0, 0777, 0, xlist2,
    &content1_hash);
  struct cvmfs_attr *stat4 = create_sample_stat("foo", 0, 0777, 0, xlist2,
    &content2_hash);

  dest->touch(context, stat1);
  const char *ident1 = dest->get_identifier(context, stat1);
  dest->do_link(context, "file1.txt", ident1);
  dest->touch(context, stat2);
  const char *ident2 = dest->get_identifier(context, stat2);
  dest->do_link(context, "file1.txt", ident2);  // unlinks ident1
  dest->touch(context, stat3);
  const char *ident3 = dest->get_identifier(context, stat3);
  dest->do_link(context, "file3.txt", ident3);
  dest->touch(context, stat4);
  const char *ident4 = dest->get_identifier(context, stat4);
  dest->do_link(context, "file4.txt", ident4);

  dest->do_unlink(context, "file3.txt");

  dest->finalize(context);
  context = dest->initialize("./", "posix", "./data", 4, NULL);
  dest->garbage_collector(context);

  std::string data_base_path = "./data/";
  ASSERT_STRNE(ident1, ident2);
  ASSERT_STRNE(ident1, ident3);
  ASSERT_STRNE(ident1, ident4);
  ASSERT_STRNE(ident2, ident3);
  ASSERT_STRNE(ident2, ident4);
  ASSERT_STRNE(ident3, ident4);
  ASSERT_FALSE(FileExists(data_base_path + ident1));
  ASSERT_TRUE(FileExists(data_base_path + ident2));
  ASSERT_FALSE(FileExists(data_base_path + ident3));
  ASSERT_TRUE(FileExists(data_base_path + ident4));

  delete stat1;
  delete stat2;
  delete stat3;
  delete stat4;
  delete xlist1;
  delete xlist2;
}
