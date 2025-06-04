/**
 * This file is part of the CernVM File System.
 */

#include <algorithm>

#include <cerrno>
#include <string>
#include <gtest/gtest.h>

#include "acl.h"

using namespace std;  // NOLINT

class T_Acl : public ::testing::Test {
};

// The function should return success.
// But it shouldn't be expected to return exactly the same as libacl.
static void should_pass_noncompat(const char *textual, unsigned char *acl_binary_expected, unsigned int acl_binary_expected_len)
{
  size_t binary_size;
  char *binary_acl;
  bool equiv_mode;
  int const ret = acl_from_text_to_xattr_value(string(textual), binary_acl, binary_size, equiv_mode);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(binary_size, acl_binary_expected_len);
  if (binary_size == 0) {
    ASSERT_TRUE(equiv_mode);
    ASSERT_EQ(binary_acl, nullptr);
  } else {
    ASSERT_FALSE(equiv_mode);
    ASSERT_NE(binary_acl, nullptr);
    ASSERT_EQ(0, memcmp(binary_acl, acl_binary_expected, binary_size));
  }
  free(binary_acl);
}

static void should_pass(const char *textual, unsigned char *acl_binary_expected, unsigned int acl_binary_expected_len)
{
  size_t binary_size;
  char *binary_acl;
  bool equiv_mode;
#ifdef COMPARE_TO_LIBACL
  int ret = acl_from_text_to_xattr_value_both_impl(string(textual), binary_acl, binary_size, equiv_mode);
#else // COMPARE_TO_LIBACL
  int const ret = acl_from_text_to_xattr_value(string(textual), binary_acl, binary_size, equiv_mode);
#endif // COMPARE_TO_LIBACL
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(binary_size, acl_binary_expected_len);
  if (binary_size == 0) {
    ASSERT_TRUE(equiv_mode);
    ASSERT_EQ(binary_acl, nullptr);
  } else {
    ASSERT_FALSE(equiv_mode);
    ASSERT_NE(binary_acl, nullptr);
    ASSERT_EQ(0, memcmp(binary_acl, acl_binary_expected, binary_size));
  }
  free(binary_acl);
}

static void should_fail(const char *textual)
{
  size_t binary_size;
  char *binary_acl;
  bool equiv_mode;
#ifdef COMPARE_TO_LIBACL
  int ret = acl_from_text_to_xattr_value_both_impl(string(textual), binary_acl, binary_size, equiv_mode);
#else // COMPARE_TO_LIBACL
  int const ret = acl_from_text_to_xattr_value(string(textual), binary_acl, binary_size, equiv_mode);
#endif // COMPARE_TO_LIBACL
  ASSERT_EQ(ret, EINVAL);
}

TEST_F(T_Acl, t1) {
  const char *textual =
    "user::rwx\n"
    "group::r-x\n"
    "group:root:rwx\n"
    "group:1000:rwx\n"
    "mask::rwx\n"
    "other::---\n";

  // setfacl --modify-file acl.txt /tmp/test
  // getfattr --name=system.posix_acl_access  --only-values /tmp/test > acl_binary_expected
  // xxd --include acl_binary_expected
  unsigned char acl_binary_expected[] = {
    0x02, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x07, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x04, 0x00, 0x05, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x08, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x08, 0x00, 0x07, 0x00, 0xe8, 0x03, 0x00, 0x00,
    0x10, 0x00, 0x07, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x20, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff
  };
  unsigned int const acl_binary_expected_len = 52;
  should_pass(textual, acl_binary_expected, acl_binary_expected_len);
}

TEST_F(T_Acl, t2) {

  // no required entries u::,g::,o::
  should_fail("u::r");
  should_fail("u:bin:rw");
  should_fail("u:bin:rw,g::r,o::-");
  should_fail("");
  should_fail(",");
  should_fail(",,");
  should_fail("\n,");
  should_fail(",\n");

  // since u:name: entry is there, mask:: entry is required
  should_fail("u:bin:rw,u::rw,g::r,o::-");

  // setfacl does not produce system.posix_acl_access attribute in this case,
  // because this ACL corresponds to simple mermissions mode expression.
  // libacl's acl_from_text() returns EINVAL.
  should_pass("u::rw,g::r,o::-", nullptr, 0);
  should_pass("u::rw\ng::r\no::-\n", nullptr, 0);
  should_pass("u::rw-,g::r--,o::---", nullptr, 0);

  // excessive delimiters
  should_pass_noncompat("u::rw,,g::r,,,o::-", nullptr, 0);
  should_pass_noncompat("u::rw\ng::r,\no::-\n", nullptr, 0);
  should_pass_noncompat("u::rw-,g::r--,o::---", nullptr, 0);

  should_fail("u::-,u::rw,g::r,o::-"); // duplicate u::
}

TEST_F(T_Acl, t3) {
  const char *textual = "u:1:rw,u::rw,g::r,o::-,mask::rwx";
  unsigned char acl_binary_expected[] = {
    0x02, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x06, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x02, 0x00, 0x06, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x04, 0x00, 0x04, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x10, 0x00, 0x07, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x20, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff
  };
  unsigned int const acl_binary_expected_len = 44;
  should_pass(textual, acl_binary_expected, acl_binary_expected_len);
}

TEST_F(T_Acl, t5) {
  const char *textual = "u::-,g::r,o::rwx,mask::rwx,u:1:rw";
  unsigned char acl_binary_expected[] = {
    0x02, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x02, 0x00, 0x06, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x04, 0x00, 0x04, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x10, 0x00, 0x07, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x20, 0x00, 0x07, 0x00, 0xff, 0xff, 0xff, 0xff
  };

  unsigned int const acl_binary_expected_len = 44;
  should_pass(textual, acl_binary_expected, acl_binary_expected_len);
}

TEST_F(T_Acl, t6) {
  const char *textual = "u:1:rw,u:2:r,u::-,g::-,o::-,m::-";
  unsigned char acl_binary_expected[] = {
    0x02, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x02, 0x00, 0x06, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x02, 0x00, 0x04, 0x00, 0x02, 0x00, 0x00, 0x00,
    0x04, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x10, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x20, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff
  };
  unsigned int const acl_binary_expected_len = 52;
  should_pass(textual, acl_binary_expected, acl_binary_expected_len);
}

TEST_F(T_Acl, t8) {
  // our code doesn't support "d(efault):tag:id:perm" and doesn't need to.
  const char *textual = "u:bin:rwx,u:daemon:rw,d:u:bin:rwx,d:m:rx,u::-,g::-,o::-,m::-";
  size_t binary_size;
  char *binary_acl;
  bool equiv_mode;
  int const ret = acl_from_text_to_xattr_value(string(textual), binary_acl, binary_size, equiv_mode);
  ASSERT_EQ(ret, EINVAL);
}

TEST_F(T_Acl, t9) {
  const char *textual = "u:2:rx,g:root:rx,g:2:rwx,u::-,g::-,o::-,m::-";
  unsigned char acl_binary_expected[] = {
    0x02, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x02, 0x00, 0x05, 0x00, 0x02, 0x00, 0x00, 0x00,
    0x04, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x08, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x08, 0x00, 0x07, 0x00, 0x02, 0x00, 0x00, 0x00,
    0x10, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
    0x20, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff
  };
  unsigned int const acl_binary_expected_len = 60;
  should_pass(textual, acl_binary_expected, acl_binary_expected_len);
}
