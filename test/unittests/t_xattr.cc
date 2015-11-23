/**
 * This file is part of the CernVM File System.
 */

#include <algorithm>

#include <cerrno>
#include <gtest/gtest.h>

#include "../../cvmfs/platform.h"
#include "../../cvmfs/util.h"
#include "../../cvmfs/xattr.h"

using namespace std;  // NOLINT

class T_Xattr : public ::testing::Test {
 protected:
  virtual void SetUp() {
    default_list.Set("keya", "valuea");
    default_list.Set("keyb", "valueb");
    default_list.Set("empty_key", "");
  }

  unsigned int CountAttributesInFile(const std::string &path) const {
    ssize_t sz_list = platform_llistxattr(path.c_str(), NULL, 0);
    if (sz_list <= 0) {
      return 0;
    }

    char *list = reinterpret_cast<char *>(malloc(sz_list));
    sz_list = platform_llistxattr(path.c_str(), list, sz_list);
    if (sz_list <= 0) {
      return 0;
    }

    const std::string attrs(list, sz_list);
    free(list);
    return std::count(attrs.begin(), attrs.end(), '\0');
  }

  XattrList default_list;
};


TEST_F(T_Xattr, CreateFromFile) {
  EXPECT_EQ(NULL, XattrList::CreateFromFile("/no/such/file"));

  // Create extended attributes
  string tmp_path;
  FILE *f = CreateTempFile("./cvmfs_ut_xattr", 0600, "w", &tmp_path);
  ASSERT_TRUE(f != NULL);
  fclose(f);
  UnlinkGuard unlink_guard(tmp_path);

  // check if xattr is supported on the current platform
  const bool success = platform_setxattr(tmp_path, "user.check", "foo");
  if (!success && errno == EOPNOTSUPP) {
    SUCCEED() << "extended attributes are not supported on " << tmp_path;
    return;
  }

  const unsigned int default_attrs = CountAttributesInFile(tmp_path);

  UniquePtr<XattrList> from_file1(XattrList::CreateFromFile(tmp_path));
  ASSERT_TRUE(from_file1.IsValid());
  EXPECT_EQ(default_attrs, from_file1->ListKeys().size());

  string value;
  ASSERT_TRUE(platform_setxattr(tmp_path, "user.test", "value"))
    << "failed to set user defined extended attribute (errno: " << errno << ")";
  UniquePtr<XattrList> from_file2(XattrList::CreateFromFile(tmp_path));
  ASSERT_TRUE(from_file2.IsValid());
  EXPECT_EQ(default_attrs + 1, from_file2->ListKeys().size());
  EXPECT_TRUE(from_file2->Get("user.test", &value));
  EXPECT_TRUE(from_file2->Has("user.test"));
  EXPECT_EQ("value", value);

#ifndef __APPLE__
  ASSERT_TRUE(platform_setxattr(tmp_path, "user.test2", "value2"));
  string long_string = "user." + string(250, 'x');
  string too_long_string = "user." + string(300, 'x');
  ASSERT_TRUE(platform_setxattr(tmp_path, long_string, long_string));
  ASSERT_TRUE(platform_setxattr(tmp_path, "user.test3", too_long_string));
  UniquePtr<XattrList> from_file3(XattrList::CreateFromFile(tmp_path));
  ASSERT_TRUE(from_file3.IsValid());
  EXPECT_EQ(default_attrs + 3, from_file3->ListKeys().size());
  EXPECT_TRUE(from_file3->Get("user.test", &value));
  EXPECT_TRUE(from_file3->Has("user.test"));
  EXPECT_EQ("value", value);
  EXPECT_TRUE(from_file3->Get("user.test2", &value));
  EXPECT_TRUE(from_file3->Has("user.test2"));
  EXPECT_EQ("value2", value);
  EXPECT_TRUE(from_file3->Get(long_string, &value));
  EXPECT_TRUE(from_file3->Has(long_string));
  EXPECT_EQ(long_string, value);
#endif
}


TEST_F(T_Xattr, Deserialize) {
  unsigned char *buf;
  unsigned size;
  default_list.Serialize(&buf, &size);
  EXPECT_GT(size, 0U);
  EXPECT_TRUE(buf != NULL);

  UniquePtr<XattrList> xattr_list(XattrList::Deserialize(buf, size));
  free(buf);
  ASSERT_TRUE(xattr_list.IsValid());
  EXPECT_EQ(3U, xattr_list->ListKeys().size());
  string value;
  EXPECT_TRUE(xattr_list->Get("keya", &value));
  EXPECT_TRUE(xattr_list->Has("keya"));
  EXPECT_EQ("valuea", value);
  EXPECT_TRUE(xattr_list->Get("keyb", &value));
  EXPECT_TRUE(xattr_list->Has("keyb"));
  EXPECT_EQ("valueb", value);
  EXPECT_TRUE(xattr_list->Get("empty_key", &value));
  EXPECT_TRUE(xattr_list->Has("empty_key"));
  EXPECT_EQ("", value);
}

TEST_F(T_Xattr, DeserializeNull) {
  UniquePtr<XattrList> xattr_list(XattrList::Deserialize(NULL, 0));
  ASSERT_TRUE(xattr_list.IsValid());
  EXPECT_EQ(0U, xattr_list->ListKeys().size());
}

TEST_F(T_Xattr, DeserializeInvalid) {
  unsigned char *buf;
  unsigned size;
  default_list.Serialize(&buf, &size);
  EXPECT_GT(size, 0U);
  EXPECT_TRUE(buf != NULL);

  UniquePtr<XattrList> xl1(XattrList::Deserialize(buf, 0));
  EXPECT_FALSE(xl1.IsValid());

  buf[0] = 255;
  UniquePtr<XattrList> xl2(XattrList::Deserialize(buf, size));
  EXPECT_FALSE(xl2.IsValid());
  buf[0] = XattrList::kVersion;

  UniquePtr<XattrList> xl3(XattrList::Deserialize(buf, 3));
  EXPECT_FALSE(xl3.IsValid());

  UniquePtr<XattrList> xl4(XattrList::Deserialize(buf, 5));
  EXPECT_FALSE(xl4.IsValid());

  unsigned char save0, save1;
  save0 = buf[2];
  save1 = buf[3];
  buf[2] = 0;
  buf[3] = 0;
  UniquePtr<XattrList> xl5(XattrList::Deserialize(buf, size));
  EXPECT_FALSE(xl5.IsValid());
  buf[3] = save1;
  UniquePtr<XattrList> xl6(XattrList::Deserialize(buf, size));
  EXPECT_FALSE(xl6.IsValid());

  buf[2] = save0;
  UniquePtr<XattrList> xl7(XattrList::Deserialize(buf, size));
  EXPECT_TRUE(xl7.IsValid());

  free(buf);
}


TEST_F(T_Xattr, Has) {
  string value;
  EXPECT_TRUE(default_list.Has("keya"));
  EXPECT_TRUE(default_list.Has("empty_key"));
  EXPECT_FALSE(default_list.Has("not here"));
}


TEST_F(T_Xattr, Get) {
  string value;
  EXPECT_TRUE(default_list.Get("keya", &value));
  EXPECT_EQ("valuea", value);
  EXPECT_TRUE(default_list.Get("empty_key", &value));
  EXPECT_EQ("", value);
  EXPECT_FALSE(default_list.Get("not here", &value));
}


TEST_F(T_Xattr, ListKeys) {
  XattrList empty;
  EXPECT_TRUE(empty.ListKeys().empty());
  ASSERT_EQ(3U, default_list.ListKeys().size());
  EXPECT_EQ("empty_key", default_list.ListKeys()[0]);
  EXPECT_EQ("keya", default_list.ListKeys()[1]);
  EXPECT_EQ("keyb", default_list.ListKeys()[2]);
}


TEST_F(T_Xattr, ListKeysPosix) {
  const char base_list[] = "user.a\0user.b\0keya\0";
  string existing_list(base_list, sizeof(base_list)-1);
  XattrList empty;
  EXPECT_EQ("", empty.ListKeysPosix(""));
  const char expect1[] = "user.a\0user.b\0keya\0";
  EXPECT_EQ(string(expect1, sizeof(expect1)-1),
            empty.ListKeysPosix(existing_list));
  const char expect2[] = "empty_key\0keya\0keyb\0";
  EXPECT_EQ(string(expect2, sizeof(expect2)-1),
            default_list.ListKeysPosix(""));
  const char expect3[] = "user.a\0user.b\0empty_key\0keya\0keyb\0";
  EXPECT_EQ(string(expect3, sizeof(expect3)-1),
            default_list.ListKeysPosix(existing_list));
}


TEST_F(T_Xattr, Set) {
  string value;
  // Set a key
  EXPECT_TRUE(default_list.Set("keyc", "valuec"));
  EXPECT_TRUE(default_list.Get("keyc", &value));
  EXPECT_EQ("valuec", value);
  // Change a key
  EXPECT_TRUE(default_list.Set("keyc", ""));
  EXPECT_TRUE(default_list.Get("keyc", &value));
  EXPECT_EQ("", value);

  // Invalid operations
  string longstring(257, 'a');
  EXPECT_FALSE(default_list.Set(longstring, "value"));
  EXPECT_FALSE(default_list.Set("key", longstring));
  string nullstring(1, '\0');
  EXPECT_FALSE(default_list.Set(nullstring, "value"));

  XattrList new_list;
  for (unsigned i = 0; i < 256; ++i) {
    EXPECT_TRUE(new_list.Set(StringifyInt(i), ""));
  }
  EXPECT_FALSE(new_list.Set("256", ""));
}


TEST_F(T_Xattr, Remove) {
  string value;
  EXPECT_TRUE(default_list.Has("keya"));
  EXPECT_TRUE(default_list.Get("keya", &value));
  EXPECT_TRUE(default_list.Remove("keya"));
  EXPECT_FALSE(default_list.Get("keya", &value));
  EXPECT_FALSE(default_list.Has("keya"));
  EXPECT_FALSE(default_list.Remove("keya"));
}


TEST_F(T_Xattr, IsEmpty) {
  EXPECT_TRUE(XattrList().IsEmpty());
  EXPECT_FALSE(default_list.IsEmpty());
  vector<string> keys = default_list.ListKeys();
  for (unsigned i = 0; i < keys.size(); ++i) {
    EXPECT_TRUE(default_list.Remove(keys[i]));
  }
  EXPECT_TRUE(default_list.IsEmpty());
}


// The normal serialization part is tested as part of Deserialize
TEST_F(T_Xattr, SerializeNull) {
  unsigned char *buf;
  unsigned size;
  XattrList empty_list;
  empty_list.Serialize(&buf, &size);
  EXPECT_EQ(NULL, buf);
  EXPECT_EQ(0U, size);
}


TEST_F(T_Xattr, Limits) {
  XattrList large_list;
  string large_value(256, 'a');
  for (unsigned i = 0; i < 256; ++i) {
    char hex[3];
    snprintf(hex, sizeof(hex), "%02x", i);
    string large_key(128, hex[0]);
    large_key += string(128, hex[1]);
    EXPECT_TRUE(large_list.Set(large_key, large_value));
  }
  EXPECT_EQ(256U, large_list.ListKeys().size());
  for (unsigned i = 0; i < 256; ++i) {
    string value;
    char hex[3];
    snprintf(hex, sizeof(hex), "%02x", i);
    string large_key(128, hex[0]);
    large_key += string(128, hex[1]);
    EXPECT_TRUE(large_list.Get(large_key, &value));
    EXPECT_EQ(large_value, value);
  }
}
