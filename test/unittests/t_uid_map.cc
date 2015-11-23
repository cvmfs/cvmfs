/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/uid_map.h"
#include "../../cvmfs/util.h"

template <typename MapT>
class T_UidMap : public ::testing::Test {
 protected:
  static const std::string sandbox;

 protected:
  typedef typename MapT::key_type   key_type;
  typedef typename MapT::value_type value_type;

 protected:
  void SetUp() {
    const bool retval = MkdirDeep(sandbox, 0700);
    ASSERT_TRUE(retval) << "failed to create sandbox";
  }

  void TearDown() {
    const bool retval = RemoveTree(sandbox);
    ASSERT_TRUE(retval) << "failed to remove sandbox";
  }

  void WriteFile(const std::string &path, const std::string &content) {
    FILE *f = fopen(path.c_str(), "w+");
    ASSERT_NE(static_cast<FILE*>(NULL), f)
      << "failed to open. errno: " << errno;
    const size_t bytes_written = fwrite(content.data(), 1, content.length(), f);
    ASSERT_EQ(bytes_written, content.length())
      << "failed to write. errno: " << errno;

    const int retval = fclose(f);
    ASSERT_EQ(0, retval) << "failed to close. errno: " << errno;
  }

  std::string GetValidFile() {
    const std::string valid_file =
      "# comment\n"  // comment
      "42 3\n"       // ordinary rule
      "1  2\n"       // ordinary rule with multiple spaces
      "\n"           // empty line
      "# comment\n"  // comment
      "*   1337\n";  // default value

    const std::string path = CreateTempPath(sandbox + "/valid", 0600);
    WriteFile(path, valid_file);
    return path;
  }

  std::string GetInvalidFile1() {
    const std::string invalid_file =
      "# comment\n"  // comment
      "42 3\n"       // ordinary rule
      "1 *\n"        // invalid rule (non-numeric map result)    <---
      "\n"           // empty line
      "# comment\n"  // comment
      "*   1337\n";  // default value

    const std::string path = CreateTempPath(sandbox + "/invalid", 0600);
    WriteFile(path, invalid_file);
    return path;
  }

  std::string GetInvalidFile2() {
    const std::string invalid_file =
      "# comment\n"  // comment
      "12 4\n"       // ordinary rule
      "1\n";         // invalid rule (no map result) <---

    const std::string path = CreateTempPath(sandbox + "/invalid", 0600);
    WriteFile(path, invalid_file);
    return path;
  }

  std::string GetInvalidFile3() {
    const std::string invalid_file =
      "# empty file\n"  // comment
      "foo 14";         // invalid rule (non-numeric ID value) <---

    const std::string path = CreateTempPath(sandbox + "/invalid", 0600);
    WriteFile(path, invalid_file);
    return path;
  }

  template <typename T>
  key_type k(const T k) const { return key_type(k); }
  template <typename T>
  value_type v(const T v) const { return value_type(v); }
};

template <typename MapT>
const std::string T_UidMap<MapT>::sandbox = "./cvmfs_ut_uid_map";

typedef ::testing::Types<
  UidMap,
  GidMap > UidMapTypes;
TYPED_TEST_CASE(T_UidMap, UidMapTypes);

TYPED_TEST(T_UidMap, Initialize) {
  TypeParam map;
  EXPECT_TRUE(map.IsValid());
  EXPECT_FALSE(map.HasDefault());
  EXPECT_TRUE(map.IsEmpty());
  EXPECT_FALSE(map.HasEffect());
}


TYPED_TEST(T_UidMap, Insert) {
  TypeParam map;
  map.Set(TestFixture::k(0), TestFixture::v(1));
  map.Set(TestFixture::k(1), TestFixture::v(2));
  EXPECT_TRUE(map.IsValid());
  EXPECT_FALSE(map.HasDefault());
  EXPECT_EQ(2u, map.RuleCount());
  EXPECT_FALSE(map.IsEmpty());
  EXPECT_TRUE(map.HasEffect());
}


TYPED_TEST(T_UidMap, SetDefault) {
  TypeParam map;
  map.SetDefault(TestFixture::v(42));
  EXPECT_EQ(0u, map.RuleCount());
  EXPECT_TRUE(map.IsValid());
  EXPECT_TRUE(map.HasDefault());
  EXPECT_EQ(0u, map.RuleCount());
  EXPECT_EQ(42u, map.GetDefault());
}


TYPED_TEST(T_UidMap, Contains) {
  TypeParam map;
  map.Set(TestFixture::k(0), TestFixture::v(1));
  map.Set(TestFixture::k(1), TestFixture::v(2));

  EXPECT_TRUE(map.Contains(TestFixture::k(0)));
  EXPECT_TRUE(map.Contains(TestFixture::k(1)));
  EXPECT_FALSE(map.Contains(TestFixture::k(2)));

  map.SetDefault(TestFixture::v(42));
  EXPECT_FALSE(map.Contains(TestFixture::k(2)));
}


TYPED_TEST(T_UidMap, Empty) {
  TypeParam map;
  EXPECT_TRUE(map.IsEmpty());
  EXPECT_FALSE(map.HasEffect());
  map.SetDefault(TestFixture::v(42));
  EXPECT_TRUE(map.IsEmpty());
  EXPECT_TRUE(map.HasEffect());
}


TYPED_TEST(T_UidMap, MapWithoutDefault) {
  TypeParam map;
  map.Set(TestFixture::k(0), TestFixture::v(1));
  map.Set(TestFixture::k(1), TestFixture::v(2));

  EXPECT_EQ(TestFixture::v(1), map.Map(TestFixture::k(0)));
  EXPECT_EQ(TestFixture::v(2), map.Map(TestFixture::k(1)));
  EXPECT_EQ(TestFixture::v(3), map.Map(TestFixture::k(3)));
}


TYPED_TEST(T_UidMap, MapWithDefault) {
  TypeParam map;
  map.Set(TestFixture::k(0), TestFixture::v(1));
  map.Set(TestFixture::k(1), TestFixture::v(2));
  map.SetDefault(TestFixture::v(42));

  EXPECT_EQ(TestFixture::v(1),  map.Map(TestFixture::k(0)));
  EXPECT_EQ(TestFixture::v(2),  map.Map(TestFixture::k(1)));
  EXPECT_EQ(TestFixture::v(42), map.Map(TestFixture::k(3)));
}


TYPED_TEST(T_UidMap, ReadFromFile) {
  TypeParam map;
  const std::string path = TestFixture::GetValidFile();
  ASSERT_TRUE(map.Read(path));
  EXPECT_TRUE(map.IsValid());
  EXPECT_EQ(2u, map.RuleCount());
  EXPECT_TRUE(map.HasDefault());
  EXPECT_EQ(1337u, map.GetDefault());

  EXPECT_TRUE(map.Contains(TestFixture::k(42)));
  EXPECT_TRUE(map.Contains(TestFixture::k(1)));

  EXPECT_FALSE(map.Contains(TestFixture::k(2)));
  EXPECT_FALSE(map.Contains(TestFixture::k(1337)));

  EXPECT_EQ(TestFixture::v(3),  map.Map(TestFixture::k(42)));
  EXPECT_EQ(TestFixture::v(2),  map.Map(TestFixture::k(1)));
  EXPECT_EQ(TestFixture::v(1337), map.Map(TestFixture::k(3)));
  EXPECT_EQ(TestFixture::v(1337), map.Map(TestFixture::k(4)));
  EXPECT_EQ(TestFixture::v(1337), map.Map(TestFixture::k(0)));
}


TYPED_TEST(T_UidMap, ReadFromCorruptFiles) {
  TypeParam map1;
  TypeParam map2;
  TypeParam map3;
  const std::string path1 = TestFixture::GetInvalidFile1();
  const std::string path2 = TestFixture::GetInvalidFile2();
  const std::string path3 = TestFixture::GetInvalidFile3();

  ASSERT_FALSE(map1.Read(path1));
  EXPECT_FALSE(map1.IsValid());

  ASSERT_FALSE(map2.Read(path2));
  EXPECT_FALSE(map2.IsValid());

  ASSERT_FALSE(map3.Read(path3));
  EXPECT_FALSE(map3.IsValid());
}
