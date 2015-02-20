/**
 * This file is part of the CernVM File System.
 */


#include "gtest/gtest.h"

#include "../../cvmfs/options.h"
#include "../../cvmfs/util.h"

using namespace std;  // NOLINT

template <class OptionsT>
class T_Options : public ::testing::Test {
 protected:
  virtual void SetUp() {
    FILE *temp_file = CreateTempFile("/tmp/cvmfs-test", 0600, "w",
        &config_file_);
    ASSERT_TRUE(temp_file != NULL);
    unlink_guard_.Set(config_file_);
    fprintf(temp_file,
            "CVMFS_CACHE_BASE=/root/cvmfs_testing/cache\n"
            "CVMFS_RELOAD_SOCKETS=/root/cvmfs_testing/cache\n"
            "CVMFS_SERVER_URL=http://volhcb28:3128/data\n"
            "IdontHaveAnEqual\n"
            "I=have=twoEquals\n"
            "I = and spaces\n"
            "value=\n"
            "CVMFS_SHARED_CACHE=no\n"
            "CVMFS_HTTP_PROXY=DIRECT\n"
            "export A=B\n");
    int result = fclose(temp_file);
    ASSERT_EQ(0, result);
  }

 protected:
  // type-based overlaoded instantiation of History object wrapper
  // Inspired from here:
  //   http://stackoverflow.com/questions/5512910/
  //          explicit-specialization-of-template-class-member-function
  template <typename T> struct type {};

  unsigned ExpectedValues(const type<BashOptionsManager>  type_specifier) {
    return 9u;
  }

  unsigned ExpectedValues(const type<SimpleOptionsParser>  type_specifier) {
    return 6u;
  }

  unsigned ExpectedValues() {
    return ExpectedValues(type<OptionsT>());
  }

 protected:
  OptionsT     options_manager_;
  UnlinkGuard  unlink_guard_;
  string       config_file_;
};  // class T_Options

typedef ::testing::Types<BashOptionsManager, SimpleOptionsParser> OptionsManagers;
TYPED_TEST_CASE(T_Options, OptionsManagers);


TYPED_TEST(T_Options, ParsePath) {
  string container;
  OptionsManager &options_manager = TestFixture::options_manager_;
  const string &config_file = TestFixture::config_file_;
  const unsigned expected_number_elements = TestFixture::ExpectedValues();
  options_manager.ParsePath(config_file, false);

  ASSERT_EQ(expected_number_elements, options_manager.GetAllKeys().size());

  EXPECT_TRUE(options_manager.GetValue("CVMFS_CACHE_BASE", &container));
  EXPECT_EQ("/root/cvmfs_testing/cache", container);
  EXPECT_TRUE(options_manager.GetSource("CVMFS_CACHE_BASE", &container));
  EXPECT_EQ(config_file, container);

  EXPECT_TRUE(options_manager.GetValue("CVMFS_RELOAD_SOCKETS", &container));
  EXPECT_EQ("/root/cvmfs_testing/cache", container);
  EXPECT_TRUE(options_manager.GetSource("CVMFS_RELOAD_SOCKETS", &container));
  EXPECT_EQ(config_file, container);

  EXPECT_TRUE(options_manager.GetValue("CVMFS_SERVER_URL", &container));
  EXPECT_EQ("http://volhcb28:3128/data", container);
  EXPECT_TRUE(options_manager.GetSource("CVMFS_SERVER_URL", &container));
  EXPECT_EQ(config_file, container);

  EXPECT_TRUE(options_manager.GetValue("CVMFS_SHARED_CACHE", &container));
  EXPECT_EQ("no", container);
  EXPECT_TRUE(options_manager.GetSource("CVMFS_SHARED_CACHE", &container));
  EXPECT_EQ(config_file, container);

  EXPECT_TRUE(options_manager.GetValue("CVMFS_HTTP_PROXY", &container));
  EXPECT_EQ("DIRECT", container);
  EXPECT_TRUE(options_manager.GetSource("CVMFS_HTTP_PROXY", &container));
  EXPECT_EQ(config_file, container);

  EXPECT_TRUE(options_manager.GetValue("value", &container));
  EXPECT_EQ("", container);
  EXPECT_TRUE(options_manager.GetSource("value", &container));
  EXPECT_EQ(config_file, container);
}

TYPED_TEST(T_Options, ParsePathNoFile) {
  string fileName = "somethingThatDoesntExists";
  TestFixture::options_manager_.ParsePath(fileName, false);
  ASSERT_EQ(0u, TestFixture::options_manager_.GetAllKeys().size());
}

