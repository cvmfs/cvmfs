/**
 * This file is part of the CernVM File System.
 */


#include "gtest/gtest.h"

#include "../../cvmfs/options.h"
#include "../../cvmfs/util.h"

using namespace std;  // NOLINT

namespace options {

class T_Options : public ::testing::Test {
 protected:
  virtual void SetUp() {
    string filename;
    FILE *temp_file = CreateTempFile("/tmp/cvmfs-test", 0600, "w", &filename);
    ASSERT_TRUE(temp_file != NULL);
    config_file = filename;
	unlink_guard.Set(filename);

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
    fflush(temp_file);
    fclose(temp_file);
  }

 protected:
  FastOptionsManager options_manager;
  UnlinkGuard unlink_guard;
  string config_file;

};  // class T_Options


TEST_F(T_Options, ParsePathSimple) {
  string container;
  options_manager.ParsePath(config_file, false);

  ASSERT_EQ(6u, options_manager.GetAllKeys().size());

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

TEST_F(T_Options, ParsePathSimpleNoFile) {
  string fileName="somethingThatDoesntExists";
  options_manager.ParsePath(fileName, false);
  ASSERT_EQ(0u, options_manager.GetAllKeys().size());
}

}  // namespace options
