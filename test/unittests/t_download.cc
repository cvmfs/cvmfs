#include "gtest/gtest.h"

#include <unistd.h>

#include <cstdio>

#include "../../cvmfs/download.h"
#include "../../cvmfs/util.h"

using namespace std;  // NOLINT

namespace download {

class T_Download : public ::testing::Test {
 protected:
  virtual void SetUp() {
    download_mgr.Init(8, false /* use_system_proxy */);
    ffoo = CreateTempFile("/tmp/cvmfstest", 0600, "w+", &foo_path);
    assert(ffoo);
    foo_url = "file://" + foo_path;
  }

  virtual ~T_Download() {
    download_mgr.Fini();
    fclose(ffoo);
    unlink(foo_path.c_str());
  }

  DownloadManager download_mgr;
  FILE *ffoo;
  string foo_path;
  string foo_url;
};


//------------------------------------------------------------------------------


// A placeholder test for future unit testing of the download module
TEST_F(T_Download, File) {
  string dest_path;
  FILE *fdest = CreateTempFile("/tmp/cvmfstest", 0600, "w+", &dest_path);
  ASSERT_TRUE(fdest != NULL);
  UnlinkGuard unlink_guard(dest_path);

  JobInfo info(&foo_url, false /* compressed */, false /* probe hosts */,
               fdest, NULL);
  download_mgr.Fetch(&info);
  EXPECT_EQ(info.error_code, kFailOk);
  fclose(fdest);
}


TEST_F(T_Download, SortWrtGeoReply) {
  vector<string> input_hosts;
  EXPECT_FALSE(download_mgr.SortWrtGeoReply("", &input_hosts));

  input_hosts.push_back("a");
  EXPECT_FALSE(download_mgr.SortWrtGeoReply("a", &input_hosts));
  EXPECT_FALSE(download_mgr.SortWrtGeoReply("1,1", &input_hosts));
  EXPECT_FALSE(download_mgr.SortWrtGeoReply("1,3", &input_hosts));
  EXPECT_FALSE(download_mgr.SortWrtGeoReply("2,3", &input_hosts));
  EXPECT_FALSE(download_mgr.SortWrtGeoReply("2", &input_hosts));
  EXPECT_TRUE(download_mgr.SortWrtGeoReply("1", &input_hosts));
  EXPECT_EQ(input_hosts.size(), 1U);
  EXPECT_EQ(input_hosts[0], "a");

  input_hosts.push_back("b");
  EXPECT_FALSE(download_mgr.SortWrtGeoReply(",", &input_hosts));
  EXPECT_FALSE(download_mgr.SortWrtGeoReply("2,", &input_hosts));
  EXPECT_FALSE(download_mgr.SortWrtGeoReply("1", &input_hosts));
  EXPECT_FALSE(download_mgr.SortWrtGeoReply("3,2,1", &input_hosts));
  EXPECT_TRUE(download_mgr.SortWrtGeoReply("2,1", &input_hosts));
  EXPECT_EQ(input_hosts.size(), 2U);
  EXPECT_EQ(input_hosts[0], "b");
  EXPECT_EQ(input_hosts[1], "a");

  EXPECT_TRUE(download_mgr.SortWrtGeoReply("2,1\n", &input_hosts));
  EXPECT_EQ(input_hosts.size(), 2U);
  EXPECT_EQ(input_hosts[0], "a");
  EXPECT_EQ(input_hosts[1], "b");

  input_hosts.push_back("c");
  input_hosts.push_back("d");
  EXPECT_TRUE(download_mgr.SortWrtGeoReply("4,3,1,2\n", &input_hosts));
  EXPECT_EQ(input_hosts.size(), 4U);
  EXPECT_EQ(input_hosts[0], "d");
  EXPECT_EQ(input_hosts[1], "c");
  EXPECT_EQ(input_hosts[2], "a");
  EXPECT_EQ(input_hosts[3], "b");
}

}  // namespace download
