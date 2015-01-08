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
               fdest,  NULL);
  download_mgr.Fetch(&info);
  EXPECT_EQ(info.error_code, kFailOk);
  fclose(fdest);
}


TEST_F(T_Download, LocalFile2Mem) {
  string dest_path;
  FILE *fdest = CreateTempFile("/tmp/cvmfstest", 0600, "w+", &dest_path);
  ASSERT_TRUE(fdest != NULL);
  UnlinkGuard unlink_guard(dest_path);
  char buf = '1';
  fwrite(&buf, 1, 1, fdest);
  fclose(fdest);

  string url = "file://" + dest_path;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */, NULL);
  download_mgr.Fetch(&info);
  ASSERT_EQ(info.error_code, kFailOk);
  ASSERT_EQ(info.destination_mem.size, 1U);
  EXPECT_EQ(info.destination_mem.data[0], '1');
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
  EXPECT_EQ(1U, input_hosts.size());
  EXPECT_EQ("a", input_hosts[0]);

  input_hosts.push_back("b");
  EXPECT_FALSE(download_mgr.SortWrtGeoReply(",", &input_hosts));
  EXPECT_FALSE(download_mgr.SortWrtGeoReply("2,", &input_hosts));
  EXPECT_FALSE(download_mgr.SortWrtGeoReply("1", &input_hosts));
  EXPECT_FALSE(download_mgr.SortWrtGeoReply("3,2,1", &input_hosts));
  EXPECT_TRUE(download_mgr.SortWrtGeoReply("2,1", &input_hosts));
  EXPECT_EQ(2U, input_hosts.size());
  EXPECT_EQ("b", input_hosts[0]);
  EXPECT_EQ("a", input_hosts[1]);

  EXPECT_TRUE(download_mgr.SortWrtGeoReply("2,1\n", &input_hosts));
  EXPECT_EQ(2U, input_hosts.size());
  EXPECT_EQ("a", input_hosts[0]);
  EXPECT_EQ("b", input_hosts[1]);

  input_hosts.push_back("c");
  input_hosts.push_back("d");
  EXPECT_TRUE(download_mgr.SortWrtGeoReply("4,3,1,2\n", &input_hosts));
  EXPECT_EQ(4U, input_hosts.size());
  EXPECT_EQ("d", input_hosts[0]);
  EXPECT_EQ("c", input_hosts[1]);
  EXPECT_EQ("a", input_hosts[2]);
  EXPECT_EQ("b", input_hosts[3]);
}

}  // namespace download
