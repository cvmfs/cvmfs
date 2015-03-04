/**
 * This file is part of the CernVM File System.
 */

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


TEST_F(T_Download, StripDirect) {
  string cleaned = "FALSE";
  EXPECT_FALSE(download_mgr.StripDirect("", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect("DIRECT", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect("DIRECT;DIRECT", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect("DIRECT;DIRECT|DIRECT", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect("DIRECT;DIRECT|", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect(";", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect(";||;;;|||", &cleaned));
  EXPECT_EQ("", cleaned);
  EXPECT_FALSE(download_mgr.StripDirect("A|B", &cleaned));
  EXPECT_EQ("A|B", cleaned);
  EXPECT_FALSE(download_mgr.StripDirect("A|B;C|D;E|F|G", &cleaned));
  EXPECT_EQ("A|B;C|D;E|F|G", cleaned);
  EXPECT_TRUE(download_mgr.StripDirect("A|DIRECT;C|D;E|F;DIRECT", &cleaned));
  EXPECT_EQ("A;C|D;E|F", cleaned);
}


TEST_F(T_Download, ValidateGeoReply) {
  vector<uint64_t> geo_order;
  EXPECT_FALSE(download_mgr.ValidateGeoReply("", geo_order.size(), &geo_order));

  geo_order.push_back(0);
  EXPECT_FALSE(
    download_mgr.ValidateGeoReply("a", geo_order.size(), &geo_order));
  EXPECT_FALSE(
    download_mgr.ValidateGeoReply("1,1", geo_order.size(), &geo_order));
  EXPECT_FALSE(
    download_mgr.ValidateGeoReply("1,3", geo_order.size(), &geo_order));
  EXPECT_FALSE(
    download_mgr.ValidateGeoReply("2,3", geo_order.size(), &geo_order));
  EXPECT_FALSE(
    download_mgr.ValidateGeoReply("2", geo_order.size(), &geo_order));
  EXPECT_TRUE(
    download_mgr.ValidateGeoReply("1", geo_order.size(), &geo_order));
  EXPECT_EQ(geo_order.size(), 1U);
  EXPECT_EQ(geo_order[0], 0U);

  geo_order.push_back(0);
  EXPECT_FALSE(
    download_mgr.ValidateGeoReply(",", geo_order.size(), &geo_order));
  EXPECT_FALSE(
    download_mgr.ValidateGeoReply("2,", geo_order.size(), &geo_order));
  EXPECT_FALSE(
    download_mgr.ValidateGeoReply("1", geo_order.size(), &geo_order));
  EXPECT_FALSE(
    download_mgr.ValidateGeoReply("3,2,1", geo_order.size(), &geo_order));
  EXPECT_TRUE(
    download_mgr.ValidateGeoReply("2,1", geo_order.size(), &geo_order));
  EXPECT_EQ(geo_order.size(), 2U);
  EXPECT_EQ(geo_order[0], 1U);
  EXPECT_EQ(geo_order[1], 0U);

  EXPECT_TRUE(
    download_mgr.ValidateGeoReply("2,1\n", geo_order.size(), &geo_order));
  EXPECT_EQ(geo_order.size(), 2U);
  EXPECT_EQ(geo_order[0], 1U);
  EXPECT_EQ(geo_order[1], 0U);

  geo_order.push_back(0);
  geo_order.push_back(0);
  EXPECT_TRUE(
    download_mgr.ValidateGeoReply("4,3,1,2\n", geo_order.size(), &geo_order));
  EXPECT_EQ(geo_order.size(), 4U);
  EXPECT_EQ(geo_order[0], 3U);
  EXPECT_EQ(geo_order[1], 2U);
  EXPECT_EQ(geo_order[2], 0U);
  EXPECT_EQ(geo_order[3], 1U);
}

}  // namespace download
