/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"

#include <unistd.h>

#include <cstdio>

#include "../../cvmfs/compression.h"
#include "../../cvmfs/download.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/prng.h"
#include "../../cvmfs/sink.h"
#include "../../cvmfs/statistics.h"
#include "../../cvmfs/util.h"

using namespace std;  // NOLINT

namespace download {

class T_Download : public ::testing::Test {
 protected:
  virtual void SetUp() {
    download_mgr.Init(8, false, /* use_system_proxy */
        &statistics);
    ffoo = CreateTemporaryFile(&foo_path);
    assert(ffoo);
    foo_url = "file://" + foo_path;
  }

  virtual ~T_Download() {
    download_mgr.Fini();
    fclose(ffoo);
    unlink(foo_path.c_str());
  }

  FILE *CreateTemporaryFile(std::string *path) const {
    return CreateTempFile(GetCurrentWorkingDirectory() + "/cvmfs_ut_download",
                          0600, "w+", path);
  }

  perf::Statistics statistics;
  DownloadManager download_mgr;
  FILE *ffoo;
  string foo_path;
  string foo_url;
};


class TestSink : public cvmfs::Sink {
 public:
  TestSink() {
    FILE *f = CreateTempFile("./cvmfs_ut_download", 0600, "w+", &path);
    assert(f);
    fd = dup(fileno(f));
    assert(f >= 0);
    fclose(f);
  }

  virtual int64_t Write(const void *buf, uint64_t size) {
    return write(fd, buf, size);
  }

  virtual int Reset() {
    int retval = ftruncate(fd, 0);
    assert(retval == 0);
    return 0;
  }

  ~TestSink() {
    close(fd);
    unlink(path.c_str());
  }

  int fd;
  string path;
};


//------------------------------------------------------------------------------


TEST_F(T_Download, File) {
  string dest_path;
  FILE *fdest = CreateTemporaryFile(&dest_path);
  ASSERT_TRUE(fdest != NULL);
  UnlinkGuard unlink_guard(dest_path);

  JobInfo info(&foo_url, false /* compressed */, false /* probe hosts */,
               fdest,  NULL);
  download_mgr.Fetch(&info);
  EXPECT_EQ(info.error_code, kFailOk);
  fclose(fdest);
}


TEST_F(T_Download, Multiple) {
  string dest_path;
  FILE *fdest = CreateTemporaryFile(&dest_path);
  ASSERT_TRUE(fdest != NULL);
  UnlinkGuard unlink_guard(dest_path);

  DownloadManager second_mgr;
  second_mgr.Init(8, false, /* use_system_proxy */ &statistics, "second");

  JobInfo info(&foo_url, false /* compressed */, false /* probe hosts */,
               fdest,  NULL);
  JobInfo info2(&foo_url, false /* compressed */, false /* probe hosts */,
                fdest,  NULL);
  download_mgr.Fetch(&info);
  second_mgr.Fetch(&info2);
  EXPECT_EQ(info.error_code, kFailOk);
  EXPECT_EQ(info2.error_code, kFailOk);
  fclose(fdest);
  second_mgr.Fini();
}


TEST_F(T_Download, LocalFile2Mem) {
  string dest_path;
  FILE *fdest = CreateTemporaryFile(&dest_path);
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


TEST_F(T_Download, LocalFile2Sink) {
  string dest_path;
  FILE *fdest = CreateTemporaryFile(&dest_path);
  ASSERT_TRUE(fdest != NULL);
  UnlinkGuard unlink_guard(dest_path);
  char buf = '1';
  fwrite(&buf, 1, 1, fdest);
  fflush(fdest);

  TestSink test_sink;
  string url = "file://" + dest_path;
  JobInfo info(&url, false /* compressed */, false /* probe hosts */,
               &test_sink, NULL /* expected hash */);
  download_mgr.Fetch(&info);
  EXPECT_EQ(info.error_code, kFailOk);
  EXPECT_EQ(1, pread(test_sink.fd, &buf, 1, 0));
  EXPECT_EQ('1', buf);

  rewind(fdest);
  Prng prng;
  prng.InitLocaltime();
  unsigned N = 16*1024;
  unsigned size = N*sizeof(uint32_t);
  uint32_t rnd_buf[N];  // 64kB
  for (unsigned i = 0; i < N; ++i)
    rnd_buf[i] = prng.Next(2147483647);
  shash::Any checksum(shash::kMd5);
  EXPECT_TRUE(
    zlib::CompressMem2File(reinterpret_cast<const unsigned char *>(rnd_buf),
                           size, fdest, &checksum));
  fclose(fdest);

  TestSink test_sink2;
  JobInfo info2(&url, true /* compressed */, false /* probe hosts */,
                &test_sink2, &checksum /* expected hash */);
  download_mgr.Fetch(&info2);
  EXPECT_EQ(info2.error_code, kFailOk);
  EXPECT_EQ(size, GetFileSize(test_sink2.path));

  uint32_t validation[N];
  EXPECT_EQ(static_cast<int>(size),
    pread(test_sink2.fd, &validation, size, 0));
  EXPECT_EQ(0, memcmp(validation, rnd_buf, size));
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


TEST_F(T_Download, ParseHttpCode) {
  char digits[3];
  digits[0] = '0';  digits[1] = '0';  digits[2] = 'a';
  EXPECT_EQ(-1, DownloadManager::ParseHttpCode(digits));
  digits[0] = '0';  digits[1] = '0';  digits[2] = '0';
  EXPECT_EQ(0, DownloadManager::ParseHttpCode(digits));
  digits[0] = '0';  digits[1] = '0';  digits[2] = '1';
  EXPECT_EQ(1, DownloadManager::ParseHttpCode(digits));
  digits[0] = '1';  digits[1] = '0';  digits[2] = '1';
  EXPECT_EQ(101, DownloadManager::ParseHttpCode(digits));
  digits[0] = '9';  digits[1] = '9';  digits[2] = '9';
  EXPECT_EQ(999, DownloadManager::ParseHttpCode(digits));
}

}  // namespace download
