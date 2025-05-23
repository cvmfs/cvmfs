/**
 * This file is part of the CernVM File System.
 */

#include <unistd.h>

#include <cassert>
#include <cstdio>

#include "c_file_sandbox.h"
#include "c_http_server.h"
#include "gtest/gtest.h"
#include "network/sink.h"
#include "network/sink_file.h"
#include "network/sink_mem.h"
#include "network/sink_path.h"
#include "util/file_guard.h"
#include "util/posix.h"
#include "util/prng.h"

using namespace std;  // NOLINT

namespace download {

class T_Sink : public FileSandbox {
 public:
  T_Sink() : FileSandbox(string(tmp_path) + "/server_dir") { }

 protected:
  virtual void SetUp() { CreateSandbox(); }

  virtual void TearDown() { RemoveSandbox(); }

  virtual ~T_Sink() { }

  FILE *CreateTemporaryFile(std::string *path) const {
    return CreateTempFile(GetCurrentWorkingDirectory() + "/cvmfs_uT_Sink", 0600,
                          "w+", path);
  }

  static const char tmp_path[];
};

const char T_Sink::tmp_path[] = "./cvmfs_uT_Sink";

//------------------------------------------------------------------------------

TEST_F(T_Sink, MemSinkWrite) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  cvmfs::MemSink sink;
  sink.Reserve(src_content.length());
  int64_t ret = sink.Write(src_content.c_str(), src_content.length());

  ASSERT_EQ(ret, static_cast<int64_t>(src_content.length()));
  ASSERT_EQ(sink.pos(), src_content.length());
  EXPECT_STREQ(reinterpret_cast<char *>(sink.data()), src_content.c_str());
}


TEST_F(T_Sink, MemSinkResetOrPurge) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  cvmfs::MemSink sink;
  sink.Reserve(src_content.length());
  int64_t ret = sink.Write(src_content.c_str(), src_content.length());

  ASSERT_EQ(ret, static_cast<int64_t>(src_content.length()));
  ASSERT_TRUE(sink.IsValid());
  ASSERT_EQ(sink.Reset(), 0);
  ASSERT_TRUE(sink.IsValid());

  ret = sink.Write(src_content.c_str(), src_content.length());

  ASSERT_EQ(ret, static_cast<int64_t>(src_content.length()));
  ASSERT_GT(sink.size(), 0ul);
  ASSERT_EQ(sink.pos(), src_content.length());
  EXPECT_STREQ(reinterpret_cast<char *>(sink.data()), src_content.c_str());
}

TEST_F(T_Sink, MemSinkTooSmall) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  cvmfs::MemSink sink;
  sink.Reserve(src_content.length() - 10);
  int64_t ret = sink.Write(src_content.c_str(), src_content.length());

  ASSERT_EQ(ret, static_cast<int64_t>(src_content.length()));
  ASSERT_EQ(sink.pos(), src_content.length());
  ASSERT_GT(sink.size(), src_content.length());

  sink.Reset();
  EXPECT_EQ(sink.size(), 0ul);

  sink.Release();
  sink.Reserve(src_content.length() - 10);
  ret = sink.Write(src_content.c_str(), src_content.length());
  ASSERT_LT(ret, 0);  // some error occured
}

TEST_F(T_Sink, MemSinkUnprivAdopt) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  unsigned char *buf = static_cast<unsigned char *>(
      malloc(src_content.length()));
  memcpy(buf, src_content.c_str(), src_content.length());

  cvmfs::MemSink sink;
  sink.Adopt(src_content.length(), src_content.length(), buf, false);

  ASSERT_EQ(sink.pos(), src_content.length());
  EXPECT_STREQ(reinterpret_cast<char *>(sink.data()), src_content.c_str());

  free(buf);
}

TEST_F(T_Sink, MemSinkNotValid) {
  unsigned char *buf = static_cast<unsigned char *>(malloc(10));
  cvmfs::MemSink sink;
  EXPECT_TRUE(sink.IsValid());

  sink.Adopt(10, 10, buf, false);
  EXPECT_TRUE(sink.IsValid());

  sink.Adopt(10, 10, NULL, false);
  EXPECT_FALSE(sink.IsValid());

  sink.Adopt(0, 0, buf, false);
  EXPECT_FALSE(sink.IsValid());

  free(buf);
}

//------------------------------------------------------------------------------
// FileSink
//------------------------------------------------------------------------------
TEST_F(T_Sink, FileSinkWrite) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  string dest_path;
  FILE *f = CreateTempFile("./cvmfs_ut_download", 0600, "w+", &dest_path);

  cvmfs::FileSink sink(f);
  ASSERT_TRUE(sink.IsValid());
  int64_t ret = sink.Write(src_content.c_str(), src_content.length());

  ASSERT_EQ(ret, static_cast<int64_t>(src_content.length()));

  // check content
  FILE *fcheck = fopen(dest_path.c_str(), "r");
  char dest_content[ret];

  size_t read = fread(dest_content, 1, ret, fcheck);
  ASSERT_GT(read, static_cast<size_t>(0));
  ASSERT_EQ(fclose(fcheck), 0);

  EXPECT_STREQ(dest_content, src_content.c_str());
}

TEST_F(T_Sink, FileSinkReset) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  string dest_path;
  FILE *f = CreateTempFile("./cvmfs_ut_download", 0600, "w+", &dest_path);

  cvmfs::FileSink sink(f);
  ASSERT_TRUE(sink.IsValid());
  int64_t ret = sink.Write(src_content.c_str(), src_content.length());

  ASSERT_EQ(ret, static_cast<int64_t>(src_content.length()));

  // check content
  FILE *fcheck = fopen(dest_path.c_str(), "r");
  char dest_content[ret];

  size_t read = fread(dest_content, 1, ret, fcheck);
  ASSERT_GT(read, static_cast<size_t>(0));
  ASSERT_EQ(fclose(fcheck), 0);

  EXPECT_STREQ(dest_content, src_content.c_str());

  ASSERT_EQ(sink.Reset(), 0);

  // recheck content
  fcheck = fopen(dest_path.c_str(), "r");
  char dest_content2[ret];
  memset(dest_content2, 0, ret);

  read = fread(dest_content2, 1, ret, fcheck);
  ASSERT_EQ(read, static_cast<size_t>(0));
  ASSERT_EQ(fclose(fcheck), 0);

  EXPECT_STREQ(dest_content2, "");
}

TEST_F(T_Sink, FileSinkPurge) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  string dest_path;
  FILE *f = CreateTempFile("./cvmfs_ut_download", 0600, "w+", &dest_path);

  cvmfs::FileSink sink(f, true);
  ASSERT_TRUE(sink.IsValid());
  int64_t ret = sink.Write(src_content.c_str(), src_content.length());

  ASSERT_EQ(ret, static_cast<int64_t>(src_content.length()));

  // check content
  FILE *fcheck = fopen(dest_path.c_str(), "r");
  char dest_content[ret];

  size_t read = fread(dest_content, 1, ret, fcheck);
  ASSERT_GT(read, static_cast<size_t>(0));
  ASSERT_EQ(fclose(fcheck), 0);

  EXPECT_STREQ(dest_content, src_content.c_str());

  ASSERT_EQ(sink.Purge(), 0);

  // recheck content
  ASSERT_FALSE(sink.IsValid());
}

TEST_F(T_Sink, FileSinkUnprivPurge) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  string dest_path;
  FILE *f = CreateTempFile("./cvmfs_ut_download", 0600, "w+", &dest_path);

  cvmfs::FileSink sink(f);
  ASSERT_TRUE(sink.IsValid());
  int64_t ret = sink.Write(src_content.c_str(), src_content.length());

  ASSERT_EQ(ret, static_cast<int64_t>(src_content.length()));

  // check content
  FILE *fcheck = fopen(dest_path.c_str(), "r");
  char dest_content[ret];

  size_t read = fread(dest_content, 1, ret, fcheck);
  ASSERT_GT(read, static_cast<size_t>(0));
  ASSERT_EQ(fclose(fcheck), 0);

  EXPECT_STREQ(dest_content, src_content.c_str());

  sink.Release();
  ASSERT_EQ(sink.Purge(), 0);

  // recheck content
  fcheck = fopen(dest_path.c_str(), "r");
  char dest_content2[ret];
  memset(dest_content2, 0, ret);

  read = fread(dest_content2, 1, ret, fcheck);
  ASSERT_EQ(read, static_cast<size_t>(0));
  ASSERT_EQ(fclose(fcheck), 0);

  EXPECT_STREQ(dest_content2, "");
}

TEST_F(T_Sink, FileSinkNotValid) {
  cvmfs::FileSink sink(NULL);
  ASSERT_FALSE(sink.IsValid());
}

//------------------------------------------------------------------------------
// PathSink
//------------------------------------------------------------------------------
TEST_F(T_Sink, PathSinkWrite) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  string dest_path;
  FILE *f = CreateTempFile("./cvmfs_ut_download", 0600, "w+", &dest_path);
  ASSERT_EQ(fclose(f), 0);

  cvmfs::PathSink sink(dest_path);
  ASSERT_TRUE(sink.IsValid());
  int64_t ret = sink.Write(src_content.c_str(), src_content.length());

  ASSERT_EQ(ret, static_cast<int64_t>(src_content.length()));

  // check content
  FILE *fcheck = fopen(dest_path.c_str(), "r");
  char dest_content[ret];

  size_t read = fread(dest_content, 1, ret, fcheck);
  ASSERT_GT(read, static_cast<size_t>(0));
  ASSERT_EQ(fclose(fcheck), 0);

  EXPECT_STREQ(dest_content, src_content.c_str());
}

TEST_F(T_Sink, PathSinkReset) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  string dest_path;
  FILE *f = CreateTempFile("./cvmfs_ut_download", 0600, "w+", &dest_path);
  ASSERT_EQ(fclose(f), 0);

  cvmfs::PathSink sink(dest_path);
  ASSERT_TRUE(sink.IsValid());
  int64_t ret = sink.Write(src_content.c_str(), src_content.length());

  ASSERT_EQ(ret, static_cast<int64_t>(src_content.length()));

  // check content
  FILE *fcheck = fopen(dest_path.c_str(), "r");
  char dest_content[ret];

  size_t read = fread(dest_content, 1, ret, fcheck);
  ASSERT_GT(read, static_cast<size_t>(0));
  ASSERT_EQ(fclose(fcheck), 0);

  EXPECT_STREQ(dest_content, src_content.c_str());

  ASSERT_EQ(sink.Reset(), 0);

  // recheck content
  fcheck = fopen(dest_path.c_str(), "r");
  char dest_content2[ret];

  memset(dest_content2, 0, ret);

  read = fread(dest_content2, 1, ret, fcheck);
  ASSERT_EQ(read, static_cast<size_t>(0));
  ASSERT_EQ(fclose(fcheck), 0);

  EXPECT_STREQ(dest_content2, "");
}

TEST_F(T_Sink, PathSinkPurge) {
  string src_path = GetSmallFile();
  string src_content = GetFileContents(src_path);

  string dest_path;
  FILE *f = CreateTempFile("./cvmfs_ut_download", 0600, "w+", &dest_path);
  ASSERT_EQ(fclose(f), 0);

  cvmfs::PathSink sink(dest_path);
  ASSERT_TRUE(sink.IsValid());
  int64_t ret = sink.Write(src_content.c_str(), src_content.length());

  ASSERT_EQ(ret, static_cast<int64_t>(src_content.length()));

  // check content
  FILE *fcheck = fopen(dest_path.c_str(), "r");
  char dest_content[ret];

  size_t read = fread(dest_content, 1, ret, fcheck);
  ASSERT_GT(read, static_cast<size_t>(0));
  ASSERT_EQ(fclose(fcheck), 0);

  EXPECT_STREQ(dest_content, src_content.c_str());

  ASSERT_EQ(sink.Purge(), 0);

  // recheck content
  ASSERT_FALSE(sink.IsValid());
  int val = unlink(dest_path.c_str());

  ASSERT_EQ(val, -1);
  ASSERT_EQ(errno, ENOENT);
}

TEST_F(T_Sink, PathSinkNotValid) {
  cvmfs::PathSink sink("");
  ASSERT_FALSE(sink.IsValid());
}


}  // namespace download
