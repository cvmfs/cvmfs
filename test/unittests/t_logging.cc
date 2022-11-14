/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cassert>
#include <cstdio>
#include <string>

#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

class T_Logging : public ::testing::Test {
 protected:
  virtual void SetUp() {
    tmp_path_ = CreateTempDir("./cvmfs_ut_logging");
    EXPECT_NE("", tmp_path_);
  }

  virtual void TearDown() {
    LogShutdown();
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
  }

  bool SearchInFile(const string &path, const string &needle) {
    bool result = false;
    FILE *f = fopen(path.c_str(), "r");
    assert(f != NULL);
    string line;
    while (GetLineFile(f, &line)) {
      if (line.find(needle) != std::string::npos) {
        result = true;
        break;
      }
    }
    fclose(f);
    return result;
  }

 protected:
  string tmp_path_;
};


TEST_F(T_Logging, MicroSyslog) {
  EXPECT_DEATH(SetLogMicroSyslog("/no/such/path"), ".*");
  SetLogMicroSyslog(tmp_path_ + "/usyslog");
  EXPECT_TRUE(FileExists(tmp_path_ + "/usyslog"));
  EXPECT_TRUE(FileExists(tmp_path_ + "/usyslog.1"));
  LogCvmfs(kLogCvmfs, kLogSyslog, "Line1");
  EXPECT_TRUE(SearchInFile(tmp_path_ + "/usyslog", "Line1"));
  EXPECT_FALSE(SearchInFile(tmp_path_ + "/usyslog.1", "Line1"));

  SetLogSyslogPrefix("prefix");
  LogCvmfs(kLogCvmfs, kLogSyslog, "Line2");
  EXPECT_TRUE(SearchInFile(tmp_path_ + "/usyslog", "(prefix) Line2"));

  LogCvmfs(kLogCvmfs, kLogSyslogWarn, "Warning");
  EXPECT_TRUE(SearchInFile(tmp_path_ + "/usyslog", "Warning"));
  LogCvmfs(kLogCvmfs, kLogSyslogErr, "Error");
  EXPECT_TRUE(SearchInFile(tmp_path_ + "/usyslog", "Error"));

  SetLogMicroSyslog("");
  LogCvmfs(kLogCvmfs, kLogSyslog, "Nirvana");
  EXPECT_FALSE(SearchInFile(tmp_path_ + "/usyslog", "Nirvana"));
}


TEST_F(T_Logging, MicroSyslogRotateSlow) {
  SetLogMicroSyslog(tmp_path_ + "/usyslog");
  SetLogMicroSyslogMaxSize(4000);  // 4kB
  EXPECT_TRUE(FileExists(tmp_path_ + "/usyslog"));
  EXPECT_TRUE(FileExists(tmp_path_ + "/usyslog.1"));

  for (unsigned i = 0; i < 1000; ++i) {
    LogCvmfs(kLogCvmfs, kLogSyslog, "Line");
  }

  EXPECT_TRUE(SearchInFile(tmp_path_ + "/usyslog", "Line"));
  EXPECT_TRUE(SearchInFile(tmp_path_ + "/usyslog.1", "Line"));

  int64_t sz_usyslog = GetFileSize(tmp_path_ + "/usyslog");
  EXPECT_GT(sz_usyslog, 0);
  EXPECT_LT(sz_usyslog, 4100);
  sz_usyslog = GetFileSize(tmp_path_ + "/usyslog.1");
  EXPECT_GT(sz_usyslog, 0);
  EXPECT_LT(sz_usyslog, 4100);
}


TEST_F(T_Logging, Custom) {
  EXPECT_DEATH(LogCvmfs(kLogCvmfs, kLogCustom0, "Line"), ".*");
  EXPECT_DEATH(LogCvmfs(kLogCvmfs, kLogCustom1, "Line"), ".*");
  EXPECT_DEATH(LogCvmfs(kLogCvmfs, kLogCustom2, "Line"), ".*");

  EXPECT_DEATH(SetLogCustomFile(0, tmp_path_ + "/no/such/path"), ".*");
  SetLogCustomFile(0, tmp_path_ + "/custom.0");
  SetLogCustomFile(1, tmp_path_ + "/custom.1");
  SetLogCustomFile(2, tmp_path_ + "/custom.2");
  EXPECT_DEATH(SetLogCustomFile(3, tmp_path_ + "/custom.3"), ".*");

  LogCvmfs(kLogCvmfs, kLogCustom0, "Line0");
  EXPECT_TRUE(SearchInFile(tmp_path_ + "/custom.0", "Line0"));
  LogCvmfs(kLogCvmfs, kLogCustom1, "Line1");
  EXPECT_TRUE(SearchInFile(tmp_path_ + "/custom.1", "Line1"));
  LogCvmfs(kLogCvmfs, kLogCustom2, "Line2");
  EXPECT_TRUE(SearchInFile(tmp_path_ + "/custom.2", "Line2"));

  SetLogSyslogPrefix("prefix");
  LogCvmfs(kLogCvmfs, kLogCustom0, "Line");
  EXPECT_TRUE(SearchInFile(tmp_path_ + "/custom.0", "(prefix) Line"));

  LogShutdown();
  EXPECT_DEATH(LogCvmfs(kLogCvmfs, kLogCustom0, "Line"), ".*");
}


TEST_F(T_Logging, Buffer) {
  std::vector<LogBufferEntry> buffer;

  ClearLogBuffer();
  buffer = GetLogBuffer();
  EXPECT_TRUE(buffer.empty());

  LogCvmfs(kLogCvmfs, kLogSensitive, "test line");
  buffer = GetLogBuffer();
  EXPECT_TRUE(buffer.empty());
  LogCvmfs(kLogCvmfs, 0, "test line");
  buffer = GetLogBuffer();
  EXPECT_EQ(1U, buffer.size());
  EXPECT_EQ("test line", buffer[0].message);

  for (unsigned i = 0; i < 5000; ++i)
    LogCvmfs(kLogCvmfs, 0, "%d", i);

  buffer = GetLogBuffer();
  EXPECT_EQ(10U, buffer.size());  // see LogBuffer::kBufferSize
  for (unsigned i = 0; i < buffer.size(); ++i) {
    EXPECT_EQ(4999 - i, String2Int64(buffer[i].message));
  }
}
