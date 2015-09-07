/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <unistd.h>

#include <cstdio>
#include <string>

#include "../../cvmfs/duplex_sqlite3.h"
#include "../../cvmfs/libcvmfs.h"
#include "../../cvmfs/util.h"

using namespace std;  // NOLINT

static void cvmfs_log_ignore(const char *msg) {
  // Remove comment to debug test failures
  // fprintf(stderr, "%s\n", msg);
}

class T_Libcvmfs : public ::testing::Test {
 protected:
  virtual void SetUp() {
    if (first_test) {
      sqlite3_shutdown();
      first_test = false;
    }

    previous_working_directory_ = GetCurrentWorkingDirectory();

    cvmfs_set_log_fn(cvmfs_log_ignore);
    tmp_path_ = CreateTempDir("./cvmfs_ut_libcvmfs");
    ASSERT_NE("", tmp_path_);
    opt_cache_ = "quota_limit=0,quota_threshold=0,rebuild_cachedb,"
                 "cache_directory=" + tmp_path_;
    alien_path_ = tmp_path_ + "/alien";

    fdevnull_ = fopen("/dev/null", "w");
    ASSERT_FALSE(fdevnull_ == NULL);
  }

  virtual void TearDown() {
    fclose(fdevnull_);
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
    cvmfs_set_log_fn(NULL);

    EXPECT_EQ(0, chdir(previous_working_directory_.c_str()));
  }

  // Other tests might have initialized sqlite3.  This cannot happen in real
  // use because sqlite3 is statically linked with the library.  This static
  // variable is used to de-initialize sqlite once, so that proper shutdown
  // within the library can still be tested in the tests following the first
  // one.
  static bool first_test;

  // Libcvmfs chdir()s internally. This must be reverted since the unit tests
  // depend on the binary's working directory for sandboxing temporary files.
  // We note the working directory before executing any test code and chdir()
  // back to it in the test's TearDown().
  string previous_working_directory_;

  string tmp_path_;
  string alien_path_;
  string opt_cache_;
  FILE *fdevnull_;
};

bool T_Libcvmfs::first_test = true;


TEST_F(T_Libcvmfs, Init) {
  int retval;
  string opt_cache = "cache_directory=" + tmp_path_;
  retval = cvmfs_init(opt_cache_.c_str());
  ASSERT_EQ(LIBCVMFS_FAIL_OK, retval);
  EXPECT_DEATH(cvmfs_init(opt_cache_.c_str()), ".*");
  cvmfs_fini();
}


TEST_F(T_Libcvmfs, InitFailures) {
  int retval;
  FILE *save_stderr = stderr;
  stderr = fdevnull_;
  retval = cvmfs_init("bad option");
  stderr = save_stderr;
  ASSERT_EQ(LIBCVMFS_FAIL_BADOPT, retval);

  retval = cvmfs_init((opt_cache_ + ",max_open_files=100000000").c_str());
  ASSERT_EQ(LIBCVMFS_FAIL_NOFILES, retval);

  retval = cvmfs_init("");
  ASSERT_EQ(LIBCVMFS_FAIL_MKCACHE, retval);

  sqlite3_shutdown();
}


TEST_F(T_Libcvmfs, InitConcurrent) {
  // Align with Parrot use
  string default_opts =
    "max_open_files=500,change_to_cache_directory,log_prefix=libcvmfs";

  string opt_var1_p1 = default_opts +
    ",cache_directory=" + tmp_path_ + "/p1,alien_cachedir=" + alien_path_;
  string opt_var1_p2 = default_opts +
    ",cache_directory=" + tmp_path_ + "/p2,alien_cachedir=" + alien_path_;
  string opt_var2_p1 = default_opts +
    ",alien_cache,cache_directory=" + alien_path_ +
    ",lock_directory=" + tmp_path_ + "/p1";
  string opt_var2_p2 = default_opts +
    ",alien_cache,cache_directory=" + alien_path_ +
    ",lock_directory=" + tmp_path_ + "/p2";

  int pipe_c2p[2];
  int pipe_p2c[2];
  MakePipe(pipe_c2p);
  MakePipe(pipe_p2c);
  int retval;
  pid_t child_pid = fork();
  switch (child_pid) {
    case 0:
      retval = cvmfs_init(opt_var1_p1.c_str());
      WritePipe(pipe_c2p[1], &retval, sizeof(retval));
      ReadPipe(pipe_p2c[0], &retval, sizeof(retval));
      cvmfs_fini();

      retval = cvmfs_init(opt_var2_p1.c_str());
      WritePipe(pipe_c2p[1], &retval, sizeof(retval));
      ReadPipe(pipe_p2c[0], &retval, sizeof(retval));
      exit(0);
    default:
      // Parent
      ASSERT_GT(child_pid, 0);

      ReadPipe(pipe_c2p[0], &retval, sizeof(retval));
      EXPECT_EQ(LIBCVMFS_FAIL_OK, retval);
      retval = cvmfs_init(opt_var1_p2.c_str());
      EXPECT_EQ(LIBCVMFS_FAIL_OK, retval);
      WritePipe(pipe_p2c[1], &retval, sizeof(retval));
      cvmfs_fini();

      ReadPipe(pipe_c2p[0], &retval, sizeof(retval));
      EXPECT_EQ(LIBCVMFS_FAIL_OK, retval);
      retval = cvmfs_init(opt_var2_p2.c_str());
      EXPECT_EQ(LIBCVMFS_FAIL_OK, retval);
      WritePipe(pipe_p2c[1], &retval, sizeof(retval));
      cvmfs_fini();

      int status;
      waitpid(child_pid, &status, 0);
      EXPECT_TRUE(WIFEXITED(status));
      EXPECT_EQ(0, WEXITSTATUS(status));
  }
  ClosePipe(pipe_p2c);
  ClosePipe(pipe_c2p);
}


TEST_F(T_Libcvmfs, OptionAliases) {
  int retval;
  FILE *save_stderr = stderr;

  retval = cvmfs_init((opt_cache_ +
    ",nofiles=100000000,max_open_files=100000000").c_str());
  ASSERT_EQ(LIBCVMFS_FAIL_NOFILES, retval);

  retval = cvmfs_init((opt_cache_ + ",nofiles=100000000").c_str());
  ASSERT_EQ(LIBCVMFS_FAIL_NOFILES, retval);

  retval = cvmfs_init((opt_cache_ + ",max_open_files=100000000").c_str());
  ASSERT_EQ(LIBCVMFS_FAIL_NOFILES, retval);

  stderr = fdevnull_;
  retval = cvmfs_init((opt_cache_ + ",nofiles=999,max_open_files=888").c_str());
  stderr = save_stderr;
  ASSERT_EQ(LIBCVMFS_FAIL_BADOPT, retval);

  stderr = fdevnull_;
  retval =
    cvmfs_init((opt_cache_ + ",syslog_level=0,log_syslog_level=1").c_str());
  stderr = save_stderr;
  ASSERT_EQ(LIBCVMFS_FAIL_BADOPT, retval);

  stderr = fdevnull_;
  retval =
    cvmfs_init((opt_cache_ + ",log_file=abc,logfile=efg").c_str());
  stderr = save_stderr;
  ASSERT_EQ(LIBCVMFS_FAIL_BADOPT, retval);

  stderr = fdevnull_;
  retval =
    cvmfs_init((opt_cache_ + ",cachedir=/abc").c_str());
  stderr = save_stderr;
  ASSERT_EQ(LIBCVMFS_FAIL_BADOPT, retval);

  retval =
    cvmfs_init((opt_cache_ + ",cachedir=" + tmp_path_).c_str());
  stderr = save_stderr;
  EXPECT_EQ(LIBCVMFS_FAIL_OK, retval);
  cvmfs_fini();
}
