/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <errno.h>
#include <string.h>
#include <unistd.h>

#include <cstdio>
#include <string>

#include "catalog_test_tools.h"
#include "duplex_sqlite3.h"
#include "libcvmfs.h"
#include "options.h"
#include "util/posix.h"

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
    if (repo_path_ != "")
      RemoveTree(repo_path_);
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

  SimpleOptionsParser options_mgr_;

  string tmp_path_;
  string repo_path_;
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


TEST_F(T_Libcvmfs, Initv2) {
  cvmfs_option_map *opts = cvmfs_options_init();

  cvmfs_options_set(opts, "CVMFS_CACHE_DIR", tmp_path_.c_str());
  EXPECT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));
  cvmfs_fini();

  cvmfs_options_set(opts, "CVMFS_NFILES", "100000000");
  EXPECT_EQ(LIBCVMFS_ERR_PERMISSION, cvmfs_init_v2(opts));

  cvmfs_options_fini(opts);
}


TEST_F(T_Libcvmfs, Attachv2) {
  cvmfs_option_map *opts = cvmfs_options_init();

  cvmfs_options_set(opts, "CVMFS_CACHE_DIR", tmp_path_.c_str());

  ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

  cvmfs_option_map *opts_repo = cvmfs_options_clone(opts);
  cvmfs_options_set(opts_repo, "CVMFS_HTTP_PROXY", "DIRECT");
  cvmfs_options_set(opts_repo, "CVMFS_SERVER_URL", "file:///no/such/dir");
  cvmfs_options_set(opts_repo, "CVMFS_MAX_RETRIES", "0");
  cvmfs_context *ctx = reinterpret_cast<cvmfs_context *>(1);
  EXPECT_EQ(LIBCVMFS_ERR_CATALOG, cvmfs_attach_repo_v2("xxx", opts_repo, &ctx));
  EXPECT_EQ(NULL, ctx);
  cvmfs_options_fini(opts_repo);

  cvmfs_fini();
  cvmfs_options_fini(opts);
}

TEST_F(T_Libcvmfs, Templating) {
  cvmfs_option_map *opts = cvmfs_options_init();

  cvmfs_options_set(opts, "CVMFS_CACHE_DIR", "/tmp/@org@/");
  cvmfs_options_set(opts, "CVMFS_HTTP_PROXY", "DIRECT");
  cvmfs_options_set(opts, "CVMFS_SERVER_URL",
    "test.cern.ch");
  cvmfs_options_set(opts, "CVMFS_MAX_RETRIES", "2");
  ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

  cvmfs_option_map *opts_repo = cvmfs_options_clone(opts);
  cvmfs_options_set(opts_repo, "CVMFS_DEBUG_LOG", "/tmp/@fqrn@.debug.log");
  cvmfs_context *ctx = reinterpret_cast<cvmfs_context *>(1);
  // This will not actually load a repository
  // however it should reparse the attributes anyway...
  cvmfs_attach_repo_v2("test.cern.ch", opts_repo, &ctx);

  EXPECT_STREQ("/tmp/test/", cvmfs_options_get(opts_repo, "CVMFS_CACHE_DIR"));
  EXPECT_STREQ("/tmp/test.cern.ch.debug.log",
    cvmfs_options_get(opts_repo, "CVMFS_DEBUG_LOG"));
  cvmfs_options_fini(opts_repo);

  cvmfs_fini();
  cvmfs_options_fini(opts);
}

/* Create some default hashes for DirSpec */
const char* hashes[] = {"b026324c6904b2a9cb4b88d6d61c81d1000000",
                        "26ab0db90d72e28ad0ba1e22ee510510000000",
                        "6d7fce9fee471194aa8b5b6e47267f03000000",
                        "48a24b70a0b376535542b996af517398000000",
                        "1dcca23355272056f04fe8bf20edfce0000000",
                        "11111111111111111111111111111111111111"};

const size_t file_size = 4096;

/* Create directory specification for later repositories */
DirSpec MakeBaseSpec() {
  DirSpec spec;

  // adding "/dir"
  EXPECT_TRUE(spec.AddDirectory("dir", "", file_size));

  // adding "/dir/file1"
  EXPECT_TRUE(spec.AddFile("file1", "dir", hashes[0], file_size));

  // adding "/dir/dir"
  EXPECT_TRUE(spec.AddDirectory("dir",  "dir", file_size));
  EXPECT_TRUE(spec.AddDirectory("dir2", "dir", file_size));
  EXPECT_TRUE(spec.AddDirectory("dir3", "dir", file_size));

  // adding "/file3"
  EXPECT_TRUE(spec.AddFile("file3", "", hashes[2], file_size));

  // adding "/dir/dir/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir", hashes[1], file_size));

  // adding "/dir/dir2/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir2", hashes[3], file_size));

  // adding "/dir/dir3/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir3", hashes[4], file_size));

  // Adding Deeply nested catalog
  EXPECT_TRUE(spec.AddDirectory("dir",  "dir/dir", file_size));
  EXPECT_TRUE(spec.AddDirectory("dir",  "dir/dir/dir", file_size));
  EXPECT_TRUE(spec.AddFile("file1",  "dir/dir/dir/dir", hashes[0], file_size));

  return spec;
}


TEST_F(T_Libcvmfs, Listdir) {
  /* Initialize options */
  cvmfs_option_map *opts = cvmfs_options_init();

  /* Create and initialize repository named "list" */
  CatalogTestTool tester("list");
  EXPECT_TRUE(tester.Init());

  /* Create file structure */
  DirSpec spec1 = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec1));

  /* Set CVMFS options to reflect created repository */
  cvmfs_options_set(opts,
    "CVMFS_ROOT_HASH", tester.manifest()->catalog_hash().ToString().c_str());
  cvmfs_options_set(opts,
    "CVMFS_SERVER_URL", ("file://" + tester.repo_name()).c_str());
  cvmfs_options_set(opts, "CVMFS_HTTP_PROXY", "DIRECT");
  cvmfs_options_set(opts,
    "CVMFS_PUBLIC_KEY", tester.public_key().c_str());
  cvmfs_options_set(opts,
    "CVMFS_CACHE_DIR", (tester.repo_name()+"/data/txn").c_str());
  cvmfs_options_set(opts,
    "CVMFS_MOUNT_DIR", ("/cvmfs" + tester.repo_name()).c_str());


  /* Initialize client repo based on options */
  ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

  /* Attach to client repo */
  cvmfs_context *ctx;
  EXPECT_EQ(LIBCVMFS_ERR_OK,
      cvmfs_attach_repo_v2((tester.repo_name().c_str()), opts, &ctx));

  char **buf = NULL;
  size_t buflen = 0;
  cvmfs_listdir(ctx, "/dir/dir", &buf, &buflen);
  // The buf length should be at least 3
  EXPECT_LE(3, buflen);
  // Check that listed info matches specified dir
  EXPECT_FALSE(strcmp(".", buf[0]));
  EXPECT_FALSE(strcmp("..", buf[1]));
  EXPECT_FALSE(strcmp("dir",  buf[2]));
  EXPECT_FALSE(strcmp("file2",  buf[3]));
  // The 'last' value is null
  EXPECT_FALSE(buf[4]);
  cvmfs_list_free(buf);

  /* Finalize and close repo and options */
  cvmfs_detach_repo(ctx);
  cvmfs_fini();
  cvmfs_options_fini(opts);
}

TEST_F(T_Libcvmfs, StatNestedCatalog) {
  /* Initialize options */
  cvmfs_option_map *opts = cvmfs_options_init();

  /* Create and initialize repository named "stat_nc" */
  CatalogTestTool tester("stat_nc");
  EXPECT_TRUE(tester.Init());

  /* Create file structure and add Nested Catalog */
  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));
  EXPECT_TRUE(tester.AddNestedCatalog(tester.manifest()->catalog_hash(),
                                      "dir/dir"));
  EXPECT_TRUE(tester.AddNestedCatalog(tester.manifest()->catalog_hash(),
                                      "dir/dir/dir/dir"));

  /* Find the hash of the newly added Nested Catalog for comparison */
  PathString d2_mp;
  shash::Any d2_hash;
  uint64_t d2_size;
  EXPECT_TRUE(tester.LookupNestedCatalog(tester.manifest()->catalog_hash(),
                                         "/dir/dir", &d2_mp,
                                         &d2_hash, &d2_size));
  EXPECT_FALSE(strcmp(d2_mp.ToString().c_str(), "/dir/dir"));

  PathString d4_mp;
  shash::Any d4_hash;
  uint64_t d4_size;
  EXPECT_TRUE(tester.LookupNestedCatalog(tester.manifest()->catalog_hash(),
                                         "/dir/dir/dir/dir", &d4_mp,
                                         &d4_hash, &d4_size));
  EXPECT_FALSE(strcmp(d4_mp.ToString().c_str(), "/dir/dir/dir/dir"));

  /* Set CVMFS options to reflect created repository */
  cvmfs_options_set(opts,
    "CVMFS_ROOT_HASH", tester.manifest()->catalog_hash().ToString().c_str());
  cvmfs_options_set(opts,
    "CVMFS_SERVER_URL", ("file://" + tester.repo_name()).c_str());
  cvmfs_options_set(opts, "CVMFS_HTTP_PROXY", "DIRECT");
  cvmfs_options_set(opts,
    "CVMFS_PUBLIC_KEY", tester.public_key().c_str());
  cvmfs_options_set(opts,
    "CVMFS_CACHE_DIR", (tester.repo_name()+"/data/txn").c_str());
  cvmfs_options_set(opts,
    "CVMFS_MOUNT_DIR", ("/cvmfs" + tester.repo_name()).c_str());

  /* Initialize client repo based on options */
  ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

  /* Attach to client repo */
  cvmfs_context *ctx;
  EXPECT_EQ(LIBCVMFS_ERR_OK,
    cvmfs_attach_repo_v2((tester.repo_name().c_str()), opts, &ctx));

  const char *orig_nc = NULL;

  /* stat nested catalog using client repo */
  struct cvmfs_nc_attr *nc_attr;
  nc_attr = cvmfs_nc_attr_init();
  EXPECT_FALSE(cvmfs_stat_nc(ctx, "/dir/dir/dir/dir", nc_attr));
  orig_nc = d4_hash.ToString().c_str();
  EXPECT_FALSE(strcmp(orig_nc, nc_attr->hash));
  EXPECT_FALSE(strcmp("/dir/dir/dir/dir", nc_attr->mountpoint));
  EXPECT_EQ(d4_size, nc_attr->size);
  cvmfs_nc_attr_free(nc_attr);


  /* stat nested catalog using client repo */
  nc_attr = cvmfs_nc_attr_init();
  EXPECT_FALSE(cvmfs_stat_nc(ctx, "/dir", nc_attr));
  orig_nc = tester.manifest()->catalog_hash().ToString().c_str();
  EXPECT_FALSE(strcmp(orig_nc, nc_attr->hash));
  EXPECT_FALSE(strcmp("", nc_attr->mountpoint));
  /* Size of root catalog is 0 as a Nested Catalog */
  EXPECT_EQ(nc_attr->size, 0);
  cvmfs_nc_attr_free(nc_attr);


  /* stat nested catalog using client repo */
  nc_attr = cvmfs_nc_attr_init();
  EXPECT_FALSE(cvmfs_stat_nc(ctx, "/dir/dir", nc_attr));
  orig_nc = d2_hash.ToString().c_str();
  EXPECT_FALSE(strcmp(orig_nc, nc_attr->hash));
  EXPECT_FALSE(strcmp("/dir/dir", nc_attr->mountpoint));
  EXPECT_EQ(d2_size, nc_attr->size);
  cvmfs_nc_attr_free(nc_attr);

  /* Finalize and close repo and options */
  cvmfs_detach_repo(ctx);
  cvmfs_fini();
  cvmfs_options_fini(opts);
}


TEST_F(T_Libcvmfs, ListNestedCatalogs) {
  /* Initialize options */
  cvmfs_option_map *opts = cvmfs_options_init();

  /* Create and initialize repository named "list_nc" */
  CatalogTestTool tester("list_nc");
  EXPECT_TRUE(tester.Init());

  /* Create file structure and add Nested Catalogs */
  DirSpec spec1 = MakeBaseSpec();
  EXPECT_TRUE(
    tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec1));
  EXPECT_TRUE(
    tester.AddNestedCatalog(tester.manifest()->catalog_hash(), "dir"));
  EXPECT_TRUE(
    tester.AddNestedCatalog(tester.manifest()->catalog_hash(), "dir/dir"));
  EXPECT_TRUE(
    tester.AddNestedCatalog(tester.manifest()->catalog_hash(), "dir/dir2"));
  EXPECT_TRUE(
    tester.AddNestedCatalog(tester.manifest()->catalog_hash(), "dir/dir3"));

  /* Set CVMFS options to reflect created repository */
  cvmfs_options_set(opts,
    "CVMFS_ROOT_HASH", tester.manifest()->catalog_hash().ToString().c_str());
  cvmfs_options_set(opts,
    "CVMFS_SERVER_URL", ("file://" + tester.repo_name()).c_str());
  cvmfs_options_set(opts, "CVMFS_HTTP_PROXY", "DIRECT");
  cvmfs_options_set(opts,
    "CVMFS_PUBLIC_KEY", tester.public_key().c_str());
  cvmfs_options_set(opts,
    "CVMFS_CACHE_DIR", (tester.repo_name()+"/data/txn").c_str());
  cvmfs_options_set(opts,
    "CVMFS_MOUNT_DIR", ("/cvmfs" + tester.repo_name()).c_str());

  /* Initialize client repo based on options */
  ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

  /* Attach to client repo */
  cvmfs_context *ctx;
  EXPECT_EQ(LIBCVMFS_ERR_OK,
    cvmfs_attach_repo_v2((tester.repo_name().c_str()), opts, &ctx));

  /* Check that the listing includes itself and its children */
  char **buf = NULL;
  size_t buflen = 0;
  EXPECT_FALSE(cvmfs_list_nc(ctx, "/dir", &buf, &buflen));
  EXPECT_FALSE(strcmp("",  buf[0]));
  EXPECT_FALSE(strcmp("/dir", buf[1]));
  EXPECT_FALSE(strcmp("/dir/dir",  buf[2]));
  EXPECT_FALSE(strcmp("/dir/dir2",  buf[3]));
  EXPECT_FALSE(strcmp("/dir/dir3",  buf[4]));
  EXPECT_FALSE(buf[5]);
  cvmfs_list_free(buf);

  buf = NULL;
  buflen = 0;
  EXPECT_FALSE(cvmfs_list_nc(ctx, "/dir/dir", &buf, &buflen));
  EXPECT_FALSE(strcmp("",  buf[0]));
  EXPECT_FALSE(strcmp("/dir", buf[1]));
  EXPECT_FALSE(strcmp("/dir/dir",  buf[2]));
  EXPECT_FALSE(buf[3]);
  cvmfs_list_free(buf);

  /* Finalize and close repo and options */
  cvmfs_detach_repo(ctx);
  cvmfs_fini();
  cvmfs_options_fini(opts);
}
