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

TEST_F(T_Libcvmfs, TemplatingSlow) {
  cvmfs_option_map *opts = cvmfs_options_init();

  cvmfs_options_set(opts, "CVMFS_CACHE_DIR", "/tmp/@org@/");
  cvmfs_options_set(opts, "CVMFS_HTTP_PROXY", "DIRECT");
  cvmfs_options_set(opts, "CVMFS_SERVER_URL",
    "test.cern.ch");
  cvmfs_options_set(opts, "CVMFS_MAX_RETRIES", "2");
  ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

  cvmfs_option_map *opts_repo = cvmfs_options_clone(opts);
  cvmfs_options_set(opts_repo, "CVMFS_DEBUGLOG", "/tmp/@fqrn@.debug.log");
  cvmfs_context *ctx = reinterpret_cast<cvmfs_context *>(1);
  // This will not actually load a repository
  // however it should reparse the attributes anyway...
  cvmfs_attach_repo_v2("test.cern.ch", opts_repo, &ctx);

  EXPECT_STREQ("/tmp/test/", cvmfs_options_get(opts_repo, "CVMFS_CACHE_DIR"));
  EXPECT_STREQ("/tmp/test.cern.ch.debug.log",
    cvmfs_options_get(opts_repo, "CVMFS_DEBUGLOG"));
  cvmfs_options_fini(opts_repo);

  cvmfs_fini();
  cvmfs_options_fini(opts);
}

namespace {
// Create some default hashes for DirSpec
const char* g_hashes[] = {"b026324c6904b2a9cb4b88d6d61c81d100000000",
                          "26ab0db90d72e28ad0ba1e22ee51051000000000",
                          "6d7fce9fee471194aa8b5b6e47267f0300000000",
                          "48a24b70a0b376535542b996af51739800000000",
                          "1dcca23355272056f04fe8bf20edfce000000000",
                          "1111111111111111111111111111111111111111"};

const size_t g_file_size = 4096;
}  // anonymous namespace

// Create directory specification for later repositories
DirSpec MakeBaseSpec() {
  DirSpec spec;

  // adding "/dir"
  EXPECT_TRUE(spec.AddDirectory("dir", "", g_file_size));

  // adding "/dir/file1"
  EXPECT_TRUE(spec.AddFile("file1", "dir", g_hashes[0], g_file_size));

  // adding "/dir/dir"
  EXPECT_TRUE(spec.AddDirectory("dir",  "dir", g_file_size));
  EXPECT_TRUE(spec.AddDirectory("dir2", "dir", g_file_size));
  EXPECT_TRUE(spec.AddDirectory("dir3", "dir", g_file_size));

  // adding "/file3"
  EXPECT_TRUE(spec.AddFile("file3", "", g_hashes[2], g_file_size));

  // adding "/dir/dir/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir", g_hashes[1], g_file_size));

  // adding "/dir/dir2/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir2", g_hashes[3], g_file_size));

  // adding "/dir/dir3/file2"
  EXPECT_TRUE(spec.AddFile("file2", "dir/dir3", g_hashes[4], g_file_size));

  // Adding Deeply nested catalog
  EXPECT_TRUE(spec.AddDirectory("dir",  "dir/dir", g_file_size));
  EXPECT_TRUE(spec.AddDirectory("dir",  "dir/dir/dir", g_file_size));
  EXPECT_TRUE(
    spec.AddFile("file1",  "dir/dir/dir/dir", g_hashes[0], g_file_size));

  return spec;
}


TEST_F(T_Libcvmfs, Stat) {
  // Initialize options
  cvmfs_option_map *opts = cvmfs_options_init();

  // Create and initialize repository named "stat"
  CatalogTestTool tester("stat");
  EXPECT_TRUE(tester.Init());

  // Create file structure
  DirSpec spec1 = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec1));
  tester.DestroyCatalogManager();

  // Find directory entry for use later
  catalog::DirectoryEntry entry;
  EXPECT_TRUE(
    tester.FindEntry(tester.manifest()->catalog_hash(), "/dir/file1", &entry));

  // Set CVMFS options to reflect created repository
  cvmfs_options_set(opts, "CVMFS_ROOT_HASH",
                        tester.manifest()->catalog_hash().ToString().c_str());
  cvmfs_options_set(opts, "CVMFS_SERVER_URL",
                        ("file://" + tester.repo_name()).c_str());
  cvmfs_options_set(opts, "CVMFS_HTTP_PROXY", "DIRECT");
  cvmfs_options_set(opts, "CVMFS_PUBLIC_KEY",
                        tester.public_key().c_str());
  cvmfs_options_set(opts, "CVMFS_CACHE_DIR",
                        (tester.repo_name()+"/data/txn").c_str());
  cvmfs_options_set(opts, "CVMFS_MOUNT_DIR",
                        ("/cvmfs" + tester.repo_name()).c_str());

  // Initialize client repo based on options
  ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

  // Attach to client repo
  cvmfs_context *ctx;
  EXPECT_EQ(LIBCVMFS_ERR_OK,
    cvmfs_attach_repo_v2((tester.repo_name().c_str()), opts, &ctx));

  struct stat st;
  // file1 should exist and be a regular file
  int retval = cvmfs_stat(ctx, "dir/file1", &st);
  EXPECT_EQ(0, retval);
  EXPECT_TRUE(st.st_mode & S_IFREG);
  // dir should exist and be a directory
  retval = cvmfs_stat(ctx, "dir/dir", &st);
  EXPECT_EQ(0, retval);
  EXPECT_TRUE(st.st_mode & S_IFDIR);
  // file4 does not exist
  retval = cvmfs_stat(ctx, "dir/file4", &st);
  EXPECT_EQ(-1, retval);

  // Finalize and close repo and options
  cvmfs_detach_repo(ctx);
  cvmfs_fini();
  cvmfs_options_fini(opts);
}


TEST_F(T_Libcvmfs, Attr) {
  // Initialize options
  cvmfs_option_map *opts = cvmfs_options_init();

  // Create and initialize repository named "attr"
  CatalogTestTool tester("attr");
  EXPECT_TRUE(tester.Init());

  // Create file structure
  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(spec.LinkFile("link", "dir", "file1", g_file_size));
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));
  tester.DestroyCatalogManager();

  // Find directory entry for use later
  catalog::DirectoryEntry entry;
  EXPECT_TRUE(
    tester.FindEntry(tester.manifest()->catalog_hash(), "/dir/file1", &entry));
  char *file1_hash = strdup(entry.checksum().ToString().c_str());

  catalog::DirectoryEntry link;
  EXPECT_TRUE(
    tester.FindEntry(tester.manifest()->catalog_hash(), "/dir/link", &link));
  char *link_hash = strdup(link.checksum().ToString().c_str());

  // Set CVMFS options to reflect created repository
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

  // Initialize client repo based on options
  ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

  // Attach to client repo
  cvmfs_context *ctx;
  EXPECT_EQ(LIBCVMFS_ERR_OK,
    cvmfs_attach_repo_v2((tester.repo_name().c_str()), opts, &ctx));

  struct cvmfs_attr *attr;
  int retval;

  attr = cvmfs_attr_init();
  // Find file1
  retval = cvmfs_stat_attr(ctx, "/dir/file1", attr);
  EXPECT_EQ(0, retval);
  // Compare hash and size
  EXPECT_STREQ(file1_hash, attr->cvm_checksum);
  // Verify the file size
  EXPECT_EQ(static_cast<int>(g_file_size), attr->st_size);
  EXPECT_EQ(NULL, attr->cvm_xattrs);
  cvmfs_attr_free(attr);

  attr = cvmfs_attr_init();
  // Find link
  retval = cvmfs_stat_attr(ctx, "/dir/link", attr);
  EXPECT_EQ(0, retval);
  // Compare link path
  EXPECT_STREQ("file1", attr->cvm_symlink);
  // Link checksum is different than the linked file
  EXPECT_STREQ(link_hash, attr->cvm_checksum);
  EXPECT_STRNE(file1_hash, attr->cvm_checksum);
  // Link size is small, not eq to file_size
  EXPECT_FALSE(attr->cvm_xattrs);
  cvmfs_attr_free(attr);

  // Free the hash strings
  free(file1_hash);
  free(link_hash);

  // Lookup non-existent file
  attr = cvmfs_attr_init();
  retval = cvmfs_stat_attr(ctx, "/dir/file40", attr);
  EXPECT_EQ(-1, retval);
  cvmfs_attr_free(attr);

  // Finalize and close repo and options
  cvmfs_detach_repo(ctx);
  cvmfs_fini();
  cvmfs_options_fini(opts);
}


TEST_F(T_Libcvmfs, Listdir) {
  // Initialize options
  cvmfs_option_map *opts = cvmfs_options_init();

  // Create and initialize repository named "list"
  CatalogTestTool tester("list");
  EXPECT_TRUE(tester.Init());

  // Create file structure
  DirSpec spec1 = MakeBaseSpec();

  // Apply the created DirSpec
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec1));
  tester.DestroyCatalogManager();

  // Set CVMFS options to reflect created repository
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


  // Initialize client repo based on options
  ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

  // Attach to client repo
  cvmfs_context *ctx;
  EXPECT_EQ(LIBCVMFS_ERR_OK,
      cvmfs_attach_repo_v2((tester.repo_name().c_str()), opts, &ctx));

  char **buf = NULL;
  size_t buflen = 0;
  cvmfs_listdir(ctx, "/dir/dir", &buf, &buflen);
  // The buf length should be at least 4
  EXPECT_LE(4U, buflen);
  // Check that listed info matches specified dir
  EXPECT_STREQ(".", buf[0]);
  EXPECT_STREQ("..", buf[1]);
  EXPECT_STREQ("dir",  buf[2]);
  EXPECT_STREQ("file2",  buf[3]);
  // The 'last' value is null
  EXPECT_FALSE(buf[4]);
  cvmfs_list_free(buf);

  // Finalize and close repo and options
  cvmfs_detach_repo(ctx);
  cvmfs_fini();
  cvmfs_options_fini(opts);
}


TEST_F(T_Libcvmfs, StatNestedCatalog) {
  // Initialize options
  cvmfs_option_map *opts = cvmfs_options_init();

  // Create and initialize repository named "stat_nc"
  CatalogTestTool tester("stat_nc");
  EXPECT_TRUE(tester.Init());

  // Create file structure and add Nested Catalog
  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir"));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir/dir/dir"));

  // Apply the created DirSpec
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));
  tester.DestroyCatalogManager();
  char *d0_hash = strdup(tester.manifest()->catalog_hash().ToString().c_str());

  // Find the hash of the newly added Nested Catalog for comparison
  char *d2_hash;
  EXPECT_TRUE(tester.LookupNestedCatalogHash(tester.manifest()->catalog_hash(),
                                             "/dir/dir", &d2_hash));

  char *d4_hash;
  EXPECT_TRUE(tester.LookupNestedCatalogHash(tester.manifest()->catalog_hash(),
                                             "/dir/dir/dir/dir", &d4_hash));

  // Set CVMFS options to reflect created repository
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

  // Initialize client repo based on options
  ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

  // Attach to client repo
  cvmfs_context *ctx;
  EXPECT_EQ(LIBCVMFS_ERR_OK,
    cvmfs_attach_repo_v2((tester.repo_name().c_str()), opts, &ctx));

  // stat nested catalog using client repo
  struct cvmfs_nc_attr *nc_attr;
  nc_attr = cvmfs_nc_attr_init();
  EXPECT_FALSE(cvmfs_stat_nc(ctx, "/dir/dir/dir/dir", nc_attr));
  EXPECT_STREQ(d4_hash, nc_attr->hash);
  EXPECT_STREQ("/dir/dir/dir/dir", nc_attr->mountpoint);
  cvmfs_nc_attr_free(nc_attr);
  free(d4_hash);

  // stat nested catalog using client repo
  nc_attr = cvmfs_nc_attr_init();
  EXPECT_FALSE(cvmfs_stat_nc(ctx, "/dir", nc_attr));
  EXPECT_STREQ(d0_hash, nc_attr->hash);
  EXPECT_STREQ("", nc_attr->mountpoint);
  // Size of root catalog is 0 as a Nested Catalog
  EXPECT_EQ(0U, nc_attr->size);
  cvmfs_nc_attr_free(nc_attr);
  free(d0_hash);

  // stat nested catalog using client repo
  nc_attr = cvmfs_nc_attr_init();
  EXPECT_FALSE(cvmfs_stat_nc(ctx, "/dir/dir", nc_attr));
  EXPECT_STREQ(d2_hash, nc_attr->hash);
  EXPECT_STREQ("/dir/dir", nc_attr->mountpoint);
  cvmfs_nc_attr_free(nc_attr);
  free(d2_hash);

  // Finalize and close repo and options
  cvmfs_detach_repo(ctx);
  cvmfs_fini();
  cvmfs_options_fini(opts);
}


TEST_F(T_Libcvmfs, ListNestedCatalogs) {
  // Initialize options
  cvmfs_option_map *opts = cvmfs_options_init();

  // Create and initialize repository named "list_nc"
  CatalogTestTool tester("list_nc");
  EXPECT_TRUE(tester.Init());

  // Create file structure and add Nested Catalogs
  DirSpec spec = MakeBaseSpec();
  EXPECT_TRUE(spec.AddNestedCatalog("dir"));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir"));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir2"));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir3"));
  EXPECT_TRUE(spec.AddNestedCatalog("dir/dir/dir/dir"));

  // Apply created DirSpec
  EXPECT_TRUE(
    tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec));
  tester.DestroyCatalogManager();

  // Set CVMFS options to reflect created repository
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

  // Initialize client repo based on options
  ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

  // Attach to client repo
  cvmfs_context *ctx;
  EXPECT_EQ(LIBCVMFS_ERR_OK,
    cvmfs_attach_repo_v2((tester.repo_name().c_str()), opts, &ctx));

  // Check that the listing includes itself and its children
  char **buf = NULL;
  size_t buflen = 0;
  EXPECT_FALSE(cvmfs_list_nc(ctx, "/dir/dir", &buf, &buflen));
  EXPECT_STREQ("",  buf[0]);
  EXPECT_STREQ("/dir", buf[1]);
  EXPECT_STREQ("/dir/dir",  buf[2]);
  EXPECT_STREQ("/dir/dir/dir/dir",  buf[3]);
  EXPECT_FALSE(buf[4]);
  cvmfs_list_free(buf);

  buf = NULL;
  buflen = 0;

  EXPECT_FALSE(cvmfs_list_nc(ctx, "/dir", &buf, &buflen));
  EXPECT_STREQ("",  buf[0]);
  EXPECT_STREQ("/dir", buf[1]);
  EXPECT_STREQ("/dir/dir",  buf[2]);
  EXPECT_STREQ("/dir/dir2",  buf[3]);
  EXPECT_STREQ("/dir/dir3",  buf[4]);
  EXPECT_FALSE(buf[5]);
  cvmfs_list_free(buf);

  // Finalize and close repo and options
  cvmfs_detach_repo(ctx);
  cvmfs_fini();
  cvmfs_options_fini(opts);
}

TEST_F(T_Libcvmfs, ListStat) {
  // Initialize options
  cvmfs_option_map *opts = cvmfs_options_init();

  // Create and initialize repository named "stat"
  CatalogTestTool tester("list_stat");
  EXPECT_TRUE(tester.Init());

  // Create file structure
  DirSpec spec1 = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec1));
  tester.DestroyCatalogManager();

  // Set CVMFS options to reflect created repository
  cvmfs_options_set(opts, "CVMFS_ROOT_HASH",
                        tester.manifest()->catalog_hash().ToString().c_str());
  cvmfs_options_set(opts, "CVMFS_SERVER_URL",
                        ("file://" + tester.repo_name()).c_str());
  cvmfs_options_set(opts, "CVMFS_HTTP_PROXY", "DIRECT");
  cvmfs_options_set(opts, "CVMFS_PUBLIC_KEY",
                        tester.public_key().c_str());
  cvmfs_options_set(opts, "CVMFS_CACHE_DIR",
                        (tester.repo_name()+"/data/txn").c_str());
  cvmfs_options_set(opts, "CVMFS_MOUNT_DIR",
                        ("/cvmfs" + tester.repo_name()).c_str());

  // Initialize client repo based on options
  ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

  // Attach to client repo
  cvmfs_context *ctx;
  EXPECT_EQ(LIBCVMFS_ERR_OK,
    cvmfs_attach_repo_v2((tester.repo_name().c_str()), opts, &ctx));

  struct cvmfs_stat_t *buf = 0;
  size_t buflen = 0;
  size_t listlen = 0;

  int retval = cvmfs_listdir_stat(ctx, "dir", &buf, &listlen, &buflen);
  EXPECT_EQ(0, retval);

  EXPECT_EQ(4U, listlen);

  for (size_t i = 0; i < listlen; ++i) {
    if (!strcmp(buf[i].name, "dir")) {
      EXPECT_TRUE(buf[i].info.st_mode & S_IFDIR);
    } else if (!strcmp(buf[i].name, "dir2")) {
      EXPECT_TRUE(buf[i].info.st_mode & S_IFDIR);

    } else if (!strcmp(buf[i].name, "dir3")) {
      EXPECT_TRUE(buf[i].info.st_mode & S_IFDIR);

    } else if (!strcmp(buf[i].name, "file1")) {
      EXPECT_TRUE(buf[i].info.st_mode & S_IFREG);
      EXPECT_EQ(static_cast<int>(g_file_size), buf[i].info.st_size);

    } else {
      FAIL() << "unexpected object in list: " << buf[i].name;
    }
  }

  // Finalize and close repo and options
  cvmfs_detach_repo(ctx);
  cvmfs_fini();
  cvmfs_options_fini(opts);
}


TEST_F(T_Libcvmfs, Remount) {
  // Initialize options
  cvmfs_option_map *opts = cvmfs_options_init();

  // Create and initialize repository
  CatalogTestTool tester("remount");
  EXPECT_TRUE(tester.Init());

  // Set CVMFS options to reflect created repository
  cvmfs_options_set(opts, "CVMFS_SERVER_URL",
                        ("file://" + tester.repo_name()).c_str());
  cvmfs_options_set(opts, "CVMFS_HTTP_PROXY", "DIRECT");
  cvmfs_options_set(opts, "CVMFS_PUBLIC_KEY",
                        tester.public_key().c_str());
  cvmfs_options_set(opts, "CVMFS_CACHE_DIR",
                        (tester.repo_name()+"/data/txn").c_str());
  cvmfs_options_set(opts, "CVMFS_MOUNT_DIR",
                        ("/cvmfs" + tester.repo_name()).c_str());

  // libcvmfs changes the sqlite global state, so we have to call it in another
  // process in order to change the catalog along the way.
  int pipe_send[2];
  int pipe_recv[2];
  MakePipe(pipe_send);
  MakePipe(pipe_recv);
  pid_t pid;
  if ((pid = fork()) == 0) {
    // Initialize client repo based on options
    ASSERT_EQ(LIBCVMFS_ERR_OK, cvmfs_init_v2(opts));

    // Attach to client repo
    cvmfs_context *ctx;
    EXPECT_EQ(LIBCVMFS_ERR_OK,
              cvmfs_attach_repo_v2("keys.cern.ch", opts, &ctx));

    EXPECT_EQ(0u, cvmfs_get_revision(ctx));
    EXPECT_EQ(0, cvmfs_remount(ctx));
    EXPECT_EQ(0u, cvmfs_get_revision(ctx));

    char c = '1';
    WritePipe(pipe_send[1], &c, 1);
    ReadPipe(pipe_recv[0], &c, 1);

    EXPECT_EQ(0, cvmfs_remount(ctx));
    EXPECT_EQ(1u, cvmfs_get_revision(ctx));

    // Finalize and close repo and options
    cvmfs_detach_repo(ctx);
    cvmfs_fini();
    exit(HasFailure() ? 1 : 0);
  }

  char c;
  ReadPipe(pipe_send[0], &c, 1);
  DirSpec spec1 = MakeBaseSpec();
  EXPECT_TRUE(tester.ApplyAtRootHash(tester.manifest()->catalog_hash(), spec1));
  tester.UpdateManifest();
  tester.DestroyCatalogManager();
  WritePipe(pipe_recv[1], &c, 1);

  int retcode = WaitForChild(pid);
  EXPECT_EQ(0, retcode);

  ClosePipe(pipe_send);
  ClosePipe(pipe_recv);
  cvmfs_options_fini(opts);
}
