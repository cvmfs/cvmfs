/**
 * This file is part of the CernVM File System.
 *
 * This is a simple test program to test the facilities of the libcvmfs C
 * (not C++) library, which is used by Parrot and some other tools.
 *
 * The goal here is not so much to build the ultimate testing tool, but to
 * provide a simple build target which can verify that libcvmfs is exporting the
 * proper set of symbols to be used by a C program.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"

#include <errno.h>
#include <inttypes.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <map>
#include <string>

#include "libcvmfs.h"

#define TEST_LINE_MAX 1024

typedef std::map<std::string, cvmfs_context*> RepoMap;
static RepoMap attached_repos;

void cvmfs_test_help()
{
  printf("commands are:\n");
  printf("   attach <repo name (without .cern.ch)>\n");
  printf("   detach <repo name (without .cern.ch)>\n");
  printf("   list <path>\n");
  printf("   cat  <path>\n");
  printf("   quit\n");
}

int cvmfs_test_list(cvmfs_context *ctx, const char *path)
{
  if (ctx == NULL) {
    fprintf(stderr, "%s\n", "please attach a repo first!");
    return -1;
  }

  char filepath[TEST_LINE_MAX];
  struct stat info;

  char **buffer = 0;
  size_t length = 0;
  int i;

  int result = cvmfs_listdir(ctx, path, &buffer, &length);
  if (result < 0) {
    fprintf(stderr, "%s: %s\n", path, strerror(errno));
    return -1;
  }


  for (i = 0; buffer[i]; i++) {
    snprintf(filepath, TEST_LINE_MAX, "%s/%s", path, buffer[i]);
    cvmfs_stat(ctx, filepath, &info);
    printf("%10"PRIu64" %s\n", static_cast<uint64_t>(info.st_size), buffer[i]);
  }

  free(buffer);

  return 0;
}

int cvmfs_test_cat(cvmfs_context *ctx, const char *path)
{
  if (ctx == NULL) {
    fprintf(stderr, "%s\n", "please attach a repo first!");
    return -1;
  }

  char buffer[TEST_LINE_MAX];

  int fd = cvmfs_open(ctx, path);
  if (fd < 0) {
    fprintf(stderr, "%s: %s\n", path, strerror(errno));
    return fd;
  }

  while (1) {
    int length = read(fd, buffer, sizeof(buffer));
    if (length <= 0) break;
    int retval = write(1, buffer, length);
    assert(retval == length);
  }

  cvmfs_close(ctx, fd);

  return 0;
}

cvmfs_context* cvmfs_test_attach(const char *repo_name)
{
  cvmfs_context *ctx = NULL;

  RepoMap::const_iterator i = attached_repos.find(repo_name);
  if (i == attached_repos.end()) {
    const char *repo_options =
      "repo_name=%s.cern.ch,url=http://cvmfs-stratum-one.cern.ch/opt/%s;"
      "http://cernvmfs.gridpp.rl.ac.uk/opt/%s;http://cvmfs.racf.bnl.gov/opt/%s,"
      "pubkey=/etc/cvmfs/keys/cern.ch/cern.ch.pub";
    char options[TEST_LINE_MAX];
    snprintf(options, TEST_LINE_MAX, repo_options, repo_name, repo_name,
             repo_name, repo_name);

    printf("attaching repo with options: %s\n", options);
    ctx = cvmfs_attach_repo(options);
    if (ctx == NULL) {
      fprintf(stderr, "couldn't initialize cvmfs!\n");
    } else {
      attached_repos[repo_name] = ctx;
    }
  } else {
    printf("switching to previously attached repo: %s\n", repo_name);
    ctx = i->second;
  }

  return ctx;
}

void cvmfs_test_detach(const char *repo_name, const cvmfs_context *active_ctx) {
  RepoMap::iterator i = attached_repos.find(repo_name);
  if (i == attached_repos.end()) {
    printf("Did not find '%s' to detach\n", repo_name);
    return;
  }

  cvmfs_context *ctx = i->second;
  if (ctx == active_ctx) {
    printf("'%s' is currently active and cannot be detached.\n", repo_name);
    return;
  }

  attached_repos.erase(i);
  cvmfs_detach_repo(ctx);
}


int main(int argc, char *argv[])
{
  char line[TEST_LINE_MAX];
  char path[TEST_LINE_MAX];
  char repo_name[TEST_LINE_MAX];

  const char *global_options = "cache_directory=/tmp/test-libcvmfs-cache";

  printf("%s: initializing with options: %s\n", argv[0], global_options);
  int retval = cvmfs_init(global_options);
  if (retval != 0) {
    fprintf(stderr, "couldn't initialize libcvmfs!\n");
    return -1;
  }

  cvmfs_context *ctx = NULL;

  while (1) {
    printf("cvmfs> ");
    fflush(stdout);

    if (!fgets(line, sizeof(line), stdin)) break;

    line[strlen(line)-1] = 0;

    if (sscanf(line, "list %s", path) == 1) {
      cvmfs_test_list(ctx, path);
    } else if (sscanf(line, "cat %s", path) == 1) {
      cvmfs_test_cat(ctx, path);
    } else if (sscanf(line, "attach %s", repo_name) == 1) {
      ctx = cvmfs_test_attach(repo_name);
    } else if (sscanf(line, "detach %s", repo_name) == 1) {
      cvmfs_test_detach(repo_name, ctx);
    } else if (!strcmp(line, "quit")) {
      break;
    } else {
      cvmfs_test_help();
    }
  }

  cvmfs_fini();

  return 0;
}

