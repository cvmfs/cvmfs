/**
 * This file is part of the CernVM File System.
 */
#include <errno.h>
#include <stdio.h>

#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

#include "libcvmfs.h"

#define TEST_LINE_MAX 1024

using namespace std;  // NOLINT

const char *repo_options =
  "repo_name=%s,url=%s,"
  "pubkey=/etc/cvmfs/keys/%s.pub,"
  "proxies=DIRECT";

int main(int argc, char *argv[]) {
  if (argc < 4) {
    printf("Usage: %s <base URL> <base repo name> <# repos>\n", argv[0]);
    return -2;
  }
  const char *global_options = "cache_directory=/tmp/test-libcvmfs-cache";
  int retval = cvmfs_init(global_options);
  if (retval != 0) {
    fprintf(stderr, "couldn't initialize libcvmfs!\n");
    return -1;
  }
  cvmfs_context *ctx = NULL;
  string base_url       = argv[1];
  string base_repo_name = argv[2];
  int    num_repos      = atoi(argv[3]);
  vector<cvmfs_context*> ctx_vector;
  char options[TEST_LINE_MAX];

  for (int counter = 1; counter <= num_repos; ++counter) {
    ostringstream rn;
    rn << base_repo_name << counter;
    string repo_name = rn.str();

    ostringstream ru;
    ru << base_url
      << '/'
      << repo_name;
    string repo_url = ru.str();
    snprintf(options, TEST_LINE_MAX, repo_options, repo_name.c_str(),
      repo_url.c_str(), repo_name.c_str());
    printf("Trying to mount with options:\n%s\n", options);
    ctx = cvmfs_attach_repo(options);
    ctx_vector.push_back(ctx);
    if (ctx == NULL) {
      printf("couldn't attach the repository %s\n", repo_name.c_str());
      return counter;
    }

    // first test: check that the content of /${testname}/main/mainfile
    // is ${counter}
    string mainfile_path = "/main/mainfile";
    int fd = cvmfs_open(ctx, mainfile_path.c_str());
    if (fd < 0) {
      printf("Couldn't perform the lookup operation in %s\n",
        mainfile_path.c_str());
      return fd;
    }
    char buffer[10];
    read(fd, buffer, sizeof(buffer));
    int number = atoi(buffer);
    if (number != counter) {
      printf("Failed to verify value: %d != %d", number, counter);
      return -3;
    }
    cvmfs_close(ctx, fd);

    // second test: check that listing /${testname}/list produces
    // ${counter} entries
    string list_path = "/list";
    size_t length = 0;
    char **list_buffer = 0;
    int result = cvmfs_listdir(ctx, list_path.c_str(), &list_buffer, &length);
    if (result < 0) {
      printf("Couldn't perform the list operation in %s\n", list_path.c_str());
      return result;
    }
    int num_elem = 0;
    for (int i = 0; list_buffer[i]; ++i) {
      ++num_elem;
    }
    // remove the . and .. entries
    num_elem -= 2;
    if (num_elem != counter) {
      printf("Failed to list the correct number of files\n"
        "Current: %d       Expected: %d\n", num_elem, num_repos);
      return -6;
    }

    // third test: read large file
    string largefile_path = "/large";
    fd = cvmfs_open(ctx, largefile_path.c_str());
    if (fd < 0) {
      printf("Couldn't perform the lookup operation in %s\n",
             largefile_path.c_str());
      return fd;
    }
    if (fd < (1 << 30)) {
      printf("Expected a chunk file but this is not (%d)\n", fd);
      return -7;
    }
    char page_buf[4096];
    ssize_t nbytes;
    off_t off = 0;
    while ((nbytes = cvmfs_pread(ctx, fd, page_buf, 4096, off)) > 0) {
      off += nbytes;
    }
    if (nbytes < 0) {
      printf("Error reading chunked file %s (%d), read so far %u\n",
             largefile_path.c_str(), errno, off);
      return nbytes;
    }
    cvmfs_close(ctx, fd);
  }

  for (int i = 0; i < ctx_vector.size(); ++i) {
    cvmfs_detach_repo(ctx_vector[i]);
  }

  cvmfs_fini();
  return 0;
}
