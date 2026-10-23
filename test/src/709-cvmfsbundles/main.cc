/**
 * This file is part of the CernVM File System.
 *
 * Opens the bundle trigger file of the given repository through libcvmfs
 * and waits until the expected number of prefetched objects arrived in the
 * client cache.  See the `main` script for the surrounding test.
 */

#include <ctype.h>
#include <dirent.h>
#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <string>

#include "libcvmfs.h"

// Number of finalized content objects in the cache: the files inside the
// two-hex-digit CAS directories directly below the cache root (excludes
// txn/ scratch files and quota bookkeeping)
static int CountCacheObjects(const std::string &cache_dir) {
  int count = 0;
  DIR *top = opendir(cache_dir.c_str());
  if (top == NULL)
    return 0;
  struct dirent *entry;
  while ((entry = readdir(top)) != NULL) {
    const std::string name = entry->d_name;
    if ((name.length() != 2) || !isxdigit(name[0]) || !isxdigit(name[1]))
      continue;
    const std::string subdir_path = cache_dir + "/" + name;
    DIR *subdir = opendir(subdir_path.c_str());
    if (subdir == NULL)
      continue;
    struct dirent *object;
    while ((object = readdir(subdir)) != NULL) {
      if (object->d_name[0] != '.')
        ++count;
    }
    closedir(subdir);
  }
  closedir(top);
  return count;
}

int main(int argc, char **argv) {
  if (argc < 6) {
    fprintf(stderr,
            "Usage: %s <repo url> <repo name> <cache dir> "
            "<# expected new objects> <timeout in s>\n",
            argv[0]);
    return 1;
  }
  const char *url = argv[1];
  const char *fqrn = argv[2];
  const std::string cache_dir = argv[3];
  const int expected_new = atoi(argv[4]);
  const int timeout_s = atoi(argv[5]);

  cvmfs_option_map *global_opts = cvmfs_options_init();
  cvmfs_options_set(global_opts, "CVMFS_CACHE_DIR", cache_dir.c_str());
  if (cvmfs_init_v2(global_opts) != LIBCVMFS_ERR_OK) {
    fprintf(stderr, "couldn't initialize libcvmfs\n");
    return 2;
  }

  cvmfs_option_map *repo_opts = cvmfs_options_clone(global_opts);
  const std::string pubkey = std::string("/etc/cvmfs/keys/") + fqrn + ".pub";
  cvmfs_options_set(repo_opts, "CVMFS_SERVER_URL", url);
  cvmfs_options_set(repo_opts, "CVMFS_PUBLIC_KEY", pubkey.c_str());
  cvmfs_options_set(repo_opts, "CVMFS_HTTP_PROXY", "DIRECT");
  cvmfs_options_set(repo_opts, "CVMFS_PREFETCH_FILEBUNDLES", "yes");

  cvmfs_context *ctx = NULL;
  if (cvmfs_attach_repo_v2(fqrn, repo_opts, &ctx) != LIBCVMFS_ERR_OK) {
    fprintf(stderr, "couldn't attach repository %s\n", fqrn);
    return 3;
  }

  // Attaching fetched the certificate and the root catalog; everything
  // arriving from here on is due to opening the bundle trigger.
  const int objects_before = CountCacheObjects(cache_dir);

  const int fd = cvmfs_open(ctx, "/trigger.txt");
  if (fd < 0) {
    fprintf(stderr, "couldn't open /trigger.txt\n");
    return 4;
  }
  cvmfs_close(ctx, fd);

  // Prefetching is asynchronous; wait for the expected objects to arrive.
  // The repository must stay attached while waiting: detaching drains the
  // prefetch queues.
  const int objects_expected = objects_before + expected_new;
  printf("waiting for the cache to grow from %d to %d objects\n",
         objects_before, objects_expected);
  fflush(stdout);
  int waited = 0;
  while (CountCacheObjects(cache_dir) < objects_expected) {
    if (waited >= timeout_s) {
      fprintf(stderr, "timeout: %d of %d objects after %ds\n",
              CountCacheObjects(cache_dir), objects_expected, waited);
      return 5;
    }
    sleep(1);
    ++waited;
  }
  printf("prefetch finished after %ds\n", waited);

  cvmfs_detach_repo(ctx);
  cvmfs_fini();
  cvmfs_options_fini(repo_opts);
  cvmfs_options_fini(global_opts);
  return 0;
}
