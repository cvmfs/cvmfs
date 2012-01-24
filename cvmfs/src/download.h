/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DOWNLOAD_H_
#define CVMFS_DOWNLOAD_H_

#include <cstdio>
#include <string>

#include "curl_duplex.h"
extern "C" {
  #include "compression.h"
}

namespace hash {
struct t_sha1;
}

namespace download {

/**
 * Where to store downloaded data.
 */
enum Destination {
  kDestinationMem = 1,
  kDestinationFile,
  kDestinationPath
};

/**
 * Possible return values.
 */
enum Failures {
  kFailOk = 1,
  kFailLocalIO,
  kFailBadUrl,
  kFailConnection,
  kFailHttp,
  kFailBadData,
  kFailOther,
};


/**
 * Contains all the information to specify a download job.
 */
struct JobInfo {
  const std::string *url;
  bool compressed;
  bool nocache;
  Destination destination;
  struct {
    size_t size;
    size_t pos;
    char *data;
  } destination_mem;
  FILE *destination_file;
  const std::string *destination_path;
  const hash::t_sha1 *expected_hash;

  // One constructor per destination
  JobInfo(const std::string *u, const bool c, const bool n,
          const std::string *p, const hash::t_sha1 *h) : url(u), compressed(c),
          nocache(n), destination(kDestinationPath), destination_path(p),
          expected_hash(h)
          { wait_at[0] = wait_at[1] = -1; }
  JobInfo(const std::string *u, const bool c, const bool n,
          FILE *f, const hash::t_sha1 *h) : url(u), compressed(c), nocache(n),
          destination(kDestinationFile), destination_file(f), expected_hash(h)
          { wait_at[0] = wait_at[1] = -1; }
  JobInfo(const std::string *u, const bool c, const bool n,
          const hash::t_sha1 *h) : url(u), compressed(c), nocache(n),
          destination(kDestinationMem), expected_hash(h)
          { wait_at[0] = wait_at[1] = -1; }
  ~JobInfo() {
    if (wait_at[0] >= 0) {
      close(wait_at[0]);
      close(wait_at[1]);
    }
  }

  // Internal state, don't touch
  CURL *curl_handle;
  z_stream zstream;
  sha1_context_t sha1_context;
  int wait_at[2];  /**< Pipe used for the return value */
  Failures error_code;
  int num_retries;
};


void Init();
void Fini();
void Spawn();
Failures Fetch(JobInfo *info);

}  // namespace download

#endif  // CVMFS_DOWNLOAD_H_
