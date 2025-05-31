/**
 * This file is part of the CernVM File System.
 */


#include "cmd_hash.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdint.h>

#include <cstdio>
#include <cstring>

#include "crypto/hash.h"
#include "publish/except.h"
#include "util/logging.h"

int publish::CmdHash::Main(const Options &options) {
  const std::string algorithm = options.GetString("algorithm");
  shash::Any hash(shash::ParseHashAlgorithm(algorithm));
  // MD5 is not a content hash algorithm but we deal with it in this utility
  // nevertheless
  if (algorithm == "md5")
    hash.algorithm = shash::kMd5;
  if (hash.algorithm == shash::kAny)
    throw EPublish("unknown hash algorithm: " + algorithm);

  if (options.Has("input")) {
    shash::HashString(options.GetString("input"), &hash);
  } else {
    shash::HashFd(fileno(stdin), &hash);
  }
  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "%s",
           options.Has("fingerprint") ? hash.ToFingerprint().c_str()
                                      : hash.ToString().c_str());
  if (options.Has("split")) {
    if (hash.algorithm != shash::kMd5)
      throw EPublish("split int representation only supported for MD5");

    uint64_t high, low;
    shash::Md5 md5_hash;
    memcpy(md5_hash.digest, hash.digest, shash::kDigestSizes[shash::kMd5]);
    md5_hash.ToIntPair(&high, &low);
    // SQLite uses int64, not uint64
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             " [%" PRId64 " %" PRId64 "]", static_cast<int64_t>(high),
             static_cast<int64_t>(low));
  }
  LogCvmfs(kLogCvmfs, kLogStdout, "");

  return 0;
}
