/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_hash.h"

#include <alloca.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cstdio>

#include "hash.h"
#include "logging.h"
#include "util.h"

// Hash stdin and print the digest to stdout
int swissknife::CommandHash::Main(const swissknife::ArgumentList &args) {
  bool fingerprint = (args.find('f') != args.end());
  shash::Any hash(shash::ParseHashAlgorithm(*args.find('a')->second));

  shash::ContextPtr context_ptr(hash.algorithm);
  context_ptr.buffer = smalloc(context_ptr.size);

  unsigned char buf[kPageSize];
  size_t n;
  shash::Init(context_ptr);
  while ((n = read(fileno(stdin), buf, kPageSize)) > 0) {
    shash::Update(buf, n, context_ptr);
  }
  shash::Final(context_ptr, &hash);
  LogCvmfs(kLogCvmfs, kLogStdout, "%s", fingerprint ?
           hash.ToFingerprint().c_str() : hash.ToString().c_str());

  return 0;
}
