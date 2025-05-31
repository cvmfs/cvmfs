/**
 * This file is part of the CernVM File System.
 *
 * Implements the C wrapper of the OptionsManager
 */


#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>

#include "libcvmfs_cache.h"
#include "options.h"

using namespace std;  // NOLINT


SimpleOptionsParser *cvmcache_options_clone(SimpleOptionsParser *opts) {
  SimpleOptionsParser *result = new SimpleOptionsParser(
      *reinterpret_cast<SimpleOptionsParser *>(opts));
  return result;
}


void cvmcache_options_fini(SimpleOptionsParser *opts) { delete opts; }


void cvmcache_options_free(char *value) { free(value); }


char *cvmcache_options_get(SimpleOptionsParser *opts, const char *key) {
  string arg;
  const bool retval = opts->GetValue(key, &arg);
  if (!retval)
    return NULL;
  char *result = strdup(arg.c_str());
  assert(result != NULL);
  return result;
}


char *cvmcache_options_dump(SimpleOptionsParser *opts) {
  char *result = strdup(opts->Dump().c_str());
  assert(result != NULL);
  return result;
}


SimpleOptionsParser *cvmcache_options_init() {
  SimpleOptionsParser *result = new SimpleOptionsParser();
  // In contrast to the fuse module, we don't want to taint the process'
  // environment with parameters from the cvmfs configuration in libcvmfs
  result->set_taint_environment(false);
  // Not strictly necessary but avoids a failure log message
  result->SetValue("CVMFS_MOUNT_DIR", "/cvmfs");
  return result;
}


void cvmcache_options_set(SimpleOptionsParser *opts, const char *key,
                          const char *value) {
  opts->SetValue(key, value);
}


int cvmcache_options_parse(SimpleOptionsParser *opts, const char *path) {
  const bool result = opts->TryParsePath(path);
  return result ? 0 : -1;
}


void cvmcache_options_unset(SimpleOptionsParser *opts, const char *key) {
  opts->UnsetValue(key);
}
