/**
 * This file is part of the CernVM File System.
 *
 * Implements the C wrapper of the OptionsManager
 */

#include "cvmfs_config.h"
#include "libcvmfs.h"

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>

#include "options.h"

using namespace std;  // NOLINT


OptionsManager *cvmfs_options_clone(OptionsManager *opts) {
  OptionsManager *result = new SimpleOptionsParser(
    *reinterpret_cast<SimpleOptionsParser *>(opts));
  return result;
}


void cvmfs_options_fini(OptionsManager *opts) {
  delete opts;
}


void cvmfs_options_free(char *value) {
  free(value);
}


char *cvmfs_options_get(OptionsManager *opts, const char *key) {
  string arg;
  bool retval = opts->GetValue(key, &arg);
  if (!retval)
    return NULL;
  char *result = strdup(arg.c_str());
  assert(result != NULL);
  return result;
}


OptionsManager *cvmfs_options_init() {
  OptionsManager *result = new SimpleOptionsParser();
  // In contrast to the fuse module, we don't want to taint the process'
  // environment with parameters from the cvmfs configuration in libcvmfs
  result->set_taint_environment(false);
  return result;
}


void cvmfs_options_set(
  OptionsManager *opts,
  const char *key, const
  char *value)
{
  opts->SetValue(key, value);
}


void cvmfs_options_unset(OptionsManager *opts, const char *key) {
  opts->UnsetValue(key);
}
