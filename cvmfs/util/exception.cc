/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "exception.h"

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>

#include "logging.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

void Panic(const char *coordinates, const int mask, const char *format, ...) {
  char* msg = NULL;
  va_list variadic_list;

  // Format the message string
  va_start(variadic_list, format);
  int retval = vasprintf(&msg, format, variadic_list);
  assert(retval != -1);  // else: out of memory
  va_end(variadic_list);

  // Add the coordinates
  char* msg_with_coordinates = NULL;
  retval = asprintf(&msg_with_coordinates, "%s\n%s", coordinates, msg);
  if (retval == -1) {
    free(msg_with_coordinates);
  } else {
    free(msg);
    msg = msg_with_coordinates;
  }
  // From now on we deal only with `msg`

  // Either throw the exception of log + abort
#ifdef LIBCVMFS_SERVER
  throw ECvmfsException(msg);
#else
  LogCvmfs(kLogCvmfs, mask, msg);
  abort();
#endif
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
