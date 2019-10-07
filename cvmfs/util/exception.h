/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_EXCEPTION_H_
#define CVMFS_UTIL_EXCEPTION_H_

#include <stdexcept>

#include "logging.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

class ECvmfsException : std::runtime_error {
 public:
  ECvmfsException(const std::string& what_arg) : 
    std::runtime_error(what_arg){};
};

#define PANIC(...) Panic("PANIC: __FILE__ : __LINE__", __VA_ARGS__);

void Panic(const char* coordinates, const char* format, ...) {
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
  LogCvmfs(kLogCvmfs, kLogStderr, msg);
  abort();
#endif
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_EXCEPTION_H_
