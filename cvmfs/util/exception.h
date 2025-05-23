/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_EXCEPTION_H_
#define CVMFS_UTIL_EXCEPTION_H_

#include <cassert>
#include <cstdarg>
#include <stdexcept>
#include <string>

#include "util/export.h"
#include "util/logging.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

class CVMFS_EXPORT ECvmfsException : public std::runtime_error {
 public:
  explicit ECvmfsException(const std::string &what_arg)
      : std::runtime_error(what_arg) { }
};

#define CVMFS_S1(x)           #x
#define CVMFS_S2(x)           CVMFS_S1(x)
#define CVMFS_SOURCE_LOCATION "PANIC: " __FILE__ " : " CVMFS_S2(__LINE__)
#define PANIC(...)            Panic(CVMFS_SOURCE_LOCATION, kLogCvmfs, __VA_ARGS__);

CVMFS_EXPORT
__attribute__((noreturn)) void Panic(const char *coordinates,
                                     const LogSource source, const int mask,
                                     const char *format, ...);

// For PANIC(NULL)
CVMFS_EXPORT
__attribute__((noreturn)) void Panic(const char *coordinates,
                                     const LogSource source, const char *nul);

// The AssertOrLog function is used for rare error cases that are not yet fully
// understood. By default, these error cases are considered as unreachable and
// thus an assert is thrown. Some users may prefer though to continue operation
// (even though the file system state may be scrambled).
// Ideally, we can at some point remove all AssertOrLog statements.
#ifdef CVMFS_SUPPRESS_ASSERTS
static inline bool AssertOrLog(int t, const LogSource log_source,
                               const int log_mask, const char *log_msg_format,
                               ...) {
  if (!t) {
    va_list variadic_list;
    va_start(variadic_list, log_msg_format);
    vLogCvmfs(log_source, log_mask, log_msg_format, variadic_list);
    va_end(variadic_list);
    return false;
  }
  return true;
}
#else
static inline bool AssertOrLog(int t, const LogSource /*log_source*/,
                               const int /*log_mask*/,
                               const char * /*log_msg_format*/, ...) {
  assert(t);
  return true;
}
#endif

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_EXCEPTION_H_
