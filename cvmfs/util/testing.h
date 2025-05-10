/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_TESTING_H_
#define CVMFS_UTIL_TESTING_H_


#include <cstdlib>


#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

#ifdef DEBUGMSG
// Let an external program pause cvmfs if
//  - program runs in debug mode
//  - the given environment variable is defined _and_
//  - points to an existing file
//  - whose content is stop/start
void CVMFS_TEST_INJECT_BARRIER(const char *env) {
  char *value = getenv(env);
  if (value == NULL)
    return;
  int fd = open(value, O_RDONLY);
  if (fd < 0)
    return;
  std::string action;
  bool retval = SafeReadToString(fd, &action);
  close(fd);
  if (!retval)
    return;
  action = Trim(action, true /* trim_newline */);
  if (action != "pause")
    return;
  Prng prng;
  prng.InitLocaltime();
  while (true) {
    int fd = open(value, O_RDONLY);
    if (fd < 0)
      return;
    retval = SafeReadToString(fd, &action);
    close(fd);
    action = Trim(action, true /* trim_newline */);
    if (action == "resume")
      break;
    if (action == "resume one") {
      SafeWriteToFile("pause", value, 0644);
      break;
    }
    SafeSleepMs(prng.Next(1000));
  }
}
#else
#define CVMFS_TEST_INJECT_BARRIER(...) ((void)0)
#endif

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_TESTING_H_
