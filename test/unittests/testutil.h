#ifndef CVMFS_UNITTEST_TESTUTIL
#define CVMFS_UNITTEST_TESTUTIL

#include <sys/types.h>

#include "../../cvmfs/directory_entry.h"

pid_t GetParentPid(const pid_t pid);

class DirectoryEntryTestFactory {
 public:
  static catalog::DirectoryEntry RegularFile();
  static catalog::DirectoryEntry Directory();
  static catalog::DirectoryEntry Symlink();
  static catalog::DirectoryEntry ChunkedFile();
};

#endif /* CVMFS_UNITTEST_TESTUTIL */
