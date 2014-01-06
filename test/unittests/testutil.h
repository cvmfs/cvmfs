#ifndef CVMFS_UNITTEST_TESTUTIL
#define CVMFS_UNITTEST_TESTUTIL

#include <sys/types.h>

#include "../../cvmfs/directory_entry.h"
#include "../../cvmfs/util.h"

pid_t GetParentPid(const pid_t pid);

namespace catalog {

class DirectoryEntryTestFactory {
 public:
  static catalog::DirectoryEntry RegularFile();
  static catalog::DirectoryEntry Directory();
  static catalog::DirectoryEntry Symlink();
  static catalog::DirectoryEntry ChunkedFile();
};

} /* namespace catalog */

class PolymorphicConstructionUnittestAdapter {
 public:
  template <class AbstractProductT, class ConcreteProductT>
  static void RegisterPlugin() {
    AbstractProductT::template RegisterPlugin<ConcreteProductT>();
  }

  template <class AbstractProductT>
  static void UnregisterAllPlugins() {
    AbstractProductT::UnregisterAllPlugins();
  }
};

#endif /* CVMFS_UNITTEST_TESTUTIL */
