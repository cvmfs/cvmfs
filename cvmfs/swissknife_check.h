/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_CHECK_H_
#define CVMFS_SWISSKNIFE_CHECK_H_

#include "swissknife.h"
#include "hash.h"
#include "catalog.h"

namespace download {
class DownloadManager;
}

namespace swissknife {

class CommandCheck : public Command {
 public:
  ~CommandCheck() { };
  std::string GetName() { return "check"; };
  std::string GetDescription() {
    return "CernVM File System repository sanity checker\n"
      "This command checks the consisteny of the file catalogs of a "
        "cvmfs repository.";
  };
  ParameterList GetParams() {
    ParameterList result;
    result.push_back(Parameter('r', "repository directory / url",
                               false, false));
    result.push_back(Parameter('t', "check specific repository tag",
                               true, false));
    result.push_back(Parameter('l', "log level (0-4, default: 2)", true, false));
    result.push_back(Parameter('c', "check availability of data chunks",
                               true, true));
    return result;
  }
  int Main(const ArgumentList &args);

 protected:
  bool InspectTree(const std::string &path,
                   const shash::Any &catalog_hash,
                   const uint64_t catalog_size,
                   const catalog::DirectoryEntry *transition_point,
                   catalog::DeltaCounters *computed_counters);
  std::string DecompressPiece(const shash::Any catalog_hash,
                              const char suffix);
  std::string DownloadPiece(const shash::Any catalog_hash,
                            const char suffix);
  bool Find(const catalog::Catalog *catalog,
            const PathString &path,
            catalog::DeltaCounters *computed_counters);
  bool Exists(const std::string &file);
  bool CompareCounters(const catalog::Counters &a,
                       const catalog::Counters &b);
  bool CompareEntries(const catalog::DirectoryEntry &a,
                      const catalog::DirectoryEntry &b,
                      const bool compare_names,
                      const bool is_transition_point = false);
};

}

#endif  // CVMFS_SWISSKNIFE_CHECK_H_
