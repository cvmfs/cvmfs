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
    ParameterList r;
    r.push_back(Parameter::Mandatory('r', "repository directory / url"));
    r.push_back(Parameter::Optional ('t', "check specific repository tag"));
    r.push_back(Parameter::Optional ('l', "log level (0-4, default: 2)"));
    r.push_back(Parameter::Switch   ('c', "check availability of data chunks"));
    return r;
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
