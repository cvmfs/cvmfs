/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_CHECK_H_
#define CVMFS_SWISSKNIFE_CHECK_H_

#include <set>
#include <string>

#include "catalog.h"
#include "crypto/hash.h"
#include "smallhash.h"
#include "swissknife.h"

namespace download {
class DownloadManager;
}
namespace history {
class History;
}
namespace manifest {
class Manifest;
}


namespace swissknife {

class CommandCheck : public Command {
 public:
  CommandCheck();
  ~CommandCheck() { }
  virtual std::string GetName() const { return "check"; }
  virtual std::string GetDescription() const {
    return "CernVM File System repository sanity checker\n"
           "This command checks the consistency of the file catalogs of a "
           "cvmfs repository.";
  }
  virtual ParameterList GetParams() const {
    ParameterList r;
    r.push_back(Parameter::Mandatory('r', "repository directory / url"));
    r.push_back(Parameter::Optional('n', "check specific repository tag"));
    r.push_back(Parameter::Optional('t', "temp directory (default: /tmp)"));
    r.push_back(Parameter::Optional('l', "log level (0-4, default: 2)"));
    r.push_back(Parameter::Optional('s', "check subtree (nested catalog)"));
    r.push_back(Parameter::Optional('k', "public key of the repository / dir"));
    r.push_back(Parameter::Optional('z', "trusted certificates"));
    r.push_back(Parameter::Optional('N', "name of the repository"));
    r.push_back(Parameter::Optional('R', "path to reflog.chksum file"));
    r.push_back(Parameter::Optional('@', "proxy url"));
    r.push_back(Parameter::Switch('c', "check availability of data chunks"));
    r.push_back(Parameter::Switch('d', "don't use hashmap to avoid duplicated"
                                       " lookups. Note that this is a fallback"
                                       " option that may be removed."));
    r.push_back(Parameter::Switch('L', "follow HTTP redirects"));
    return r;
  }
  int Main(const ArgumentList &args);

 protected:
  bool InspectTree(const std::string &path,
                   const shash::Any &catalog_hash,
                   const uint64_t catalog_size,
                   const bool is_nested_catalog,
                   const catalog::DirectoryEntry *transition_point,
                   catalog::DeltaCounters *computed_counters);
  catalog::Catalog *FetchCatalog(const std::string &path,
                                 const shash::Any &catalog_hash,
                                 const uint64_t catalog_size = 0);
  bool FindSubtreeRootCatalog(const std::string &subtree_path,
                              shash::Any *root_hash,
                              uint64_t *root_size);

  std::string DecompressPiece(const shash::Any catalog_hash);
  std::string DownloadPiece(const shash::Any catalog_hash);
  std::string FetchPath(const std::string &path);
  bool InspectReflog(const shash::Any &reflog_hash,
                     manifest::Manifest *manifest);
  bool InspectHistory(history::History *history);
  bool Find(const catalog::Catalog *catalog,
            const PathString &path,
            catalog::DeltaCounters *computed_counters,
            std::set<PathString> *bind_mountpoints);
  bool Exists(const std::string &file);
  bool CompareCounters(const catalog::Counters &a, const catalog::Counters &b);
  bool CompareEntries(const catalog::DirectoryEntry &a,
                      const catalog::DirectoryEntry &b,
                      const bool compare_names,
                      const bool is_transition_point = false);

 private:
  std::string temp_directory_;
  std::string repo_base_path_;
  bool check_chunks_;
  bool no_duplicates_map_;
  bool is_remote_;
  SmallHashDynamic<shash::Any, char> duplicates_map_;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_CHECK_H_
