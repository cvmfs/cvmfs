/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_LISTCATALOGS_H_
#define CVMFS_SWISSKNIFE_LISTCATALOGS_H_

#include "swissknife.h"

#include "hash.h"

namespace catalog {
  class Catalog;
}

namespace swissknife {

class CommandListCatalogs : public Command {
 public:
  CommandListCatalogs();
  ~CommandListCatalogs() { };
  std::string GetName() { return "lsrepo"; };
  std::string GetDescription() {
    return "CernVM File System Repository Traversal\n"
      "This command lists the nested catalog tree that builds up a "
      "cvmfs repository structure.";
  };
  ParameterList GetParams();

  int Main(const ArgumentList &args);

  void CatalogCallback(const catalog::Catalog* catalog,
                       const hash::Any&        catalog_hash,
                       const unsigned          tree_level);

 private:
  bool print_tree_;
  bool print_hash_;
};

}

#endif  // CVMFS_SWISSKNIFE_LISTCATALOGS_H_
