/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_LISTCATALOGS_H_
#define CVMFS_SWISSKNIFE_LISTCATALOGS_H_

#include "swissknife.h"

#include "hash.h"
#include "catalog_traversal.h"

namespace catalog {
  class Catalog;
}

namespace swissknife {

class CommandListCatalogs : public Command<CommandListCatalogs> {
 public:
  CommandListCatalogs(const std::string &param) : Command(param),
                                                  print_tree_(false),
                                                  print_hash_(false) {}
  ~CommandListCatalogs() { };
  static std::string GetName() { return "lsrepo"; };
  static std::string GetDescription() {
    return "CernVM File System Repository Traversal\n"
      "This command lists the nested catalog tree that builds up a "
      "cvmfs repository structure.";
  };
  static ParameterList GetParameters();

  int Run(const ArgumentList &args);

  void CatalogCallback(const CatalogTraversalData &data);

 private:
  bool print_tree_;
  bool print_hash_;
};

}

#endif  // CVMFS_SWISSKNIFE_LISTCATALOGS_H_
