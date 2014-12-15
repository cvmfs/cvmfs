/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_LISTCATALOGS_H_
#define CVMFS_SWISSKNIFE_LISTCATALOGS_H_

#include "swissknife.h"

#include "hash.h"
#include "object_fetcher.h"
#include "catalog_traversal.h"

namespace catalog {
  class Catalog;
}

namespace swissknife {

class CommandListCatalogs : public Command {
 protected:
  typedef HttpObjectFetcher<typename ReadonlyCatalogTraversal::Catalog>
          ReadonlyHttpObjectFetcher;

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

  void CatalogCallback(const ReadonlyCatalogTraversal::CallbackData &data);

 private:
  bool print_tree_;
  bool print_hash_;
  bool print_size_;
  bool print_entries_;

  UniquePtr<ReadonlyHttpObjectFetcher> object_fetcher_;
};

}

#endif  // CVMFS_SWISSKNIFE_LISTCATALOGS_H_
