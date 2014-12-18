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

 protected:
  template <class ObjectFetcherT>
  bool Run(ObjectFetcherT *object_fetcher) {
    typename CatalogTraversal<ObjectFetcherT>::Parameters params;
    params.object_fetcher = object_fetcher;
    CatalogTraversal<ObjectFetcherT> traversal(params);
    traversal.RegisterListener(&CommandListCatalogs::CatalogCallback, this);
    return traversal.Traverse();
  }

  void CatalogCallback(const CatalogTraversalData<catalog::Catalog> &data);

 private:
  bool print_tree_;
  bool print_hash_;
  bool print_size_;
  bool print_entries_;
};

}

#endif  // CVMFS_SWISSKNIFE_LISTCATALOGS_H_
