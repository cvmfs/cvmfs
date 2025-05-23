/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_LSREPO_H_
#define CVMFS_SWISSKNIFE_LSREPO_H_

#include <string>

#include "catalog_traversal.h"
#include "crypto/hash.h"
#include "object_fetcher.h"
#include "swissknife.h"

namespace catalog {
class Catalog;
}

namespace swissknife {

class CommandListCatalogs : public Command {
 public:
  CommandListCatalogs();
  ~CommandListCatalogs() { }
  virtual std::string GetName() const { return "lsrepo"; }
  virtual std::string GetDescription() const {
    return "CernVM File System Repository Traversal\n"
           "This command lists the nested catalog tree that builds up a "
           "cvmfs repository structure.";
  }
  virtual ParameterList GetParams() const;

  int Main(const ArgumentList &args);

 protected:
  template<class ObjectFetcherT>
  bool Run(const shash::Any &manual_root_hash, ObjectFetcherT *object_fetcher) {
    typename CatalogTraversal<ObjectFetcherT>::Parameters params;
    params.object_fetcher = object_fetcher;
    CatalogTraversal<ObjectFetcherT> traversal(params);
    traversal.RegisterListener(&CommandListCatalogs::CatalogCallback, this);
    if (manual_root_hash.IsNull())
      return traversal.Traverse();
    return traversal.Traverse(manual_root_hash);
  }

  void CatalogCallback(const CatalogTraversalData<catalog::Catalog> &data);

 private:
  bool print_tree_;
  bool print_hash_;
  bool print_size_;
  bool print_entries_;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_LSREPO_H_
