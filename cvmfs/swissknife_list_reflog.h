/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_LIST_REFLOG_H_
#define CVMFS_SWISSKNIFE_LIST_REFLOG_H_

#include <set>
#include <string>
#include <vector>

#include "catalog_traversal.h"
#include "crypto/hash.h"
#include "smallhash.h"
#include "swissknife.h"
#include "util/pointer.h"

namespace swissknife {

class CommandListReflog : public Command {
 public:
  ~CommandListReflog() { }
  virtual std::string GetName() const { return "list_reflog"; }
  virtual std::string GetDescription() const {
    return "List all objects reachable through the reference log "
           "of a CVMFS repository.";
  }
  virtual ParameterList GetParams() const;
  int Main(const ArgumentList &args);

 protected:
  UniquePtr<SmallHashDynamic<shash::Any, bool> > objects_;

  template<class ObjectFetcherT>
  bool Run(ObjectFetcherT *object_fetcher, std::string repo_name,
           std::string output_path, shash::Any reflog_hash);
  void CatalogCallback(const CatalogTraversalData<catalog::Catalog> &data);
  void InsertObjects(const std::vector<shash::Any> &list);
  void DumpObjects(FILE *stream);

  static uint32_t hasher(const shash::Any &key) {
    // Don't start with the first bytes, because == is using them as well
    return static_cast<uint32_t>(
        *(reinterpret_cast<const uint32_t *>(key.digest) + 1));
  }
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_LIST_REFLOG_H_
