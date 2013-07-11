/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_QUOTA_LISTENER_H_
#define CVMFS_QUOTA_LISTENER_H_

#include <string>

namespace catalog {
  class AbstractCatalogManager;
}

namespace quota {

void RegisterUnpinListener(catalog::AbstractCatalogManager *catalog_manager,
                           const std::string &repository_name);

}  // namespace quota

#endif  // CVMFS_QUOTA_LISTENER_H_
