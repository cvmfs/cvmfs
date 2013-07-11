/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "quota_listener.h"

#include "quota.h"
#include "catalog_mgr.h"

using namespace std;  // NOLINT

namespace quota {

/**
 * Registers a back channel that reacts on high watermark of pinned chunks
 */
void RegisterUnpinListener(catalog::AbstractCatalogManager *catalog_manager,
                           const string &repository_name)
{
}

}  // namespace quota
