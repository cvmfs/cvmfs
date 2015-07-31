/**
 * This file is part of the CernVM File System.
 *
 * The unpin listener acts as a mediator between the cache manager (quota.cc)
 * and the catalog manager.  If the cache fills up with pinned catalog, the
 * cache manager will broadcast a "release" request to all clients.  The unpin
 * listener handles the request and asks the cache manager to detach all
 * catalogs except from the root catalog.  The next access on a file deep
 * in the hierarchy will automatically trigger the respective nested catalogs
 * to be loaded.
 *
 * It allows to get rid of pinned catalogs that are lingering in the cache but
 * not used.  As a result, the cache size can be reduced.  If it is too small,
 * however, the cache starts thrashing.
 */

#ifndef CVMFS_QUOTA_LISTENER_H_
#define CVMFS_QUOTA_LISTENER_H_

#include <string>

class QuotaManager;
namespace catalog {
class AbstractCatalogManager;
}

namespace quota {

struct ListenerHandle;

ListenerHandle *
RegisterUnpinListener(QuotaManager *quota_manager,
                      catalog::AbstractCatalogManager *catalog_manager,
                      const std::string &repository_name);
ListenerHandle * RegisterWatchdogListener(QuotaManager *quota_manager,
                                          const std::string &repository_name);
void UnregisterListener(ListenerHandle *handle);

}  // namespace quota

#endif  // CVMFS_QUOTA_LISTENER_H_
