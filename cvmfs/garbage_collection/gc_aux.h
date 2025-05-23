/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_GARBAGE_COLLECTION_GC_AUX_H_
#define CVMFS_GARBAGE_COLLECTION_GC_AUX_H_

#include <string>

#include "garbage_collection/garbage_collector.h"
#include "reflog_sql.h"

/**
 * Garbage collection for auxiliary files is much simpler than for catalogs.
 * Auxiliary files are the tag database, meta info, and the certificate.  These
 * objects do not reference other objects.  Also, they are not referenced from
 * catalogs but from the manifest.  Thus they become garbage immediately on
 * publishing of a new revision with different objects (we still apply a
 * grace period before deletion).
 *
 * The reference log can provide a time-sorted list of object hashes. Since the
 * hashes are unique (primary key), it is sufficient to walk through the list
 * and remove all auxiliary objects older than X.  In addition, we keep the
 * versions of all auxiliary files provided through a hash filter.  This is
 * usually the objects referenced by the current manifest.
 *
 * The timestamp X comes from the catalog garbage collection, which, as part of
 * the catalog traversal, figures out the oldest catalog from the "previous
 * catalog" chain that needs to be preserved (named snapshots can be older).
 * Auxiliary files produced during publish are at least as young as their
 * corresponding root catalog.
 */
template<class CatalogTraversalT, class HashFilterT>
class GarbageCollectorAux {
  typedef typename CatalogTraversalT::ObjectFetcherTN ObjectFetcherTN;
  typedef typename ObjectFetcherTN::ReflogTN ReflogTN;
  typedef
      typename GarbageCollector<CatalogTraversalT, HashFilterT>::Configuration
          ConfigurationTN;

 public:
  explicit GarbageCollectorAux(const ConfigurationTN &config);

  bool CollectOlderThan(uint64_t timestamp,
                        const HashFilterT &preserved_objects);

 private:
  std::string PrintAuxType(SqlReflog::ReferenceType type);
  bool Sweep(const shash::Any &hash);

  const ConfigurationTN config_;
};  // class GarbageCollectorAux

#include "gc_aux_impl.h"

#endif  // CVMFS_GARBAGE_COLLECTION_GC_AUX_H_
