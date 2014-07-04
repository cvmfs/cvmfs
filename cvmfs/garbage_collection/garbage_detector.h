/**
 * This file is part of the CernVM File System.
 *
 * The GarbageDetector class is figuring out which data objects (represented by
 * their content hashes) can be deleted as outdated garbage.
 * Garbage collection is performed on the granularity of catalog revisions, thus
 * a complete repository revision is either considered to be outdated or active.
 * This way, a mountable repository revision says completely usable (no nested
 * catalogs or data objects become unavailable). A revision is defined by it's
 * root catalog; the revision numbers of nested catalogs are irrelevant, since
 * they might be referenced by newer (preserved) repository revisions.
 * Thus, garbage objects are those that are _not_ referenced by any of the pre-
 * served root catalogs or their direct subordinate nested catalog try.
 *
 * We use a two-stage approach:
 *   1st Stage - Initialization
 *               The GarbageDetector is reading all the catalogs that are meant
 *               to be preserved. It builds up a filter (HashFilterT) containing
 *               all content hashes that are _not_ to be deleted
 *   2nd Stage - Sweeping
 *               The initialized GarbageDetector is presented with all content
 *               hashes found in condemned catalogs and decides if they are
 *               referenced by the preserved catalog revisions or not.
 *
 * The GarbageDetector is templated with CatalogT mainly for testability and
 * with HashFilterT as an instance of the Strategy Pattern to abstract from the
 * actual hash filtering method to be used.
 */

#ifndef CVMFS_GARBAGE_COLLECTION_GARBAGE_DETECTOR_H
#define CVMFS_GARBAGE_COLLECTION_GARBAGE_DETECTOR_H

template <class CatalogT, class HashFilterT>
class GarbageDetector {
 protected:
  typedef typename CatalogT::HashVector HashVector;

 public:
  GarbageDetector(HashFilterT &hash_filter) :
    preserved_hashes_(hash_filter),
    initializing_(true) {}

  /**
   * Reads the given catalog and marks all contained content hashes 'preserved',
   * thus not to be deleted in the sweeping stage.
   * Note: Once the GarbageDetector left the initialization state, this method
   *       should not be called anymore.
   * @param catalog  the catalog to be preserved
   */
  void Preserve(const CatalogT *catalog);

  /**
   * Called after the initialization with preserved catalogs is finished and
   * the GarbageDetector should switch into the 'sweeping' state.
   */
  void FreezePreservation();

  /**
   * Checks if a presented hash is matching the internal filters or not. Keep in
   * mind, that the result of this method might vary depending on the HashFilter
   * used. HashFilters could be implemented using probabilistic algorithms!
   *
   * @param hash  the hash to be looked up in the GarbageDetector
   * @return      true if the object with this hash can be safely deleted
   */
  bool IsCondemned(const shash::Any &hash) const;

 private:
  HashFilterT  &preserved_hashes_;
  bool          initializing_;
};

#include "garbage_detector_impl.h"

#endif /* CVMFS_GARBAGE_COLLECTION_GARBAGE_DETECTOR_H */
