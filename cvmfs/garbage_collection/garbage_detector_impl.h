/**
 * This file is part of the CernVM File System.
 */

#include <cassert>

template <class CatalogT, class HashFilterT>
void GarbageDetector<CatalogT, HashFilterT>::Preserve(const CatalogT *catalog) {
  assert (initializing_);
  // the hash of the actual catalog needs to preserved
  preserved_hashes_.Fill(catalog->catalog_hash());

  // all the objects referenced from this catalog need to be preserved
  const HashVector referenced_hashes    = catalog->ListReferencedObjects();
        typename HashVector::const_iterator i    = referenced_hashes.begin();
  const typename HashVector::const_iterator iend = referenced_hashes.end();
  for (; i != iend; ++i) {
    preserved_hashes_.Fill(*i);
  }
}


template <class CatalogT, class HashFilterT>
void GarbageDetector<CatalogT, HashFilterT>::FreezePreservation() {
  initializing_ = false;
  preserved_hashes_.Freeze();
}


template <class CatalogT, class HashFilterT>
bool GarbageDetector<CatalogT, HashFilterT>::IsCondemned(const shash::Any &hash) const {
  return ! preserved_hashes_.Contains(hash);
}


