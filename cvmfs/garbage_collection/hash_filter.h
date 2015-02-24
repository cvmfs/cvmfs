/**
 * This file is part of the CernVM File System.
 *
 * HashFilters are a container classes that get initialized with a number of
 * hashes. Later they can serve queries for other hashes and decide if they are
 * contained in the filter or not.
 */

#ifndef CVMFS_GARBAGE_COLLECTION_HASH_FILTER_H_
#define CVMFS_GARBAGE_COLLECTION_HASH_FILTER_H_

#include <set>

#include "../hash.h"
#include "../smallhash.h"

/**
 * Abstract base class of a HashFilter to define the common interface.
 */
class AbstractHashFilter {
 public:
  virtual ~AbstractHashFilter() {}

  /**
   * Adds the given hash to the filter
   *
   * @param hash  the hash to be added to the HashFilter
   */
  virtual void Fill(const shash::Any &hash) = 0;

  /**
   * Decides if a presented hash is in the filter or not
   * Depending on the concrete implementation of this method it could be a prob-
   * abilistic answer. However, implementations should ensure a recall rate of
   * 100%, say: never produce false negatives.
   *
   * @param hash  the hash to be queried
   * @return  true   if the hash is (probably) contained in the set
   *          false  if it is definitely not in the set
   */
  virtual bool Contains(const shash::Any &hash) const = 0;

  /**
   * Freezes the filter after filling it with all values. This is not necessary
   * but could be used for certain optimizations depending on the implementation
   * of the AbstractHashFilter.
   * Note: After Freeze() has been called, Fill() should fail!
   */
  virtual void Freeze() {}

  /**
   * Returns the number of objects already inserted into the filter.
   * @return number of objects in the filter
   */
  virtual size_t Count() const = 0;
};


//------------------------------------------------------------------------------


/**
 * This is a simplistic implementation of AbstractHashFilter mainly used for
 * testing purposes. It uses an std::set and thus is highly suboptimal.
 */
class SimpleHashFilter : public AbstractHashFilter {
 public:
  SimpleHashFilter() : frozen_(false) {}

  void Fill(const shash::Any &hash) {
    assert(!frozen_);
    hashes_.insert(hash);
  }

  bool Contains(const shash::Any &hash) const {
    return hashes_.find(hash) != hashes_.end();
  }

  void Freeze() { frozen_ = true; }
  size_t Count() const { return hashes_.size(); }

 private:
  std::set<shash::Any>  hashes_;
  bool                  frozen_;
};


//------------------------------------------------------------------------------


/**
 * This is an implementation of AbstractHashFilter using the SmallHash structure
 * for internal storage.
 */
class SmallhashFilter : public AbstractHashFilter {
 protected:
  static uint32_t hasher(const shash::Any &key) {
    // Don't start with the first bytes, because == is using them as well
    return (uint32_t) *(reinterpret_cast<uint32_t *>(key.digest) + 1);
  }

 public:
  SmallhashFilter() : frozen_(false) {
    // zero_element is MD5("unobtanium")
    shash::Any zero_element(shash::kMd5,
                            shash::HexPtr("d61f853acc5a39e01f3906f73e31d256"));
    hashmap_.Init(1048576, zero_element, &SmallhashFilter::hasher);
  }

  void Fill(const shash::Any &hash) {
    assert(!frozen_);
    hashmap_.Insert(hash, true);
  }

  bool Contains(const shash::Any &hash) const {
    return hashmap_.Contains(hash);
  }

  void   Freeze()      { frozen_ = true;         }
  size_t Count() const { return hashmap_.size(); }

 private:
  SmallHashDynamic<shash::Any, bool>  hashmap_;
  bool                                frozen_;
};

#endif  // CVMFS_GARBAGE_COLLECTION_HASH_FILTER_H_
