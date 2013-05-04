/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SMALLHASH_H_
#define CVMFS_SMALLHASH_H_

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdint.h>

#include <cstdlib>

#include <algorithm>

/**
 * Hash table with linear probing as collision resolution.  Works only for
 * a fixed (maximum) number of elements, i.e. no resizing.  Load factor fixed
 * to 0.7.
 */
template<class Key, class Value>
class SmallHash {
public:
  static const double kLoadFactor;
  
  SmallHash() {
    keys_ = NULL;
    values_ = NULL;
    hasher_ = NULL;
    bytes_allocated_ = 0;
    num_collisions_ = 0;
    max_collisions_ = 0;
  }
  
  void Init(uint32_t size, Key empty, uint32_t (*hasher)(const Key &key)) {
    capacity_ = static_cast<uint32_t>(static_cast<double>(size)/kLoadFactor);
    keys_ = new Key[capacity_];
    values_ = new Value[capacity_];
    bytes_allocated_ = (sizeof(Key) + sizeof(Value)) * capacity_;
    hasher_ = hasher;
    empty_key_ = empty;
    this->Clear();
  }
  
  bool Lookup(const Key &key, Value *value) const {
    uint32_t bucket;
    uint32_t collisions;
    const bool found = DoLookup(key, &bucket, &collisions);
    if (found)
      *value = values_[bucket];
    return found;
  }
  
  void Insert(const Key &key, const Value &value) {
    DoInsert(key, value, true);
  }
  
  void Erase(const Key &key) {
    uint32_t bucket;
    uint32_t collisions;
    const bool found = DoLookup(key, &bucket, &collisions);
    if (found) {
      keys_[bucket] = empty_key_;
      bucket = (bucket+1) % capacity_;
      while (!(keys_[bucket] == empty_key_)) {
        Key rehash = keys_[bucket];
        keys_[bucket] = empty_key_;
        DoInsert(rehash, values_[bucket], false);
        bucket = (bucket+1) % capacity_;
      }
    }
  }
  
  void Clear() {
    for (uint32_t i = 0; i < capacity_; ++i)
      keys_[i] = empty_key_;
  }
  
  uint64_t bytes_allocated() const { return bytes_allocated_; }
  static double GetEntrySize() {
    const double unit = sizeof(Key) + sizeof(Value);
    return unit/kLoadFactor;
  }
  
  void GetCollisionStats(uint64_t *num_collisions,
                         uint32_t *max_collisions) const
  {
    *num_collisions = num_collisions_;
    *max_collisions = max_collisions_;
  }
  
  ~SmallHash() {
    delete[] keys_;
    delete[] values_;
  }
  
private:
  uint32_t ScaleHash(const Key &key) const {
    double bucket = (double(hasher_(key)) * double(capacity_) /
                     double((uint32_t)(-1)));
    return (uint32_t)bucket % capacity_;
  }
  
  void DoInsert(const Key &key, const Value &value,
                const bool count_collisions)
  {
    uint32_t bucket;
    uint32_t collisions;
    DoLookup(key, &bucket, &collisions);
    if (count_collisions) {
      num_collisions_ += collisions;
      max_collisions_ = std::max(collisions, max_collisions_);
    }
    keys_[bucket] = key;
    values_[bucket] = value;
  }
  
  bool DoLookup(const Key &key, uint32_t *bucket, uint32_t *collisions) const {
    *bucket = ScaleHash(key);
    *collisions = 0;
    while (!(keys_[*bucket] == empty_key_)) {
      if (keys_[*bucket] == key)
        return true;
      *bucket = (*bucket+1) % capacity_;
      (*collisions)++;
    }
    return false;
  }
  
  // Separate key and value arrays for better locality
  Key *keys_;
  Value *values_;
  uint32_t capacity_;
  uint32_t (*hasher_)(const Key &key);
  uint64_t bytes_allocated_;
  uint64_t num_collisions_;
  uint32_t max_collisions_;  /**< maximum collisions for a single insert */
  Key empty_key_;
};

// initialize the static load factor field
template<class Key, class Value>
const double SmallHash<Key, Value>::kLoadFactor = 0.7;



#endif  // CVMFS_SMALLHASH_H_
