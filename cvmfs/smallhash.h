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

#include <cassert>
#include <cstdlib>
#include <algorithm>

#include "smalloc.h"

/**
 * Hash table with linear probing as collision resolution.  Works only for
 * a fixed (maximum) number of elements, i.e. no resizing.  Load factor fixed
 * to 0.7.
 */
template<class Key, class Value, class Derived>
class SmallHashBase {
 public:
  static const double kLoadFactor;  // mainly useless for the dynamic version
  static const double kThresholdGrow;  // only used for resizable version
  static const double kThresholdShrink;  // only used for resizable version

  SmallHashBase() {
    keys_ = NULL;
    values_ = NULL;
    hasher_ = NULL;
    bytes_allocated_ = 0;
    num_collisions_ = 0;
    max_collisions_ = 0;
  }

  void Init(uint32_t expected_size, Key empty,
            uint32_t (*hasher)(const Key &key))
  {
    hasher_ = hasher;
    empty_key_ = empty;
    capacity_ =
      static_cast<uint32_t>(static_cast<double>(expected_size)/kLoadFactor);
    initial_capacity_ = capacity_;
    static_cast<Derived *>(this)->SetThresholds();  // No-op for fixed size
    InitMemory();
    this->DoClear(false);
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
    static_cast<Derived *>(this)->Grow();  // No-op if fixed-size
    const bool overwritten = DoInsert(key, value, true);
    size_ += !overwritten;  // size + 1 if the key was not yet in the map
  }

  void Erase(const Key &key) {
    uint32_t bucket;
    uint32_t collisions;
    const bool found = DoLookup(key, &bucket, &collisions);
    if (found) {
      keys_[bucket] = empty_key_;
      size_--;
      bucket = (bucket+1) % capacity_;
      while (!(keys_[bucket] == empty_key_)) {
        Key rehash = keys_[bucket];
        keys_[bucket] = empty_key_;
        DoInsert(rehash, values_[bucket], false);
        bucket = (bucket+1) % capacity_;
      }
      static_cast<Derived *>(this)->Shrink();  // No-op if fixed-size
    }
  }

  void Clear() {
    DoClear(true);
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

  ~SmallHashBase() {
    free(keys_);
    free(values_);
  }

 protected:
  uint32_t ScaleHash(const Key &key) const {
    double bucket = (double(hasher_(key)) * double(capacity_) /
                     double((uint32_t)(-1)));
    return (uint32_t)bucket % capacity_;
  }

  void InitMemory() {
    keys_ = static_cast<Key *>(smalloc(capacity_ * sizeof(Key)));
    values_ = static_cast<Value *>(smalloc(capacity_ * sizeof(Value)));
    bytes_allocated_ = (sizeof(Key) + sizeof(Value)) * capacity_;
  }

  // Returns true iff the key is overwritten
  bool DoInsert(const Key &key, const Value &value,
                const bool count_collisions)
  {
    uint32_t bucket;
    uint32_t collisions;
    const bool overwritten = DoLookup(key, &bucket, &collisions);
    if (count_collisions) {
      num_collisions_ += collisions;
      max_collisions_ = std::max(collisions, max_collisions_);
    }
    keys_[bucket] = key;
    values_[bucket] = value;
    return overwritten;
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

  void DoClear(const bool reset_capacity) {
    if (reset_capacity)
      static_cast<Derived *>(this)->ResetCapacity();  // No-op if fixed-size
    for (uint32_t i = 0; i < capacity_; ++i)
      keys_[i] = empty_key_;
    size_ = 0;
  }

  // Methods for resizable version
  void SetThresholds() { }
  void Grow() { }
  void Shrink() { }
  void ResetCapacity() { }

  // Separate key and value arrays for better locality
  Key *keys_;
  Value *values_;
  uint32_t capacity_;
  uint32_t initial_capacity_;
  uint32_t size_;
  uint32_t (*hasher_)(const Key &key);
  uint64_t bytes_allocated_;
  uint64_t num_collisions_;
  uint32_t max_collisions_;  /**< maximum collisions for a single insert */
  Key empty_key_;
};


template<class Key, class Value>
class SmallHashFixed :
  public SmallHashBase< Key, Value, SmallHashFixed<Key, Value> >
{
  friend class SmallHashBase< Key, Value, SmallHashFixed<Key, Value> >;
 protected:
  // No-ops
  void SetThresholds() { }
  void Grow() { }
  void Shrink() { }
  void ResetCapacity() { }
};


template<class Key, class Value>
class SmallHashDynamic :
  public SmallHashBase< Key, Value, SmallHashDynamic<Key, Value> >
{
  friend class SmallHashBase< Key, Value, SmallHashDynamic<Key, Value> >;
 public:
  typedef SmallHashBase< Key, Value, SmallHashDynamic<Key, Value> > Base;
  static const double kThresholdGrow;
  static const double kThresholdShrink;

  SmallHashDynamic() : Base() {
    num_migrates_ = 0;
  }

  uint32_t capacity() const { return Base::capacity_; }
  uint32_t size() const { return Base::size_; }
  uint32_t num_migrates() const { return num_migrates_; }

 protected:
  void SetThresholds() {
    threshold_grow_ =
      static_cast<uint32_t>(static_cast<double>(capacity()) * kThresholdGrow);
    threshold_shrink_ =
      static_cast<uint32_t>(static_cast<double>(capacity()) * kThresholdShrink);
  }

  void Grow() {
    if (size() > threshold_grow_)
      Migrate(capacity() * 2);
  }

  void Shrink() {
    if (size() < threshold_shrink_) {
      uint32_t target_capacity = capacity() / 2;
      if (target_capacity >= Base::initial_capacity_)
        Migrate(target_capacity);
    }
  }

  void ResetCapacity() {
    free(Base::keys_);
    free(Base::values_);
    Base::capacity_ = Base::initial_capacity_;
    Base::InitMemory();
    SetThresholds();
  }

 private:
  void Migrate(const uint32_t new_capacity) {
    Key *old_keys = Base::keys_;
    Value *old_values = Base::values_;
    uint32_t old_capacity = capacity();
    uint32_t old_size = size();

    Base::capacity_ = new_capacity;
    SetThresholds();
    Base::InitMemory();
    Base::DoClear(false);
    for (uint32_t i = 0; i < old_capacity; ++i) {
      if (old_keys[i] != Base::empty_key_)
        Base::Insert(old_keys[i], old_values[i]);
    }
    assert(size() == old_size);

    free(old_keys);
    free(old_values);
    num_migrates_++;
  }

  uint32_t num_migrates_;
  uint32_t threshold_grow_;
  uint32_t threshold_shrink_;
};


/**
 * Distributes the key-value pairs over $n$ dynamic hash maps with individual
 * mutexes.  Hence low mutex contention, and benefits from multiple processors.
 */
template<class Key, class Value>
class MultiHash {

};


// initialize the static fields
template<class Key, class Value, class Derived>
const double SmallHashBase<Key, Value, Derived>::kLoadFactor = 0.7;
template<class Key, class Value>
const double SmallHashDynamic<Key, Value>::kThresholdGrow = 0.7;
template<class Key, class Value>
const double SmallHashDynamic<Key, Value>::kThresholdShrink = 0.15;

#endif  // CVMFS_SMALLHASH_H_
