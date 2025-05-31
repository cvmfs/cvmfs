/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SMALLHASH_H_
#define CVMFS_SMALLHASH_H_

#ifndef __STDC_FORMAT_MACROS
// NOLINTNEXTLINE
#define __STDC_FORMAT_MACROS
#endif

#include <gtest/gtest_prod.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <new>

#include "util/atomic.h"
#include "util/murmur.hxx"
#include "util/prng.h"
#include "util/smalloc.h"

/**
 * Hash table with linear probing as collision resolution.  Works only for
 * a fixed (maximum) number of elements, i.e. no resizing.  Load factor fixed
 * to 0.7.
 */
template<class Key, class Value, class Derived>
class SmallHashBase {
  FRIEND_TEST(T_Smallhash, InsertAndCopyMd5Slow);

 public:
  static const double kLoadFactor;     // mainly useless for the dynamic version
  static const double kThresholdGrow;  // only used for resizable version
  static const double kThresholdShrink;  // only used for resizable version

  SmallHashBase() {
    keys_ = NULL;
    values_ = NULL;
    hasher_ = NULL;
    bytes_allocated_ = 0;
    num_collisions_ = 0;
    max_collisions_ = 0;

    // Properly initialized by Init()
    capacity_ = 0;
    initial_capacity_ = 0;
    size_ = 0;
  }

  ~SmallHashBase() { DeallocMemory(keys_, values_, capacity_); }

  void Init(uint32_t expected_size, Key empty,
            uint32_t (*hasher)(const Key &key)) {
    hasher_ = hasher;
    empty_key_ = empty;
    capacity_ = static_cast<uint32_t>(static_cast<double>(expected_size)
                                      / kLoadFactor);
    initial_capacity_ = capacity_;
    static_cast<Derived *>(this)->SetThresholds();  // No-op for fixed size
    AllocMemory();
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

  /**
   * Returns both the key and the value. That is useful if Key's equality
   * operator implements an equivalence relation on Key. In this case, LookupEx
   * returns the key representing the equivalence class that has been used
   * during Insert().
   * Used to return a glue::InodeEx element when looking for an inode.
   */
  bool LookupEx(Key *key, Value *value) const {
    uint32_t bucket = ScaleHash(*key);
    while (!(keys_[bucket] == empty_key_)) {
      if (keys_[bucket] == *key) {
        *key = keys_[bucket];
        *value = values_[bucket];
        return true;
      }
      bucket = (bucket + 1) % capacity_;
    }
    return false;
  }

  bool Contains(const Key &key) const {
    uint32_t bucket;
    uint32_t collisions;
    const bool found = DoLookup(key, &bucket, &collisions);
    return found;
  }

  void Insert(const Key &key, const Value &value) {
    static_cast<Derived *>(this)->Grow();  // No-op if fixed-size
    const bool overwritten = DoInsert(key, value, true);
    size_ += !overwritten;  // size + 1 if the key was not yet in the map
  }

  bool Erase(const Key &key) {
    uint32_t bucket;
    uint32_t collisions;
    const bool found = DoLookup(key, &bucket, &collisions);
    if (found) {
      keys_[bucket] = empty_key_;
      size_--;
      bucket = (bucket + 1) % capacity_;
      while (!(keys_[bucket] == empty_key_)) {
        Key rehash = keys_[bucket];
        keys_[bucket] = empty_key_;
        DoInsert(rehash, values_[bucket], false);
        bucket = (bucket + 1) % capacity_;
      }
      static_cast<Derived *>(this)->Shrink();  // No-op if fixed-size
    }
    return found;
  }

  void Clear() { DoClear(true); }

  uint64_t bytes_allocated() const { return bytes_allocated_; }
  static double GetEntrySize() {
    const double unit = sizeof(Key) + sizeof(Value);
    return unit / kLoadFactor;
  }

  void GetCollisionStats(uint64_t *num_collisions,
                         uint32_t *max_collisions) const {
    *num_collisions = num_collisions_;
    *max_collisions = max_collisions_;
  }

  // Careful with the direct access TODO: iterator
  uint32_t capacity() const { return capacity_; }
  Key empty_key() const { return empty_key_; }
  Key *keys() const { return keys_; }
  Value *values() const { return values_; }

  // Only needed by compat
  void SetHasher(uint32_t (*hasher)(const Key &key)) { hasher_ = hasher; }

 protected:
  uint32_t ScaleHash(const Key &key) const {
    const double bucket =
        (static_cast<double>(hasher_(key)) * static_cast<double>(capacity_) /
         static_cast<double>(static_cast<uint32_t>(-1)));
    return static_cast<uint32_t>(bucket) % capacity_;
  }

  void AllocMemory() {
    keys_ = static_cast<Key *>(smmap(capacity_ * sizeof(Key)));
    values_ = static_cast<Value *>(smmap(capacity_ * sizeof(Value)));
    for (uint32_t i = 0; i < capacity_; ++i) {
      /*keys_[i] =*/new (keys_ + i) Key();
    }
    for (uint32_t i = 0; i < capacity_; ++i) {
      /*values_[i] =*/new (values_ + i) Value();
    }
    bytes_allocated_ = (sizeof(Key) + sizeof(Value)) * capacity_;
  }

  void DeallocMemory(Key *k, Value *v, uint32_t c) {
    for (uint32_t i = 0; i < c; ++i) {
      k[i].~Key();
    }
    for (uint32_t i = 0; i < c; ++i) {
      v[i].~Value();
    }
    if (k)
      smunmap(k);
    if (v)
      smunmap(v);
    k = NULL;
    v = NULL;
  }

  // Returns true iff the key is overwritten
  bool DoInsert(const Key &key, const Value &value,
                const bool count_collisions) {
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
      *bucket = (*bucket + 1) % capacity_;
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
  uint32_t max_collisions_; /**< maximum collisions for a single insert */
  Key empty_key_;
};


template<class Key, class Value>
class SmallHashFixed
    : public SmallHashBase<Key, Value, SmallHashFixed<Key, Value> > {
  friend class SmallHashBase<Key, Value, SmallHashFixed<Key, Value> >;

 protected:
  // No-ops
  void SetThresholds() { }
  void Grow() { }
  void Shrink() { }
  void ResetCapacity() { }
};


template<class Key, class Value>
class SmallHashDynamic
    : public SmallHashBase<Key, Value, SmallHashDynamic<Key, Value> > {
  friend class SmallHashBase<Key, Value, SmallHashDynamic<Key, Value> >;

 public:
  typedef SmallHashBase<Key, Value, SmallHashDynamic<Key, Value> > Base;
  static const double kThresholdGrow;
  static const double kThresholdShrink;

  SmallHashDynamic() : Base() {
    num_migrates_ = 0;

    // Properly set by Init
    threshold_grow_ = 0;
    threshold_shrink_ = 0;
  }

  SmallHashDynamic(const SmallHashDynamic<Key, Value> &other) : Base() {
    num_migrates_ = 0;
    CopyFrom(other);
  }

  SmallHashDynamic<Key, Value> &operator=(
      const SmallHashDynamic<Key, Value> &other) {
    if (&other == this)
      return *this;

    CopyFrom(other);
    return *this;
  }

  uint32_t capacity() const { return Base::capacity_; }
  uint32_t size() const { return Base::size_; }
  uint32_t num_migrates() const { return num_migrates_; }

 protected:
  void SetThresholds() {
    threshold_grow_ = static_cast<uint32_t>(static_cast<double>(capacity())
                                            * kThresholdGrow);
    threshold_shrink_ = static_cast<uint32_t>(static_cast<double>(capacity())
                                              * kThresholdShrink);
  }

  void Grow() {
    if (size() > threshold_grow_)
      Migrate(capacity() * 2);
  }

  void Shrink() {
    if (size() < threshold_shrink_) {
      const uint32_t target_capacity = capacity() / 2;
      if (target_capacity >= Base::initial_capacity_)
        Migrate(target_capacity);
    }
  }

  void ResetCapacity() {
    Base::DeallocMemory(Base::keys_, Base::values_, Base::capacity_);
    Base::capacity_ = Base::initial_capacity_;
    Base::AllocMemory();
    SetThresholds();
  }

 private:
  // Returns a random permutation of indices [0..N-1] that is allocated
  // by smmap (Knuth's shuffle algorithm)
  uint32_t *ShuffleIndices(const uint32_t N) {
    uint32_t *shuffled = static_cast<uint32_t *>(smmap(N * sizeof(uint32_t)));
    // Init with identity
    for (unsigned i = 0; i < N; ++i)
      shuffled[i] = i;
    // Shuffle (no shuffling for the last element)
    for (unsigned i = 0; i < N - 1; ++i) {
      const uint32_t swap_idx = i + g_prng.Next(N - i);
      const uint32_t tmp = shuffled[i];
      shuffled[i] = shuffled[swap_idx];
      shuffled[swap_idx] = tmp;
    }
    return shuffled;
  }

  void Migrate(const uint32_t new_capacity) {
    Key *old_keys = Base::keys_;
    Value *old_values = Base::values_;
    const uint32_t old_capacity = capacity();
    const uint32_t old_size = size();

    Base::capacity_ = new_capacity;
    SetThresholds();
    Base::AllocMemory();
    Base::DoClear(false);
    if (new_capacity < old_capacity) {
      uint32_t *shuffled_indices = ShuffleIndices(old_capacity);
      for (uint32_t i = 0; i < old_capacity; ++i) {
        if (old_keys[shuffled_indices[i]] != Base::empty_key_) {
          Base::Insert(old_keys[shuffled_indices[i]],
                       old_values[shuffled_indices[i]]);
        }
      }
      smunmap(shuffled_indices);
    } else {
      for (uint32_t i = 0; i < old_capacity; ++i) {
        if (old_keys[i] != Base::empty_key_)
          Base::Insert(old_keys[i], old_values[i]);
      }
    }
    assert(size() == old_size);

    Base::DeallocMemory(old_keys, old_values, old_capacity);
    num_migrates_++;
  }

  void CopyFrom(const SmallHashDynamic<Key, Value> &other) {
    uint32_t *shuffled_indices = ShuffleIndices(other.capacity_);
    for (uint32_t i = 0; i < other.capacity_; ++i) {
      if (other.keys_[shuffled_indices[i]] != other.empty_key_) {
        this->Insert(other.keys_[shuffled_indices[i]],
                     other.values_[shuffled_indices[i]]);
      }
    }
    smunmap(shuffled_indices);
  }

  uint32_t num_migrates_;
  uint32_t threshold_grow_;
  uint32_t threshold_shrink_;
  static Prng g_prng;
};


/**
 * Distributes the key-value pairs over $n$ dynamic hash maps with individual
 * mutexes.  Hence low mutex contention, and benefits from multiple processors.
 */
template<class Key, class Value>
class MultiHash {
 public:
  MultiHash() {
    num_hashmaps_ = 0;
    hashmaps_ = NULL;
    locks_ = NULL;
  }

  void Init(const uint8_t num_hashmaps, const Key &empty_key,
            uint32_t (*hasher)(const Key &key)) {
    assert(num_hashmaps > 0);
    const uint8_t N = num_hashmaps;
    num_hashmaps_ = N;
    hashmaps_ = new SmallHashDynamic<Key, Value>[N]();
    locks_ = static_cast<pthread_mutex_t *>(
        smalloc(N * sizeof(pthread_mutex_t)));
    for (uint8_t i = 0; i < N; ++i) {
      int retval = pthread_mutex_init(&locks_[i], NULL);
      assert(retval == 0);
      hashmaps_[i].Init(128, empty_key, hasher);
    }
  }

  ~MultiHash() {
    for (uint8_t i = 0; i < num_hashmaps_; ++i) {
      pthread_mutex_destroy(&locks_[i]);
    }
    free(locks_);
    delete[] hashmaps_;
  }

  bool Lookup(const Key &key, Value *value) {
    uint8_t target = SelectHashmap(key);
    Lock(target);
    const bool result = hashmaps_[target].Lookup(key, value);
    Unlock(target);
    return result;
  }

  void Insert(const Key &key, const Value &value) {
    uint8_t target = SelectHashmap(key);
    Lock(target);
    hashmaps_[target].Insert(key, value);
    Unlock(target);
  }

  void Erase(const Key &key) {
    uint8_t target = SelectHashmap(key);
    Lock(target);
    hashmaps_[target].Erase(key);
    Unlock(target);
  }

  void Clear() {
    for (uint8_t i = 0; i < num_hashmaps_; ++i)
      Lock(i);
    for (uint8_t i = 0; i < num_hashmaps_; ++i)
      hashmaps_[i].Clear();
    for (uint8_t i = 0; i < num_hashmaps_; ++i)
      Unlock(i);
  }

  uint8_t num_hashmaps() const { return num_hashmaps_; }

  void GetSizes(uint32_t *sizes) {
    for (uint8_t i = 0; i < num_hashmaps_; ++i) {
      Lock(i);
      sizes[i] = hashmaps_[i].size();
      Unlock(i);
    }
  }

  void GetCollisionStats(uint64_t *num_collisions, uint32_t *max_collisions) {
    for (uint8_t i = 0; i < num_hashmaps_; ++i) {
      Lock(i);
      hashmaps_[i].GetCollisionStats(&num_collisions[i], &max_collisions[i]);
      Unlock(i);
    }
  }

 private:
  inline uint8_t SelectHashmap(const Key &key) {
    uint32_t hash = MurmurHash2(&key, sizeof(key), 0x37);
    double bucket = static_cast<double>(hash)
                    * static_cast<double>(num_hashmaps_)
                    / static_cast<double>(static_cast<uint32_t>(-1));
    return static_cast<uint32_t>(bucket) % num_hashmaps_;
  }

  inline void Lock(const uint8_t target) {
    int retval = pthread_mutex_lock(&locks_[target]);
    assert(retval == 0);
  }

  inline void Unlock(const uint8_t target) {
    int retval = pthread_mutex_unlock(&locks_[target]);
    assert(retval == 0);
  }

  uint8_t num_hashmaps_;
  SmallHashDynamic<Key, Value> *hashmaps_;
  pthread_mutex_t *locks_;
};


// initialize the static fields
template<class Key, class Value>
Prng SmallHashDynamic<Key, Value>::g_prng;

template<class Key, class Value, class Derived>
const double SmallHashBase<Key, Value, Derived>::kLoadFactor = 0.75;

template<class Key, class Value>
const double SmallHashDynamic<Key, Value>::kThresholdGrow = 0.75;

template<class Key, class Value>
const double SmallHashDynamic<Key, Value>::kThresholdShrink = 0.25;

#endif  // CVMFS_SMALLHASH_H_
