/**
 * This file is part of the CernVM File System.
 *
 * This class provides an Least Recently Used (LRU) cache for arbitrary data
 * It stores Key-Value pairs of arbitrary data types in a hash table and
 * automatically deletes the entries which are least touched in the last time
 * to prevent the structure from growing beyond a given maximal cache size.
 * The cache uses a hand crafted memory allocator to use memory efficiently
 *
 * Hash functions have to be provided.  They should return an equal distribution
 * of keys in uint32_t.  In addition, a special key has to be provided that is
 * used to mark "empty" elements in the hash table.
 *
 * The cache size has to be a multiply of 64.
 *
 * usage:
 *   // 100 entries, -1 special key
 *   LruCache<int, string> cache(100, -1, hasher_int);
 *
 *   // Inserting some stuff
 *   cache.insert(42, "fourtytwo");
 *   cache.insert(2, "small prime number");
 *   cache.insert(1337, "leet");
 *
 *   // Trying to retrieve a value
 *   int result;
 *   if (cache.lookup(21, result)) {
 *      cout << "cache hit: " << result << endl;
 *   } else {
 *      cout << "cache miss" << endl;
 *   }
 *
 *   cache.drop();  // Empty the cache
 */

#ifndef CVMFS_LRU_H_
#define CVMFS_LRU_H_

#define FUSE_USE_VERSION 26
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

// If defined the cache is secured by a posix mutex
#define LRU_CACHE_THREAD_SAFE

#include <inttypes.h>
#include <stdint.h>

#include <cstring>
#include <cassert>

#include <map>
#include <algorithm>
#include <functional>
#include <string>

#include <fuse/fuse_lowlevel.h>

#include "platform.h"
#include "murmur.h"
#include "logging.h"
#include "smalloc.h"
#include "directory_entry.h"
#include "hash.h"
#include "atomic.h"
#include "util.h"
#include "shortstring.h"
#include "smallhash.h"

namespace lru {

/**
 * Counting of cache operations.
 */
struct Statistics {
  int64_t size;
  atomic_int64 num_hit;
  atomic_int64 num_miss;
  atomic_int64 num_insert;
  atomic_int64 num_insert_negative;
  uint64_t num_collisions;
  uint32_t max_collisions;
  atomic_int64 num_update;
  atomic_int64 num_replace;
  atomic_int64 num_forget;
  atomic_int64 num_drop;
  atomic_int64 allocated;

  Statistics() {
    size = 0;
    num_collisions = 0;
    max_collisions = 0;
    atomic_init64(&num_hit);
    atomic_init64(&num_miss);
    atomic_init64(&num_insert);
    atomic_init64(&num_insert_negative);
    atomic_init64(&num_update);
    atomic_init64(&num_replace);
    atomic_init64(&num_forget);
    atomic_init64(&num_drop);
    atomic_init64(&allocated);
  }

  std::string Print() {
    return "size: " + StringifyInt(size) + "  " +
      "hits: " + StringifyInt(atomic_read64(&num_hit)) + "  " +
      "misses: " + StringifyInt(atomic_read64(&num_miss)) + "  " +
      "inserts(all): " + StringifyInt(atomic_read64(&num_insert)) + "  " +
      "inserts(negative): " + StringifyInt(atomic_read64(&num_insert_negative))
        + "  " +
      "collisions: " + StringifyInt(num_collisions) + "  " +
      "collisions(max): " + StringifyInt(max_collisions) + "  " +
      "updates: " + StringifyInt(atomic_read64(&num_update)) + "  " +
      "replacements: " + StringifyInt(atomic_read64(&num_replace)) + "  " +
      "forgets: " + StringifyInt(atomic_read64(&num_forget)) + "  " +
      "drops: " + StringifyInt(atomic_read64(&num_drop)) + "  " +
      "allocated: " + StringifyInt(atomic_read64(&allocated) / 1024) + " KB\n";
  }
};


/**
 * Template class to create a LRU cache
 * @param Key type of the key values
 * @param Value type of the value values
 */
template<class Key, class Value>
class LruCache {
 private:
  // Forward declarations of private internal data structures
  template<class T> class ListEntry;
  template<class T> class ListEntryHead;
  template<class T> class ListEntryContent;
  template<class M> class MemoryAllocator;

  // Helpers to get the template magic right
  typedef ListEntryContent<Key> ConcreteListEntryContent;
  typedef MemoryAllocator<ConcreteListEntryContent> ConcreteMemoryAllocator;

  /**
   * This structure wraps the user data and relates it to the LRU list entry
   */
  typedef struct {
    ListEntryContent<Key> *list_entry;
    Value value;
  } CacheEntry;

  //static uint64_t GetEntrySize() { return sizeof(Key) + sizeof(Value); }

  // Internal data fields
  unsigned int cache_gauge_;
  unsigned int cache_size_;
  static ConcreteMemoryAllocator *allocator_;

  /**
   * A doubly linked list to keep track of the least recently used data entries.
   * New entries get pushed back to the list. If an entry is touched
   * it is moved to the back of the list again.
   * If the cache gets too long, the first element (the oldest) gets
   * deleted to obtain some space.
   */
  ListEntryHead<Key> *lru_list_;
  SmallHashFixed<Key, CacheEntry> cache_;
#ifdef LRU_CACHE_THREAD_SAFE
  pthread_mutex_t lock_;  /**< Mutex to make cache thread safe. */
#endif

  /**
   * A special purpose memory allocator for the cache entries.
   * It allocates enough memory for the maximal number of cache entries at
   * startup, and assigns new ListEntryContent objects to a free spot in this
   * memory pool (by overriding the 'new' and 'delete' operators of
   * ListEntryContent).
   *
   * @param T the type of object to be allocated by this MemoryAllocator
   */
  template<class T> class MemoryAllocator {
   public:
    /**
     * Creates a MemoryAllocator to handle a memory pool for objects of type T
     * @param num_slots the number of slots to be allocated for the given datatype T
     */
    MemoryAllocator(const unsigned int num_slots) {
      // how many bitmap chunks (chars) do we need?
      unsigned int num_bytes_bitmap = num_slots / 8;
      bits_per_block_ = 8 * sizeof(bitmap_[0]);
      assert((num_slots % bits_per_block_) == 0);
      assert(num_slots >= 2*bits_per_block_);

      // How much actual memory do we need?
      const unsigned int num_bytes_memory = sizeof(T) * num_slots;

      // Allocate zero'd memory
      bitmap_ = reinterpret_cast<uint64_t *>(scalloc(num_bytes_bitmap, 1));
      memory_ = reinterpret_cast<T *>(scalloc(num_bytes_memory, 1));

      // Create initial state
      num_slots_ = num_slots;
      num_free_slots_ = num_slots;
      next_free_slot_ = 0;
      bytes_allocated_ = num_bytes_bitmap + num_bytes_memory;
    }

    /**
     * Number of bytes for a single entry
     */
    static double GetEntrySize() {
      return static_cast<double>(sizeof(T)) + 1.0/8.0;
    }

    /**
     * The memory allocator also frees all allocated data
     */
    virtual ~MemoryAllocator() {
      free(bitmap_);
      free(memory_);
    }

    /**
     * Check if the memory pool is full.
     * @return true if all slots are occupied, otherwise false
     */
    inline bool IsFull() const { return num_free_slots_ == 0; }

    /**
     * Allocate a slot and returns a pointer to the memory.
     * @return a pointer to a chunk of the memory pool
     */
    T* Allocate() {
      if (this->IsFull())
        return NULL;

      // Allocate a slot
      this->SetBit(next_free_slot_);
      --num_free_slots_;
      T *slot = memory_ + next_free_slot_;

      // Find a new free slot if there are some left
      if (!this->IsFull()) {
        unsigned bitmap_block = next_free_slot_ / bits_per_block_;
        while (~bitmap_[bitmap_block] == 0)
          bitmap_block = (bitmap_block + 1) % (num_slots_ / bits_per_block_);
        // TODO: faster search inside the int
        next_free_slot_ = bitmap_block * bits_per_block_;
        while (this->GetBit(next_free_slot_))
          next_free_slot_++;
      }

      return slot;
    }

    /**
     * Free a given slot in the memory pool
     * @param slot a pointer to the slot be freed
     */
    void Deallocate(T* slot) {
      // Check if given slot is in bounds
      assert((slot >= memory_) && (slot <= memory_ + num_slots_));

      // Get position of slot
      const unsigned int position = slot - memory_;

      // Check if slot was already freed
      assert(this->GetBit(position));

      // Free slot, save the position of this slot as free (faster reallocation)
      this->UnsetBit(position);
      next_free_slot_ = position;
      ++num_free_slots_;
    }

    uint64_t bytes_allocated() { return bytes_allocated_; }

   private:
    /**
     * Check a bit in the internal allocation bitmap.
     * @param position the position to check
     * @return true if bit is set, otherwise false
     */
    inline bool GetBit(const unsigned position) {
      assert(position < num_slots_);
      return ((bitmap_[position / bits_per_block_] &
               (uint64_t(1) << (position % bits_per_block_))) != 0);
    }

    /**
     *  set a bit in the internal allocation bitmap
     *  @param position the number of the bit to be set
     */
    inline void SetBit(const unsigned position) {
      assert(position < num_slots_);
      bitmap_[position / bits_per_block_] |=
        uint64_t(1) << (position % bits_per_block_);
    }

    /**
     * Clear a bit in the internal allocation bitmap
     * @param position the number of the bit to be cleared
     */
    inline void UnsetBit(const unsigned position) {
      assert(position < num_slots_);
      bitmap_[position / bits_per_block_] &=
        ~(uint64_t(1) << (position % bits_per_block_));
    }

    unsigned int num_slots_;  /**< Overall number of slots in memory pool. */
    unsigned int num_free_slots_;  /**< Current number of free slots left. */
    unsigned int next_free_slot_;  /**< Position of next free slot in pool. */
    uint64_t bytes_allocated_;
    uint64_t *bitmap_;  /**< A bitmap to mark slots as allocated. */
    unsigned bits_per_block_;
    T *memory_;  /**< The memory pool, array of Ms. */
  };


  /**
   * Internal LRU list entry, to maintain the doubly linked list.
   * The list keeps track of the least recently used keys in the cache.
   */
  template<class T> class ListEntry {
   public:
    /**
     * Create a new list entry as lonely, both next and prev pointing to this.
     */
    ListEntry() {
      this->next = this;
      this->prev = this;
    }
    virtual ~ListEntry() {}

    /**
     * Checks if the ListEntry is the list head
     * @return true if ListEntry is list head otherwise false
     */
    virtual bool IsListHead() const = 0;

    /**
     * A lonely ListEntry has no connection to other elements.
     * @return true if ListEntry is lonely otherwise false
     */
    bool IsLonely() const { return (this->next == this && this->prev == this); }

    ListEntry<T> *next;  /**< Pointer to next element in the list. */
    ListEntry<T> *prev;  /**< Pointer to previous element in the list. */

   protected:
    /**
     * Insert a given ListEntry after this one.
     * @param entry the ListEntry to insert after this one
     */
    inline void InsertAsSuccessor(ListEntryContent<T> *entry) {
      assert(entry->IsLonely());

      // Mount the new element between this and this->next
      entry->next = this->next;
      entry->prev = this;

      // Fix pointers of existing list elements
      this->next->prev = entry;
      this->next = entry;
      assert(!entry->IsLonely());
    }

    /**
     * Insert a given ListEntry in front of this one
     * @param entry the ListEntry to insert in front of this one
     */
    inline void InsertAsPredecessor(ListEntryContent<T> *entry) {
      assert(entry->IsLonely());
      assert(!entry->IsListHead());

      // Mount the new element between this and this->prev
      entry->next = this;
      entry->prev = this->prev;

      // Fix pointers of existing list elements
      this->prev->next = entry;
      this->prev = entry;

      assert(!entry->IsLonely());
    }

    /**
     * Remove this element from it's list.
     * The function connects this->next with this->prev leaving the list
     * in a consistent state.  The ListEntry itself is lonely afterwards,
     * but not deleted.
     */
    virtual void RemoveFromList() = 0;
  };

  /**
   * Specialized ListEntry to contain a data entry of type T
   */
  template<class T> class ListEntryContent : public ListEntry<T> {
   public:
    ListEntryContent(Key content) {
      content_ = content;
    };

    /**
     * Overwritten the new operator of this class to redirect it to our own
     * memory allocator.  This ensures that heap is not fragmented by loads of
     * malloc and free calls.
     */
    static void* operator new(size_t size) {
      assert(LruCache::allocator_ != NULL);
      return (void *)LruCache::allocator_->Allocate();
    }

    /**
     * Overwritten delete operator to redirect deallocation to our own memory
     * allocator.
     */
    static void operator delete (void *p) {
      assert(LruCache::allocator_ != NULL);
      LruCache::allocator_->Deallocate(static_cast<ListEntryContent<T> *>(p));
    }

    inline bool IsListHead() const { return false; }
    inline T content() const { return content_; }

    /**
     * See ListEntry base class.
     */
    inline void RemoveFromList() {
      assert (!this->IsLonely());

      // Remove this from list
      this->prev->next = this->next;
      this->next->prev = this->prev;

      // Make this lonely
      this->next = this;
      this->prev = this;
    }
   private:
    T content_;  /**< The data content of this ListEntry */
  };

  /**
   * Specialized ListEntry to form a list head.
   * Every list has exactly one list head which is also the entry point
   * in the list. It is used to manipulate the list.
   */
  template<class T> class ListEntryHead : public ListEntry<T> {
   public:
    virtual ~ListEntryHead() {
      this->clear();
    }

    /**
     * Remove all entries from the list.
     * ListEntry objects are deleted but contained data keeps available
     */
    void clear() {
      // Delete all list entries
      ListEntry<T> *entry = this->next;
      ListEntry<T> *delete_me;
      while (!entry->IsListHead()) {
        delete_me = entry;
        entry = entry->next;
        delete delete_me;
      }

      // Reset the list to lonely
      this->next = this;
      this->prev = this;
    }

    inline bool IsListHead() const { return true; }
    inline bool IsEmpty() const { return this->IsLonely(); }

    /**
     * Push a new data object to the end of the list.
     * @param the data object to insert
     * @return the ListEntryContent structure wrapped around the data object
     */
    inline ListEntryContent<T>* PushBack(T content) {
      ListEntryContent<T> *new_entry = new ListEntryContent<T>(content);
      this->InsertAsPredecessor(new_entry);
      return new_entry;
    }

    /**
     * Pop the first object of the list.
     * The object is returned and removed from the list
     * @return the data object which resided in the first list entry
     */
    inline T PopFront() {
      assert (!this->IsEmpty());
      return Pop(this->next);
    }

    /**
     * Take a list entry out of it's list and reinsert at the end of this list.
     * @param the ListEntry to be moved to the end of this list
     */
    inline void MoveToBack(ListEntryContent<T> *entry) {
      assert(!entry->IsLonely());

      entry->RemoveFromList();
      this->InsertAsPredecessor(entry);
    }

    /**
     * See ListEntry base class
     */
    inline void RemoveFromList() { assert(false); }

   private:
    /**
     * Pop a ListEntry from the list (arbitrary position).
     * The given ListEntry is removed from the list, deleted and it's
     * data content is returned
     * @param popped_entry the entry to be popped
     * @return the data object of the popped ListEntry
     */
    inline T Pop(ListEntry<T> *popped_entry) {
      assert(!popped_entry->IsListHead());

      ListEntryContent<T> *popped = (ListEntryContent<T> *)popped_entry;
      popped->RemoveFromList();
      T result = popped->content();
      delete popped_entry;
      return result;
    }
  };

 public:  // LruCache
  /**
   * Create a new LRU cache object
   * @param cache_size the maximal size of the cache
   */
  LruCache(const unsigned cache_size, const Key &empty_key,
           uint32_t (*hasher)(const Key &key))
  {
    assert(cache_size > 0);

    LruCache<Key, Value>::allocator_ = new ConcreteMemoryAllocator(cache_size);

    cache_gauge_ = 0;
    cache_size_ = cache_size;
    statistics_.size = cache_size_;
    //cache_ = Cache(cache_size_);
    cache_.Init(cache_size_, empty_key, hasher);
    atomic_xadd64(&statistics_.allocated, allocator_->bytes_allocated() +
                  cache_.bytes_allocated());
    lru_list_ = new ListEntryHead<Key>();
    pause_ = false;

#ifdef LRU_CACHE_THREAD_SAFE
    int retval = pthread_mutex_init(&lock_, NULL);
    assert(retval == 0);
#endif
  }

  static double GetEntrySize() {
    return SmallHashFixed<Key, CacheEntry>::GetEntrySize() +
           ConcreteMemoryAllocator::GetEntrySize();
  }

  virtual ~LruCache() {
    delete lru_list_;
    delete LruCache<Key, Value>::allocator_;
#ifdef LRU_CACHE_THREAD_SAFE
    pthread_mutex_destroy(&lock_);
#endif
  }

  /**
   * Insert a new key-value pair to the list.
   * If the cache is already full, the least recently used object is removed;
   * afterwards the new object is inserted.
   * If the object is already present it is updated and moved back to the end
   * of the list
   * @param key the key where the value is saved
   * @param value the value of the cache entry
   * @return true on insert, false on update
   */
  virtual bool Insert(const Key &key, const Value &value) {
    this->Lock();
    if (pause_) {
      Unlock();
      return false;
    }

    CacheEntry entry;

    // Check if we have to update an existent entry
    if (this->DoLookup(key, entry)) {
      atomic_inc64(&statistics_.num_update);
      entry.value = value;
      cache_.Insert(key, entry);
      this->Touch(entry);
      this->Unlock();
      return false;
    }

    atomic_inc64(&statistics_.num_insert);
    // Check if we have to make some space in the cache a
    if (this->IsFull())
      this->DeleteOldest();

    entry.list_entry = lru_list_->PushBack(key);
    entry.value = value;

    cache_.Insert(key, entry);
    cache_gauge_++;

    Unlock();
    return true;
  }

  /**
   * Retrieve an element from the cache.
   * If the element was found, it will be marked as 'recently used' and returned
   * @param key the key to perform a lookup on
   * @param value (out) here the result is saved (not touch in case of miss)
   * @return true on successful lookup, false if key was not found
   */
  virtual bool Lookup(const Key &key, Value *value) {
    bool found = false;
    Lock();
    if (pause_) {
      Unlock();
      return false;
    }

    CacheEntry entry;
    if (DoLookup(key, entry)) {
      // Hit
      atomic_inc64(&statistics_.num_hit);
      Touch(entry);
      *value = entry.value;
      found = true;
    } else {
      atomic_inc64(&statistics_.num_miss);
    }

    Unlock();
    return found;
  }

  /**
   * Forgets about a specific cache entry
   * @param key the key to delete from the cache
   * @return true if key was deleted, false if key was not in the cache
   */
  virtual bool Forget(const Key &key) {
    bool found = false;
    this->Lock();
    if (pause_) {
      Unlock();
      return false;
    }

    CacheEntry entry;
    if (this->DoLookup(key, entry)) {
      found = true;
      atomic_inc64(&statistics_.num_forget);

      entry.list_entry->RemoveFromList();
      delete entry.list_entry;
      cache_.Erase(key);
      --cache_gauge_;
    }

    this->Unlock();
    return found;
  }

  /**
   * Clears all elements from the cache.
   * All memory of internal data structures will be freed but data of
   * cache entries may stay in use, we do not call delete on any user data.
   */
  virtual void Drop() {
    this->Lock();

    cache_gauge_ = 0;
    lru_list_->clear();
    cache_.Clear();
    atomic_inc64(&statistics_.num_drop);
    atomic_init64(&statistics_.allocated);
    atomic_xadd64(&statistics_.allocated, allocator_->bytes_allocated() +
                  cache_.bytes_allocated());

    this->Unlock();
  }

  void Pause() {
    Lock();
    pause_ = true;
    Unlock();
  }

  void Resume() {
    Lock();
    pause_ = false;
    Unlock();
  }

  inline bool IsFull() const { return cache_gauge_ >= cache_size_; }
  inline bool IsEmpty() const { return cache_gauge_ == 0; }

  Statistics statistics() {
    Lock();
    cache_.GetCollisionStats(&statistics_.num_collisions,
                             &statistics_.max_collisions);
    Unlock();
    return statistics_;
  }

 protected:
  Statistics statistics_;

 private:
  /**
   *  this just performs a lookup in the cache
   *  WITHOUT changing the LRU order
   *  @param key the key to perform a lookup on
   *  @param entry a pointer to the entry structure
   *  @return true on successful lookup, false otherwise
   */
  inline bool DoLookup(const Key &key, CacheEntry &entry) {
    return cache_.Lookup(key, &entry);
  }

  /**
   * Touch an entry.
   * The entry will be moved to the back of the LRU list to mark it
   * as 'recently used'... this saves the entry from being deleted
   * @param entry the CacheEntry to be touched (CacheEntry is the internal wrapper data structure)
   */
  inline void Touch(const CacheEntry &entry) {
    lru_list_->MoveToBack(entry.list_entry);
  }

  /**
   * Deletes the least recently used entry from the cache.
   */
  inline void DeleteOldest() {
    assert(!this->IsEmpty());

    atomic_inc64(&statistics_.num_replace);
    Key delete_me = lru_list_->PopFront();
    cache_.Erase(delete_me);

    --cache_gauge_;
  }

  /**
   * Locks the cache (thread safety).
   */
  inline void Lock() {
#ifdef LRU_CACHE_THREAD_SAFE
    pthread_mutex_lock(&lock_);
#endif
  }

  /**
   * Unlocks the cache (thread safety).
   */
  inline void Unlock() {
#ifdef LRU_CACHE_THREAD_SAFE
    pthread_mutex_unlock(&lock_);
#endif
  }

  bool pause_;  /**< Temporarily stops the cache in order to avoid poisoning */
};  // class LruCache

// initialize the static allocator field
template<class Key, class Value>
typename LruCache<Key, Value>::ConcreteMemoryAllocator
  *LruCache<Key, Value>::allocator_ = NULL;

// Hash functions
static inline uint32_t hasher_md5(const hash::Md5 &key) {
  // Don't start with the first bytes, because == is using them as well
  return (uint32_t) *((uint32_t *)key.digest + 1);
}

static inline uint32_t hasher_inode(const fuse_ino_t &inode) {
  return MurmurHash2(&inode, sizeof(inode), 0x07387a4f);
}
//uint32_t hasher_md5(const hash::Md5 &key);
//uint32_t hasher_inode(const fuse_ino_t &inode);


class InodeCache : public LruCache<fuse_ino_t, catalog::DirectoryEntry>
{
 public:
  InodeCache(unsigned int cache_size) :
    LruCache<fuse_ino_t, catalog::DirectoryEntry>(
      cache_size, fuse_ino_t(-1), hasher_inode)
  {
  }

  bool Insert(const fuse_ino_t &inode, const catalog::DirectoryEntry &dirent) {
    LogCvmfs(kLogLru, kLogDebug, "insert inode --> dirent: %u -> '%s'",
             inode, dirent.name().c_str());
    const bool result =
      LruCache<fuse_ino_t, catalog::DirectoryEntry>::Insert(inode, dirent);
    return result;
  }

  bool Lookup(const fuse_ino_t &inode, catalog::DirectoryEntry *dirent) {
    const bool result =
      LruCache<fuse_ino_t, catalog::DirectoryEntry>::Lookup(inode, dirent);
    LogCvmfs(kLogLru, kLogDebug, "lookup inode --> dirent: %u (%s)",
             inode, result ? "hit" : "miss");
    return result;
  }

  void Drop() {
    LogCvmfs(kLogLru, kLogDebug, "dropping inode cache");
    LruCache<fuse_ino_t, catalog::DirectoryEntry>::Drop();
  }
};  // InodeCache


class PathCache : public LruCache<fuse_ino_t, PathString> {
 public:
  PathCache(unsigned int cache_size) :
    LruCache<fuse_ino_t, PathString>(cache_size, fuse_ino_t(-1), hasher_inode)
  {
  }

  bool Insert(const fuse_ino_t &inode, const PathString &path) {
    LogCvmfs(kLogLru, kLogDebug, "insert inode --> path %u -> '%s'",
             inode, path.c_str());
    const bool result =
      LruCache<fuse_ino_t, PathString>::Insert(inode, path);
    return result;
  }

  bool Lookup(const fuse_ino_t &inode, PathString *path) {
    const bool found =
      LruCache<fuse_ino_t, PathString>::Lookup(inode, path);
    LogCvmfs(kLogLru, kLogDebug, "lookup inode --> path: %u (%s)",
             inode, found ? "hit" : "miss");
    return found;
  }

  void Drop() {
    LogCvmfs(kLogLru, kLogDebug, "dropping path cache");
    LruCache<fuse_ino_t, PathString>::Drop();
  }
};  // PathCache


class Md5PathCache :
  public LruCache<hash::Md5, catalog::DirectoryEntry>
{
 public:
  Md5PathCache(unsigned int cache_size) :
    LruCache<hash::Md5, catalog::DirectoryEntry>(
      cache_size, hash::Md5(hash::AsciiPtr("!")), hasher_md5)
  {
    dirent_negative_ = catalog::DirectoryEntry(catalog::kDirentNegative);
  }

  bool Insert(const hash::Md5 &hash, const catalog::DirectoryEntry &dirent) {
    LogCvmfs(kLogLru, kLogDebug, "insert md5 --> dirent: %s -> '%s'",
             hash.ToString().c_str(), dirent.name().c_str());
    const bool result =
      LruCache<hash::Md5, catalog::DirectoryEntry>::Insert(hash, dirent);
    return result;
  }

  bool InsertNegative(const hash::Md5 &hash) {
    const bool result = Insert(hash, dirent_negative_);
    if (result)
      atomic_inc64(&statistics_.num_insert_negative);
    return result;
  }

  bool Lookup(const hash::Md5 &hash, catalog::DirectoryEntry *dirent) {
    const bool result =
      LruCache<hash::Md5, catalog::DirectoryEntry>::Lookup(hash, dirent);
    LogCvmfs(kLogLru, kLogDebug, "lookup md5 --> dirent: %s (%s)",
             hash.ToString().c_str(), result ? "hit" : "miss");
    return result;
  }

  bool Forget(const hash::Md5 &hash) {
    LogCvmfs(kLogLru, kLogDebug, "forget md5: %s",
             hash.ToString().c_str());
    return LruCache<hash::Md5, catalog::DirectoryEntry>::Forget(hash);
  }

  void Drop() {
    LogCvmfs(kLogLru, kLogDebug, "dropping md5path cache");
    LruCache<hash::Md5, catalog::DirectoryEntry>::Drop();
  }

 private:
  catalog::DirectoryEntry dirent_negative_;
};  // Md5PathCache

}  // namespace lru

#endif  // CVMFS_LRU_H_
