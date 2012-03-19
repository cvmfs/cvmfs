/**
 * This file is part of the CernVM File System.
 *
 * This class provides an Least Recently Used (LRU) cache for arbitrary data
 * It stores Key-Value pairs of arbitrary data types in a fast hash which automatically
 * deletes the entries which are least touched in the last time to maintain a given
 * maximal cache size.
 * The cache uses a hand crafted memory allocator to use memory efficiently
 * Before you do anything with your new cache, use setSpecialHashTableKeys() !!               <----- IMPORTANT  !!!!!!
 *
 * usage:
 *   LruCache<int, string> cache(100);  // cache mapping ints to strings with maximal size of 100
 *   cache.setSpecialHashTableKeys(999999999,9999999991); // DO NOT FORGET THIS!! WILL CRASH AT INSERT!!
 *
 *   // inserting some stuff
 *   cache.insert(42, "fourtytwo");
 *   cache.insert(2, "small prime number");
 *   cache.insert(1337, "leet");
 *
 *   // trying to retrieve a value
 *   int result;
 *   if (cache.lookup(21, result)) {
 *      cout << "cache hit: " << result << endl;
 *   } else {
 *      cout << "cache miss" << endl;
 *   }
 *
 *   // maintaining the cache
 *   cache.drop();     // empty the cache
 */

#ifndef CVMFS_LRU_H_
#define CVMFS_LRU_H_

// if defined the cache is secured by a posix mutex
#define LRU_CACHE_THREAD_SAFE 1

#include <cstring>
#include <cassert>

#include <map>
#include <algorithm>
#include <functional>
#include <string>

#include <google/dense_hash_map>
#include <fuse/fuse_lowlevel.h>

#include "platform.h"
#include "logging.h"
#include "smalloc.h"
#include "dirent.h"
#include "hash.h"

namespace lru {

/**
 * Template class to create a LRU cache
 * @param Key type of the key values
 * @param Value type of the value values
 */
template<class Key, class Value, class HashFunction = SPARSEHASH_HASH<Key>,
                                 class EqualKey = std::equal_to<Key> >
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

  // The actual map data structure
  typedef google::dense_hash_map<Key, CacheEntry, HashFunction, EqualKey> Cache;

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
  Cache cache_;  /**< The actual cache map. */
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
   * @param BIN the type of object to be allocated by this MemoryAllocator
   */
  template<class T> class MemoryAllocator {
   public:
    /**
     * Creates a MemoryAllocator to handle a memory pool for objects of type T
     * @param num_slots the number of slots to be allocated for the given datatype T
     */
    MemoryAllocator(const unsigned int num_slots) {
      // how many bitmap chunks (chars) do we need?
      unsigned int num_bytes_bitmap = num_slots / sizeof(char);
      if ((num_slots % sizeof(char)) != 0) num_bytes_bitmap++;

      // How much actual memory do we need?
      const unsigned int num_bytes_memory = sizeof(T) * num_slots;

      // Allocate zero'd memory
      bitmap_ = reinterpret_cast<char *>(smalloc(num_bytes_bitmap));
      memory_ = reinterpret_cast<T *>(smalloc(num_bytes_memory));
      memset(bitmap_, 0, num_bytes_bitmap);
      memset(memory_, 0, num_bytes_memory);

      // Create initial state
      num_slots_ = num_slots;
      num_free_slots_ = num_slots;
      next_free_slot_ = 0;
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
        while (this->GetBit(next_free_slot_)) {
          next_free_slot_ = (next_free_slot_ + 1) % num_slots_;
        }
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

   private:
    /**
     * Check a bit in the internal allocation bitmap.
     * @param position the position to check
     * @return true if bit is set, otherwise false
     */
    inline bool GetBit(const unsigned int position) {
      assert(position < num_slots_);
      return ((bitmap_[position / 8] & (1 << (position % 8))) != 0);
    }

    /**
     *  set a bit in the internal allocation bitmap
     *  @param position the number of the bit to be set
     */
    inline void SetBit(const unsigned int position) {
      assert(position < num_slots_);
      bitmap_[position / 8] |= 1 << (position % 8);
    }

    /**
     * Clear a bit in the internal allocation bitmap
     * @param position the number of the bit to be cleared
     */
    inline void UnsetBit(const unsigned int position) {
      assert(position < num_slots_);
      bitmap_[position / 8] &= ~(1 << (position % 8));
    }

    unsigned int num_slots_;  /**< Overall number of slots in memory pool. */
    unsigned int num_free_slots_;  /**< Current number of free slots left. */
    unsigned int next_free_slot_;  /**< Position of next free slot in pool. */
    char *bitmap_;  /**< A bitmap to mark slots as allocated. */
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
  LruCache(const unsigned int cache_size) {
    assert(cache_size > 0);

    LruCache<Key, Value, HashFunction, EqualKey>::allocator_ =
      new ConcreteMemoryAllocator(cache_size);

    cache_gauge_ = 0;
    cache_size_ = cache_size;
    lru_list_ = new ListEntryHead<Key>();

#ifdef LRU_CACHE_THREAD_SAFE
    int retval = pthread_mutex_init(&lock_, NULL);
    assert(retval == 0);
#endif
  }

  virtual ~LruCache() {
    delete lru_list_;
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
   * @return true on successful insertion otherwise false
   */
  virtual bool insert(const Key &key, const Value &value) {
    this->lock();

    // check if we have to update an existent entry
    CacheEntry entry;
    if (this->lookupCache(key, entry)) {
      entry.value = value;
      this->updateExistingEntry(key, entry);
      this->touchEntry(entry);
    } else {
      // check if we have to make some space in the cache
      if (this->IsFull()) {
        this->deleteOldestEntry();
      }

      // insert a new entry
      this->insertNewEntry(key, value);
    }

    this->unlock();
    return true;
  }

  /**
   *  retrieve an element from the cache
   *  if the element was found, it will be marked as 'recently used' and returned
   *  @param key the key to perform a lookup on
   *  @param value (out) here the result is saved (in case of cache miss this is not altered)
   *  @return true on successful lookup, false if key was not found
   */
  virtual bool lookup(const Key &key, Value *value) {
    bool found = false;
    this->lock();

    CacheEntry entry;
    if (this->lookupCache(key, entry)) {
      // cache hit
      this->touchEntry(entry);
      *value = entry.value;
      found = true;
    }

    this->unlock();
    return found;
  }

  /**
   *  forgets about a specific cache entry
   *  @param key the key to delete from the cache
   *  @return true if key was deleted, false if key was not in the cache
   */
  virtual bool forget(const Key &key) {
    bool found = false;
    this->lock();

    CacheEntry entry;
    if (this->lookupCache(key, entry)) {
      found = true;

      entry.list_entry->RemoveFromList();
      delete entry.list_entry;
      cache_.erase(key);
      --cache_gauge_;
    }

    this->unlock();
    return found;
  }

  /**
   *  clears all elements from the cache
   *  all memory of internal data structures will be freed but data of cache entries
   *  may stay in use, we do not call delete on any user data
   */
  virtual void drop() {
    this->lock();

    cache_gauge_ = 0;
    lru_list_->clear();
    cache_.clear();

    this->unlock();
  }

  /**
   *  Google dense hash needs two special Key values to mark empty hash table
   *  buckets and deleted hash table buckets
   */
  void setSpecialHashTableKeys(const Key &empty, const Key &deleted) {
    cache_.set_empty_key(empty);
    cache_.set_deleted_key(deleted);
  }

  inline bool IsFull() const { return cache_gauge_ >= cache_size_; }
  inline bool IsEmpty() const { return cache_gauge_ == 0; }

private:
  /**
   *  this just performs a lookup in the cache
   *  WITHOUT changing the LRU order
   *  @param key the key to perform a lookup on
   *  @param entry a pointer to the entry structure
   *  @return true on successful lookup, false otherwise
   */
  inline bool lookupCache(const Key &key, CacheEntry &entry) {
    typename Cache::iterator foundElement = cache_.find(key);

    if (foundElement == cache_.end()) {
      // cache miss
      return false;
    }

    // cache hit
    entry = foundElement->second;
    return true;
  }

  /**
   *  insert a new entry in the cache
   *  wraps the user data in an internal data structure
   *  @param key the key to save the value in
   *  @param value the user data
   */
  inline void insertNewEntry(const Key &key, const Value &value) {
    assert (not this->IsFull());

    CacheEntry entry;
    entry.list_entry = lru_list_->PushBack(key);
    entry.value = value;

    cache_[key] = entry;
    cache_gauge_++;
  }

  /**
   *  update an entry which is already in the cache
   *  this will not change the LRU order. Just the new data object is
   *  associated with the given key value in the cache
   *  @param key the key to save the value in
   *  @param entry the CacheEntry structure to save in the cache
   */
  inline void updateExistingEntry(const Key &key, const CacheEntry &entry) {
    cache_[key] = entry;
  }

  /**
   *  touch an entry
   *  the entry will be moved to the back of the LRU list to mark it
   *  as 'recently used'... this saves the entry from being deleted
   *  @param entry the CacheEntry to be touched (CacheEntry is the internal wrapper data structure)
   */
  inline void touchEntry(const CacheEntry &entry) {
    lru_list_->MoveToBack(entry.list_entry);
  }

  /**
   *  deletes the least recently used entry from the cache
   */
  inline void deleteOldestEntry() {
    assert (not this->IsEmpty());

    Key keyToDelete = lru_list_->PopFront();
    cache_.erase(keyToDelete);

    --cache_gauge_;
  }

  /**
   * Locks the cache (thread safety).
   */
  inline void lock() {
#ifdef LRU_CACHE_THREAD_SAFE
    pthread_mutex_lock(&lock_);
#endif
  }

  /**
   * Unlocks the cache (thread safety).
   */
  inline void unlock() {
#ifdef LRU_CACHE_THREAD_SAFE
    pthread_mutex_unlock(&lock_);
#endif
  }
};

// initialize the static allocator field
template<class Key, class Value, class HashFunction, class EqualKey >
typename LruCache<Key, Value, HashFunction, EqualKey>::ConcreteMemoryAllocator *LruCache<Key, Value, HashFunction, EqualKey>::allocator_ = NULL;


class InodeCache : public LruCache<fuse_ino_t, catalog::DirectoryEntry> {
 public:
  InodeCache(unsigned int cache_size) :
    LruCache<fuse_ino_t, catalog::DirectoryEntry>(cache_size)
  {
    // TODO
    this->setSpecialHashTableKeys(1000000000, 1000000001);
  }

  bool insert(const fuse_ino_t inode, const catalog::DirectoryEntry &dirent) {
    LogCvmfs(kLogLru, kLogDebug, "insert inode --> dirent: %d -> '%s'",
             inode, dirent.name().c_str());
    return LruCache<fuse_ino_t, catalog::DirectoryEntry>::insert(inode,
                                                                 dirent);
  }

  bool lookup(const fuse_ino_t inode, catalog::DirectoryEntry *dirent) {
    LogCvmfs(kLogLru, kLogDebug, "lookup inode --> dirent: %d", inode);
    return LruCache<fuse_ino_t, catalog::DirectoryEntry>::lookup(inode,
                                                                 dirent);
  }

  void drop() {
    LogCvmfs(kLogLru, kLogDebug, "dropping inode cache");
    LruCache<fuse_ino_t, catalog::DirectoryEntry>::drop();
  }
};


class PathCache : public LruCache<fuse_ino_t, std::string> {
 public:
  PathCache(unsigned int cache_size) :
    LruCache<fuse_ino_t, std::string>(cache_size)
  {
    this->setSpecialHashTableKeys(1000000000, 1000000001);
  }

  bool insert(const fuse_ino_t inode, const std::string &path) {
    LogCvmfs(kLogLru, kLogDebug, "insert inode --> path %d -> '%s'",
             inode, path.c_str());
    return LruCache<fuse_ino_t, std::string>::insert(inode, path);
  }

  bool lookup(const fuse_ino_t inode, std::string *path) {
    LogCvmfs(kLogLru, kLogDebug, "lookup inode --> path: %d", inode);
    return LruCache<fuse_ino_t, std::string>::lookup(inode, path);
  }

  void drop() {
    LogCvmfs(kLogLru, kLogDebug, "dropping path cache");
    LruCache<fuse_ino_t, std::string>::drop();
  }
};


struct hash_md5 {
  size_t operator() (const hash::Md5 &md5) const {
    return (size_t)*((size_t*)md5.digest);
  }
};

struct equal_md5 {
  bool operator() (const hash::Md5 &a, const hash::Md5 &b) const {
    return a == b;
  }
};

class Md5PathCache :
  public LruCache<hash::Md5, catalog::DirectoryEntry, hash_md5, equal_md5 >
{
 public:
  Md5PathCache(unsigned int cache_size) :
    LruCache<hash::Md5, catalog::DirectoryEntry,
             hash_md5, equal_md5>(cache_size)
  {
    this->setSpecialHashTableKeys(hash::Md5(hash::AsciiPtr("!")),
                                  hash::Md5(hash::AsciiPtr("?")));
  }

  bool insert(const hash::Md5 &hash, const catalog::DirectoryEntry &dirent) {
    return true;
    LogCvmfs(kLogLru, kLogDebug, "insert md5 --> dirent: %s -> '%s'",
             hash.ToString().c_str(), dirent.name().c_str());
    return LruCache<hash::Md5, catalog::DirectoryEntry,
                    hash_md5, equal_md5>::insert(hash, dirent);
  }

  bool lookup(const hash::Md5 &hash, catalog::DirectoryEntry *dirent) {
    LogCvmfs(kLogLru, kLogDebug, "lookup md5 --> dirent: %s",
             hash.ToString().c_str());
    return LruCache<hash::Md5, catalog::DirectoryEntry,
                    hash_md5, equal_md5>::lookup(hash, dirent);
  }

  bool forget(const hash::Md5 &hash) {
    return true;
    LogCvmfs(kLogLru, kLogDebug, "forget md5: %s",
             hash.ToString().c_str());
    return LruCache<hash::Md5, catalog::DirectoryEntry,
                    hash_md5, equal_md5>::forget(hash);
  }
};

}  // namespace lru

#endif  // CVMFS_LRU_H_
