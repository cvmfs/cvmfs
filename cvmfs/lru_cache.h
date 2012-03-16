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

#ifndef LRU_CACHE_H
#define LRU_CACHE_H 1

// if defined the cache is secured by a posix mutex
#define LRU_CACHE_THREAD_SAFE 1

#include <cassert>
#include <map>
#include <iostream>
#include <algorithm>
#include <functional>
#include <string>

#include <google/dense_hash_map>

#include "platform.h"
#include "logging.h"

namespace cvmfs {

	/**
	 *  template class to create a LRU cache
	 *  @param Key type of the key values
	 *  @param Value type of the value values
	 */
   template<class Key, class Value, class HashFunction = SPARSEHASH_HASH<Key>, class EqualKey = std::equal_to<Key> >
	class LruCache {

	private:
		// forward declarations of private internal data structures
		template<class T> class ListEntry;
		template<class T> class ListEntryHead;
		template<class T> class ListEntryContent;
		template<class M> class MemoryAllocator;

		// helpers to get the template magic right
      typedef ListEntryContent<Key> ConcreteListEntryContent;
      typedef MemoryAllocator<ConcreteListEntryContent> ConcreteMemoryAllocator;

      /**
       *  this structure wraps the user data and relates it to the LRU list entry
       */
		typedef struct {
			ListEntryContent<Key> *listEntry;
			Value value;
		} CacheEntry;

	   // the actual map data structure (TODO: replace this by a hashmap)
		typedef google::dense_hash_map<Key, CacheEntry, HashFunction, EqualKey> Cache;

		// internal data fields
		unsigned int mCurrentCacheSize;
		unsigned int mMaxCacheSize;
      static ConcreteMemoryAllocator *allocator;

		/**
		 *  a double linked list to keep track of the least recently
		 *  used data entries.
		 *  New entries get pushed back to the list. If an entry is touched
		 *  it is moved to the back of the list again.
		 *  If the cache gets too long, the first element (the oldest) gets
		 *  deleted to obtain some space.
		 */
		ListEntryHead<Key> *mLruList;

		/**
		 *  the actual caching data structure
		 *  it has to be a map of some kind... lookup performance is crucial
		 */
		Cache mCache;

		/**
		 *  mutex to make cache thread safe
		 */
#ifdef LRU_CACHE_THREAD_SAFE
      pthread_mutex_t mLock;
#endif

	   /**
	    *  This MemoryAllocator optimizes the usage of memory for the cache entries.
	    *  It allocates enough memory for the maximal number of cache entries at startup,
	    *  and assigns new ListEntryContent objects a free spot in this memory pool.
	    *  This is done by overriding the 'new' and 'delete' operators of ListEntryContent.
	    *  As the cache is meant to be completely filled at all times it is no problem to
	    *  allocate all memory already at startup.
	    *  @param M the type of object to be allocated by this MemoryAllocator
	    */
      template<class M>
      class MemoryAllocator {
      private:
         unsigned int mNumberOfSlots; // <-- overall number of slots in the memory pool
         unsigned int mFreeSlots;     // <-- number of free slots left
         unsigned int mNextFreeSlot;  // <-- position of next free slot in the pool
         char *mBlocks; // <-- a bitmap to mark slots as allocated
         M *mMemory;    // <-- the actual memory pool

      public:
         /**
          *  creates a MemoryAllocator to handle a memory pool
          *  @param numberOfSlots the number of slots to be allocated for the given datatype M
          */
         MemoryAllocator(const unsigned int numberOfSlots) {
            // how many bitmap chunks (chars) do we need?
            unsigned int bytesNeededForBitmap = numberOfSlots / sizeof(char);
            if (bytesNeededForBitmap * sizeof(char) < numberOfSlots) bytesNeededForBitmap++;

            // how much actual memory do we need?
            const unsigned int bytesOfMemoryNeeded = sizeof(M) * numberOfSlots;

            // allocate memory
            mBlocks = (char *)malloc(bytesNeededForBitmap);
            mMemory = (M *)malloc(bytesOfMemoryNeeded);

            // check for successfully allocated memory
            if (mBlocks == NULL || mMemory == NULL) {
               std::cerr << "memory allocation for LRU cache failed" << std::endl;
               abort();
            }

            // zero memory
            memset(mBlocks, 0, bytesNeededForBitmap);
            memset(mMemory, 0, bytesOfMemoryNeeded);

            // create initial state
            mNumberOfSlots = numberOfSlots;
            mFreeSlots = numberOfSlots;
            mNextFreeSlot = 0;
         }

         /**
          *  free all data
          *  if the MemoryAllocator is gone, all data is gone too!
          */
         virtual ~MemoryAllocator() {
            free(mBlocks);
            free(mMemory);
         }

         /**
          *  check if the memory pool is full
          *  @return true if all slots are occupied, otherwise false
          */
         inline bool isFull() const { return mFreeSlots == 0; }

         /**
          *  allocates a slot and returns a pointer to the memory
          *  @return a pointer to a chunk of the memory pool
          */
         M* allocate() {
            // check if memory is left
            if (this->isFull()) {
               return NULL;
            } else {
               // allocate a slot
               this->setBit(mNextFreeSlot);
               --mFreeSlots;
               M *slot = mMemory + mNextFreeSlot;

               // find a new free slot if there are some left
               if (not this->isFull()) {
                  while (this->getBit(mNextFreeSlot)) {
                     mNextFreeSlot = (mNextFreeSlot + 1) % mNumberOfSlots;
                  }
               }

               // done
               return slot;
            }
         }

         /**
          *  free a given slot in the memory pool
          *  @param slot a pointer to the slot be freed
          */
         void deallocate(M* slot) {
            // check if given slot is in bounds
            if (slot < mMemory || slot > mMemory + mNumberOfSlots) {
               return;
            }

            // get position of slot
            const unsigned int position = slot - mMemory;

            // check if slot was already freed
            assert (this->getBit(position));

            // free slot
            this->unsetBit(position);
            mNextFreeSlot = position; // <-- save the position of this slot as free (faster reallocation)
            ++mFreeSlots;
         }

      private:
         /**
          *  check a bit in the internal allocation bitmap
          *  @param position the position to check
          *  @return true if bit is set, otherwise false
          */
         inline bool getBit(const unsigned int position) {
            assert (position < mNumberOfSlots);
            return ((mBlocks[position / 8] & (1 << (position % 8))) != 0);
         }

         /**
          *  set a bit in the internal allocation bitmap
          *  @param position the number of the bit to be set
          */
         inline void setBit(const unsigned int position) {
            assert (position < mNumberOfSlots);
            mBlocks[position / 8] |= 1 << (position % 8);
         }

         /**
          *  clear a bit in the internal allocation bitmap
          *  @param position the number of the bit to be cleared
          */
         inline void unsetBit(const unsigned int position) {
            assert (position < mNumberOfSlots);
            mBlocks[position / 8] &= ~(1 << (position % 8));
         }
      };

		/**
		 *  INTERNAL DATA STRUCTURE
		 *  abstract ListEntry class to maintain a double linked list
		 *  the list keeps track of the least recently used keys in the cache
		 */
		template<class T>
		class ListEntry {
		public:
			ListEntry<T> *next; // <-- pointer to next element in the list
			ListEntry<T> *prev; // <-- ... take an educated guess... ;-)

		public:
			ListEntry() {
				// create a new list entry as lonely
				// both next and prev pointing to this
				this->next = this;
				this->prev = this;
			}
			virtual ~ListEntry() {}

			/**
			 *  checks if the ListEntry is the list head
			 *  @return true if ListEntry is list head otherwise false
			 */
			virtual bool isListHead() const = 0;

			/**
			 *  a lonely ListEntry has no connection to other elements
			 *  @return true if ListEntry is lonely otherwise false
			 */
			bool isLonely() const { return (this->next == this && this->prev == this); }

		protected:

			/**
			 *  insert a given ListEntry after this one
			 *  @param entry the ListEntry to insert after this one
			 */
			inline void insertAsSuccessor(ListEntryContent<T> *entry) {
				assert (entry->isLonely());

				// mount the new element between this and this->next
				entry->next = this->next;
				entry->prev = this;

				// point this->next->prev to entry and _afterwards_ overwrite this->next
				this->next->prev = entry;
				this->next = entry;

				assert (not entry->isLonely());
			}

			/**
			 *  insert a given ListEntry in front of this one
			 *  @param entry the ListEntry to insert in front of this one
			 */
			inline void insertAsPredecessor(ListEntryContent<T> *entry) {
				assert (entry->isLonely());
				assert (not entry->isListHead());

				// mount the new element between this and this->prev
				entry->next = this;
				entry->prev = this->prev;

				// point this->prev->next to entry and _afterwards_ overwrite this->prev
				this->prev->next = entry;
				this->prev = entry;

				assert (not entry->isLonely());
			}

			/**
			 *  remove this element from it's list
			 *  the function connects this->next with this->prev leaving the complete list
			 *  in a consistent state. The ListEntry itself is lonely afterwards, but not
			 *  deleted!!
			 */
			virtual void removeFromList() = 0;
		};

		/**
		 *  Specialized ListEntry to contain a data entry of type T
		 */
		template<class T>
		class ListEntryContent
					: public ListEntry<T> {
		private:
			T mContent; // <-- the data content of this ListEntry

		public:
			ListEntryContent(Key content) {
				mContent = content;
			};

			/**
			 *  overwritten the new operator of this class to redirect it to our own
			 *  memory allocator. This ensures that heap is not fragmented by loads of
			 *  malloc and free calls
			 */
			static void* operator new (size_t size) {
            assert(LruCache::allocator != NULL);
            return (void *)LruCache::allocator->allocate();
			}

			/**
			 *  overwritten delete operator to redirect deallocation to our own memory
			 *  allocator
			 */
         static void operator delete (void *p) {
            assert(LruCache::allocator != NULL);
            LruCache::allocator->deallocate(static_cast<ListEntryContent<T> *>(p));
         }

			/**
			 *  see ListEntry base class
			 */
			inline bool isListHead() const { return false; }

			/**
			 *  retrieve to content of this ListEntry
			 *  @return the content of this ListEntry
			 */
			inline T getContent() const { return mContent; }

			/**
			 *  see ListEntry base class
			 */
			inline void removeFromList() {
				assert (not this->isLonely());

				// remove this from list
				this->prev->next = this->next;
				this->next->prev = this->prev;

				// make this lonely
				this->next = this;
				this->prev = this;

				assert (this->isLonely());
			}
		};

		/**
		 *  Specialized ListEntry to form a list head.
		 *  Every list has exactly one list head which is also the entry point
		 *  in the list. It is used to manipulate the list.
		 */
		template<class T>
		class ListEntryHead
					: public ListEntry<T> {
		public:
			virtual ~ListEntryHead() {
				this->clear();
			}

			/**
			 *  remove all entries from the list
			 *  ListEntry objects are deleted but contained data keeps available
			 */
			void clear() {
				// delete all list entries
				ListEntry<T> *entry = this->next;
				ListEntry<T> *entryToDelete;
				while (not entry->isListHead()) {
					entryToDelete = entry;
					entry = entry->next;
					delete entryToDelete;
				}

				// reset the list to lonely
				this->next = this;
				this->prev = this;
			}

			/**
			 *  see ListEntry base class
			 */
			inline bool isListHead() const { return true; }

			/**
			 *  check if the list is empty
			 *  @return true if the list only contains the ListEntryHead otherwise false
			 */
			inline bool isEmpty() const { return this->isLonely(); }

			/**
			 *  retrieve the data in the front of the list
			 *  @return the first data object in the list
			 */
			inline T getFront() const {
				assert (not this->isEmpty());
				return this->next->getContent();
			}

			/**
			 *  retrieve the data in the back of the list
			 *  @return the last data object in the list
			 */
			inline T getBack() const {
				assert (not this->isEmpty());
				return this->prev->getContent();
			}

			/**
			 *  push a new data object to the end of the list
			 *  @param the data object to insert
			 *  @return the ListEntryContent structure which was wrapped around the data object
			 */
			inline ListEntryContent<T>* push_back(T content) {
				ListEntryContent<T> *newEntry = new ListEntryContent<T>(content);
				this->insertAsPredecessor(newEntry);
				return newEntry;
			}

			/**
			 *  push a new data object to the front of the list
			 *  @param the data object to insert
			 *  @return the ListEntryContent structure which was wrapped around the data object
			 */
			inline ListEntryContent<T>* push_front(T content) {
				ListEntryContent<T> *newEntry = new ListEntryContent<T>(content);
				this->insertAsSuccessor(newEntry);
				return newEntry;
			}

			/**
			 *  pop the first object of the list
			 *  the object is returned and removed from the list
			 *  @return the data object which resided in the first list entry
			 */
			inline T pop_front() {
				assert (not this->isEmpty());
				return pop(this->next);
			}

			/**
			 *  pop the last object of the list
			 *  the object is returned and removed from the list
			 *  @return the data object which resided in the last list entry
			 */
			inline T pop_back() {
				assert (not this->isEmpty());
				return pop(this->prev);
			}

			/**
			 *  take a list entry out of it's list and reinsert just behind this ListEntryHead
			 *  @param the ListEntry to be moved to the beginning of this list
			 */
			inline void moveToFront(ListEntryContent<T> *entry) {
				assert (not entry->isLonely());

				entry->removeFromList();
				this->insertAsSuccessor(entry);
			}

			/**
			 *  take a list entry out of it's list and reinsert at the end of this list
			 *  @param the ListEntry to be moved to the end of this list
			 */
			inline void moveToBack(ListEntryContent<T> *entry) {
				assert (not entry->isLonely());

				entry->removeFromList();
				this->insertAsPredecessor(entry);
			}

			/**
			 *  see ListEntry base class
			 */
			inline void removeFromList() { assert (false); }

		private:
			/**
			 *  pop a ListEntry from the list (arbitrary position)
			 *  the given ListEntry is removed from the list, deleted and it's data content is returned
			 *  @param poppedEntry the entry to be popped
			 *  @return the data object of the popped ListEntry
			 */
			inline T pop(ListEntry<T> *poppedEntry) {
				assert (not poppedEntry->isListHead());

				ListEntryContent<T> *popped = (ListEntryContent<T> *) poppedEntry;
				popped->removeFromList();
				T res = popped->getContent();
				delete popped;
				return res;
			}
		};

	public:
		/**
		 *  create a new LRU cache object
		 *  @param maxCacheSize the maximal size of the cache
		 */
		LruCache(const unsigned int maxCacheSize) {
			assert (maxCacheSize > 0);

			// create memory allocator
			ConcreteMemoryAllocator *allocator = new ConcreteMemoryAllocator(maxCacheSize);
         LruCache<Key, Value, HashFunction, EqualKey>::allocator = allocator;

		   // internal state
			mCurrentCacheSize = 0;
			mMaxCacheSize = maxCacheSize;
			mLruList = new ListEntryHead<Key>();

			// thread safety
#ifdef LRU_CACHE_THREAD_SAFE
         pthread_mutex_init(&mLock, NULL);
#endif
		}

		virtual ~LruCache() {
			delete mLruList;
#ifdef LRU_CACHE_THREAD_SAFE
         pthread_mutex_destroy(&mLock);
#endif
		}

		/**
		 *  insert a new key-value pair to the list
		 *  if the cache is already full, the least recently used object is removed
		 *  afterwards the new object is inserted
		 *  if the object is already present it is updated and moved back to the end
		 *  of the list
		 *  @param key the key where the value is saved
		 *  @param value the value of the cache entry
		 *  @return true on successful insertion otherwise false
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
   			if (this->isFull()) {
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

            entry.listEntry->removeFromList();
            delete entry.listEntry;
   			mCache.erase(key);
            --mCurrentCacheSize;
         }

         this->unlock();
         return found;
		}

		/**
		 *  checks if the cache is filled completely
		 *  if yes, an insert of a new element will delete the least recently used one
		 *  @return true if cache is fully used otherwise false
		 */
		inline bool isFull() const { return mCurrentCacheSize >= mMaxCacheSize; }

		/**
		 *  checks if there is at least one element in the cache
		 *  @return true if cache is completely empty otherwise false
		 */
		inline bool isEmpty() const { return mCurrentCacheSize == 0; }

		/**
		 *  returns the current amount of entries in the cache
		 *  @return the number of entries currently in the cache
		 */
		inline unsigned int getNumberOfEntries() const { return mCurrentCacheSize; }

		/**
		 *  clears all elements from the cache
		 *  all memory of internal data structures will be freed but data of cache entries
		 *  may stay in use, we do not call delete on any user data
		 */
		virtual void drop() {
         this->lock();

			mCurrentCacheSize = 0;
			mLruList->clear();
			mCache.clear();

         this->unlock();
		}

	   /**
	    *  google dense hash needs two special Key values to mark empty hash table
	    *  buckets and deleted hash table buckets
	    */
	    void setSpecialHashTableKeys(const Key &empty, const Key &deleted) {
          mCache.set_empty_key(empty);
          mCache.set_deleted_key(deleted);
	    }

	private:
		/**
		 *  this just performs a lookup in the cache
		 *  WITHOUT changing the LRU order
		 *  @param key the key to perform a lookup on
		 *  @param entry a pointer to the entry structure
		 *  @return true on successful lookup, false otherwise
		 */
		inline bool lookupCache(const Key &key, CacheEntry &entry) {
         typename Cache::iterator foundElement = mCache.find(key);

         if (foundElement == mCache.end()) {
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
			assert (not this->isFull());

			CacheEntry entry;
			entry.listEntry = mLruList->push_back(key);
			entry.value = value;

			mCache[key] = entry;
			mCurrentCacheSize++;
		}

		/**
		 *  update an entry which is already in the cache
		 *  this will not change the LRU order. Just the new data object is
		 *  associated with the given key value in the cache
		 *  @param key the key to save the value in
		 *  @param entry the CacheEntry structure to save in the cache
		 */
		inline void updateExistingEntry(const Key &key, const CacheEntry &entry) {
			mCache[key] = entry;
		}

		/**
		 *  touch an entry
		 *  the entry will be moved to the back of the LRU list to mark it
		 *  as 'recently used'... this saves the entry from being deleted
		 *  @param entry the CacheEntry to be touched (CacheEntry is the internal wrapper data structure)
		 */
		inline void touchEntry(const CacheEntry &entry) {
			mLruList->moveToBack(entry.listEntry);
		}

		/**
		 *  deletes the least recently used entry from the cache
		 */
		inline void deleteOldestEntry() {
			assert (not this->isEmpty());

			Key keyToDelete = mLruList->pop_front();
			mCache.erase(keyToDelete);

			--mCurrentCacheSize;
		}

		/**
		 *  locks the cache (thread safety)
		 */
		inline void lock() {
#ifdef LRU_CACHE_THREAD_SAFE
         pthread_mutex_lock(&mLock);
#endif
		}

		/**
		 *  unlocks the cache (thread safety)
		 */
		inline void unlock() {
#ifdef LRU_CACHE_THREAD_SAFE
         pthread_mutex_unlock(&mLock);
#endif
		}
	};

	// initialize the static allocator field
	template<class Key, class Value, class HashFunction, class EqualKey >
   typename LruCache<Key, Value, HashFunction, EqualKey>::ConcreteMemoryAllocator *LruCache<Key, Value, HashFunction, EqualKey>::allocator = NULL;

} // namespace cvmfs

#endif
