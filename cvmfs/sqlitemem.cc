/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS


#include "sqlitemem.h"

#include <cassert>
#include <cstddef>
#include <cstring>
#include <new>

#include "malloc_arena.h"
#include "util/concurrency.h"
#include "util/exception.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT


void *SqliteMemoryManager::LookasideBufferArena::GetBuffer() {
  for (unsigned i = 0; i < kNoBitmaps; ++i) {
    int bit_set = ffs(freemap_[i]);
    if (bit_set != 0) {
      freemap_[i] &= ~(1 << (bit_set - 1));  // set bit to zero
      const int nbuffer = i * sizeof(int) * 8 + bit_set - 1;
      return reinterpret_cast<char *>(arena_) + nbuffer * kBufferSize;
    }
  }
  return NULL;
}


bool SqliteMemoryManager::LookasideBufferArena::IsEmpty() {
  for (unsigned i = 0; i < kNoBitmaps; ++i) {
    if (~freemap_[i] != 0)
      return false;
  }
  return true;
}


bool SqliteMemoryManager::LookasideBufferArena::Contains(void *buffer) {
  if ((buffer == NULL) || (buffer < arena_))
    return false;
  return (static_cast<uint64_t>(
    (reinterpret_cast<char *>(buffer) - reinterpret_cast<char *>(arena_))) <
    kArenaSize);
}


SqliteMemoryManager::LookasideBufferArena::LookasideBufferArena()
  : arena_(sxmmap(kArenaSize))
{
  // All buffers unused, i.e. all bits set
  memset(freemap_, 0xFF, kNoBitmaps * sizeof(int));
}


SqliteMemoryManager::LookasideBufferArena::~LookasideBufferArena() {
  sxunmap(arena_, kArenaSize);
}


void SqliteMemoryManager::LookasideBufferArena::PutBuffer(void *buffer) {
  assert(buffer >= arena_);
  ptrdiff_t nbuffer =
    (reinterpret_cast<char *>(buffer) - reinterpret_cast<char *>(arena_))
    / kBufferSize;
  assert(static_cast<uint64_t>(nbuffer) < kBuffersPerArena);
  const int nfreemap = nbuffer / (sizeof(int) * 8);
  freemap_[nfreemap] |= 1 << (nbuffer % (sizeof(int) * 8));
}


//------------------------------------------------------------------------------


SqliteMemoryManager *SqliteMemoryManager::instance_ = NULL;


/**
 * Sqlite ensures that size > 0.
 */
void *SqliteMemoryManager::xMalloc(int size) {
  return instance_->GetMemory(size);
}


/**
 * Sqlite ensures that ptr != NULL.
 */
void SqliteMemoryManager::xFree(void *ptr) {
  instance_->PutMemory(ptr);
}


/**
 * Sqlite ensures that ptr != NULL and new_size > 0.
 */
void *SqliteMemoryManager::xRealloc(void *ptr, int new_size) {
  int old_size = xSize(ptr);
  if (old_size >= new_size)
    return ptr;

  void *new_ptr = xMalloc(new_size);
  memcpy(new_ptr, ptr, old_size);
  xFree(ptr);
  return new_ptr;
}


/**
 * Sqlite ensures that ptr != NULL.
 */
int SqliteMemoryManager::xSize(void *ptr) {
  return instance_->GetMemorySize(ptr);
}


int SqliteMemoryManager::xRoundup(int size) {
  return RoundUp8(size);
}


int SqliteMemoryManager::xInit(void *app_data __attribute__((unused))) {
  return SQLITE_OK;
}


void SqliteMemoryManager::xShutdown(void *app_data __attribute__((unused))) {
}


void SqliteMemoryManager::AssignGlobalArenas() {
  if (assigned_) return;
  int retval;

  retval = sqlite3_config(SQLITE_CONFIG_PAGECACHE, page_cache_memory_,
                          kPageCacheSlotSize, kPageCacheNoSlots);
  assert(retval == SQLITE_OK);

  retval = sqlite3_config(SQLITE_CONFIG_GETMALLOC, &sqlite3_mem_vanilla_);
  assert(retval == SQLITE_OK);
  retval = sqlite3_config(SQLITE_CONFIG_MALLOC, &mem_methods_);
  assert(retval == SQLITE_OK);

  assigned_ = true;
}


/**
 * Needs to be the first operation on an opened sqlite database.  Returns the
 * location of the buffer.
 */
void *SqliteMemoryManager::AssignLookasideBuffer(sqlite3 *db) {
  MutexLockGuard lock_guard(lock_);

  void *buffer = GetLookasideBuffer();
  assert(buffer != NULL);
  int retval = sqlite3_db_config(db, SQLITE_DBCONFIG_LOOKASIDE,
    buffer, kLookasideSlotSize, kLookasideSlotsPerDb);
  assert(retval == SQLITE_OK);
  return buffer;
}


void SqliteMemoryManager::CleanupInstance() {
  delete instance_;
  instance_ = NULL;
}


/**
 * Opens a new arena if necessary.
 */
void *SqliteMemoryManager::GetLookasideBuffer() {
  void *result;
  vector<LookasideBufferArena *>::reverse_iterator reverse_iter =
    lookaside_buffer_arenas_.rbegin();
  vector<LookasideBufferArena *>::reverse_iterator i_rend =
    lookaside_buffer_arenas_.rend();
  for (; reverse_iter != i_rend; ++reverse_iter) {
    result = (*reverse_iter)->GetBuffer();
    if (result != NULL)
      return result;
  }

  LookasideBufferArena *new_arena = new LookasideBufferArena();
  lookaside_buffer_arenas_.push_back(new_arena);
  return new_arena->GetBuffer();
}


int SqliteMemoryManager::GetMemorySize(void *ptr) {
  return MallocArena::GetMallocArena(ptr, kArenaSize)->GetSize(ptr);
}


/**
 * Opens new arenas as necessary.
 */
void *SqliteMemoryManager::GetMemory(int size) {
  void *p = malloc_arenas_[idx_last_arena_]->Malloc(size);
  if (p != NULL)
    return p;
  unsigned N = malloc_arenas_.size();
  for (unsigned i = 0; i < N; ++i) {
    p = malloc_arenas_[i]->Malloc(size);
    if (p != NULL) {
      idx_last_arena_ = i;
      return p;
    }
  }
  idx_last_arena_ = N;
  MallocArena *M = new MallocArena(kArenaSize);
  malloc_arenas_.push_back(M);
  p = M->Malloc(size);
  assert(p != NULL);
  return p;
}


SqliteMemoryManager::SqliteMemoryManager()
  : assigned_(false)
  , page_cache_memory_(sxmmap(kPageCacheSize))
  , idx_last_arena_(0)
{
  memset(&sqlite3_mem_vanilla_, 0, sizeof(sqlite3_mem_vanilla_));
  int retval = pthread_mutex_init(&lock_, NULL);
  assert(retval == 0);

  lookaside_buffer_arenas_.push_back(new LookasideBufferArena());
  malloc_arenas_.push_back(new MallocArena(kArenaSize));

  memset(&mem_methods_, 0, sizeof(mem_methods_));
  mem_methods_.xMalloc = xMalloc;
  mem_methods_.xFree = xFree;
  mem_methods_.xRealloc = xRealloc;
  mem_methods_.xSize = xSize;
  mem_methods_.xRoundup = xRoundup;
  mem_methods_.xInit = xInit;
  mem_methods_.xShutdown = xShutdown;
  mem_methods_.pAppData = NULL;
}


/**
 * Must be executed only after sqlite3_shutdown.
 */
SqliteMemoryManager::~SqliteMemoryManager() {
  if (assigned_) {
    // Reset sqlite to default values
    int retval;
    retval = sqlite3_config(SQLITE_CONFIG_PAGECACHE, NULL, 0, 0);
    assert(retval == SQLITE_OK);
    retval = sqlite3_config(SQLITE_CONFIG_MALLOC, &sqlite3_mem_vanilla_);
    assert(retval == SQLITE_OK);
  }

  sxunmap(page_cache_memory_, kPageCacheSize);
  for (unsigned i = 0; i < lookaside_buffer_arenas_.size(); ++i)
    delete lookaside_buffer_arenas_[i];
  for (unsigned i = 0; i < malloc_arenas_.size(); ++i)
    delete malloc_arenas_[i];
  pthread_mutex_destroy(&lock_);
}


/**
 * Only entirely empty arenas are freed to the system.  In cvmfs, catalogs
 * are gradually opened and sometimes close altogether when a new root catalog
 * arrives.  Hence there is no fragmentation.
 */
void SqliteMemoryManager::PutLookasideBuffer(void *buffer) {
  unsigned N = lookaside_buffer_arenas_.size();
  for (unsigned i = 0; i < N; ++i) {
    if (lookaside_buffer_arenas_[i]->Contains(buffer)) {
      lookaside_buffer_arenas_[i]->PutBuffer(buffer);
      if ((N > 1) && lookaside_buffer_arenas_[i]->IsEmpty()) {
        delete lookaside_buffer_arenas_[i];
        lookaside_buffer_arenas_.erase(lookaside_buffer_arenas_.begin() + i);
      }
      return;
    }
  }
  PANIC(NULL);
}


/**
 * Closes empty areas.
 */
void SqliteMemoryManager::PutMemory(void *ptr) {
  MallocArena *M = MallocArena::GetMallocArena(ptr, kArenaSize);
  M->Free(ptr);
  unsigned N = malloc_arenas_.size();
  if ((N > 1) && M->IsEmpty()) {
    for (unsigned i = 0; i < N; ++i) {
      if (malloc_arenas_[i] == M) {
        delete malloc_arenas_[i];
        malloc_arenas_.erase(malloc_arenas_.begin() + i);
        idx_last_arena_ = 0;
        return;
      }
    }
    PANIC(NULL);
  }
}


/**
 * To be used after an sqlite database has been closed.
 */
void SqliteMemoryManager::ReleaseLookasideBuffer(void *buffer) {
  MutexLockGuard lock_guard(lock_);
  PutLookasideBuffer(buffer);
}
