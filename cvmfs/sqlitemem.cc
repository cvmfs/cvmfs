/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "sqlitemem.h"

#include <cstddef>
#include <cstring>

#include "duplex_sqlite3.h"
#include "smalloc.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace sqlite {

void *MemoryManager::LookasideBufferArena::GetBuffer() {
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


bool MemoryManager::LookasideBufferArena::IsEmpty() {
  for (unsigned i = 0; i < kNoBitmaps; ++i) {
    if (~freemap_[i] != 0)
      return false;
  }
  return true;
}


bool MemoryManager::LookasideBufferArena::Contains(void *buffer) {
  if ((buffer == NULL) || (buffer < arena_))
    return false;
  return (reinterpret_cast<char *>(buffer) - reinterpret_cast<char *>(arena_)) <
         kArenaSize;
}


MemoryManager::LookasideBufferArena::LookasideBufferArena()
  : arena_(sxmmap(kArenaSize))
{
  // All buffers unused, i.e. all bits set
  memset(freemap_, 0xFF, kNoBitmaps * sizeof(int));
}


MemoryManager::LookasideBufferArena::~LookasideBufferArena() {
  sxunmap(arena_, kArenaSize);
}


void MemoryManager::LookasideBufferArena::PutBuffer(void *buffer) {
  ptrdiff_t nbuffer =
    (reinterpret_cast<char *>(buffer) - reinterpret_cast<char *>(arena_))
    / kBufferSize;
  assert(nbuffer < kBuffersPerArena);
  const int nfreemap = nbuffer / (sizeof(int) * 8);
  freemap_[nfreemap] |= 1 << (nbuffer % (sizeof(int) * 8));
}


//------------------------------------------------------------------------------


MemoryManager *MemoryManager::instance_ = NULL;

/**
 * Needs to be the first operation on an opened sqlite database.  Returns the
 * location of the buffer.
 */
void *MemoryManager::AssignLookasideBuffer(void *db) {
  MutexLockGuard lock_guard(lock_);

  sqlite3 *sqlite_db = reinterpret_cast<sqlite3 *>(db);
  void *buffer = GetLookasideBuffer();
  assert(buffer != NULL);
  int retval = sqlite3_db_config(sqlite_db, SQLITE_DBCONFIG_LOOKASIDE,
    buffer, kLookasideSlotSize, kLookasideSlotsPerDb);
  assert(retval == SQLITE_OK);
  return buffer;
}


void MemoryManager::CleanupInstance() {
  delete instance_;
  instance_ = NULL;
}


/**
 * Opens a new arena if necessary.
 */
void *MemoryManager::GetLookasideBuffer() {
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


MemoryManager::MemoryManager()
  : scratch_memory_(smalloc(kScratchSize))
  , page_cache_memory_(smalloc(kPageCacheSize))
{
  int retval;

  lock_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);

  retval = sqlite3_config(SQLITE_CONFIG_SCRATCH, scratch_memory_, 8192, 16);
  assert(retval == SQLITE_OK);

  lookaside_buffer_arenas_.push_back(new LookasideBufferArena());
}


/**
 * Must be executed only after sqlite3_shutdown.
 */
MemoryManager::~MemoryManager() {
  free(scratch_memory_);
  free(page_cache_memory_);
  for (unsigned i = 0; i < lookaside_buffer_arenas_.size(); ++i)
    delete lookaside_buffer_arenas_[i];
  pthread_mutex_destroy(lock_);
  free(lock_);
}


/**
 * Only entirely empty arenas are freed to the system.  In cvmfs, catalogs
 * are gradually opened and sometimes close altogether when a new root catalog
 * arrives.  Hence there is no fragmentation.
 */
void MemoryManager::PutLookasideBuffer(void *buffer) {
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
}


/**
 * To be used after an sqlite database has been closed.
 */
void MemoryManager::ReleaseLookasideBuffer(void *buffer) {
  MutexLockGuard lock_guard(lock_);
  PutLookasideBuffer(buffer);
}

}  // namespace sqlite
