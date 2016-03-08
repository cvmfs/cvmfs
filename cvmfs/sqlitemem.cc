/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "sqlitemem.h"

#include <cassert>
#include <cstddef>
#include <cstring>
#include <new>

#include "smalloc.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

uint32_t SqliteMemoryManager::MallocArena::GetSize(void *ptr) const {
  assert(Contains(ptr));

  ReservedBlockCtl *block_ctl = reinterpret_cast<ReservedBlockCtl *>(
    reinterpret_cast<char *>(ptr) - sizeof(ReservedBlockCtl));
  int32_t size = block_ctl->size();
  assert(size > 1);
  return size - sizeof(ReservedBlockCtl) - 1;
}

/**
 * Creates a free block at the place of the reserved block ptr points into.
 * The free block might need to be merged with adjacent lower and/or upper
 * blocks.  In these cases, the corresponding blocks are removed from the list
 * of available blocks.  Every allocated block has a predecessor and a
 * successor in the arena.  The newly created free block is added to the end of
 * the list of available blocks.
 */
void SqliteMemoryManager::MallocArena::Free(void *ptr) {
  assert(Contains(ptr));

  no_reserved_--;

  ReservedBlockCtl *block_ctl = reinterpret_cast<ReservedBlockCtl *>(
    reinterpret_cast<char *>(ptr) - sizeof(ReservedBlockCtl));
  char prior_tag = *(reinterpret_cast<char *>(block_ctl) - 1);
  assert((prior_tag == kTagAvail) || (prior_tag == kTagReserved));

  int32_t new_size = block_ctl->size();
  assert(new_size > 0);
  AvailBlockCtl *new_avail = reinterpret_cast<AvailBlockCtl *>(block_ctl);

  if (prior_tag == kTagAvail) {
    // Merge with block before and remove the block from the list
    int32_t prior_size = reinterpret_cast<AvailBlockTag *>(
      reinterpret_cast<char *>(block_ctl) - sizeof(AvailBlockTag))->size;
    assert(prior_size > 0);
    new_size += prior_size;
    new_avail = reinterpret_cast<AvailBlockCtl *>(
      reinterpret_cast<char *>(block_ctl) - prior_size);
    AvailBlockCtl *next = new_avail->GetNextPtr(arena_);
    AvailBlockCtl *prev = new_avail->GetPrevPtr(arena_);
    prev->link_next = new_avail->link_next;
    next->link_prev = new_avail->link_prev;
    if (rover_ == new_avail)
      rover_ = head_avail_;
  }

  int32_t succ_size = *reinterpret_cast<int32_t *>(
    reinterpret_cast<char *>(new_avail) + new_size);
  if (succ_size >= 0) {
    // Merge with succeeding block and remove the block from the list
    AvailBlockCtl *succ_avail = reinterpret_cast<AvailBlockCtl *>(
      reinterpret_cast<char *>(new_avail) + new_size);
    AvailBlockCtl *next = succ_avail->GetNextPtr(arena_);
    AvailBlockCtl *prev = succ_avail->GetPrevPtr(arena_);
    prev->link_next = succ_avail->link_next;
    next->link_prev = succ_avail->link_prev;
    new_size += succ_size;
    if (rover_ == succ_avail)
      rover_ = head_avail_;
  }

  // Insert new free block at the end of the list
  AvailBlockCtl *next = head_avail_;
  AvailBlockCtl *prev = head_avail_->GetPrevPtr(arena_);
  next->link_prev = new_avail->ConvertToLink(arena_);
  prev->link_next = new_avail->ConvertToLink(arena_);
  new_avail->link_next = head_avail_->ConvertToLink(arena_);
  new_avail->link_prev = prev->ConvertToLink(arena_);

  // Set new free block's boundaries
  new_avail->size = new_size;
  void *upper_tag =
    reinterpret_cast<char *>(new_avail) + new_size - sizeof(AvailBlockTag);
  new (upper_tag) AvailBlockTag(new_size);
}


/**
 * Walks the list of available blocks starting from rover and allocates the
 * first available spot that's large enough.  Puts the reserved block at the end
 * of the available one and, if necessary, removes the available one from the
 * list of free blocks.
 */
void *SqliteMemoryManager::MallocArena::Malloc(const uint32_t size) {
  assert(size > 0);

  // Control word first, block type tag last
  int32_t total_size = sizeof(ReservedBlockCtl) + size + 1;
  if (total_size < kMinBlockSize)
    total_size = kMinBlockSize;

  bool wrapped = false;
  // Generally: p = LINK(q)
  AvailBlockCtl *q = rover_;
  AvailBlockCtl *p;
  do {
    p = q->GetNextPtr(arena_);
    if (p->size >= total_size)
      break;
    if (p == head_avail_) {
      if (wrapped)
        return NULL;
      wrapped = true;
    }
    q = p;
  } while (true);

  // As of here: we found a sufficiently large free block at p following q
  no_reserved_++;

  int32_t remaining_size = p->size - total_size;
  // Avoid creation of very small blocks
  if (remaining_size < kMinBlockSize) {
    total_size += remaining_size;
    remaining_size = 0;
  }

  // Update the list of available blocks
  if (remaining_size == 0) {
    // Remove free block p from the list of available blocks
    q->link_next = p->link_next;
    q->GetNextPtr(arena_)->link_prev = q->ConvertToLink(arena_);
  } else {
    p->size = remaining_size;
    void *upper_tag =
      reinterpret_cast<char *>(p) + remaining_size - sizeof(AvailBlockTag);
    new (upper_tag) AvailBlockTag(remaining_size);
  }
  rover_ = q->GetNextPtr(arena_);

  // Place the new allocation, which also sets the block type tag at the end
  char *new_block = reinterpret_cast<char *>(p) + remaining_size;
  new (new_block) ReservedBlockCtl(total_size);
  return new_block + sizeof(ReservedBlockCtl);
}


/**
 * The arena starts with a pointer to this followed by the AvailBlockCtl of
 * head_avail_, followed by a reserved tag to prevent it from being merged,
 * followed by a free block spanning the arena until the end tag.  The end tag
 * is a single negative int, which mimics another reserved block.
 */
SqliteMemoryManager::MallocArena::MallocArena()
  : arena_(reinterpret_cast<char *>(sxmmap_align(kArenaSize)))
  , head_avail_(reinterpret_cast<AvailBlockCtl *>(arena_ + sizeof(void *)))
  , rover_(head_avail_)
  , no_reserved_(0)
{
  int32_t usable_size = kArenaSize -
    (sizeof(void *) + sizeof(AvailBlockCtl) + 1 + sizeof(int32_t));
  *reinterpret_cast<MallocArena **>(arena_) = this;
  AvailBlockCtl *free_block =
    new (arena_ + sizeof(void *) + sizeof(AvailBlockCtl) + 1) AvailBlockCtl();
  free_block->size = usable_size;
  free_block->link_next = free_block->link_prev =
    head_avail_->ConvertToLink(arena_);

  head_avail_->size = 0;
  head_avail_->link_next = head_avail_->link_prev =
    free_block->ConvertToLink(arena_);

  // Prevent succeeding blocks from merging
  *(reinterpret_cast<char *>(free_block) - 1) = kTagReserved;
  // Final tag: reserved block marker
  *reinterpret_cast<int32_t *>(arena_ + kArenaSize - sizeof(int32_t)) = -1;
}


SqliteMemoryManager::MallocArena::~MallocArena() {
  sxunmap(arena_, kArenaSize);
}


//------------------------------------------------------------------------------


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
  return (reinterpret_cast<char *>(buffer) - reinterpret_cast<char *>(arena_)) <
         kArenaSize;
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
  ptrdiff_t nbuffer =
    (reinterpret_cast<char *>(buffer) - reinterpret_cast<char *>(arena_))
    / kBufferSize;
  assert(nbuffer < kBuffersPerArena);
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
  void *new_ptr = xMalloc(new_size);
  memcpy(new_ptr, ptr, new_size);
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
  return size;
}


int SqliteMemoryManager::xInit(void *app_data __attribute__((unused))) {
  return SQLITE_OK;
}


void SqliteMemoryManager::xShutdown(void *app_data __attribute__((unused))) {
}


void SqliteMemoryManager::AssignGlobalArenas() {
  if (assigned_) return;
  int retval;

  retval = sqlite3_config(SQLITE_CONFIG_SCRATCH, scratch_memory_,
                          kScratchSlotSize, kScratchNoSlots);
  assert(retval == SQLITE_OK);

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
  return MallocArena::GetMallocArena(ptr)->GetSize(ptr);
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
  MallocArena *M = new MallocArena();
  malloc_arenas_.push_back(M);
  p = M->Malloc(size);
  assert(p != NULL);
  return p;
}


SqliteMemoryManager::SqliteMemoryManager()
  : assigned_(false)
  , scratch_memory_(sxmmap(kScratchSize))
  , page_cache_memory_(sxmmap(kPageCacheSize))
  , idx_last_arena_(0)
{
  memset(&sqlite3_mem_vanilla_, 0, sizeof(sqlite3_mem_vanilla_));
  int retval = pthread_mutex_init(&lock_, NULL);
  assert(retval == 0);

  lookaside_buffer_arenas_.push_back(new LookasideBufferArena());
  malloc_arenas_.push_back(new MallocArena());

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
    retval = sqlite3_config(SQLITE_CONFIG_SCRATCH, NULL, 0, 0);
    assert(retval == SQLITE_OK);
    retval = sqlite3_config(SQLITE_CONFIG_PAGECACHE, NULL, 0, 0);
    assert(retval == SQLITE_OK);
    retval = sqlite3_config(SQLITE_CONFIG_MALLOC, &sqlite3_mem_vanilla_);
    assert(retval == SQLITE_OK);
  }

  sxunmap(scratch_memory_, kScratchSize);
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
  assert(false);
}


/**
 * Closes empty areas.
 */
void SqliteMemoryManager::PutMemory(void *ptr) {
  MallocArena *M = MallocArena::GetMallocArena(ptr);
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
    assert(false);
  }
}


/**
 * To be used after an sqlite database has been closed.
 */
void SqliteMemoryManager::ReleaseLookasideBuffer(void *buffer) {
  MutexLockGuard lock_guard(lock_);
  PutLookasideBuffer(buffer);
}
