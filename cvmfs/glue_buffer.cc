/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS


#include "glue_buffer.h"

#include <errno.h>
#include <poll.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "util/exception.h"
#include "util/logging.h"
#include "util/mutex.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

namespace glue {

PathStore &PathStore::operator=(const PathStore &other) {
  if (&other == this)
    return *this;

  delete string_heap_;
  CopyFrom(other);
  return *this;
}


PathStore::PathStore(const PathStore &other) { CopyFrom(other); }


void PathStore::CopyFrom(const PathStore &other) {
  map_ = other.map_;

  string_heap_ = new StringHeap(other.string_heap_->used());
  shash::Md5 empty_path = map_.empty_key();
  for (unsigned i = 0; i < map_.capacity(); ++i) {
    if (map_.keys()[i] != empty_path) {
      (map_.values() + i)->name = string_heap_->AddString(
          map_.values()[i].name.length(), map_.values()[i].name.data());
    }
  }
}


//------------------------------------------------------------------------------


void InodeTracker::InitLock() {
  lock_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


void InodeTracker::CopyFrom(const InodeTracker &other) {
  assert(other.version_ == kVersion);
  version_ = kVersion;
  path_map_ = other.path_map_;
  inode_ex_map_ = other.inode_ex_map_;
  inode_references_ = other.inode_references_;
  statistics_ = other.statistics_;
}


InodeTracker::InodeTracker() {
  version_ = kVersion;
  InitLock();
}


InodeTracker::InodeTracker(const InodeTracker &other) {
  CopyFrom(other);
  InitLock();
}


InodeTracker &InodeTracker::operator=(const InodeTracker &other) {
  if (&other == this)
    return *this;

  CopyFrom(other);
  return *this;
}


InodeTracker::~InodeTracker() {
  pthread_mutex_destroy(lock_);
  free(lock_);
}


//------------------------------------------------------------------------------

DentryTracker::DentryTracker() : version_(kVersion), is_active_(true) {
  pipe_terminate_[0] = pipe_terminate_[1] = -1;
  cleaning_interval_ms_ = -1;
  InitLock();
}


DentryTracker::~DentryTracker() {
  if (pipe_terminate_[1] >= 0) {
    char t = 'T';
    WritePipe(pipe_terminate_[1], &t, 1);
    pthread_join(thread_cleaner_, NULL);
    ClosePipe(pipe_terminate_);
  }
  pthread_mutex_destroy(lock_);
  free(lock_);
}


DentryTracker::DentryTracker(const DentryTracker &other) {
  CopyFrom(other);
  pipe_terminate_[0] = pipe_terminate_[1] = -1;
  cleaning_interval_ms_ = -1;
  InitLock();
}


DentryTracker &DentryTracker::operator=(const DentryTracker &other) {
  if (&other == this)
    return *this;

  Lock();
  CopyFrom(other);
  Unlock();
  return *this;
}


void DentryTracker::CopyFrom(const DentryTracker &other) {
  assert(other.version_ == kVersion);

  version_ = kVersion;
  statistics_ = other.statistics_;
  is_active_ = other.is_active_;
  entries_ = other.entries_;
}


DentryTracker *DentryTracker::Move() {
  Lock();
  DentryTracker *new_tracker = new DentryTracker(*this);
  statistics_.num_remove += entries_.size();
  entries_.Clear();
  Unlock();
  return new_tracker;
}


void DentryTracker::SpawnCleaner(unsigned interval_s) {
  assert(pipe_terminate_[0] == -1);
  cleaning_interval_ms_ = interval_s * 1000;
  if (cleaning_interval_ms_ == 0)
    cleaning_interval_ms_ = -1;
  MakePipe(pipe_terminate_);
  int retval = pthread_create(&thread_cleaner_, NULL, MainCleaner, this);
  assert(retval == 0);
}


void *DentryTracker::MainCleaner(void *data) {
  DentryTracker *tracker = reinterpret_cast<DentryTracker *>(data);
  LogCvmfs(kLogCvmfs, kLogDebug, "starting negative entry cache cleaner");

  struct pollfd watch_term;
  watch_term.fd = tracker->pipe_terminate_[0];
  watch_term.events = POLLIN | POLLPRI;
  int timeout_ms = tracker->cleaning_interval_ms_;
  ;
  uint64_t deadline = platform_monotonic_time() + timeout_ms / 1000;
  while (true) {
    watch_term.revents = 0;
    int retval = poll(&watch_term, 1, timeout_ms);
    if (retval < 0) {
      if (errno == EINTR) {
        if (timeout_ms >= 0) {
          uint64_t now = platform_monotonic_time();
          timeout_ms = (now > deadline) ? 0 : (deadline - now) * 1000;
        }
        continue;
      }
      abort();
    }
    timeout_ms = tracker->cleaning_interval_ms_;
    deadline = platform_monotonic_time() + timeout_ms / 1000;

    if (retval == 0) {
      LogCvmfs(kLogCvmfs, kLogDebug, "negative entry cleaner: pruning");
      tracker->Prune();
      continue;
    }

    assert(watch_term.revents != 0);

    char c = 0;
    ReadPipe(tracker->pipe_terminate_[0], &c, 1);
    assert(c == 'T');
    break;
  }
  LogCvmfs(kLogCvmfs, kLogDebug, "stopping negative entry cache cleaner");
  return NULL;
}


void DentryTracker::InitLock() {
  lock_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


void DentryTracker::Prune() {
  Lock();
  DoPrune(platform_monotonic_time());
  Unlock();
}


DentryTracker::Cursor DentryTracker::BeginEnumerate() {
  Entry *head = NULL;
  Lock();
  entries_.Peek(&head);
  return Cursor(head);
}


bool DentryTracker::NextEntry(Cursor *cursor, uint64_t *inode_parent,
                              NameString *name) {
  if (cursor->head == NULL)
    return false;
  if (cursor->pos >= entries_.size())
    return false;
  Entry *e = cursor->head + cursor->pos;
  *inode_parent = e->inode_parent;
  *name = e->name;
  cursor->pos++;
  return true;
}


void DentryTracker::EndEnumerate(Cursor * /* cursor */) { Unlock(); }


//------------------------------------------------------------------------------


PageCacheTracker::PageCacheTracker() : version_(kVersion), is_active_(true) {
  map_.Init(16, 0, hasher_inode);
  InitLock();
}


PageCacheTracker::~PageCacheTracker() {
  pthread_mutex_destroy(lock_);
  free(lock_);
}


PageCacheTracker::PageCacheTracker(const PageCacheTracker &other) {
  CopyFrom(other);
  InitLock();
}


PageCacheTracker &PageCacheTracker::operator=(const PageCacheTracker &other) {
  if (&other == this)
    return *this;

  MutexLockGuard guard(lock_);
  CopyFrom(other);
  return *this;
}


void PageCacheTracker::CopyFrom(const PageCacheTracker &other) {
  assert(other.version_ == kVersion);

  version_ = kVersion;
  is_active_ = other.is_active_;
  statistics_ = other.statistics_;

  map_.Init(16, 0, hasher_inode);
  map_ = other.map_;
  stat_store_ = other.stat_store_;
}


void PageCacheTracker::InitLock() {
  lock_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}

PageCacheTracker::OpenDirectives PageCacheTracker::Open(
    uint64_t inode, const shash::Any &hash, const struct stat &info) {
  assert(inode == info.st_ino);

  OpenDirectives open_directives;
  // Old behavior: always flush page cache on open
  if (!is_active_)
    return open_directives;

  if (inode != info.st_ino) {
    PANIC(kLogStderr | kLogDebug,
          "invalid entry on open: %" PRIu64 " with st_ino=%" PRIu64,
          " hash=%s size=%" PRIu64, inode, info.st_ino, hash.ToString().c_str(),
          info.st_size);
  }

  MutexLockGuard guard(lock_);

  Entry entry;
  bool retval = map_.Lookup(inode, &entry);
  if (!retval) {
    open_directives.keep_cache = true;
    open_directives.direct_io = false;
    statistics_.n_insert++;
    statistics_.n_open_cached++;

    entry.nopen = 1;
    entry.idx_stat = stat_store_.Add(info);
    entry.hash = hash;
    map_.Insert(inode, entry);
    return open_directives;
  }

  if (entry.hash == hash) {
    open_directives.direct_io = false;
    if (entry.nopen < 0) {
      // The page cache is still in the transition phase and may contain old
      // content.  So trigger a flush of the cache in any case.
      open_directives.keep_cache = false;
      statistics_.n_open_flush++;
      entry.nopen--;
      map_.Insert(inode, entry);
      return open_directives;
    } else {
      open_directives.keep_cache = true;
      statistics_.n_open_cached++;
      if (entry.nopen++ == 0)
        entry.idx_stat = stat_store_.Add(info);
      map_.Insert(inode, entry);
      return open_directives;
    }
  }

  // Page cache mismatch and old data has still open file attached to it,
  // circumvent the page cache entirely and use direct I/O.  In this case,
  // cvmfs_close() will _not_ call Close().
  if (entry.nopen != 0) {
    open_directives.keep_cache = true;
    open_directives.direct_io = true;
    statistics_.n_open_direct++;
    return open_directives;
  }

  // Stale data in the page cache, start the transition phase in which newly
  // opened files flush the page cache and re-populate it with the new hash.
  // The first file to reach Close() will finish the transition phase and
  // mark the new hash as committed.
  open_directives.direct_io = false;
  open_directives.keep_cache = false;
  statistics_.n_open_flush++;
  entry.hash = hash;
  entry.idx_stat = stat_store_.Add(info);
  entry.nopen = -1;
  map_.Insert(inode, entry);
  return open_directives;
}

PageCacheTracker::OpenDirectives PageCacheTracker::OpenDirect() {
  OpenDirectives open_directives(true, true);
  // Old behavior: always flush page cache on open
  if (!is_active_)
    return open_directives;

  MutexLockGuard guard(lock_);
  statistics_.n_open_direct++;
  return open_directives;
}

void PageCacheTracker::Close(uint64_t inode) {
  if (!is_active_)
    return;

  MutexLockGuard guard(lock_);
  Entry entry;
  bool retval = map_.Lookup(inode, &entry);

  if (!AssertOrLog(retval, kLogCvmfs, kLogSyslogWarn | kLogDebug,
                   "PageCacheTracker::Close Race condition? "
                   "Did not find inode %lu",
                   inode)
      || !AssertOrLog(entry.nopen != 0, kLogCvmfs, kLogSyslogWarn | kLogDebug,
                      "PageCacheTracker::Close Race condition? "
                      "Inode %lu has no open entries",
                      inode)) {
    return;
  }

  const int32_t old_open = entry.nopen;
  if (entry.nopen < 0) {
    // At this point we know that any stale data has been flushed from the
    // cache and only data related to the currently booked content hash
    // can be present. So clear the transition bit (sign bit).
    entry.nopen = -entry.nopen;
  }
  entry.nopen--;
  if (entry.nopen == 0) {
    // File closed, remove struct stat information
    if (entry.idx_stat < 0) {
      PANIC(kLogSyslogErr | kLogDebug,
            "page cache tracker: missing stat entry! Entry info: inode %" PRIu64
            "  -  open counter %d  -  hash %s",
            inode, old_open, entry.hash.ToString().c_str());
    }
    uint64_t inode_update = stat_store_.Erase(entry.idx_stat);
    Entry entry_update;
    retval = map_.Lookup(inode_update, &entry_update);
    if (!retval) {
      PANIC(kLogSyslogErr | kLogDebug,
            "invalid inode in page cache tracker: inode %" PRIu64
            ", replacing %" PRIu64,
            inode_update, inode);
    }
    assert(retval);
    entry_update.idx_stat = entry.idx_stat;
    map_.Insert(inode_update, entry_update);
    entry.idx_stat = -1;
  }
  map_.Insert(inode, entry);
}

PageCacheTracker::EvictRaii::EvictRaii(PageCacheTracker *t) : tracker_(t) {
  int retval = pthread_mutex_lock(tracker_->lock_);
  assert(retval == 0);
}

PageCacheTracker::EvictRaii::~EvictRaii() {
  int retval = pthread_mutex_unlock(tracker_->lock_);
  assert(retval == 0);
}

void PageCacheTracker::EvictRaii::Evict(uint64_t inode) {
  if (!tracker_->is_active_)
    return;

  bool contained_inode = tracker_->map_.Erase(inode);
  if (contained_inode)
    tracker_->statistics_.n_remove++;
}

}  // namespace glue
