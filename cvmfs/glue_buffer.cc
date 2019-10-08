/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "glue_buffer.h"

#include <errno.h>
#include <poll.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <cstring>

#include <string>
#include <vector>

#include "logging.h"
#include "platform.h"
#include "smalloc.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace glue {

PathStore &PathStore::operator= (const PathStore &other) {
  if (&other == this)
    return *this;

  delete string_heap_;
  CopyFrom(other);
  return *this;
}


PathStore::PathStore(const PathStore &other) {
  CopyFrom(other);
}


void PathStore::CopyFrom(const PathStore &other) {
  map_ = other.map_;

  string_heap_ = new StringHeap(other.string_heap_->used());
  shash::Md5 empty_path = map_.empty_key();
  for (unsigned i = 0; i < map_.capacity(); ++i) {
    if (map_.keys()[i] != empty_path) {
      (map_.values() + i)->name =
      string_heap_->AddString(map_.values()[i].name.length(),
                              map_.values()[i].name.data());
    }
  }
}


//------------------------------------------------------------------------------


void InodeTracker::InitLock() {
  lock_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


void InodeTracker::CopyFrom(const InodeTracker &other) {
  assert(other.version_ == kVersion);
  version_ = kVersion;
  path_map_ = other.path_map_;
  inode_map_ = other.inode_map_;
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


InodeTracker &InodeTracker::operator= (const InodeTracker &other) {
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

NentryTracker::NentryTracker() : version_(kVersion), is_active_(true) {
  pipe_terminate_[0] = pipe_terminate_[1] = -1;
  cleaning_interval_ms_ = -1;
  InitLock();
}


NentryTracker::~NentryTracker() {
  if (pipe_terminate_[1] >= 0) {
    char t = 'T';
    WritePipe(pipe_terminate_[1], &t, 1);
    pthread_join(thread_cleaner_, NULL);
    ClosePipe(pipe_terminate_);
  }
}


NentryTracker::NentryTracker(const NentryTracker &other) {
  CopyFrom(other);
  pipe_terminate_[0] = pipe_terminate_[1] = -1;
  cleaning_interval_ms_ = -1;
  InitLock();
}


NentryTracker &NentryTracker::operator= (const NentryTracker &other) {
  if (&other == this)
    return *this;

  Lock();
  CopyFrom(other);
  Unlock();
  return *this;
}


void NentryTracker::CopyFrom(const NentryTracker &other) {
  assert(other.version_ == kVersion);

  version_ = kVersion;
  statistics_ = other.statistics_;
  is_active_ = other.is_active_;
  entries_ = other.entries_;
}


NentryTracker *NentryTracker::Move() {
  Lock();
  NentryTracker *new_tracker = new NentryTracker(*this);
  statistics_.num_remove += entries_.size();
  entries_.Clear();
  Unlock();
  return new_tracker;
}


void NentryTracker::SpawnCleaner(unsigned interval_s) {
  assert(pipe_terminate_[0] == -1);
  cleaning_interval_ms_ = interval_s * 1000;
  if (cleaning_interval_ms_ == 0) cleaning_interval_ms_ = -1;
  MakePipe(pipe_terminate_);
  int retval = pthread_create(&thread_cleaner_, NULL, MainCleaner, this);
  assert(retval == 0);
}


void *NentryTracker::MainCleaner(void *data) {
  NentryTracker *tracker = reinterpret_cast<NentryTracker *>(data);
  LogCvmfs(kLogCvmfs, kLogDebug, "starting negative entry cache cleaner");

  struct pollfd watch_term;
  watch_term.fd = tracker->pipe_terminate_[0];
  watch_term.events = POLLIN | POLLPRI;
  int timeout_ms = tracker->cleaning_interval_ms_;;
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


void NentryTracker::InitLock() {
  lock_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


void NentryTracker::Prune() {
  Lock();
  DoPrune(platform_monotonic_time());
  Unlock();
}


NentryTracker::Cursor NentryTracker::BeginEnumerate() {
  Entry *head = NULL;
  Lock();
  entries_.Peek(&head);
  return Cursor(head);
}


bool NentryTracker::NextEntry(Cursor *cursor,
  uint64_t *inode_parent, NameString *name)
{
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


void NentryTracker::EndEnumerate(Cursor *cursor) {
  Unlock();
}

}  // namespace glue
