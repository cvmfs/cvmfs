/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "glue_buffer.h"

#include <inttypes.h>

#include <cstdlib>
#include <cstring>
#include <cassert>

#include "smalloc.h"
#include "logging.h"

using namespace std;  // NOLINT


void GlueBuffer::InitLock() {
  rwlock_ =
    reinterpret_cast<pthread_rwlock_t *>(smalloc(sizeof(pthread_rwlock_t)));
  int retval = pthread_rwlock_init(rwlock_, NULL);
  assert(retval == 0);
}


GlueBuffer::GlueBuffer(const unsigned size) {
  assert(size >= 2);
  version_ = kVersion;
  size_ = size;
  buffer_ = new BufferEntry[size_];
  atomic_init64(&buffer_pos_);
  
  InitLock();
}


void GlueBuffer::CopyFrom(const GlueBuffer &other) {
  if (other.version_ > kVersion)
    abort();
  
  version_ = kVersion;
  size_ = other.size_;
  buffer_ = new BufferEntry[size_];
  for (unsigned i = 0; i < size_; ++i)
    buffer_[i] = other.buffer_[i];
  buffer_pos_ = other.buffer_pos_;
}


GlueBuffer::GlueBuffer(const GlueBuffer &other) {
  CopyFrom(other);
  InitLock();
}


GlueBuffer &GlueBuffer::operator= (const GlueBuffer &other) {
  if (&other == this)
    return *this;

  delete[] buffer_;
  CopyFrom(other);
  return *this;
}


GlueBuffer::~GlueBuffer() {
  delete[] buffer_;
  pthread_rwlock_destroy(rwlock_);
  free(rwlock_);
}


void GlueBuffer::Resize(const unsigned new_size) {
  if (size_ == new_size)
    return;
  
  assert(new_size >= 2);
  BufferEntry *new_buffer = new BufferEntry[new_size];

  WriteLock();
  unsigned num_entries = size_ > new_size ? new_size : size_;
  for (unsigned i = 0; i < num_entries; ++i) {
    int64_t from_pos = 
      ((int64_t)(buffer_pos_) - (int64_t)(num_entries-i)) % size_;
    if (from_pos < 0)
      from_pos = size_ - (-from_pos);
    new_buffer[i] = buffer_[from_pos];
  }
  delete[] buffer_;
  buffer_ = new_buffer;
  if (buffer_pos_ >= new_size)
    buffer_pos_ = num_entries;
  size_ = new_size;
  Unlock();
  
  // TODO
}


bool GlueBuffer::ConstructPath(const unsigned buffer_idx, PathString *path) {
  // Root inode found?
  if (buffer_[buffer_idx].name.IsEmpty())
    return true;
  
  // Construct path until buffer_idx
  LogCvmfs(kLogCvmfs, kLogDebug, "construct inode %u, parent %u, name %s", 
           buffer_[buffer_idx].inode, buffer_[buffer_idx].parent_inode, 
           buffer_[buffer_idx].name.c_str());
  uint32_t needle_generation = buffer_[buffer_idx].generation;
  uint64_t needle_inode = buffer_[buffer_idx].parent_inode;
  int parent_idx = -1;
  for (unsigned i = 0; i < size_; ++i) {
    if ((buffer_[i].inode == needle_inode) && 
        (buffer_[i].generation == needle_generation))
    {
      parent_idx = i;
      break;
    }
  }
  if (parent_idx < 0)
    return false;
  
  bool retval = ConstructPath(parent_idx, path);
  path->Append("/", 1);
  path->Append(buffer_[buffer_idx].name.GetChars(), 
               buffer_[buffer_idx].name.GetLength());  
  return retval;
}


bool GlueBuffer::AncientInode2Path(const uint64_t inode, 
                                   const uint32_t current_generation,
                                   PathString *path)
{
  assert(path->IsEmpty());
  WriteLock();
  
  // Find inode with highest revision < new_revision
  unsigned max_generation = 0;
  int index = -1;
  for (unsigned i = 0; i < size_; ++i) {
    //LogCvmfs(kLogCvmfs, kLogDebug, "GLUE: idx %d, inode %u, parent %u, "
    //         "revision %u, name %s",
    //         i, buffer_[i].inode, buffer_[i].parent_inode, buffer_[i].revision, 
    //         buffer_[i].name.c_str());
    if ((buffer_[i].inode == inode) && 
        ((buffer_[i].generation < current_generation)) &&
        (buffer_[i].generation >= max_generation))
    {
      max_generation = buffer_[i].generation;
      index = i;
    }
  }
  if (index < 0) {
    LogCvmfs(kLogCvmfs, kLogDebug, "failed to find initial needle for "
             "ancient inode %"PRIu64, inode);
    Unlock();
    atomic_inc64(&statistics_.num_ancient_misses);
    return false;
  }
  
  // Recursively build path
  bool retval = ConstructPath(index, path);
  Unlock();
  
  if (retval) {
    atomic_inc64(&statistics_.num_ancient_hits);
    return true;
  }
  atomic_inc64(&statistics_.num_ancient_misses);
  return false;
}
