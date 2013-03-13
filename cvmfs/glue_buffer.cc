/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "glue_buffer.h"

#include <cstdlib>
#include <cstring>
#include <cassert>

#include "smalloc.h"

using namespace std;  // NOLINT


void GlueBuffer::InitLock() {
  rwlock_ =
    reinterpret_cast<pthread_rwlock_t *>(smalloc(sizeof(pthread_rwlock_t)));
  int retval = pthread_rwlock_init(rwlock_, NULL);
  assert(retval == 0);
}


GlueBuffer::GlueBuffer(const unsigned size) {
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
  const unsigned num_bytes = size_ * sizeof(BufferEntry);
  buffer_ = new BufferEntry[size_];
  memcpy(buffer_, other.buffer_, num_bytes);
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
  
  // TODO
}


bool GlueBuffer::ConstructPath(const unsigned buffer_idx, PathString *path) {
  // Root inode found?
  if (buffer_[buffer_idx].name.IsEmpty())
    return true;
  
  // Construct path until buffer_idx
  uint32_t needle_revision = buffer_[buffer_idx].revision;
  uint64_t needle_inode = buffer_[buffer_idx].parent_inode;
  int parent_idx = -1;
  for (unsigned i = 0; i < size_; ++i) {
    if ((buffer_[i].inode == needle_inode) && 
        (buffer_[i].revision == needle_revision))
    {
      parent_idx = i;
      break;
    }
  }
  if (parent_idx < 0)
    return false;
  bool retval = ConstructPath(parent_idx, path);
  if (!retval)
    return false;
  
  path->Append("/", 1);
  path->Append(buffer_[buffer_idx].name.GetChars(), 
               buffer_[buffer_idx].name.GetLength());
  return true;
}


bool GlueBuffer::AncientInode2Path(const uint64_t inode, 
                                   const uint32_t current_revision,
                                   PathString *path)
{
  assert(path->IsEmpty());
  WriteLock();
  
  // Find inode with highest revision < new_revision
  unsigned max_revision = 0;
  int index = -1;
  for (unsigned i = 0; i < size_; ++i) {
    if ((buffer_[i].inode == inode) && 
        (buffer_[i].revision < current_revision) &&
        (buffer_[i].revision > max_revision))
    {
      max_revision = buffer_[i].revision;
      index = i;
    }
  }
  if (index < 0) {
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
