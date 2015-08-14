/**
 * This file is part of the CernVM File System.
 */

#include "pack.h"

#include <cassert>
#include <cstring>

#include "smalloc.h"

using namespace std;  // NOLINT

ObjectPack::Bucket::Bucket()
  : content_(reinterpret_cast<unsigned char *>(smalloc(128)))
  , size_(0)
  , capacity_(128)
{ }


void ObjectPack::Bucket::Add(const void *buf, const uint64_t buf_size) {
  while (size_ + buf_size > capacity_) {
    capacity_ *= 2;
    content_ = reinterpret_cast<unsigned char *>(srealloc(content_, capacity_));
  }
  memcpy(content_ + size_, buf, buf_size);
  size_ += buf_size;
}


ObjectPack::Bucket::~Bucket() {
  free(content_);
}


//------------------------------------------------------------------------------


void ObjectPack::AddToBucket(
  const void *buf,
  const uint64_t size,
  const ObjectPack::BucketHandle handle)
{
  handle->Add(buf, size);
}


/**
 * Can only fail due to insufficient remaining space in the ObjectPack.
 */
bool ObjectPack::CommitBucket(
  const shash::Any &id,
  const ObjectPack::BucketHandle handle)
{
  handle->SetId(id);

  if (size_ + handle->size() > limit_)
    return false;
  open_buckets_.erase(handle);
  buckets_.push_back(handle);
  return true;
}


void ObjectPack::DiscardBucket(const BucketHandle handle) {
  open_buckets_.erase(handle);
  delete handle;
}


/**
 * If a commit failed, an open Bucket can be transferred to another ObjectPack
 * with more space.
 */
void ObjectPack::TransferBucket(
  const ObjectPack::BucketHandle handle,
  ObjectPack *other)
{
  open_buckets_.erase(handle);
  other->open_buckets_.insert(handle);
}


ObjectPack::BucketHandle ObjectPack::OpenBucket() {
  BucketHandle handle = new Bucket();
  open_buckets_.insert(handle);
  return handle;
}


ObjectPack::~ObjectPack() {
  for (std::set<BucketHandle>::const_iterator i = open_buckets_.begin(),
       iEnd = open_buckets_.end(); i != iEnd; ++i)
  {
    delete *i;
  }

  for (unsigned i = 0; i < buckets_.size(); ++i)
    delete buckets_[i];
}
