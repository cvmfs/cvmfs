/**
 * This file is part of the CernVM File System.
 */

#include "pack.h"

#include <algorithm>
#include <cassert>
#include <cstring>

#include "platform.h"
#include "smalloc.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

ObjectPack::Bucket::Bucket()
  : content(reinterpret_cast<unsigned char *>(smalloc(128)))
  , size(0)
  , capacity(128)
{ }


void ObjectPack::Bucket::Add(const void *buf, const uint64_t buf_size) {
  if (buf_size == 0)
    return;

  while (size + buf_size > capacity) {
    capacity *= 2;
    content = reinterpret_cast<unsigned char *>(srealloc(content, capacity));
  }
  memcpy(content + size, buf, buf_size);
  size += buf_size;
}


ObjectPack::Bucket::~Bucket() {
  free(content);
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
  handle->id = id;

  MutexLockGuard mutex_guard(lock_);
  if (buckets_.size() >= kMaxObjects)
    return false;
  if (size_ + handle->size > limit_)
    return false;
  open_buckets_.erase(handle);
  buckets_.push_back(handle);
  size_ += handle->size;
  return true;
}


void ObjectPack::DiscardBucket(const BucketHandle handle) {
  MutexLockGuard mutex_guard(lock_);
  open_buckets_.erase(handle);
  delete handle;
}


void ObjectPack::InitLock() {
  lock_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


ObjectPack::ObjectPack() : limit_(kDefaultLimit), size_(0) {
  InitLock();
}


ObjectPack::ObjectPack(const uint64_t limit) : limit_(limit), size_(0) {
  InitLock();
}


ObjectPack::~ObjectPack() {
  for (std::set<BucketHandle>::const_iterator i = open_buckets_.begin(),
       iEnd = open_buckets_.end(); i != iEnd; ++i)
  {
    delete *i;
  }

  for (unsigned i = 0; i < buckets_.size(); ++i)
    delete buckets_[i];
  pthread_mutex_destroy(lock_);
  free(lock_);
}


ObjectPack::BucketHandle ObjectPack::OpenBucket() {
  BucketHandle handle = new Bucket();

  MutexLockGuard mutex_guard(lock_);
  open_buckets_.insert(handle);
  return handle;
}


/**
 * If a commit failed, an open Bucket can be transferred to another ObjectPack
 * with more space.
 */
void ObjectPack::TransferBucket(
  const ObjectPack::BucketHandle handle,
  ObjectPack *other)
{
  MutexLockGuard mutex_guard(lock_);
  open_buckets_.erase(handle);
  other->open_buckets_.insert(handle);
}


//------------------------------------------------------------------------------


/**
 * Hash over the header.  The hash algorithm needs to be provided by hash.
 */
void ObjectPackProducer::GetDigest(shash::Any *hash) {
  assert(hash);
  shash::HashString(header_, hash);
}


ObjectPackProducer::ObjectPackProducer(ObjectPack *pack)
  : pack_(pack)
  , big_file_(NULL)
  , pos_(0)
  , idx_(0)
  , pos_in_bucket_(0)
{
  unsigned N = pack->buckets_.size();
  // rough guess, most likely a little too much
  header_.reserve(30 + N * (2 * shash::kMaxDigestSize + 5));

  header_ = "V 1\n";
  header_ += "S " + StringifyInt(pack->size_) + "\n";
  header_ += "N " + StringifyInt(N) + "\n";
  header_ += "--\n";

  const bool with_suffix = true;
  for (unsigned i = 0; i < N; ++i) {
    header_ += pack->buckets_[i]->id.ToString(with_suffix);
    header_ += " ";
    header_ += StringifyInt(pack->buckets_[i]->size);
    header_ += "\n";
  }
}


ObjectPackProducer::ObjectPackProducer(const shash::Any &id, FILE *big_file)
  : pack_(NULL)
  , big_file_(big_file)
  , pos_(0)
  , idx_(0)
  , pos_in_bucket_(0)
{
  int fd = fileno(big_file_);
  assert(fd >= 0);
  platform_stat64 info;
  int retval = platform_fstat(fd, &info);
  assert(retval == 0);
  string str_size = StringifyInt(info.st_size);

  header_ = "V 1\n";
  header_ += "S " + str_size + "\n";
  header_ += "N 1\n";
  header_ += "--\n";

  const bool with_suffix = true;
  header_ += id.ToString(with_suffix) + " " + str_size + "\n";

  rewind(big_file);
}


/**
 * Copies as many bytes as possible into buf.  If the returned number of bytes
 * is shorter than buf_size, everything has been produced.
 */
unsigned ObjectPackProducer::ProduceNext(
  const unsigned buf_size,
  unsigned char *buf)
{
  const unsigned remaining_in_header =
    (pos_ < header_.size()) ? (header_.size() - pos_) : 0;
  const unsigned nbytes_header = std::min(remaining_in_header, buf_size);
  if (nbytes_header) {
    memcpy(buf, header_.data() + pos_, nbytes_header);
    pos_ += nbytes_header;
  }

  unsigned remaining_in_buf = buf_size - nbytes_header;
  if (remaining_in_buf == 0)
    return nbytes_header;
  unsigned nbytes_payload = 0;

  if (big_file_) {
    size_t nbytes = fread(buf + nbytes_header, 1, remaining_in_buf, big_file_);
    nbytes_payload = nbytes;
    pos_ += nbytes_payload;
  } else if (idx_ < pack_->buckets_.size()) {
    // Copy a few buckets more
    while ((remaining_in_buf) > 0 && (idx_ < pack_->buckets_.size())) {
      const unsigned remaining_in_bucket =
        pack_->buckets_[idx_]->size - pos_in_bucket_;
      const unsigned nbytes = std::min(remaining_in_buf, remaining_in_bucket);
      memcpy(buf + nbytes_header + nbytes_payload,
             pack_->buckets_[idx_]->content + pos_in_bucket_,
             nbytes);

      pos_in_bucket_ += nbytes;
      nbytes_payload += nbytes;
      remaining_in_buf -= nbytes;
      if (nbytes == remaining_in_bucket) {
        pos_in_bucket_ = 0;
        idx_++;
      }
    }
  }

  return nbytes_header + nbytes_payload;
}


//------------------------------------------------------------------------------


ObjectPackConsumer::ObjectPackConsumer(
  const shash::Any &expected_digest,
  const unsigned expected_header_size)
  : expected_digest_(expected_digest)
  , expected_header_size_(expected_header_size)
  , state_(kStateContinue)
{
  // Upper limit of 100B per entry
  if (expected_header_size > (100 * ObjectPack::kMaxObjects)) {
    state_ = kStateTooBig;
    return;
  }

  header_.reserve(expected_header_size);
}


ObjectPackConsumerBase::BuildState ObjectPackConsumer::ConsumeNext(
  const unsigned buf_size,
  const char *buf)
{
  if (state_ != kStateContinue)
    return state_;

  const unsigned remaining_in_header =
    (pos_ < expected_header_size_) ? (expected_header_size_ - pos_) : 0;
  const unsigned nbytes_header = std::min(remaining_in_header, buf_size);
  if (nbytes_header) {
    header_ += string(buf + pos_, nbytes_header);
    pos_ += nbytes_header;
  }

  if (pos_ == expected_header_size_) {
    shash::Any digest(expected_digest_.algorithm);
    shash::HashString(header_, &digest);
    if (digest != expected_digest_) {
      state_ = kStateCorrupt;
      return state_;
    } else {
      bool retval = InterpretHeader();
      if (!retval) {
        state_ = kStateBadFormat;
        return state_;
      }
      // We don't need the raw string anymore
      header_.clear();
    }
  }

  return kStateContinue;
  /*unsigned remaining_in_buf = buf_size - nbytes_header;
  if (remaining_in_buf == 0) {
    return nbytes_header;
  }
  unsigned nbytes_payload = 0;*/
}


bool ObjectPackConsumer::InterpretHeader() {
  return false;
}
