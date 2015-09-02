/**
 * This file is part of the CernVM File System.
 */

#include "pack.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <map>

#include "platform.h"
#include "smalloc.h"
#include "util.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

ObjectPack::Bucket::Bucket()
  : content(reinterpret_cast<unsigned char *>(smalloc(kInitialSize)))
  , size(0)
  , capacity(kInitialSize)
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

  header_ = "V1\n";
  header_ += "S" + StringifyInt(pack->size_) + "\n";
  header_ += "N" + StringifyInt(N) + "\n";
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

  header_ = "V1\n";
  header_ += "S" + str_size + "\n";
  header_ += "N1\n";
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
  , pos_(0)
  , idx_(0)
  , pos_in_object_(0)
  , pos_in_accu_(0)
  , state_(kStateContinue)
  , size_(0)
{
  // Upper limit of 100B per entry
  if (expected_header_size > (100 * ObjectPack::kMaxObjects)) {
    state_ = kStateHeaderTooBig;
    return;
  }

  raw_header_.reserve(expected_header_size);
}

/**
 * At the end of the function, pos_ will have progressed by buf_size (unless
 * the buffer contains trailing garbage bytes.
 */
ObjectPackConsumerBase::BuildState ObjectPackConsumer::ConsumeNext(
  const unsigned buf_size,
  const unsigned char *buf)
{
  if (buf_size == 0)
    return state_;
  if (state_ == kStateDone) {
    state_ = kStateTrailingBytes;
    return state_;
  }
  if (state_ != kStateContinue)
    return state_;

  const unsigned remaining_in_header =
    (pos_ < expected_header_size_) ? (expected_header_size_ - pos_) : 0;
  const unsigned nbytes_header = std::min(remaining_in_header, buf_size);
  if (nbytes_header) {
    raw_header_ += string(reinterpret_cast<const char *>(buf), nbytes_header);
    pos_ += nbytes_header;
  }

  if (pos_ < expected_header_size_)
    return kStateContinue;

  // This condition can only be true once through the lifetime of the Consumer.
  if (nbytes_header && (pos_ == expected_header_size_)) {
    shash::Any digest(expected_digest_.algorithm);
    shash::HashString(raw_header_, &digest);
    if (digest != expected_digest_) {
      state_ = kStateCorrupt;
      return state_;
    } else {
      bool retval = ParseHeader();
      if (!retval) {
        state_ = kStateBadFormat;
        return state_;
      }
      // We don't need the raw string anymore
      raw_header_.clear();
    }

    // Empty pack?
    if ((buf_size == nbytes_header) && (index_.size() == 0)) {
      state_ = kStateDone;
      return state_;
    }
  }

  unsigned remaining_in_buf = buf_size - nbytes_header;
  const unsigned char *payload = buf + nbytes_header;
  return ConsumePayload(remaining_in_buf, payload);
}


/**
 * Informs listeners for small complete objects.  For large objects, buffers the
 * input into reasonably sized chunks.  buf can contain both a chunk of data
 * that needs to be added to the consumer's accumulator and a bunch of
 * complete small objects.  We use the accumulator only if necessary to avoid
 * unnecessary memory copies.
 */
ObjectPackConsumerBase::BuildState ObjectPackConsumer::ConsumePayload(
  const unsigned buf_size,
  const unsigned char *buf)
{
  uint64_t pos_in_buf = 0;
  while ((pos_in_buf < buf_size) && (idx_ < index_.size())) {
    // Fill the accumulator or process next small object
    uint64_t nbytes;  // How many bytes are consumed in this iteration
    const uint64_t remaining_in_buf = buf_size - pos_in_buf;
    const uint64_t remaining_in_object = index_[idx_].size - pos_in_object_;
    const bool is_small_rest = remaining_in_buf < kAccuSize;

    // We use the accumulator if there is already something in or if we have a
    // small piece of data of a larger object.
    if ((pos_in_accu_ > 0) ||
        ((remaining_in_buf < remaining_in_object) && is_small_rest))
    {
      const uint64_t remaining_in_accu = kAccuSize - pos_in_accu_;
      nbytes = std::min(remaining_in_accu, remaining_in_buf);
      memcpy(accumulator_ + pos_in_accu_, buf + pos_in_buf, nbytes);
      pos_in_accu_ += nbytes;
      if ((pos_in_accu_ == kAccuSize) || (nbytes == remaining_in_object)) {
        NotifyListeners(BuildEvent(index_[idx_].id, index_[idx_].size,
                                   pos_in_accu_, accumulator_));
        pos_in_accu_ = 0;
      }
    } else {  // directly trigger listeners using buf
      nbytes = std::min(remaining_in_object, remaining_in_buf);
      NotifyListeners(BuildEvent(index_[idx_].id, index_[idx_].size,
                                 nbytes, buf + pos_in_buf));
    }

    pos_in_buf += nbytes;
    pos_in_object_ += nbytes;
    if (nbytes == remaining_in_object) {
      idx_++;
      pos_in_object_ = 0;
    }
  }

  pos_ += buf_size;

  if (idx_ == index_.size())
    state_ = (pos_in_buf == buf_size) ? kStateDone : kStateTrailingBytes;
  else
    state_ = kStateContinue;
  return state_;
}


bool ObjectPackConsumer::ParseHeader() {
  map<char, string> header;
  const unsigned char *data = reinterpret_cast<const unsigned char *>(
    raw_header_.data());
  ParseKeyvalMem(data, raw_header_.size(), &header);
  if (header.find('V') == header.end())
    return false;
  if (header['V'] != "1")
    return false;
  size_ = String2Uint64(header['S']);
  unsigned nobjects = String2Uint64(header['N']);

  if (nobjects == 0)
    return true;

  // Build the object index
  size_t separator_idx = raw_header_.find("--\n");
  if (separator_idx == string::npos)
    return false;
  unsigned index_idx = separator_idx + 3;
  if (index_idx >= raw_header_.size())
    return false;

  uint64_t sum_size = 0;
  do {
    const unsigned remaining_in_header = raw_header_.size() - index_idx;
    string line =
      GetLineMem(raw_header_.data() + index_idx, remaining_in_header);
    if (line == "")
      break;

    // We could use SplitString but we can have many lines so we do something
    // more efficient here
    separator_idx = line.find_first_of(' ');
    if ( (separator_idx == 0) || (separator_idx == string::npos) ||
         (separator_idx == (line.size() - 1)) )
    {
      return false;
    }
    uint64_t size = String2Uint64(line.substr(separator_idx + 1));
    sum_size += size;
    const IndexEntry index_entry(
      shash::MkFromSuffixedHexPtr(shash::HexPtr(line.substr(0, separator_idx))),
      size);
    index_.push_back(index_entry);

    index_idx += line.size() + 1;
  } while (index_idx < raw_header_.size());

  return (nobjects == index_.size()) && (size_ == sum_size);
}
