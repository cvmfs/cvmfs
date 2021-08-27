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
#include "util/exception.h"
#include "util/string.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace {  // some private utility functions used by ObjectPackProducer

void InitializeHeader(const int version, const int num_objects,
                      const size_t pack_size, std::string *header) {
  if (header) {
    *header = "V" + StringifyInt(version) + "\n";
    *header += "S" + StringifyInt(pack_size) + "\n";
    *header += "N" + StringifyInt(num_objects) + "\n";
    *header += "--\n";
  }
}

void AppendItemToHeader(ObjectPack::BucketContentType object_type,
                        const std::string &hash_str, const size_t object_size,
                        const std::string &object_name, std::string *header) {
  // If the item type is kName, the "item_name" parameter should not be empty
  assert((object_type == ObjectPack::kCas) ||
         ((object_type == ObjectPack::kNamed) && (!object_name.empty())));
  std::string line_prefix = "";
  std::string line_suffix = "";
  switch (object_type) {
    case ObjectPack::kNamed:
      line_prefix = "N ";
      line_suffix = std::string(" ") + Base64Url(object_name);
      break;
    case ObjectPack::kCas:
      line_prefix = "C ";
      break;
    default:
      PANIC(kLogStderr, "Unknown object pack type to be added to header.");
  }
  if (header) {
    *header += line_prefix + hash_str + " " + StringifyInt(object_size) +
               line_suffix + "\n";
  }
}

}  // namespace

ObjectPack::Bucket::Bucket()
    : content(reinterpret_cast<unsigned char *>(smalloc(kInitialSize))),
      size(0),
      capacity(kInitialSize),
      content_type(kEmpty),
      name() {}

void ObjectPack::Bucket::Add(const void *buf, const uint64_t buf_size) {
  if (buf_size == 0) return;

  while (size + buf_size > capacity) {
    capacity *= 2;
    content = reinterpret_cast<unsigned char *>(srealloc(content, capacity));
  }
  memcpy(content + size, buf, buf_size);
  size += buf_size;
}

ObjectPack::Bucket::~Bucket() { free(content); }

//------------------------------------------------------------------------------

ObjectPack::ObjectPack(const uint64_t limit) : limit_(limit), size_(0) {
  InitLock();
}

ObjectPack::~ObjectPack() {
  for (std::set<BucketHandle>::const_iterator i = open_buckets_.begin(),
                                              iEnd = open_buckets_.end();
       i != iEnd; ++i) {
    delete *i;
  }

  for (unsigned i = 0; i < buckets_.size(); ++i) delete buckets_[i];
  pthread_mutex_destroy(lock_);
  free(lock_);
}

void ObjectPack::AddToBucket(const void *buf, const uint64_t size,
                             const ObjectPack::BucketHandle handle) {
  handle->Add(buf, size);
}

ObjectPack::BucketHandle ObjectPack::NewBucket() {
  BucketHandle handle = new Bucket();

  MutexLockGuard mutex_guard(lock_);
  open_buckets_.insert(handle);
  return handle;
}

/**
 * Can only fail due to insufficient remaining space in the ObjectPack.
 */
bool ObjectPack::CommitBucket(const BucketContentType type,
                              const shash::Any &id,
                              const ObjectPack::BucketHandle handle,
                              const std::string &name) {
  handle->id = id;

  handle->content_type = type;
  if (type == kNamed) {
    handle->name = name;
  }

  MutexLockGuard mutex_guard(lock_);
  if (buckets_.size() >= kMaxObjects) return false;
  if (size_ + handle->size > limit_) return false;
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
  lock_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}

/**
 * If a commit failed, an open Bucket can be transferred to another ObjectPack
 * with more space.
 */
void ObjectPack::TransferBucket(const ObjectPack::BucketHandle handle,
                                ObjectPack *other) {
  MutexLockGuard mutex_guard(lock_);
  open_buckets_.erase(handle);
  other->open_buckets_.insert(handle);
}

unsigned char *ObjectPack::BucketContent(size_t idx) const {
  assert(idx < buckets_.size());
  return buckets_[idx]->content;
}

uint64_t ObjectPack::BucketSize(size_t idx) const {
  assert(idx < buckets_.size());
  return buckets_[idx]->size;
}

const shash::Any &ObjectPack::BucketId(size_t idx) const {
  assert(idx < buckets_.size());
  return buckets_[idx]->id;
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
    : pack_(pack), big_file_(NULL), pos_(0), idx_(0), pos_in_bucket_(0) {
  unsigned N = pack->GetNoObjects();
  // rough guess, most likely a little too much
  header_.reserve(30 + N * (2 * shash::kMaxDigestSize + 5));

  InitializeHeader(2, N, pack->size(), &header_);

  for (unsigned i = 0; i < N; ++i) {
    AppendItemToHeader(ObjectPack::kCas, pack->BucketId(i).ToString(true),
                       pack->BucketSize(i), "", &header_);
  }
}

ObjectPackProducer::ObjectPackProducer(const shash::Any &id, FILE *big_file,
                                       const std::string &file_name)
    : pack_(NULL), big_file_(big_file), pos_(0), idx_(0), pos_in_bucket_(0) {
  int fd = fileno(big_file_);
  assert(fd >= 0);
  platform_stat64 info;
  int retval = platform_fstat(fd, &info);
  assert(retval == 0);

  InitializeHeader(2, 1, info.st_size, &header_);

  AppendItemToHeader(ObjectPack::kNamed, id.ToString(true), info.st_size,
                     file_name, &header_);

  rewind(big_file);
}

/**
 * Copies as many bytes as possible into buf.  If the returned number of bytes
 * is shorter than buf_size, everything has been produced.
 */
unsigned ObjectPackProducer::ProduceNext(const unsigned buf_size,
                                         unsigned char *buf) {
  const unsigned remaining_in_header =
      (pos_ < header_.size()) ? (header_.size() - pos_) : 0;
  const unsigned nbytes_header = std::min(remaining_in_header, buf_size);
  if (nbytes_header) {
    memcpy(buf, header_.data() + pos_, nbytes_header);
    pos_ += nbytes_header;
  }

  unsigned remaining_in_buf = buf_size - nbytes_header;
  if (remaining_in_buf == 0) return nbytes_header;
  unsigned nbytes_payload = 0;

  if (big_file_) {
    size_t nbytes = fread(buf + nbytes_header, 1, remaining_in_buf, big_file_);
    nbytes_payload = nbytes;
    pos_ += nbytes_payload;
  } else if (idx_ < pack_->GetNoObjects()) {
    // Copy a few buckets more
    while ((remaining_in_buf) > 0 && (idx_ < pack_->GetNoObjects())) {
      const unsigned remaining_in_bucket =
          pack_->BucketSize(idx_) - pos_in_bucket_;
      const unsigned nbytes = std::min(remaining_in_buf, remaining_in_bucket);
      memcpy(buf + nbytes_header + nbytes_payload,
             pack_->BucketContent(idx_) + pos_in_bucket_, nbytes);

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

ObjectPackConsumer::ObjectPackConsumer(const shash::Any &expected_digest,
                                       const unsigned expected_header_size)
    : expected_digest_(expected_digest),
      expected_header_size_(expected_header_size),
      pos_(0),
      idx_(0),
      pos_in_object_(0),
      pos_in_accu_(0),
      state_(ObjectPackBuild::kStateContinue),
      size_(0) {
  // Upper limit of 100B per entry
  if (expected_header_size > (100 * ObjectPack::kMaxObjects)) {
    state_ = ObjectPackBuild::kStateHeaderTooBig;
    return;
  }

  raw_header_.reserve(expected_header_size);
}

/**
 * At the end of the function, pos_ will have progressed by buf_size (unless
 * the buffer contains trailing garbage bytes.
 */
ObjectPackBuild::State ObjectPackConsumer::ConsumeNext(
    const unsigned buf_size, const unsigned char *buf) {
  if (buf_size == 0) return state_;
  if (state_ == ObjectPackBuild::kStateDone) {
    state_ = ObjectPackBuild::kStateTrailingBytes;
    return state_;
  }
  if (state_ != ObjectPackBuild::kStateContinue) return state_;

  const unsigned remaining_in_header =
      (pos_ < expected_header_size_) ? (expected_header_size_ - pos_) : 0;
  const unsigned nbytes_header = std::min(remaining_in_header, buf_size);
  if (nbytes_header) {
    raw_header_ += string(reinterpret_cast<const char *>(buf), nbytes_header);
    pos_ += nbytes_header;
  }

  if (pos_ < expected_header_size_) return ObjectPackBuild::kStateContinue;

  // This condition can only be true once through the lifetime of the
  // Consumer.
  if (nbytes_header && (pos_ == expected_header_size_)) {
    shash::Any digest(expected_digest_.algorithm);
    shash::HashString(raw_header_, &digest);
    if (digest != expected_digest_) {
      state_ = ObjectPackBuild::kStateCorrupt;
      return state_;
    } else {
      bool retval = ParseHeader();
      if (!retval) {
        state_ = ObjectPackBuild::kStateBadFormat;
        return state_;
      }
      // We don't need the raw string anymore
      raw_header_.clear();
    }

    // Empty pack?
    if ((buf_size == nbytes_header) && (index_.size() == 0)) {
      state_ = ObjectPackBuild::kStateDone;
      return state_;
    }
  }

  unsigned remaining_in_buf = buf_size - nbytes_header;
  const unsigned char *payload = buf + nbytes_header;
  return ConsumePayload(remaining_in_buf, payload);
}

/**
 * Informs listeners for small complete objects.  For large objects, buffers
 * the
 * input into reasonably sized chunks.  buf can contain both a chunk of data
 * that needs to be added to the consumer's accumulator and a bunch of
 * complete small objects.  We use the accumulator only if necessary to avoid
 * unnecessary memory copies.
 */
ObjectPackBuild::State ObjectPackConsumer::ConsumePayload(
    const unsigned buf_size, const unsigned char *buf) {
  uint64_t pos_in_buf = 0;
  while ((idx_ < index_.size()) &&
         ((pos_in_buf < buf_size) || (index_[idx_].size == 0))) {
    // Fill the accumulator or process next small object
    uint64_t nbytes;  // How many bytes are consumed in this iteration
    const uint64_t remaining_in_buf = buf_size - pos_in_buf;
    const uint64_t remaining_in_object = index_[idx_].size - pos_in_object_;
    const bool is_small_rest = remaining_in_buf < kAccuSize;

    // We use the accumulator if there is already something in or if we have a
    // small piece of data of a larger object.
    nbytes = std::min(remaining_in_object, remaining_in_buf);
    if ((pos_in_accu_ > 0) ||
        ((remaining_in_buf < remaining_in_object) && is_small_rest)) {
      const uint64_t remaining_in_accu = kAccuSize - pos_in_accu_;
      nbytes = std::min(remaining_in_accu, nbytes);
      memcpy(accumulator_ + pos_in_accu_, buf + pos_in_buf, nbytes);
      pos_in_accu_ += nbytes;
      if ((pos_in_accu_ == kAccuSize) || (nbytes == remaining_in_object)) {
        NotifyListeners(ObjectPackBuild::Event(
            index_[idx_].id, index_[idx_].size, pos_in_accu_, accumulator_,
            index_[idx_].entry_type, index_[idx_].entry_name));
        pos_in_accu_ = 0;
      }
    } else {  // directly trigger listeners using buf
      NotifyListeners(ObjectPackBuild::Event(
          index_[idx_].id, index_[idx_].size, nbytes, buf + pos_in_buf,
          index_[idx_].entry_type, index_[idx_].entry_name));
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
    state_ = (pos_in_buf == buf_size) ? ObjectPackBuild::kStateDone
                                      : ObjectPackBuild::kStateTrailingBytes;
  else
    state_ = ObjectPackBuild::kStateContinue;
  return state_;
}

bool ObjectPackConsumer::ParseHeader() {
  map<char, string> header;
  const unsigned char *data =
      reinterpret_cast<const unsigned char *>(raw_header_.data());
  ParseKeyvalMem(data, raw_header_.size(), &header);
  if (header.find('V') == header.end()) return false;
  if (header['V'] != "2") return false;
  size_ = String2Uint64(header['S']);
  unsigned nobjects = String2Uint64(header['N']);

  if (nobjects == 0) return true;

  // Build the object index
  const size_t separator_idx = raw_header_.find("--\n");
  if (separator_idx == string::npos) return false;
  unsigned index_idx = separator_idx + 3;
  if (index_idx >= raw_header_.size()) return false;

  uint64_t sum_size = 0;
  do {
    const unsigned remaining_in_header = raw_header_.size() - index_idx;
    string line =
        GetLineMem(raw_header_.data() + index_idx, remaining_in_header);
    if (line == "") break;

    IndexEntry entry;
    if (!ParseItem(line, &entry, &sum_size)) {
      break;
    }

    index_.push_back(entry);
    index_idx += line.size() + 1;
  } while (index_idx < raw_header_.size());

  return (nobjects == index_.size()) && (size_ == sum_size);
}

bool ObjectPackConsumer::ParseItem(const std::string &line,
                                   ObjectPackConsumer::IndexEntry *entry,
                                   uint64_t *sum_size) {
  if (!entry || !sum_size) {
    return false;
  }

  if (line[0] == 'C') {  // CAS blob
    const ObjectPack::BucketContentType entry_type = ObjectPack::kCas;

    // We could use SplitString but we can have many lines so we do something
    // more efficient here
    const size_t separator = line.find(' ', 2);
    if ((separator == string::npos) || (separator == (line.size() - 1))) {
      return false;
    }

    uint64_t size = String2Uint64(line.substr(separator + 1));
    *sum_size += size;

    // Warning do not construct a HexPtr with an rvalue!
    // The constructor takes the address of its argument.
    const std::string hash_string = line.substr(2, separator - 2);
    shash::HexPtr hex_ptr(hash_string);

    entry->id = shash::MkFromSuffixedHexPtr(hex_ptr);
    entry->size = size;
    entry->entry_type = entry_type;
    entry->entry_name = "";
  } else if (line[0] == 'N') {  // Named file
    const ObjectPack::BucketContentType entry_type = ObjectPack::kNamed;

    // First separator, before the size field
    const size_t separator1 = line.find(' ', 2);
    if ((separator1 == string::npos) || (separator1 == (line.size() - 1))) {
      return false;
    }

    // Second separator, before the name field
    const size_t separator2 = line.find(' ', separator1 + 1);
    if ((separator1 == 0) || (separator1 == string::npos) ||
        (separator1 == (line.size() - 1))) {
      return false;
    }

    uint64_t size =
        String2Uint64(line.substr(separator1 + 1, separator2 - separator1 - 1));

    std::string name;
    if (!Debase64(line.substr(separator2 + 1), &name)) {
      return false;
    }

    *sum_size += size;

    // Warning do not construct a HexPtr with an rvalue!
    // The constructor takes the address of its argument.
    const std::string hash_string = line.substr(2, separator1 - 2);
    shash::HexPtr hex_ptr(hash_string);

    entry->id = shash::MkFromSuffixedHexPtr(hex_ptr);
    entry->size = size;
    entry->entry_type = entry_type;
    entry->entry_name = name;
  } else {  // Error
    return false;
  }

  return true;
}
