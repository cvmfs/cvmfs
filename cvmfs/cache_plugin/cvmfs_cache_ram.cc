/**
 * This file is part of the CernVM File System.
 *
 * A cache plugin that stores all data in a fixed-size memory chunk.
 */

#define __STDC_FORMAT_MACROS

#include <alloca.h>
#include <inttypes.h>
#include <stdint.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "cache_plugin/libcvmfs_cache.h"
#include "logging.h"
#include "lru.h"
#include "malloc_heap.h"
#include "murmur.h"
#include "platform.h"
#include "smallhash.h"
#include "smalloc.h"
#include "util/string.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

/**
 * Header of the data pieces in the cache.  After the object header, the
 * zero-terminated description and the object data follows.
 */
struct ObjectHeader {
  ObjectHeader() {
    txn_id = uint64_t(-1);
    size_data = 0;
    size_desc = 0;
    refcnt = 0;
    type = CVMCACHE_OBJECT_REGULAR;
    memset(&id, 0, sizeof(id));
  }

  char *GetDescription() {
    if (size_desc == 0)
      return NULL;
    return reinterpret_cast<char *>(this) + sizeof(ObjectHeader);
  }

  void SetDescription(char *description) {
    if (description == NULL)
      return;
    memcpy(reinterpret_cast<char *>(this) + sizeof(ObjectHeader),
           description, strlen(description) + 1);
  }

  unsigned char *GetData() {
    return reinterpret_cast<unsigned char *>(this) +
           sizeof(ObjectHeader) + size_desc;
  }

  /**
   * Set during a running transaction so that we know where do look for pointers
   * when the memory block gets compacted.  Once committed, this is
   * uint64_t(-1).
   */
  uint64_t txn_id;
  /**
   * Can be zero.
   */
  uint32_t size_data;
  /**
   * String length + 1 (null terminated) or null if the description is NULL.
   */
  uint32_t size_desc;
  /**
   * During a transaction, neg_nbytes_written is used to track the number of
   * already written bytes.  On commit, refcnt is set to 1.
   */
  union {
    int32_t refcnt;
    int32_t neg_nbytes_written;
  };
  cvmcache_object_type type;
  struct cvmcache_hash id;
};


/**
 * Listings are generated and cached during the entire life time of a listing
 * id.  Not very memory efficient but we don't optimize for listings.
 */
struct Listing {
  Listing() : pos(0) { }
  uint64_t pos;
  vector<struct cvmcache_object_info> elems;
};


/**
 * Allows us to use a cvmcache_hash in (hash) maps.
 */
struct ComparableHash {
  ComparableHash() { }
  explicit ComparableHash(const struct cvmcache_hash &h) : hash(h) { }
  bool operator ==(const ComparableHash &other) const {
    return cvmcache_hash_cmp(const_cast<cvmcache_hash *>(&(this->hash)),
                             const_cast<cvmcache_hash *>(&(other.hash))) == 0;
  }
  bool operator !=(const ComparableHash &other) const {
    return cvmcache_hash_cmp(const_cast<cvmcache_hash *>(&(this->hash)),
                             const_cast<cvmcache_hash *>(&(other.hash))) != 0;
  }
  bool operator <(const ComparableHash &other) const {
    return cvmcache_hash_cmp(const_cast<cvmcache_hash *>(&(this->hash)),
                             const_cast<cvmcache_hash *>(&(other.hash))) < 0;
  }
  bool operator >(const ComparableHash &other) const {
    return cvmcache_hash_cmp(const_cast<cvmcache_hash *>(&(this->hash)),
                             const_cast<cvmcache_hash *>(&(other.hash))) > 0;
  }

  struct cvmcache_hash hash;
};


namespace {

static inline uint32_t hasher_uint64(const uint64_t &key) {
  return MurmurHash2(&key, sizeof(key), 0x07387a4f);
}

static inline uint32_t hasher_any(const ComparableHash &key) {
  return (uint32_t) *(reinterpret_cast<const uint32_t *>(&key.hash));
}

}  // anonymous namespace


/**
 * Used in the PluginRamCache when detaching nested catalogs.
 */
struct cvmcache_context *ctx;


/**
 * Implements all the cache plugin callbacks.  Singelton.
 */
class PluginRamCache : public Callbackable<MallocHeap::BlockPtr> {
 public:
  static PluginRamCache *Create(const string &mem_size_str) {
    assert(instance_ == NULL);

    uint64_t mem_size_bytes;
    if (HasSuffix(mem_size_str, "%", false)) {
      mem_size_bytes = platform_memsize() * String2Uint64(mem_size_str) / 100;
    } else {
      mem_size_bytes = String2Uint64(mem_size_str) * 1024 * 1024;
    }
    instance_ = new PluginRamCache(mem_size_bytes);
    return instance_;
  }

  ~PluginRamCache() {
    delete storage_;
    delete objects_all_;
    delete objects_volatile_;
    instance_ = NULL;
  }

  static int ram_chrefcnt(struct cvmcache_hash *id, int32_t change_by) {
    ComparableHash h(*id);
    ObjectHeader *object;
    if (!Me()->objects_all_->Lookup(h, &object))
      return CVMCACHE_STATUS_NOENTRY;

    if (object->type == CVMCACHE_OBJECT_VOLATILE)
      Me()->objects_volatile_->Update(h);

    if (change_by == 0)
      return CVMCACHE_STATUS_OK;
    if ((object->refcnt + change_by) < 0)
      return CVMCACHE_STATUS_BADCOUNT;

    if (object->refcnt == 0) {
      Me()->cache_info_.pinned_bytes += object->size_data;
      if (!Me()->in_danger_zone_ && Me()->IsInDangerZone()) {
        Me()->in_danger_zone_ = true;
        cvmcache_ask_detach(ctx);
      }
    }
    object->refcnt += change_by;
    if (object->refcnt == 0) {
      Me()->cache_info_.pinned_bytes -= object->size_data;
      Me()->in_danger_zone_ = Me()->IsInDangerZone();
    }
    return CVMCACHE_STATUS_OK;
  }


  static int ram_obj_info(
    struct cvmcache_hash *id,
    struct cvmcache_object_info *info)
  {
    ComparableHash h(*id);
    ObjectHeader *object;
    if (!Me()->objects_all_->Lookup(h, &object, false))
      return CVMCACHE_STATUS_NOENTRY;

    info->size = object->size_data;
    info->type = object->type;
    info->pinned = object->refcnt > 0;
    info->description = (object->GetDescription() == NULL)
                        ? NULL
                        : strdup(object->GetDescription());
    return CVMCACHE_STATUS_OK;
  }


  static int ram_pread(struct cvmcache_hash *id,
                      uint64_t offset,
                      uint32_t *size,
                      unsigned char *buffer)
  {
    ComparableHash h(*id);
    ObjectHeader *object;
    bool retval = Me()->objects_all_->Lookup(h, &object, false);
    assert(retval);
    if (offset > object->size_data)
      return CVMCACHE_STATUS_OUTOFBOUNDS;
    unsigned nbytes =
      std::min(*size, static_cast<uint32_t>(object->size_data - offset));
    memcpy(buffer, object->GetData() + offset, nbytes);
    *size = nbytes;
    return CVMCACHE_STATUS_OK;
  }


  static int ram_start_txn(
    struct cvmcache_hash *id,
    uint64_t txn_id,
    struct cvmcache_object_info *info)
  {
    ObjectHeader object_header;
    object_header.txn_id = txn_id;
    if (info->size != CVMCACHE_SIZE_UNKNOWN)
      object_header.size_data = info->size;
    else
      object_header.size_data = 4096;
    if (info->description != NULL)
      object_header.size_desc = strlen(info->description) + 1;
    object_header.refcnt = 1;
    object_header.type = info->type;
    object_header.id = *id;

    uint32_t total_size = sizeof(object_header) +
                          object_header.size_desc + object_header.size_data;
    Me()->TryFreeSpace(total_size);
    ObjectHeader *allocd_object = reinterpret_cast<ObjectHeader *>(
      Me()->storage_->Allocate(total_size,
                               &object_header, sizeof(object_header)));
    if (allocd_object == NULL)
      return CVMCACHE_STATUS_NOSPACE;

    allocd_object->SetDescription(info->description);
    Me()->transactions_.Insert(txn_id, allocd_object);
    return CVMCACHE_STATUS_OK;
  }


  static int ram_write_txn(
    uint64_t txn_id,
    unsigned char *buffer,
    uint32_t size)
  {
    LogCvmfs(kLogCvmfs, kLogDebug, "writine %lu", size);
    ObjectHeader *txn_object;
    int retval = Me()->transactions_.Lookup(txn_id, &txn_object);
    assert(retval);
    assert(size > 0);

    if (txn_object->neg_nbytes_written > 0)
      txn_object->neg_nbytes_written = 0;
    if ((size - txn_object->neg_nbytes_written) > txn_object->size_data) {
      uint32_t current_size = Me()->storage_->GetSize(txn_object);
      uint32_t header_size = current_size - txn_object->size_data;
      uint32_t new_size = std::max(
        header_size + size - txn_object->neg_nbytes_written,
        uint32_t(current_size * kObjectExpandFactor));
      bool did_compact = Me()->TryFreeSpace(new_size);
      if (did_compact) {
        retval = Me()->transactions_.Lookup(txn_id, &txn_object);
        assert(retval);
      }
      txn_object = reinterpret_cast<ObjectHeader *>(
        Me()->storage_->Expand(txn_object, new_size));
      if (txn_object == NULL)
        return CVMCACHE_STATUS_NOSPACE;
      txn_object->size_data = new_size - header_size;
      Me()->transactions_.Insert(txn_id, txn_object);
    }

    memcpy(txn_object->GetData() - txn_object->neg_nbytes_written,
           buffer, size);
    txn_object->neg_nbytes_written -= size;
    return CVMCACHE_STATUS_OK;
  }


  static int ram_commit_txn(uint64_t txn_id) {
    Me()->TryFreeSpace(0);
    if (Me()->objects_all_->IsFull())
      return CVMCACHE_STATUS_NOSPACE;

    ObjectHeader *txn_object;
    int retval = Me()->transactions_.Lookup(txn_id, &txn_object);
    assert(retval);

    Me()->transactions_.Erase(txn_id);
    ComparableHash h(txn_object->id);
    ObjectHeader *existing_object;
    if (Me()->objects_all_->Lookup(h, &existing_object)) {
      // Concurrent addition of same objects, drop the one at hand and
      // increase ref count of existing copy
      Me()->storage_->MarkFree(txn_object);
      if (existing_object->refcnt == 0)
        Me()->cache_info_.pinned_bytes += existing_object->size_data;
      existing_object->refcnt++;
    } else {
      txn_object->txn_id = uint64_t(-1);
      if (txn_object->neg_nbytes_written > 0)
        txn_object->neg_nbytes_written = 0;
      txn_object->size_data = -(txn_object->neg_nbytes_written);
      txn_object->refcnt = 1;
      Me()->cache_info_.used_bytes += txn_object->size_data;
      Me()->cache_info_.pinned_bytes += txn_object->size_data;
      Me()->objects_all_->Insert(h, txn_object);
      if (txn_object->type == CVMCACHE_OBJECT_VOLATILE) {
        assert(!Me()->objects_volatile_->IsFull());
        Me()->objects_volatile_->Insert(h, txn_object);
      }
    }
    return CVMCACHE_STATUS_OK;
  }


  static int ram_abort_txn(uint64_t txn_id) {
    ObjectHeader *txn_object;
    int retval = Me()->transactions_.Lookup(txn_id, &txn_object);
    assert(retval);
    Me()->transactions_.Erase(txn_id);
    Me()->storage_->MarkFree(txn_object);
    return CVMCACHE_STATUS_OK;
  }


  static int ram_info(struct cvmcache_info *info) {
    *info = Me()->cache_info_;
    return CVMCACHE_STATUS_OK;
  }


  static int ram_shrink(uint64_t shrink_to, uint64_t *used) {
    *used = Me()->cache_info_.used_bytes;
    if (*used <= shrink_to)
      return CVMCACHE_STATUS_OK;

    Me()->DoShrink(shrink_to);
    *used = Me()->cache_info_.used_bytes;
    return (*used <= shrink_to) ? CVMCACHE_STATUS_OK : CVMCACHE_STATUS_PARTIAL;
  }


  static int ram_listing_begin(
    uint64_t lst_id,
    enum cvmcache_object_type type)
  {
    Listing *lst = new Listing();
    Me()->objects_all_->FilterBegin();
    while (Me()->objects_all_->FilterNext()) {
      ComparableHash h;
      ObjectHeader *object;
      Me()->objects_all_->FilterGet(&h, &object);
      if (object->type != type)
        continue;

      struct cvmcache_object_info item;
      item.id = object->id;
      item.size = object->size_data;
      item.type = type;
      item.pinned = object->refcnt != 0;
      item.description = (object->size_desc > 0)
                         ? strdup(object->GetDescription())
                         : NULL;
      lst->elems.push_back(item);
    }
    Me()->objects_all_->FilterEnd();

    Me()->listings_.Insert(lst_id, lst);
    return CVMCACHE_STATUS_OK;
  }


  static int ram_listing_next(
    int64_t listing_id,
    struct cvmcache_object_info *item)
  {
    Listing *lst;
    bool retval = Me()->listings_.Lookup(listing_id, &lst);
    assert(retval);
    if (lst->pos >= lst->elems.size())
      return CVMCACHE_STATUS_OUTOFBOUNDS;
    *item = lst->elems[lst->pos];
    lst->pos++;
    return CVMCACHE_STATUS_OK;
  }


  static int ram_listing_end(int64_t listing_id) {
    Listing *lst;
    bool retval = Me()->listings_.Lookup(listing_id, &lst);
    assert(retval);

    // Don't free description strings, done by the library
    delete lst;
    Me()->listings_.Erase(listing_id);
    return CVMCACHE_STATUS_OK;
  }

 private:
  static const uint64_t kMinSize;  // 100 * 1024 * 1024;
  static const double kShrinkFactor;  //  = 0.75;
  static const double kObjectExpandFactor;  // = 1.5;
  static const double kSlotFraction;  // = 0.04;
  static const double kDangerZoneThreshold;  // = 0.7

  static PluginRamCache *instance_;
  static PluginRamCache *Me() {
    return instance_;
  }
  explicit PluginRamCache(uint64_t mem_size) {
    in_danger_zone_ = false;

    uint64_t heap_size = RoundUp8(
      std::max(kMinSize, uint64_t(mem_size * (1.0 - kSlotFraction))));
    memset(&cache_info_, 0, sizeof(cache_info_));
    cache_info_.size_bytes = heap_size;
    storage_ = new MallocHeap(
      heap_size, this->MakeCallback(&PluginRamCache::OnBlockMove, this));

    struct cvmcache_hash hash_empty;
    memset(&hash_empty, 0, sizeof(hash_empty));

    transactions_.Init(64, uint64_t(-1), hasher_uint64);
    listings_.Init(8, uint64_t(-1), hasher_uint64);

    double slot_size =
      lru::LruCache<ComparableHash, ObjectHeader *>::GetEntrySize();
    uint64_t num_slots = uint64_t((heap_size * kSlotFraction) /
                         (2.0 * slot_size));
    const unsigned mask_64 = ~((1 << 6) - 1);

    LogCvmfs(kLogCvmfs, kLogStdout, "Allocating %" PRIu64 "MB of memory "
             "for up to %" PRIu64 " objects",
             heap_size / (1024 * 1024), num_slots & mask_64);

    // Number of cache entries must be a multiple of 64
    objects_all_ = new lru::LruCache<ComparableHash, ObjectHeader *>(
      num_slots & mask_64,
      ComparableHash(hash_empty),
      hasher_any,
      &statistics_,
      "objects_all");
    objects_volatile_ = new lru::LruCache<ComparableHash, ObjectHeader *>(
      num_slots & mask_64,
      ComparableHash(hash_empty),
      hasher_any,
      &statistics_,
      "objects_volatile");
  }

  /**
   * Returns true if memory compaction took place and pointers might have been
   * invalidated.
   */
  bool TryFreeSpace(uint64_t bytes_required) {
    if (!objects_all_->IsFull() && storage_->HasSpaceFor(bytes_required))
      return false;

    uint64_t shrink_to = std::min(
      storage_->capacity() - (bytes_required + 8),
      uint64_t(storage_->used_bytes() * kShrinkFactor));
    DoShrink(shrink_to);
    return true;
  }

  void OnBlockMove(const MallocHeap::BlockPtr &ptr) {
    assert(ptr.pointer);
    ObjectHeader *object = reinterpret_cast<ObjectHeader *>(ptr.pointer);
    ComparableHash h(object->id);
    if (object->txn_id == uint64_t(-1)) {
      bool retval = objects_all_->UpdateValue(h, object);
      assert(retval);
      if (object->type == CVMCACHE_OBJECT_VOLATILE) {
        retval = objects_volatile_->UpdateValue(h, object);
        assert(retval);
      }
    } else {
      uint64_t old_size = transactions_.size();
      transactions_.Insert(object->txn_id, object);
      assert(old_size == transactions_.size());
    }
  }

  void DoShrink(uint64_t shrink_to) {
    ComparableHash h;
    ObjectHeader *object;

    objects_volatile_->FilterBegin();
    while (objects_volatile_->FilterNext()) {
      objects_volatile_->FilterGet(&h, &object);
      if (object->refcnt != 0)
        continue;
      cache_info_.used_bytes -= object->size_data;
      storage_->MarkFree(object);
      objects_volatile_->FilterDelete();
      objects_all_->Forget(h);
      if (cache_info_.used_bytes <= shrink_to)
        break;
    }
    objects_volatile_->FilterEnd();

    objects_all_->FilterBegin();
    while ((cache_info_.used_bytes > shrink_to) && objects_all_->FilterNext()) {
      objects_all_->FilterGet(&h, &object);
      if (object->refcnt != 0)
        continue;
      assert(object->type != CVMCACHE_OBJECT_VOLATILE);
      cache_info_.used_bytes -= object->size_data;
      storage_->MarkFree(object);
      objects_all_->FilterDelete();
    }
    objects_all_->FilterEnd();

    storage_->Compact();
    cache_info_.no_shrink++;
  }

  bool IsInDangerZone() {
    return (static_cast<double>(cache_info_.pinned_bytes) /
            static_cast<double>(cache_info_.size_bytes)) >
           kDangerZoneThreshold;
  }


  struct cvmcache_info cache_info_;
  perf::Statistics statistics_;
  SmallHashDynamic<uint64_t, ObjectHeader *> transactions_;
  SmallHashDynamic<uint64_t, Listing *> listings_;
  lru::LruCache<ComparableHash, ObjectHeader *> *objects_all_;
  lru::LruCache<ComparableHash, ObjectHeader *> *objects_volatile_;
  MallocHeap *storage_;
  bool in_danger_zone_;
};  // class PluginRamCache

PluginRamCache *PluginRamCache::instance_ = NULL;
const uint64_t PluginRamCache::kMinSize = 100 * 1024 * 1024;
const double PluginRamCache::kShrinkFactor = 0.75;
const double PluginRamCache::kObjectExpandFactor = 1.5;
const double PluginRamCache::kSlotFraction = 0.04;
const double PluginRamCache::kDangerZoneThreshold = 0.7;


static void Usage(const char *progname) {
  printf("%s <config file>\n", progname);
}


int main(int argc, char **argv) {
  if (argc < 2) {
    Usage(argv[0]);
    return 1;
  }

  cvmcache_init_global();

  cvmcache_option_map *options = cvmcache_options_init();
  if (cvmcache_options_parse(options, argv[1]) != 0) {
    printf("cannot parse options file %s\n", argv[1]);
    return 1;
  }
  char *locator = cvmcache_options_get(options, "CVMFS_CACHE_EXTERNAL_LOCATOR");
  if (locator == NULL) {
    printf("CVMFS_CACHE_EXTERNAL_LOCATOR missing\n");
    cvmcache_options_fini(options);
    return 1;
  }
  char *mem_size = cvmcache_options_get(options, "CVMFS_CACHE_EXTERNAL_SIZE");
  if (mem_size == NULL) {
    printf("CVMFS_CACHE_EXTERNAL_SIZE missing\n");
    cvmcache_options_fini(options);
    return 1;
  }

  cvmcache_spawn_watchdog(NULL);
  PluginRamCache *plugin = PluginRamCache::Create(mem_size);

  struct cvmcache_callbacks callbacks;
  memset(&callbacks, 0, sizeof(callbacks));
  callbacks.cvmcache_chrefcnt = plugin->ram_chrefcnt;
  callbacks.cvmcache_obj_info = plugin->ram_obj_info;
  callbacks.cvmcache_pread = plugin->ram_pread;
  callbacks.cvmcache_start_txn = plugin->ram_start_txn;
  callbacks.cvmcache_write_txn = plugin->ram_write_txn;
  callbacks.cvmcache_commit_txn = plugin->ram_commit_txn;
  callbacks.cvmcache_abort_txn = plugin->ram_abort_txn;
  callbacks.cvmcache_info = plugin->ram_info;
  callbacks.cvmcache_shrink = plugin->ram_shrink;
  callbacks.cvmcache_listing_begin = plugin->ram_listing_begin;
  callbacks.cvmcache_listing_next = plugin->ram_listing_next;
  callbacks.cvmcache_listing_end = plugin->ram_listing_end;
  callbacks.capabilities = CVMCACHE_CAP_ALL;

  ctx = cvmcache_init(&callbacks);
  int retval = cvmcache_listen(ctx, locator);
  if (!retval) {
    fprintf(stderr, "failed to listen on %s\n", locator);
    return 1;
  }
  printf("Listening for cvmfs clients on %s\n", locator);
  printf("NOTE: this process needs to run as user cvmfs\n\n");


  cvmcache_process_requests(ctx, 0);
  if (!cvmcache_is_supervised()) {
    printf("Press <Ctrl+D> to quit\n");
    while (true) {
      char buf;
      retval = read(fileno(stdin), &buf, 1);
      if (retval != 1)
        break;
    }
    cvmcache_terminate(ctx);
  } else {
    LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
             "CernVM-FS RAM cache plugin started in supervised mode");
  }

  cvmcache_wait_for(ctx);
  printf("  ... good bye\n");
  cvmcache_options_free(mem_size);
  cvmcache_options_free(locator);
  cvmcache_options_fini(options);
  cvmcache_terminate_watchdog();
  cvmcache_cleanup_global();
  return 0;
}
