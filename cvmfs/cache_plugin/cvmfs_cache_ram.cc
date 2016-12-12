/**
 * This file is part of the CernVM File System.
 *
 * A cache plugin that stores all data in memory.
 */

#define __STDC_FORMAT_MACROS

#include <alloca.h>
#include <inttypes.h>
#include <stdint.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "cache_plugin/libcvmfs_cache.h"
#include "lru.h"
#include "malloc_heap.h"
#include "platform.h"
#include "smallhash.h"

using namespace std;  // NOLINT

/**
 * Header of the data pieces in the cache.  After the object header, the
 * zero-terminated description and the object data follows.
 */
struct ObjectHeader {
  ObjectHeader() {
    memset(&id, 0, sizeof(id));
    size_data = 0;
    size_desc = 0;
    refcnt = 0;
    type = CVMCACHE_OBJECT_REGULAR;
  }

  char *GetDescription() {
    return reinterpret_cast<char *>(this) + sizeof(ObjectHeader);
  }

  void SetDescription(char *description) {
    if (description == NULL)
      return;
    memcpy(reinterpret_cast<char *>(this) + sizeof(ObjectHeader),
           description, strlen(description) + 1);
  }

  unsigned char *GetData() {
    if (size_desc == 0)
      return NULL;
    return reinterpret_cast<unsigned char *>(this) +
           sizeof(ObjectHeader) + size_desc;
  }

  struct cvmcache_hash id;
  uint32_t size_data;
  uint32_t size_desc;
  // During a transaction, neg_nbytes_written is used to track the number of
  // already written bytes.  On commit, refcnt is set to 1.
  union {
    int32_t refcnt;
    int32_t neg_nbytes_written;
  };
  cvmcache_object_type type;
};


struct Listing {
  Listing() : pos(0) { }
  uint64_t pos;
  vector<struct cvmcache_object_info> elems;
};


struct ComparableHash {
  ComparableHash() { }
  explicit ComparableHash(const struct cvmcache_hash &h) : hash(h) { }
  bool operator ==(const ComparableHash &other) const {
    return cvmcache_hash_cmp(const_cast<cvmcache_hash *>(&(this->hash)),
                             const_cast<cvmcache_hash *>(&(other.hash))) == 0;
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


uint64_t g_max_objects;
perf::Statistics *g_statistics;
lru::LruCache<ComparableHash, ObjectHeader *> *g_objects_all;
lru::LruCache<ComparableHash, ObjectHeader *> *g_objects_volatile;
SmallHashDynamic<uint64_t, ObjectHeader *> *g_transactions;
SmallHashDynamic<uint64_t, Listing *> *g_listings;
MallocHeap *g_storage;
struct cvmcache_info *g_cache_info;

struct cvmcache_context *ctx;


static void TryFreeSpace(uint64_t bytes_required);


static int ram_chrefcnt(struct cvmcache_hash *id, int32_t change_by) {
  ComparableHash h(*id);
  ObjectHeader *object;
  if (!g_objects_all->Lookup(h, &object))
    return CVMCACHE_STATUS_NOENTRY;

  if (object->type == CVMCACHE_OBJECT_VOLATILE)
    g_objects_volatile->Update(h);

  if (change_by == 0)
    return CVMCACHE_STATUS_OK;
  if ((object->refcnt + change_by) < 0)
    return CVMCACHE_STATUS_BADCOUNT;

  if (object->refcnt == 0)
    g_cache_info->pinned_bytes += object->size_data;
  object->refcnt += change_by;
  if (object->refcnt == 0)
    g_cache_info->pinned_bytes -= object->size_data;
  return CVMCACHE_STATUS_OK;
}


static int ram_obj_info(
  struct cvmcache_hash *id,
  struct cvmcache_object_info *info)
{
  ComparableHash h(*id);
  ObjectHeader *object;
  if (!g_objects_all->Lookup(h, &object, false))
    return CVMCACHE_STATUS_NOENTRY;

  info->size = object->size_data;
  info->type = object->type;
  info->pinned = object->refcnt > 0;
  info->description = strdup(object->GetDescription());
  return CVMCACHE_STATUS_OK;
}


static int ram_pread(struct cvmcache_hash *id,
                    uint64_t offset,
                    uint32_t *size,
                    unsigned char *buffer)
{
  ComparableHash h(*id);
  ObjectHeader *object;
  bool retval = g_objects_all->Lookup(h, &object, false);
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
  object_header.id = *id;
  if (info->size != CVMCACHE_SIZE_UNKNOWN)
    object_header.size_data = info->size;
  else
    object_header.size_data = 4096;
  char *description = NULL;
  if (info->description != 0) {
    description = strdupa(info->description);
    object_header.size_desc = strlen(info->description) + 1;
  }
  object_header.refcnt = 1;
  object_header.type = info->type;

  uint32_t total_size = sizeof(object_header) +
                        object_header.size_desc + object_header.size_data;
  TryFreeSpace(total_size);
  ObjectHeader *allocd_object = reinterpret_cast<ObjectHeader *>(
    g_storage->Allocate(total_size, &object_header, sizeof(object_header)));
  if (allocd_object == NULL)
    return CVMCACHE_STATUS_NOSPACE;

  allocd_object->SetDescription(description);
  g_transactions->Insert(txn_id, allocd_object);
  return CVMCACHE_STATUS_OK;
}


static int ram_write_txn(
  uint64_t txn_id,
  unsigned char *buffer,
  uint32_t size)
{
  ObjectHeader *txn_object;
  int retval = g_transactions->Lookup(txn_id, &txn_object);
  assert(retval);
  assert(size > 0);

  if (txn_object->neg_nbytes_written > 0)
    txn_object->neg_nbytes_written = 0;
  if ((size - txn_object->neg_nbytes_written) > txn_object->size_data) {
    uint32_t new_size = std::max(
      size - txn_object->neg_nbytes_written,
      uint32_t(txn_object->size_data * 0.25));
    TryFreeSpace(new_size);
    txn_object = reinterpret_cast<ObjectHeader *>(
      g_storage->Expand(txn_object, new_size));
    if (txn_object == NULL)
      return CVMCACHE_STATUS_NOSPACE;
    g_transactions->Insert(txn_id, txn_object);
  }

  memcpy(txn_object->GetData() - txn_object->neg_nbytes_written, buffer, size);
  txn_object->neg_nbytes_written -= size;
  return CVMCACHE_STATUS_OK;
}


static int ram_commit_txn(uint64_t txn_id) {
  ObjectHeader *txn_object;
  int retval = g_transactions->Lookup(txn_id, &txn_object);
  assert(retval);
  TryFreeSpace(0);
  if (g_objects_all->IsFull())
    return CVMCACHE_STATUS_NOSPACE;

  g_transactions->Erase(txn_id);
  txn_object->size_data = -(txn_object->neg_nbytes_written);
  txn_object->refcnt = 1;
  g_cache_info->used_bytes += txn_object->size_data;
  g_cache_info->pinned_bytes += txn_object->size_data;
  ComparableHash h(txn_object->id);
  g_objects_all->Insert(h, txn_object);
  if (txn_object->type == CVMCACHE_OBJECT_VOLATILE) {
    assert(!g_objects_volatile->IsFull());
    g_objects_volatile->Insert(h, txn_object);
  }
  return CVMCACHE_STATUS_OK;
}


static int ram_abort_txn(uint64_t txn_id) {
  ObjectHeader *txn_object;
  int retval = g_transactions->Lookup(txn_id, &txn_object);
  assert(retval);
  g_storage->MarkFree(txn_object);
  g_transactions->Erase(txn_id);
  return CVMCACHE_STATUS_OK;
}


static int ram_info(struct cvmcache_info *info) {
  *info = *g_cache_info;
  return CVMCACHE_STATUS_OK;
}


static int ram_shrink(uint64_t shrink_to, uint64_t *used) {
  *used = g_cache_info->used_bytes;
  if (g_cache_info->used_bytes <= shrink_to)
    return CVMCACHE_STATUS_OK;

  ComparableHash h;
  ObjectHeader *object;

  g_objects_volatile->FilterBegin();
  while (g_objects_volatile->FilterNext()) {
    g_objects_volatile->FilterGet(&h, &object);
    if (object->refcnt != 0)
      continue;
    g_cache_info->used_bytes -= object->size_data;
    g_storage->MarkFree(object);
    g_objects_volatile->FilterDelete();
    g_objects_all->Forget(h);
    if (g_cache_info->used_bytes <= shrink_to)
      break;
  }
  g_objects_volatile->FilterEnd();

  g_objects_all->FilterBegin();
  while ((g_cache_info->used_bytes > shrink_to) &&
         g_objects_all->FilterNext())
  {
    g_objects_all->FilterGet(&h, &object);
    if (object->refcnt != 0)
      continue;
    assert(object->type != CVMCACHE_OBJECT_VOLATILE);
    g_cache_info->used_bytes -= object->size_data;
    g_storage->MarkFree(object);
    g_objects_all->FilterDelete();
  }
  g_objects_all->FilterEnd();

  g_storage->Compact();
  g_cache_info->no_shrink++;
  *used = g_cache_info->used_bytes;
  return (*used <= shrink_to) ? CVMCACHE_STATUS_OK : CVMCACHE_STATUS_PARTIAL;
}


static int ram_listing_begin(
  uint64_t lst_id,
  enum cvmcache_object_type type)
{
  Listing *lst = new Listing();
  g_objects_all->FilterBegin();
  while (g_objects_all->FilterNext()) {
    ComparableHash h;
    ObjectHeader *object;
    g_objects_all->FilterGet(&h, &object);
    if (object->type != type)
      continue;

    struct cvmcache_object_info item;
    item.id = h.hash;
    item.size = object->size_data;
    item.type = type;
    item.pinned = object->refcnt != 0;
    item.description = (object->size_desc > 0)
                       ? strdup(object->GetDescription())
                       : NULL;
    lst->elems.push_back(item);
  }
  g_objects_all->FilterEnd();

  g_listings->Insert(lst_id, lst);
  return CVMCACHE_STATUS_OK;
}


static int ram_listing_next(
  int64_t listing_id,
  struct cvmcache_object_info *item)
{
  Listing *lst;
  bool retval = g_listings->Lookup(listing_id, &lst);
  assert(retval);
  if (lst->pos >= lst->elems.size())
    return CVMCACHE_STATUS_OUTOFBOUNDS;
  *item = lst->elems[lst->pos];
  lst->pos++;
  return CVMCACHE_STATUS_OK;
}


static int ram_listing_end(int64_t listing_id) {
  Listing *lst;
  bool retval = g_listings->Lookup(listing_id, &lst);
  assert(retval);

  for (unsigned i = 0; i < lst->elems.size(); ++i) {
    free(lst->elems[i].description);
  }
  delete lst;
  g_listings->Erase(listing_id);
  return CVMCACHE_STATUS_OK;
}


static void TryFreeSpace(uint64_t bytes_required) {
  if (!g_objects_all->IsFull() && g_storage->HasSpaceFor(bytes_required))
    return;

  uint64_t shrink_to = std::min(
    g_storage->capacity() - (bytes_required + 8),
    uint64_t(g_storage->used_bytes() * 0.75));
  uint64_t used;
  ram_shrink(shrink_to, &used);
}

static void Usage(const char *progname) {
  printf("%s <config file>\n", progname);
}


int main(int argc, char **argv) {
  if (argc < 2) {
    Usage(argv[0]);
    return 1;
  }

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

  struct cvmcache_callbacks callbacks;
  memset(&callbacks, 0, sizeof(callbacks));
  callbacks.cvmcache_chrefcnt = ram_chrefcnt;
  callbacks.cvmcache_obj_info = ram_obj_info;
  callbacks.cvmcache_pread = ram_pread;
  callbacks.cvmcache_start_txn = ram_start_txn;
  callbacks.cvmcache_write_txn = ram_write_txn;
  callbacks.cvmcache_commit_txn = ram_commit_txn;
  callbacks.cvmcache_abort_txn = ram_abort_txn;
  callbacks.cvmcache_info = ram_info;
  callbacks.cvmcache_shrink = ram_shrink;
  callbacks.cvmcache_listing_begin = ram_listing_begin;
  callbacks.cvmcache_listing_next = ram_listing_next;
  callbacks.cvmcache_listing_end = ram_listing_end;
  callbacks.capabilities = CVMCACHE_CAP_ALL;

  ctx = cvmcache_init(&callbacks);
  int retval = cvmcache_listen(ctx, locator);
  assert(retval);
  printf("Listening for cvmfs clients on %s\n", locator);
  printf("NOTE: this process needs to run as user cvmfs\n\n");
  printf("Press <R ENTER> to ask clients to release nested catalogs\n");
  printf("Press <Ctrl+D> to quit\n");

  cvmcache_process_requests(ctx, 0);
  while (true) {
    char buf;
    retval = read(fileno(stdin), &buf, 1);
    if (retval != 1)
      break;
    if (buf == 'R') {
      printf("  ... asking clients to release nested catalogs\n");
      cvmcache_ask_detach(ctx);
    }
  }
  printf("  ... good bye\n");
  cvmcache_options_free(locator);
  cvmcache_options_fini(options);
  return 0;
}
