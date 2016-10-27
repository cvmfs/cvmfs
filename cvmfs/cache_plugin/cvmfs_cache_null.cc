/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include <alloca.h>
#include <inttypes.h>
#include <stdint.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <map>
#include <string>
#include <vector>

#include "libcvmfs_cache.h"

using namespace std;

struct Object {
  struct cvmcache_hash id;
  string data;
  cvmcache_object_type type;
  int32_t refcnt;
  string description;
};

struct TxnInfo {
  struct cvmcache_hash id;
  Object partial_object;
};

struct Listing {
  Listing() : pos(0) { }
  cvmcache_object_type type;
  uint64_t pos;
  vector<Object> *elems;
};

struct ComparableHash {
  ComparableHash() { }
  explicit ComparableHash(const struct cvmcache_hash &h) : hash(h) { }
  struct cvmcache_hash hash;
  bool operator <(const ComparableHash &other) const {
    return cvmcache_hash_cmp(const_cast<cvmcache_hash *>(&(this->hash)),
                             const_cast<cvmcache_hash *>(&(other.hash))) < 0;
  }
};

map<uint64_t, TxnInfo> transactions;
map<ComparableHash, Object> storage;
map<uint64_t, Listing> listings;

struct cvmcache_context *ctx;
uint64_t next_listing_id = 1;

static int null_chrefcnt(struct cvmcache_hash *id, int32_t change_by) {
  ComparableHash h(*id);
  if (storage.find(h) == storage.end())
    return CVMCACHE_STATUS_NOENTRY;

  Object obj = storage[h];
  obj.refcnt += change_by;
  if (obj.refcnt < 0)
    return CVMCACHE_STATUS_BADCOUNT;
  storage[h] = obj;
  return CVMCACHE_STATUS_OK;
}


static int null_obj_info(
  struct cvmcache_hash *id,
  struct cvmcache_object_info *info)
{
  ComparableHash h(*id);
  if (storage.find(h) == storage.end())
    return CVMCACHE_STATUS_NOENTRY;

  Object obj = storage[h];
  info->size = obj.data.length();
  info->type = obj.type;
  info->pinned = obj.refcnt > 0;
  info->description = strdup(obj.description.c_str());
  return CVMCACHE_STATUS_OK;
}


static int null_pread(struct cvmcache_hash *id,
                    uint64_t offset,
                    uint32_t *size,
                    unsigned char *buffer)
{
  ComparableHash h(*id);
  string data = storage[h].data;
  if (offset >= data.length())
    return CVMCACHE_STATUS_OUTOFBOUNDS;
  unsigned nbytes =
    std::min(*size, static_cast<uint32_t>(data.length() - offset));
  memcpy(buffer, data.data() + offset, nbytes);
  *size = nbytes;
  return CVMCACHE_STATUS_OK;
}


static int null_start_txn(
  struct cvmcache_hash *id,
  uint64_t txn_id,
  struct cvmcache_object_info *info)
{
  Object partial_object;
  partial_object.id = *id;
  partial_object.type = info->type;
  partial_object.refcnt = 1;
  if (info->description != NULL)
    partial_object.description = string(info->description);
  TxnInfo txn;
  txn.id = *id;
  txn.partial_object = partial_object;
  transactions[txn_id] = txn;
  return CVMCACHE_STATUS_OK;
}


static int null_write_txn(
  uint64_t txn_id,
  unsigned char *buffer,
  uint32_t size)
{
  TxnInfo txn = transactions[txn_id];
  txn.partial_object.data += string(reinterpret_cast<char *>(buffer), size);
  transactions[txn_id] = txn;
  return CVMCACHE_STATUS_OK;
}


static int null_commit_txn(uint64_t txn_id) {
  TxnInfo txn = transactions[txn_id];
  ComparableHash h(txn.id);
  storage[h] = txn.partial_object;
  return CVMCACHE_STATUS_OK;
}

static int null_abort_txn(uint64_t txn_id) {
  transactions.erase(txn_id);
  return CVMCACHE_STATUS_OK;
}

static int null_info(uint64_t *size, uint64_t *used, uint64_t *pinned) {
  *size = uint64_t(-1);
  *used = *pinned = 0;
  for (map<ComparableHash, Object>::const_iterator i = storage.begin(),
       i_end = storage.end(); i != i_end; ++i)
  {
    *used += i->second.data.length();
    if (i->second.refcnt > 0)
      *pinned += i->second.data.length();
  }
  return CVMCACHE_STATUS_OK;
}

static int null_shrink(uint64_t shrink_to, uint64_t *used) {
  uint64_t size, pinned;
  null_info(&size, used, &pinned);
  if (*used <= shrink_to)
    return CVMCACHE_STATUS_OK;

  // Volatile objects
  for (map<ComparableHash, Object>::iterator i = storage.begin(),
       i_end = storage.end(); i != i_end; )
  {
    if ((i->second.refcnt > 0) || (i->second.type != CVMCACHE_OBJECT_VOLATILE))
    {
      ++i;
      continue;
    }
    unsigned length = i->second.data.length();
    map<ComparableHash, Object>::iterator delete_me = i++;
    storage.erase(delete_me);
    *used -= length;
    if (*used <= shrink_to)
      return CVMCACHE_STATUS_OK;
  }
  // All other objects
  for (map<ComparableHash, Object>::iterator i = storage.begin(),
       i_end = storage.end(); i != i_end; )
  {
    if (i->second.refcnt > 0) {
      ++i;
      continue;
    }
    unsigned length = i->second.data.length();
    map<ComparableHash, Object>::iterator delete_me = i++;
    storage.erase(delete_me);
    *used -= length;
    if (*used <= shrink_to)
      return CVMCACHE_STATUS_OK;
  }

  return CVMCACHE_STATUS_PARTIAL;
}

static int64_t null_listing_begin(enum cvmcache_object_type type) {
  Listing lst;
  lst.type = type;
  lst.elems = new vector<Object>();
  for (map<ComparableHash, Object>::const_iterator i = storage.begin(),
       i_end = storage.end(); i != i_end; ++i)
  {
    lst.elems->push_back(i->second);
  }
  listings[next_listing_id] = lst;
  return next_listing_id++;
}

static int null_listing_next(
  int64_t listing_id,
  struct cvmcache_object_info *item)
{
  Listing lst = listings[listing_id];
  do {
    if (lst.pos >= lst.elems->size())
      return CVMCACHE_STATUS_OUTOFBOUNDS;

    vector<Object> *elems = lst.elems;
    if ((*elems)[lst.pos].type == lst.type) {
      item->id = (*elems)[lst.pos].id;
      item->size = (*elems)[lst.pos].data.length();
      item->type = (*elems)[lst.pos].type;
      item->pinned = (*elems)[lst.pos].refcnt > 0;
      item->description = (*elems)[lst.pos].description.empty()
                          ? NULL
                          : strdup((*elems)[lst.pos].description.c_str());
      break;
    }
    lst.pos++;
  } while (true);
  lst.pos++;
  listings[listing_id] = lst;
  return CVMCACHE_STATUS_OK;
}

static int null_listing_end(int64_t listing_id) {
  delete listings[listing_id].elems;
  listings.erase(listing_id);
  return CVMCACHE_STATUS_OK;
}

static void Usage(const char *progname) {
  printf("%s <Cvmfs cache socket>\n", progname);
}


int main(int argc, char **argv) {
  if (argc < 2) {
    Usage(argv[0]);
    return 1;
  }

  struct cvmcache_callbacks callbacks;
  memset(&callbacks, 0, sizeof(callbacks));
  callbacks.cvmcache_chrefcnt = null_chrefcnt;
  callbacks.cvmcache_obj_info = null_obj_info;
  callbacks.cvmcache_pread = null_pread;
  callbacks.cvmcache_start_txn = null_start_txn;
  callbacks.cvmcache_write_txn = null_write_txn;
  callbacks.cvmcache_commit_txn = null_commit_txn;
  callbacks.cvmcache_abort_txn = null_abort_txn;
  callbacks.cvmcache_info = null_info;
  callbacks.cvmcache_shrink = null_shrink;
  callbacks.cvmcache_listing_begin = null_listing_begin;
  callbacks.cvmcache_listing_next = null_listing_next;
  callbacks.cvmcache_listing_end = null_listing_end;
  callbacks.capabilities = CVMCACHE_CAP_ALL;

  ctx = cvmcache_init(&callbacks);
  int retval = cvmcache_listen(ctx, argv[1]);
  assert(retval);
  printf("Listening for cvmfs clients on %s\n", argv[1]);
  cvmcache_process_requests(ctx, 0);
  while (true) {
    sleep(1);
  }
  return 0;
}
