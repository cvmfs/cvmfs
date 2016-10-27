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

#include "libcvmfs_cache.h"

using namespace std;

struct Object {
  string data;
  cvmcache_object_type type;
  int32_t refcnt;
  string description;
};

struct TxnInfo {
  struct cvmcache_hash id;
  Object partial_object;
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

struct cvmcache_context *ctx;

static int null_chrefcnt(struct cvmcache_hash *id, int32_t change_by) {
  ComparableHash h(*id);
  if (storage.find(h) == storage.end())
    return STATUS_NOENTRY;

  Object obj = storage[h];
  obj.refcnt += change_by;
  if (obj.refcnt < 0)
    return STATUS_BADCOUNT;
  storage[h] = obj;
  return STATUS_OK;
}


int null_obj_info(struct cvmcache_hash *id,
                struct cvmcache_object_info *info)
{
  ComparableHash h(*id);
  if (storage.find(h) == storage.end())
    return STATUS_NOENTRY;

  Object obj = storage[h];
  info->size = obj.data.length();
  info->type = obj.type;
  info->pinned = obj.refcnt > 0;
  info->description = strdup(obj.description.c_str());
  return STATUS_OK;
}


static int null_pread(struct cvmcache_hash *id,
                    uint64_t offset,
                    uint32_t *size,
                    unsigned char *buffer)
{
  ComparableHash h(*id);
  string data = storage[h].data;
  if (offset >= data.length())
    return STATUS_OUTOFBOUNDS;
  unsigned nbytes =
    std::min(*size, static_cast<uint32_t>(data.length() - offset));
  memcpy(buffer, data.data() + offset, nbytes);
  *size = nbytes;
  return STATUS_OK;
}


static int null_start_txn(struct cvmcache_hash *id,
                        uint64_t txn_id,
                        struct cvmcache_object_info *info)
{
  Object partial_object;
  partial_object.type = info->type;
  partial_object.refcnt = 1;
  if (info->description != NULL)
    partial_object.description = string(info->description);
  TxnInfo txn;
  txn.id = *id;
  txn.partial_object = partial_object;
  transactions[txn_id] = txn;
  return STATUS_OK;
}


int null_write_txn(uint64_t txn_id,
                 unsigned char *buffer,
                 uint32_t size)
{
  TxnInfo txn = transactions[txn_id];
  txn.partial_object.data += string(reinterpret_cast<char *>(buffer), size);
  transactions[txn_id] = txn;
  return STATUS_OK;
}


int null_commit_txn(uint64_t txn_id) {
  TxnInfo txn = transactions[txn_id];
  ComparableHash h(txn.id);
  storage[h] = txn.partial_object;
  return STATUS_OK;
}

int null_abort_txn(uint64_t txn_id) {
  transactions.erase(txn_id);
  return STATUS_OK;
}

void Usage(const char *progname) {
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

  ctx = cvmcache_init(&callbacks);
  int retval = cvmcache_listen(ctx, argv[1]);
  assert(retval);
  printf("Listening for cvmfs clients on %s\n", argv[1]);
  chown(argv[1], 993, 992);
  cvmcache_process_requests(ctx);
  while (true) {
    sleep(1);
  }
  return 0;
}
