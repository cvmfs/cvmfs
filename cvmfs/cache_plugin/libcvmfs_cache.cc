/**
 * This file is part of the CernVM File System.
 */

#define _FILE_OFFSET_BITS 64
#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "libcvmfs_cache.h"

#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <string>

#include "cache_plugin/channel.h"
#include "hash.h"
#include "util/pointer.h"

using namespace std;  // NOLINT

namespace {

static shash::Any Chash2Cpphash(struct cvmcache_hash *h) {
  shash::Any hash;
  memcpy(hash.digest, h->digest, sizeof(h->digest));
  hash.algorithm = static_cast<shash::Algorithms>(h->algorithm);
  return hash;
}

static struct cvmcache_hash Cpphash2Chash(const shash::Any &hash) {
  struct cvmcache_hash h;
  memcpy(h.digest, hash.digest, sizeof(h.digest));
  h.algorithm = hash.algorithm;
  return h;
}

class ForwardCachePlugin : public CachePlugin {
 public:
  ForwardCachePlugin(struct cvmcache_callbacks *callbacks)
    : CachePlugin(callbacks->capabilities)
    , callbacks_(*callbacks)
  {
    if (callbacks->capabilities & CAP_REFCOUNT)
      assert(callbacks->cvmcache_chrefcnt != NULL);
    if (callbacks->capabilities & CAP_INFO)
      assert(callbacks->cvmcache_info != NULL);
    if (callbacks->capabilities & CAP_SHRINK)
      assert(callbacks->cvmcache_shrink != NULL);
  }
  virtual ~ForwardCachePlugin() { }

 protected:
  virtual cvmfs::EnumStatus ChangeRefcount(
    const shash::Any &id,
    int32_t change_by)
  {
    if (!(callbacks_.capabilities & CAP_REFCOUNT))
      return cvmfs::STATUS_NOSUPPORT;

    struct cvmcache_hash c_hash = Cpphash2Chash(id);
    int result = callbacks_.cvmcache_chrefcnt(&c_hash, change_by);
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus GetObjectInfo(
    const shash::Any &id,
    ObjectInfo *info)
  {
    struct cvmcache_hash c_hash = Cpphash2Chash(id);
    cvmcache_object_info c_info;
    c_info.size = CachePlugin::kSizeUnknown;
    c_info.type = OBJECT_REGULAR;
    c_info.description = NULL;
    int result = callbacks_.cvmcache_obj_info(&c_hash, &c_info);
    info->size = c_info.size;
    info->object_type = static_cast<cvmfs::EnumObjectType>(c_info.type);
    if (c_info.description) {
      info->description = string(c_info.description);
      free(c_info.description);
    }
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus Pread(
    const shash::Any &id,
    uint64_t offset,
    uint32_t *size,
    unsigned char *buffer)
  {
    struct cvmcache_hash c_hash = Cpphash2Chash(id);
    int result = callbacks_.cvmcache_pread(&c_hash, offset, size, buffer);
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus StartTxn(
    const shash::Any &id,
    const uint64_t txn_id,
    const ObjectInfo &info)
  {
    struct cvmcache_hash c_hash = Cpphash2Chash(id);
    cvmcache_object_info c_info;
    c_info.size = info.size;
    switch (info.object_type) {
      case cvmfs::OBJECT_REGULAR:
        c_info.type = OBJECT_REGULAR;
        break;
      case cvmfs::OBJECT_CATALOG:
        c_info.type = OBJECT_CATALOG;
        break;
      case cvmfs::OBJECT_VOLATILE:
        c_info.type = OBJECT_VOLATILE;
        break;
      default:
        abort();
    }
    if (info.description.empty()) {
      c_info.description = NULL;
    } else {
      c_info.description = strdup(info.description.c_str());
    }
    int result = callbacks_.cvmcache_start_txn(&c_hash, txn_id, &c_info);
    free(c_info.description);
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus WriteTxn(
    const uint64_t txn_id,
    unsigned char *buffer,
    uint32_t size)
  {
    int result = callbacks_.cvmcache_write_txn(txn_id, buffer, size);
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus CommitTxn(const uint64_t txn_id) {
    int result = callbacks_.cvmcache_commit_txn(txn_id);
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus AbortTxn(const uint64_t txn_id) {
    int result = callbacks_.cvmcache_abort_txn(txn_id);
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus GetInfo(
    uint64_t *size,
    uint64_t *used,
    uint64_t *pinned)
  {
    if (!(callbacks_.capabilities & CAP_INFO))
      return cvmfs::STATUS_NOSUPPORT;

    int result = callbacks_.cvmcache_info(size, used, pinned);
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus Shrink(uint64_t shrink_to, uint64_t *used) {
    if (!(callbacks_.capabilities & CAP_SHRINK))
      return cvmfs::STATUS_NOSUPPORT;

    int result = callbacks_.cvmcache_shrink(shrink_to, used);
    return static_cast<cvmfs::EnumStatus>(result);
  }

 private:
  struct cvmcache_callbacks callbacks_;
};

}  // anonymous namespace


struct cvmcache_context {
  explicit cvmcache_context(ForwardCachePlugin *p) : plugin(p) { }
  UniquePtr<ForwardCachePlugin> plugin;
};


int cvmcache_hash_cmp(struct cvmcache_hash *a, struct cvmcache_hash *b) {
  const shash::Any hash_a = Chash2Cpphash(a);
  const shash::Any hash_b = Chash2Cpphash(b);
  if (hash_a < hash_b) return -1;
  else if (hash_a == hash_b) return 0;
  else return 1;
}

char *cvmcache_hash_print(struct cvmcache_hash *h) {
  const shash::Any hash = Chash2Cpphash(h);
  return strdup(hash.ToString().c_str());
}



struct cvmcache_context *cvmcache_init(struct cvmcache_callbacks *callbacks) {
  return new cvmcache_context(new ForwardCachePlugin(callbacks));
}

int cvmcache_listen(struct cvmcache_context *ctx, char *socket_path) {
  return ctx->plugin->Listen(socket_path);
}

void cvmcache_process_requests(struct cvmcache_context *ctx) {
  ctx->plugin->ProcessRequests();
}

void cvmcache_terminate(struct cvmcache_context *ctx) {
  delete ctx;
}

uint32_t cvmcache_max_object_size(struct cvmcache_context *ctx) {
  return ctx->plugin->max_object_size();
}
