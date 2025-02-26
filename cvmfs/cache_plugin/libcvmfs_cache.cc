/**
 * This file is part of the CernVM File System.
 */

#define _FILE_OFFSET_BITS 64
#define __STDC_FORMAT_MACROS


#include "libcvmfs_cache.h"

#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>

#include "cache_plugin/channel.h"
#include "cache_transport.h"
#include "crypto/hash.h"
#include "manifest.h"
#include "monitor.h"
#include "util/pointer.h"

using namespace std;  // NOLINT

namespace {

static shash::Any Chash2Cpphash(const struct cvmcache_hash *h) {
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

static enum cvmcache_object_type ObjectType2CType(cvmfs::EnumObjectType type) {
  switch (type) {
    case cvmfs::OBJECT_REGULAR:
      return CVMCACHE_OBJECT_REGULAR;
    case cvmfs::OBJECT_CATALOG:
      return CVMCACHE_OBJECT_CATALOG;
    case cvmfs::OBJECT_VOLATILE:
      return CVMCACHE_OBJECT_VOLATILE;
  }
  abort();
}

class ForwardCachePlugin : public CachePlugin {
 public:
  explicit ForwardCachePlugin(struct cvmcache_callbacks *callbacks)
    : CachePlugin(callbacks->capabilities)
    , callbacks_(*callbacks)
  {
    assert(callbacks->cvmcache_chrefcnt != NULL);
    assert(callbacks->cvmcache_obj_info != NULL);
    assert(callbacks->cvmcache_pread != NULL);
    if (callbacks->capabilities & CVMCACHE_CAP_WRITE) {
      assert(callbacks->cvmcache_start_txn != NULL);
      assert(callbacks->cvmcache_write_txn != NULL);
      assert(callbacks->cvmcache_commit_txn != NULL);
      assert(callbacks->cvmcache_abort_txn != NULL);
    }
    if (callbacks->capabilities & CVMCACHE_CAP_INFO)
      assert(callbacks->cvmcache_info != NULL);
    if (callbacks->capabilities & CVMCACHE_CAP_SHRINK_RATE)
      assert(callbacks->capabilities & CVMCACHE_CAP_INFO);
    if (callbacks->capabilities & CVMCACHE_CAP_SHRINK)
      assert(callbacks->cvmcache_shrink != NULL);
    if (callbacks->capabilities & CVMCACHE_CAP_LIST) {
      assert(callbacks->cvmcache_listing_begin != NULL);
      assert(callbacks->cvmcache_listing_next != NULL);
      assert(callbacks->cvmcache_listing_end != NULL);
    }
    if (callbacks->capabilities & CVMCACHE_CAP_BREADCRUMB) {
      assert(callbacks->cvmcache_breadcrumb_store != NULL);
      assert(callbacks->cvmcache_breadcrumb_load != NULL);
    }
  }
  virtual ~ForwardCachePlugin() { }

 protected:
  virtual cvmfs::EnumStatus ChangeRefcount(
    const shash::Any &id,
    int32_t change_by)
  {
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
    memset(&c_info, 0, sizeof(c_info));
    c_info.size = CachePlugin::kSizeUnknown;
    int result = callbacks_.cvmcache_obj_info(&c_hash, &c_info);
    info->size = c_info.size;
    info->object_type = static_cast<cvmfs::EnumObjectType>(c_info.type);
    info->pinned = c_info.pinned;
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
    if (!(callbacks_.capabilities & CVMCACHE_CAP_WRITE))
      return cvmfs::STATUS_NOSUPPORT;

    struct cvmcache_hash c_hash = Cpphash2Chash(id);
    cvmcache_object_info c_info;
    memset(&c_info, 0, sizeof(c_info));
    c_info.size = info.size;
    c_info.type = ObjectType2CType(info.object_type);
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
    if (!(callbacks_.capabilities & CVMCACHE_CAP_WRITE))
      return cvmfs::STATUS_NOSUPPORT;

    int result = callbacks_.cvmcache_write_txn(txn_id, buffer, size);
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus CommitTxn(const uint64_t txn_id) {
    if (!(callbacks_.capabilities & CVMCACHE_CAP_WRITE))
      return cvmfs::STATUS_NOSUPPORT;

    int result = callbacks_.cvmcache_commit_txn(txn_id);
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus AbortTxn(const uint64_t txn_id) {
    if (!(callbacks_.capabilities & CVMCACHE_CAP_WRITE))
      return cvmfs::STATUS_NOSUPPORT;

    int result = callbacks_.cvmcache_abort_txn(txn_id);
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus GetInfo(Info *info) {
    if (!(callbacks_.capabilities & CVMCACHE_CAP_INFO))
      return cvmfs::STATUS_NOSUPPORT;

    cvmcache_info c_info;
    c_info.size_bytes = info->size_bytes;
    c_info.used_bytes = info->used_bytes;
    c_info.pinned_bytes = info->pinned_bytes;
    c_info.no_shrink = info->no_shrink;
    int result = callbacks_.cvmcache_info(&c_info);
    if (result == CVMCACHE_STATUS_OK) {
      info->size_bytes = c_info.size_bytes;
      info->used_bytes = c_info.used_bytes;
      info->pinned_bytes = c_info.pinned_bytes;
      info->no_shrink = c_info.no_shrink;
    }
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus Shrink(uint64_t shrink_to, uint64_t *used) {
    if (!(callbacks_.capabilities & CVMCACHE_CAP_SHRINK))
      return cvmfs::STATUS_NOSUPPORT;

    int result = callbacks_.cvmcache_shrink(shrink_to, used);
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus ListingBegin(
    uint64_t lst_id,
    cvmfs::EnumObjectType type)
  {
    if (!(callbacks_.capabilities & CVMCACHE_CAP_LIST))
      return cvmfs::STATUS_NOSUPPORT;

    int result =
      callbacks_.cvmcache_listing_begin(lst_id, ObjectType2CType(type));
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus ListingNext(
    int64_t lst_id,
    ObjectInfo *item)
  {
    if (!(callbacks_.capabilities & CVMCACHE_CAP_LIST))
      return cvmfs::STATUS_NOSUPPORT;

    struct cvmcache_object_info c_item;
    memset(&c_item, 0, sizeof(c_item));
    int result = callbacks_.cvmcache_listing_next(lst_id, &c_item);
    if (result == CVMCACHE_STATUS_OK) {
      item->id = Chash2Cpphash(&c_item.id);
      item->size = c_item.size;
      item->object_type = static_cast<cvmfs::EnumObjectType>(c_item.type);
      item->pinned = c_item.pinned;
      if (c_item.description) {
        item->description = string(c_item.description);
        free(c_item.description);
      }
    }
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus ListingEnd(int64_t lst_id) {
    if (!(callbacks_.capabilities & CVMCACHE_CAP_LIST))
      return cvmfs::STATUS_NOSUPPORT;

    int result = callbacks_.cvmcache_listing_end(lst_id);
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus LoadBreadcrumb(
    const std::string &fqrn, manifest::Breadcrumb *breadcrumb)
  {
    if (!(callbacks_.capabilities & CVMCACHE_CAP_BREADCRUMB))
      return cvmfs::STATUS_NOSUPPORT;

    cvmcache_breadcrumb c_breadcrumb;
    int result =
      callbacks_.cvmcache_breadcrumb_load(fqrn.c_str(), &c_breadcrumb);
    if (result == CVMCACHE_STATUS_OK) {
      breadcrumb->catalog_hash = Chash2Cpphash(&c_breadcrumb.catalog_hash);
      breadcrumb->timestamp = c_breadcrumb.timestamp;
      breadcrumb->revision = c_breadcrumb.revision;
    }
    return static_cast<cvmfs::EnumStatus>(result);
  }

  virtual cvmfs::EnumStatus StoreBreadcrumb(
    const std::string &fqrn, const manifest::Breadcrumb &breadcrumb)
  {
    if (!(callbacks_.capabilities & CVMCACHE_CAP_BREADCRUMB))
      return cvmfs::STATUS_NOSUPPORT;

    cvmcache_breadcrumb c_breadcrumb;
    c_breadcrumb.catalog_hash = Cpphash2Chash(breadcrumb.catalog_hash);
    c_breadcrumb.timestamp = breadcrumb.timestamp;
    c_breadcrumb.revision = breadcrumb.revision;
    int result =
      callbacks_.cvmcache_breadcrumb_store(fqrn.c_str(), &c_breadcrumb);
    return static_cast<cvmfs::EnumStatus>(result);
  }

 private:
  struct cvmcache_callbacks callbacks_;
};

Watchdog *g_watchdog = NULL;

}  // anonymous namespace


struct cvmcache_context {
  explicit cvmcache_context(ForwardCachePlugin *p) : plugin(p) { }
  UniquePtr<ForwardCachePlugin> plugin;
};


int cvmcache_hash_cmp(struct cvmcache_hash *a, struct cvmcache_hash *b) {
  const shash::Any hash_a = Chash2Cpphash(a);
  const shash::Any hash_b = Chash2Cpphash(b);
  if (hash_a < hash_b)
    return -1;
  else if (hash_a == hash_b)
    return 0;
  else
    return 1;
}

char *cvmcache_hash_print(const struct cvmcache_hash *h) {
  const shash::Any hash = Chash2Cpphash(h);
  return strdup(hash.ToString().c_str());
}


void cvmcache_init_global() { }


void cvmcache_cleanup_global() { }

int cvmcache_is_supervised() {
  return getenv(CacheTransport::kEnvReadyNotifyFd) != NULL;
}

struct cvmcache_context *cvmcache_init(struct cvmcache_callbacks *callbacks) {
  return new cvmcache_context(new ForwardCachePlugin(callbacks));
}

int cvmcache_listen(struct cvmcache_context *ctx, char *locator) {
  return ctx->plugin->Listen(locator);
}

void cvmcache_process_requests(struct cvmcache_context *ctx, unsigned nworkers)
{
  ctx->plugin->ProcessRequests(nworkers);
}

void cvmcache_ask_detach(struct cvmcache_context *ctx) {
  ctx->plugin->AskToDetach();
}

void cvmcache_wait_for(struct cvmcache_context *ctx) {
  ctx->plugin->WaitFor();
  delete ctx;
}

void cvmcache_terminate(struct cvmcache_context *ctx) {
  ctx->plugin->Terminate();
}

uint32_t cvmcache_max_object_size(struct cvmcache_context *ctx) {
  return ctx->plugin->max_object_size();
}

void cvmcache_get_session(cvmcache_session *session) {
  assert(session != NULL);
  SessionCtx *session_ctx = SessionCtx::GetInstance();
  assert(session_ctx);
  session_ctx->Get(&(session->id),
                   &(session->repository_name),
                   &(session->client_instance));
}

void cvmcache_spawn_watchdog(const char *crash_dump_file) {
  if (g_watchdog != NULL)
    return;
  g_watchdog = Watchdog::Create(NULL);
  assert(g_watchdog != NULL);
  g_watchdog->Spawn((crash_dump_file != NULL) ? string(crash_dump_file) : "");
}

void cvmcache_terminate_watchdog() {
  delete g_watchdog;
  g_watchdog = NULL;
}
