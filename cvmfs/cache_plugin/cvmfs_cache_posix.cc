/**
 * This file is part of the CernVM File System.
 *
 **/

#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <cstring>
#include <string>

#include "cache_plugin/libcvmfs_cache.h"
#include "cache_posix.h"
#include "smallhash.h"
#include "util/atomic.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

bool operator==(const cvmcache_hash &a, const cvmcache_hash &b) {
  return memcmp(a.digest, b.digest, 20) == 0;
}

bool operator!=(const cvmcache_hash &a, const cvmcache_hash &b) {
  return memcmp(a.digest, b.digest, 20) != 0;
}

namespace {

struct CacheObject {
  uint32_t refcnt;
  int fd;
  uint64_t size;
};

struct Txn {
  struct cvmcache_hash hash;
  uint64_t size;
  cvmcache_object_type type;
  void *txn;
  int fd;
};

struct Listing {
  cvmcache_object_type type;
  std::vector<std::string> list;
  std::vector<std::string>::iterator it;
};

struct Settings {
  Settings()
      : is_alien(false)
      , cache_base_defined(false)
      , cache_dir_defined(false)
      , quota_limit(0) { }

  bool IsValid() {
    if (is_alien && quota_limit > 0) {
      error_reason = "Alien cache cannot be managed (no quota manager allowed)";
      return false;
    }
    if (is_alien && workspace == "") {
      error_reason = "Workspace option needs to be set for alien cache";
      return false;
    }
    if ((is_alien ? 1 : 0) + (cache_base_defined ? 1 : 0)
            + (cache_dir_defined ? 1 : 0)
        != 1) {
      error_reason = "CVMFS_CACHE_DIR, CVMFS_CACHE_BASE and CVMFS_CACHE_ALIEN "
                     "are mutually exclusive. Exactly one needs to be defined.";
      return false;
    }
    return true;
  }

  bool is_alien;
  bool cache_base_defined;
  bool cache_dir_defined;
  /**
   * Soft limit in bytes for the cache.  The quota manager removes half the
   * cache when the limit is exceeded.
   */
  int64_t quota_limit;
  std::string cache_path;
  /**
   * Different from cache_path only if CVMFS_WORKSPACE or
   * CVMFS_CACHE_WORKSPACE is set.
   */
  std::string workspace;

  std::string error_reason;
};


Settings GetSettings(cvmcache_option_map *options) {
  Settings settings;
  char *optarg = NULL;

  if ((optarg = cvmcache_options_get(options, "CVMFS_CACHE_QUOTA_LIMIT"))) {
    settings.quota_limit = String2Int64(optarg) * 1024 * 1024;
    cvmcache_options_free(optarg);
  }

  if ((optarg = cvmcache_options_get(options, "CVMFS_CACHE_BASE"))) {
    settings.cache_base_defined = true;
    settings.cache_path = MakeCanonicalPath(optarg);
    settings.cache_path += "/shared";          // this cache is always shared
    settings.workspace = settings.cache_path;  // default value for workspace
    cvmcache_options_free(optarg);
  }

  if ((optarg = cvmcache_options_get(options, "CVMFS_CACHE_DIR"))) {
    settings.cache_dir_defined = true;
    settings.cache_path = optarg;
    settings.workspace = settings.cache_path;  // default value for workspace
    cvmcache_options_free(optarg);
  }
  if ((optarg = cvmcache_options_get(options, "CVMFS_CACHE_ALIEN"))) {
    settings.is_alien = true;
    settings.cache_path = optarg;
    cvmcache_options_free(optarg);
  }

  if ((optarg = cvmcache_options_get(options, "CVMFS_CACHE_WORKSPACE"))) {
    // Used for the shared quota manager
    settings.workspace = optarg;
    cvmcache_options_free(optarg);
  }
  return settings;
}

uint32_t cvmcache_hash_hasher(const struct cvmcache_hash &key) {
  return static_cast<uint32_t>(
      *(reinterpret_cast<const uint32_t *>(key.digest) + 1));
}

uint32_t uint64_hasher(const uint64_t &key) { return (uint32_t)key; }

shash::Any Chash2Cpphash(const struct cvmcache_hash *h) {
  shash::Any hash;
  memcpy(hash.digest, h->digest, sizeof(h->digest));
  hash.algorithm = static_cast<shash::Algorithms>(h->algorithm);
  return hash;
}

struct cvmcache_hash Cpphash2Chash(const shash::Any &hash) {
  struct cvmcache_hash h;
  memset(h.digest, 0, 20);  // ensure deterministic digest
  memcpy(h.digest, hash.digest, sizeof(h.digest));
  h.algorithm = hash.algorithm;
  return h;
}

SmallHashDynamic<struct cvmcache_hash, CacheObject> *g_opened_objects;
SmallHashDynamic<uint64_t, Txn> *g_transactions;
SmallHashDynamic<uint64_t, Listing> *g_listings;
PosixCacheManager *g_cache_mgr;
cvmcache_context *g_ctx;
atomic_int32 g_terminated;
uint64_t g_pinned_size;
uint64_t g_used_size;
uint64_t g_capacity;

int posix_chrefcnt(struct cvmcache_hash *id, int32_t change_by) {
  CacheObject object;
  if (!g_opened_objects->Lookup(*id, &object)) {
    if (change_by < 0) {
      return CVMCACHE_STATUS_BADCOUNT;
    }
    const CacheManager::LabeledObject labeled_object(Chash2Cpphash(id));
    const int fd = g_cache_mgr->Open(labeled_object);
    if (fd < 0) {
      return CVMCACHE_STATUS_NOENTRY;
    }
    object.fd = fd;
    object.size = g_cache_mgr->GetSize(fd);
    object.refcnt = 0;
    g_pinned_size += object.size;
  } else if (static_cast<int32_t>(object.refcnt) + change_by < 0) {
    return CVMCACHE_STATUS_BADCOUNT;
  }

  object.refcnt += change_by;
  if (object.refcnt == 0) {
    if (g_cache_mgr->Close(object.fd) != 0) {
      return CVMCACHE_STATUS_IOERR;
    }
    g_pinned_size -= object.size;
    g_opened_objects->Erase(*id);
  } else {
    g_opened_objects->Insert(*id, object);
  }
  return CVMCACHE_STATUS_OK;
}

// Only gives info for opened objects.
// Should be fine, since cvmfs only requests info for opened objects.
int posix_obj_info(struct cvmcache_hash *id,
                   struct cvmcache_object_info *info) {
  CacheObject object;
  if (!g_opened_objects->Lookup(*id, &object)) {
    return CVMCACHE_STATUS_NOENTRY;
  }
  info->id = *id;
  info->size = object.size;
  return CVMCACHE_STATUS_OK;
}

int posix_pread(struct cvmcache_hash *id, uint64_t offset, uint32_t *size,
                unsigned char *buffer) {
  CacheObject object;
  if (!g_opened_objects->Lookup(*id, &object)) {
    return CVMCACHE_STATUS_NOENTRY;
  }
  if (offset > object.size) {
    return CVMCACHE_STATUS_OUTOFBOUNDS;
  }
  const int64_t bytes_read = g_cache_mgr->Pread(object.fd, buffer, *size,
                                                offset);
  if (bytes_read < 0) {
    return CVMCACHE_STATUS_IOERR;
  }
  *size = static_cast<uint32_t>(bytes_read);
  return CVMCACHE_STATUS_OK;
}

int posix_start_txn(struct cvmcache_hash *id,
                    uint64_t txn_id,
                    struct cvmcache_object_info *info) {
  // cachemgr deletes txn in commit_txn
  void *txn = malloc(g_cache_mgr->SizeOfTxn());
  const int fd = g_cache_mgr->StartTxn(Chash2Cpphash(id), info->size, txn);
  if (fd < 0) {
    return CVMCACHE_STATUS_IOERR;
  }
  Txn transaction;
  transaction.fd = fd;
  transaction.hash = *id;
  transaction.txn = txn;
  transaction.size = info->size;
  transaction.type = info->type;
  g_transactions->Insert(txn_id, transaction);

  CacheManager::Label label;
  if (info->type == CVMCACHE_OBJECT_CATALOG) {
    label.flags |= CacheManager::kLabelCatalog;
  } else if (info->type == CVMCACHE_OBJECT_VOLATILE) {
    label.flags = CacheManager::kLabelVolatile;
  }
  if (info->description) {
    label.path = info->description;
  }
  g_cache_mgr->CtrlTxn(label, 0, txn);
  return CVMCACHE_STATUS_OK;
}

int posix_write_txn(uint64_t txn_id, unsigned char *buffer, uint32_t size) {
  Txn transaction;
  if (!g_transactions->Lookup(txn_id, &transaction)) {
    return CVMCACHE_STATUS_NOENTRY;
  }
  const int64_t bytes_written = g_cache_mgr->Write(buffer, size,
                                                   transaction.txn);
  if ((bytes_written >= 0) && (static_cast<uint32_t>(bytes_written) == size)) {
    return CVMCACHE_STATUS_OK;
  } else {
    return CVMCACHE_STATUS_IOERR;
  }
}

int posix_commit_txn(uint64_t txn_id) {
  Txn transaction;
  if (!g_transactions->Lookup(txn_id, &transaction)) {
    return CVMCACHE_STATUS_NOENTRY;
  }
  CacheObject object;
  if (!g_opened_objects->Lookup(transaction.hash, &object)) {
    object.fd = g_cache_mgr->OpenFromTxn(transaction.txn);
    if (object.fd < 0) {
      return CVMCACHE_STATUS_IOERR;
    }
    const int result = g_cache_mgr->CommitTxn(transaction.txn);
    if (result) {
      return CVMCACHE_STATUS_IOERR;
    }
    object.refcnt = 0;
    object.size = g_cache_mgr->GetSize(object.fd);
    g_pinned_size += object.size;
    g_used_size += object.size;
  } else {
    if (g_cache_mgr->AbortTxn(transaction.txn) != 0) {
      return CVMCACHE_STATUS_IOERR;
    }
  }
  object.refcnt += 1;

  g_opened_objects->Insert(transaction.hash, object);
  g_transactions->Erase(txn_id);
  return CVMCACHE_STATUS_OK;
}

int posix_abort_txn(uint64_t txn_id) {
  Txn transaction;
  if (!g_transactions->Lookup(txn_id, &transaction)) {
    return CVMCACHE_STATUS_NOENTRY;
  }
  if (g_cache_mgr->AbortTxn(transaction.txn)) {
    return CVMCACHE_STATUS_IOERR;
  }
  g_transactions->Erase(txn_id);
  return CVMCACHE_STATUS_OK;
}

int posix_info(struct cvmcache_info *info) {
  info->no_shrink = -1;
  info->size_bytes = g_capacity;
  info->used_bytes = g_used_size;
  info->pinned_bytes = g_pinned_size;
  return CVMCACHE_STATUS_OK;
}

int posix_breadcrumb_store(const char *fqrn,
                           const cvmcache_breadcrumb *breadcrumb) {
  const manifest::Breadcrumb bc(Chash2Cpphash(&breadcrumb->catalog_hash),
                                breadcrumb->timestamp, breadcrumb->revision);
  if (!g_cache_mgr->StoreBreadcrumb(fqrn, bc)) {
    return CVMCACHE_STATUS_IOERR;
  }
  return CVMCACHE_STATUS_OK;
}

int posix_breadcrumb_load(const char *fqrn, cvmcache_breadcrumb *breadcrumb) {
  const manifest::Breadcrumb bc = g_cache_mgr->LoadBreadcrumb(fqrn);
  if (!bc.IsValid()) {
    return CVMCACHE_STATUS_NOENTRY;
  }
  breadcrumb->catalog_hash = Cpphash2Chash(bc.catalog_hash);
  breadcrumb->timestamp = bc.timestamp;
  breadcrumb->revision = bc.revision;
  return CVMCACHE_STATUS_OK;
}

void handle_sigint(int sig) {
  cvmcache_terminate(g_ctx);
  atomic_inc32(&g_terminated);
}

}  // namespace

int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(stderr, "Missing argument: path to config file\n");
    return 1;
  }

  cvmcache_init_global();

  cvmcache_option_map *options = cvmcache_options_init();
  if (cvmcache_options_parse(options, argv[1]) != 0) {
    LogCvmfs(kLogCache, kLogStderr | kLogSyslogErr,
             "cannot parse options file %s", argv[1]);
    return 1;
  }
  char *debug_log = cvmcache_options_get(options,
                                         "CVMFS_CACHE_PLUGIN_DEBUGLOG");
  if (debug_log != NULL) {
    SetLogDebugFile(debug_log);
    cvmcache_options_free(debug_log);
  } else {
    SetLogDebugFile("/dev/null");
  }
  char *locator = cvmcache_options_get(options, "CVMFS_CACHE_PLUGIN_LOCATOR");
  if (locator == NULL) {
    LogCvmfs(kLogCache, kLogStderr | kLogSyslogErr,
             "CVMFS_CACHE_PLUGIN_LOCATOR missing");
    cvmcache_options_fini(options);
    return 1;
  }
  char *test_mode = cvmcache_options_get(options, "CVMFS_CACHE_PLUGIN_TEST");
  if (!test_mode) {
    char *watchdog_crash_dump_path = cvmcache_options_get(
        options, "CVMFS_CACHE_PLUGIN_CRASH_DUMP");
    cvmcache_spawn_watchdog(watchdog_crash_dump_path);
    if (watchdog_crash_dump_path)
      cvmcache_options_free(watchdog_crash_dump_path);
  }

  Settings settings = GetSettings(options);
  if (!settings.IsValid()) {
    LogCvmfs(kLogCache, kLogStderr | kLogSyslogErr,
             "Invalid config in file %s: %s", argv[1],
             settings.error_reason.c_str());
    return 1;
  }

  g_cache_mgr = PosixCacheManager::Create(settings.cache_path,
                                          settings.is_alien);

  cvmcache_hash empty_hash;
  empty_hash.algorithm = 0;
  memset(empty_hash.digest, 0, 20);
  g_opened_objects = new SmallHashDynamic<cvmcache_hash, CacheObject>;
  g_transactions = new SmallHashDynamic<uint64_t, Txn>;
  g_listings = new SmallHashDynamic<uint64_t, Listing>;
  g_opened_objects->Init(32, empty_hash, cvmcache_hash_hasher);
  g_transactions->Init(32, (uint64_t(-1)), uint64_hasher);
  g_listings->Init(32, (uint64_t(-1)), uint64_hasher);
  g_pinned_size = 0;
  g_used_size = 0;
  g_capacity = CVMCACHE_SIZE_UNKNOWN;

  struct cvmcache_callbacks callbacks;
  memset(&callbacks, 0, sizeof(callbacks));
  callbacks.cvmcache_chrefcnt = posix_chrefcnt;
  callbacks.cvmcache_obj_info = posix_obj_info;
  callbacks.cvmcache_pread = posix_pread;
  callbacks.cvmcache_start_txn = posix_start_txn;
  callbacks.cvmcache_write_txn = posix_write_txn;
  callbacks.cvmcache_commit_txn = posix_commit_txn;
  callbacks.cvmcache_abort_txn = posix_abort_txn;
  callbacks.cvmcache_info = posix_info;
  callbacks.cvmcache_breadcrumb_store = posix_breadcrumb_store;
  callbacks.cvmcache_breadcrumb_load = posix_breadcrumb_load;
  callbacks.capabilities = CVMCACHE_CAP_WRITE + CVMCACHE_CAP_REFCOUNT
                           + CVMCACHE_CAP_INFO + CVMCACHE_CAP_BREADCRUMB;

  g_ctx = cvmcache_init(&callbacks);
  const int retval = cvmcache_listen(g_ctx, locator);
  if (!retval) {
    LogCvmfs(kLogCache, kLogStderr | kLogSyslogErr, "failed to listen on %s",
             locator);
    return 1;
  }

  if (test_mode) {
    // Daemonize, print out PID
    pid_t pid;
    int statloc;
    if ((pid = fork()) == 0) {
      if ((pid = fork()) == 0) {
        const int null_read = open("/dev/null", O_RDONLY);
        const int null_write = open("/dev/null", O_WRONLY);
        assert((null_read >= 0) && (null_write >= 0));
        int retval = dup2(null_read, 0);
        assert(retval == 0);
        retval = dup2(null_write, 1);
        assert(retval == 1);
        retval = dup2(null_write, 2);
        assert(retval == 2);
        close(null_read);
        close(null_write);
      } else {
        assert(pid > 0);
        printf("%d\n", pid);
        fflush(stdout);
        fsync(1);
        _exit(0);
      }
    } else {
      assert(pid > 0);
      waitpid(pid, &statloc, 0);
      _exit(0);
    }
  }

  LogCvmfs(kLogCache, kLogStdout, "Listening for cvmfs clients on %s", locator);

  cvmcache_process_requests(g_ctx, 0);

  if (!cvmcache_is_supervised()) {
    LogCvmfs(kLogCache, kLogStdout,
             "Running unsupervised. Quit by SIGINT (CTRL+C)");
    atomic_init32(&g_terminated);
    signal(SIGINT, handle_sigint);
    while (atomic_read32(&g_terminated) == 0)
      sleep(1);
  }

  cvmcache_wait_for(g_ctx);

  delete g_opened_objects;
  g_opened_objects = NULL;
  delete g_transactions;
  g_transactions = NULL;
  delete g_listings;
  g_listings = NULL;
  delete g_cache_mgr;
  g_cache_mgr = NULL;

  cvmcache_options_free(locator);
  cvmcache_options_fini(options);

  cvmcache_terminate_watchdog();
  cvmcache_cleanup_global();
  return 0;
}
