/**
 * This file is part of the CernVM File System.
 *
 * See cvmfs_cache_null plugin for a demo user of the library.
 */
#ifndef CVMFS_CACHE_PLUGIN_LIBCVMFS_CACHE_H_
#define CVMFS_CACHE_PLUGIN_LIBCVMFS_CACHE_H_

// Revision Changelog
// 1 --> 2:
//   - Add CVMCACHE_CAP_WRITE capability, adjust other capability constants
// 2 --> 3:
//   - Add cvmcache_get_session()
// 3 --> 4:
//   - Add breadcrumb management
// 4 --> 5:
//   - Add revision to breadcrumb
#define LIBCVMFS_CACHE_REVISION 5

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
// Map C++ classes to their C interface names
typedef class SimpleOptionsParser cvmcache_option_map;
#else
typedef struct OptionsManager cvmcache_option_map;
#endif

// Mirrors cvmfs::EnumHashAlgorithm protobuf definition
enum cvmcache_hash_algorithm {
  CVMCACHE_HASH_SHA1 = 1,
  CVMCACHE_HASH_RIPEMD160,
  CVMCACHE_HASH_SHAKE128
};

// Mirrors cvmfs::EnumStatus protobuf definition
enum cvmcache_status {
  CVMCACHE_STATUS_UNKNOWN = 0,
  CVMCACHE_STATUS_OK,
  CVMCACHE_STATUS_NOSUPPORT,  // Not implemented by the cache plugin
  CVMCACHE_STATUS_FORBIDDEN,  // Client is not allowed to perform the operation
  CVMCACHE_STATUS_NOSPACE,    // Cache is full
  CVMCACHE_STATUS_NOENTRY,    // Object is not in cache
  CVMCACHE_STATUS_MALFORMED,  // Malformed request
  CVMCACHE_STATUS_IOERR,      // General I/O error
  CVMCACHE_STATUS_CORRUPTED,  // Crc32 verification failed
  // Certain parts of a multipart request never arrived
  CVMCACHE_STATUS_TIMEOUT,
  CVMCACHE_STATUS_BADCOUNT,  // Attempt to set a negative reference count
  // Attempt to read from an offset larger than the object size
  CVMCACHE_STATUS_OUTOFBOUNDS,
  // Cache content could not be evicted to requested size
  CVMCACHE_STATUS_PARTIAL
};

// Mirrors cvmfs::EnumObjectType protobuf definition
enum cvmcache_object_type {
  CVMCACHE_OBJECT_REGULAR = 0,
  CVMCACHE_OBJECT_CATALOG,
  CVMCACHE_OBJECT_VOLATILE
};

// Mirrors cvmfs::EnumCapability protobuf definition
enum cvmcache_capabilities {
  CVMCACHE_CAP_NONE = 0,
  // A read-only cache needs to be pre-populated by other means
  CVMCACHE_CAP_WRITE = 1,
  // Proper refcounting is implemented; for lower tier caches, this capability
  // can be unset and reference counting can simply beomce file existence check
  CVMCACHE_CAP_REFCOUNT = 2,
  CVMCACHE_CAP_SHRINK = 4,        // clients can ask the cache to shrink
  CVMCACHE_CAP_INFO = 8,          // cache plugin knows about its fill level
  CVMCACHE_CAP_SHRINK_RATE = 16,  // cache knows number of cleanup operations
  CVMCACHE_CAP_LIST = 32,         // cache can return a list of objects
  CVMCACHE_CAP_ALL_V1 = 63,
  CVMCACHE_CAP_BREADCRUMB = 64,  // cache can load and store breadcrumps
  CVMCACHE_CAP_ALL_V2 = 127,
};

#define CVMCACHE_SIZE_UNKNOWN (uint64_t(-1))

struct cvmcache_hash {
  unsigned char digest[20];
  char algorithm;
} __attribute__((__packed__));

struct cvmcache_object_info {
  struct cvmcache_hash id;
  uint64_t size;
  enum cvmcache_object_type type;
  int pinned;
  char *description;
};

struct cvmcache_info {
  uint64_t size_bytes;
  uint64_t used_bytes;
  uint64_t pinned_bytes;
  int64_t no_shrink;
};

struct cvmcache_context;

struct cvmcache_session {
  uint64_t id;
  char *repository_name;
  char *client_instance;
};

struct cvmcache_breadcrumb {
  struct cvmcache_hash catalog_hash;
  uint64_t timestamp;
  uint64_t revision;
};

/**
 * Returns -1, 0, or 1 like other C comparison functions
 */
int cvmcache_hash_cmp(struct cvmcache_hash *a, struct cvmcache_hash *b);
/**
 * The caller has to free the resulting string
 */
char *cvmcache_hash_print(const struct cvmcache_hash *h);

/**
 * According to capabilities, some of the callbacks can be NULL
 */
struct cvmcache_callbacks {
  /**
   * Returns CVMCACHE_STATUS_OK or CVMCACHE_STATUS_BADCOUNT if objects reference
   * counter would fall below zero.
   */
  int (*cvmcache_chrefcnt)(struct cvmcache_hash *id, int32_t change_by);
  /**
   * Needs to fill only the size of the object.
   */
  int (*cvmcache_obj_info)(struct cvmcache_hash *id,
                           struct cvmcache_object_info *info);
  /**
   * Returns CVMCACHE_STATUS_OUTOFBOUNDS if offset is larger than file size.
   * Otherwise must work if object's reference counter is larger than zero.
   */
  int (*cvmcache_pread)(struct cvmcache_hash *id,
                        uint64_t offset,
                        uint32_t *size,
                        unsigned char *buffer);
  /**
   * The same object might be uploaded concurrently by multiple users.
   */
  int (*cvmcache_start_txn)(struct cvmcache_hash *id,
                            uint64_t txn_id,
                            struct cvmcache_object_info *info);
  /**
   * A full block is appended except possibly for the file's last block
   */
  int (*cvmcache_write_txn)(uint64_t txn_id,
                            unsigned char *buffer,
                            uint32_t size);
  /**
   * Only as of commit the object must appear to other clients.
   */
  int (*cvmcache_commit_txn)(uint64_t txn_id);
  int (*cvmcache_abort_txn)(uint64_t txn_id);

  int (*cvmcache_info)(struct cvmcache_info *info);
  int (*cvmcache_shrink)(uint64_t shrink_to, uint64_t *used);
  /**
   * Listing can be "approximate", e.g. if files are removed and/or added in
   * the meantime, this may or may not be reflected.
   */
  int (*cvmcache_listing_begin)(uint64_t lst_id,
                                enum cvmcache_object_type type);
  int (*cvmcache_listing_next)(int64_t lst_id,
                               struct cvmcache_object_info *item);
  int (*cvmcache_listing_end)(int64_t lst_id);

  int (*cvmcache_breadcrumb_store)(const char *fqrn,
                                   const cvmcache_breadcrumb *breadcrumb);
  int (*cvmcache_breadcrumb_load)(const char *fqrn,
                                  cvmcache_breadcrumb *breadcrumb);

  int capabilities;
};

/**
 * Should be called before any other cvmcache_... function.
 */
void cvmcache_init_global();
/**
 * Deletes global state, afterwards no further calls to cvmcache_... functions
 * should take place.
 */
void cvmcache_cleanup_global();
/**
 * True if the plugin was started from a cvmfs mountpoint and thus will
 * terminate by itself when the last mount point disconnects.
 */
int cvmcache_is_supervised();

struct cvmcache_context *cvmcache_init(struct cvmcache_callbacks *callbacks);
/**
 * The locator is either a UNIX domain socket (unix=/path/to/socket) or a
 * tcp socket (tcp=hostname:port)
 */
int cvmcache_listen(struct cvmcache_context *ctx, char *locator);
/**
 * Spawns a separate I/O thread that can be stopped with cvmcache_terminate.
 * The nworkers parameter is currently unused.
 */
void cvmcache_process_requests(struct cvmcache_context *ctx, unsigned nworkers);
/**
 * Politely ask connected clients to release open nested catalogs so that more
 * objects in the cache become unpinned.
 */
void cvmcache_ask_detach(struct cvmcache_context *ctx);
/**
 * Stops the processing thread.
 */
void cvmcache_terminate(struct cvmcache_context *ctx);
/**
 * Blocks until the processing thread finishes.  Can either happen due to a call
 * to cvmcache_terminate or -- when the plugin is started from cvmfs -- when
 * the last repository is unmounted.  Invalidates the context object.
 */
void cvmcache_wait_for(struct cvmcache_context *ctx);
uint32_t cvmcache_max_object_size(struct cvmcache_context *ctx);

/**
 * Can be used to spawn a second process that superwises the cache plugin.
 * The watchdog can use gdb/lldb to generate stack traces.  Must be closed by
 * a call to cvmcache_close_watchdog(), otherwise the main process will be
 * reported has having died unexpectedly.
 */
void cvmcache_spawn_watchdog(const char *crash_dump_file);
void cvmcache_terminate_watchdog();

/**
 * Returns a static pointer to an origin struct that identifies the client
 * connection that triggered a callback. Calling this function is only valid
 * from within a callback.  Otherwise the function returns NULL values.
 */
void cvmcache_get_session(cvmcache_session *session);


// Options parsing from libcvmfs without "libcvmfs legacy" support

cvmcache_option_map *cvmcache_options_init();
/**
 * Frees the resources of a cvmfs_options_map, which was created by a call to
 * cvmfs_options_init().
 */
void cvmcache_options_fini(cvmcache_option_map *opts);
/**
 * Fills a cvmfs_options_map.  Use the same key/value pairs as the configuration
 * parameters used by the fuse module in /etc/cvmfs/...
 */
void cvmcache_options_set(cvmcache_option_map *opts, const char *key,
                          const char *value);
/**
 * Sets options from a file with linewise KEY=VALUE pairs.  Returns 0 on success
 * and -1 otherwise.
 */
int cvmcache_options_parse(cvmcache_option_map *opts, const char *path);
/**
 * Removes a key-value pair from a cvmfs_options_map.  The key may or may not
 * exist before the call.
 */
void cvmcache_options_unset(cvmcache_option_map *opts, const char *key);
/**
 * Retrieves the value for a given key or NULL of the key does not exist.  If
 * the result is not NULL, it must be freed by a call to cvmfs_options_free().
 */
char *cvmcache_options_get(cvmcache_option_map *opts, const char *key);
/**
 * Prints the key-value pairs of cvmfs_option_map line-by-line.  The resulting
 * string needs to be freed by a call to cvmfs_options_free().
 */
char *cvmcache_options_dump(cvmcache_option_map *opts);
/**
 * Frees a string returned from cvmfs_options_get() or cvmfs_options_dump().
 */
void cvmcache_options_free(char *value);

#ifdef __cplusplus
}
#endif

#endif  // CVMFS_CACHE_PLUGIN_LIBCVMFS_CACHE_H_
