/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_LIBCVMFS_CACHE_H_
#define CVMFS_LIBCVMFS_CACHE_H_

// Revision Changelog
#define LIBCVMFS_CACHE_REVISION 1

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum cvmcache_hash_algorithm {
  CVMCACHE_HASH_SHA1 = 1,
  CVMCACHE_HASH_RIPEMD160,
  CVMCACHE_HASH_SHAKE128
};

struct cvmcache_hash {
  unsigned char digest[20];
  char algorithm;
} __attribute__((__packed__));

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
  CVMCACHE_STATUS_BADCOUNT,   // Attempt to set a negative reference count
  // Attempt to read from an offset larger than the object size
  CVMCACHE_STATUS_OUTOFBOUNDS,
  // Cache content could not be evicted to requested size
  CVMCACHE_STATUS_PARTIAL
};

enum cvmcache_object_type {
  CVMCACHE_OBJECT_REGULAR = 0,
  CVMCACHE_OBJECT_CATALOG,
  CVMCACHE_OBJECT_VOLATILE
};

enum cvmcache_capabilities {
  CVMCACHE_CAP_NONE      = 0,
  CVMCACHE_CAP_REFCOUNT  = 1,
  CVMCACHE_CAP_SHRINK    = 2,
  CVMCACHE_CAP_INFO      = 4,
  CVMCACHE_CAP_LIST      = 8,
  CVMCACHE_CAP_ALL       = 15
};

struct cvmcache_object_info {
  struct cvmcache_hash id;
  uint64_t size;
  enum cvmcache_object_type type;
  int pinned;
  char *description;
};

struct cvmcache_context;

int cvmcache_hash_cmp(struct cvmcache_hash *a, struct cvmcache_hash *b);
char *cvmcache_hash_print(struct cvmcache_hash *h);

struct cvmcache_callbacks {
  int (*cvmcache_chrefcnt)(struct cvmcache_hash *id, int32_t change_by);
  int (*cvmcache_obj_info)(struct cvmcache_hash *id,
                           struct cvmcache_object_info *info);
  int (*cvmcache_pread)(struct cvmcache_hash *id,
                        uint64_t offset,
                        uint32_t *size,
                        unsigned char *buffer);
  int (*cvmcache_start_txn)(struct cvmcache_hash *id,
                            uint64_t txn_id,
                            struct cvmcache_object_info *info);
  int (*cvmcache_write_txn)(uint64_t txn_id,
                            unsigned char *buffer,
                            uint32_t size);
  int (*cvmcache_commit_txn)(uint64_t txn_id);
  int (*cvmcache_abort_txn)(uint64_t txn_id);

  int (*cvmcache_info)(uint64_t *size, uint64_t *used, uint64_t *pinned);
  int (*cvmcache_shrink)(uint64_t shrink_to, uint64_t *used);
  int64_t (*cvmcache_listing_begin)(enum cvmcache_object_type type);
  int (*cvmcache_listing_next)(int64_t listing_id,
                               struct cvmcache_object_info *item);
  int (*cvmcache_listing_end)(int64_t listing_id);

  int capabilities;
};

struct cvmcache_context *cvmcache_init(struct cvmcache_callbacks *callbacks);
int cvmcache_listen(struct cvmcache_context *ctx, char *socket_path);
void cvmcache_process_requests(struct cvmcache_context *ctx, unsigned nworkers);
void cvmcache_terminate(struct cvmcache_context *ctx);

uint32_t cvmcache_max_object_size(struct cvmcache_context *ctx);

#ifdef __cplusplus
}
#endif

#endif  // CVMFS_LIBCVMFS_CACHE_H_
