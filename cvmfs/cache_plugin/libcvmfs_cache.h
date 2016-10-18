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

enum cvmcache_status {
  STATUS_UNKNOWN = 0,
  STATUS_OK,
  STATUS_FORBIDDEN,  // Client is not allowed to perform the operation
  STATUS_NOSPACE,    // Cache is full
  STATUS_NOENTRY,    // Object is not in cache
  STATUS_MALFORMED,  // Malformed request
  STATUS_CORRUPTED,  // Crc32 verification failed
  STATUS_TIMEOUT,    // Certain parts of a multipart request never arrived
  STATUS_BADCOUNT,   // Attempt to set a negative reference count
  // Attempt to read from an offset larger than the object size
  STATUS_OUTOFBOUNDS
};

enum cvmcache_object_type {
  OBJECT_REGULAR = 0,
  OBJECT_CATALOG,
  OBJECT_VOLATILE
};

struct cvmcache_object_info {
  uint64_t size;
  enum cvmcache_object_type type;
  char *description;
};

typedef void cvmcache_hash;
struct cvmcache_context;

int cvmcache_hash_cmp(const cvmcache_hash *a, const cvmcache_hash *b);
char *cvmcache_hash_print(const cvmcache_hash *hash);

struct cvmcache_callbacks {
  int (*cvmcache_chrefcnt)(const cvmcache_hash *id, int32_t change_by);
  int (*cvmcache_obj_info)(const cvmcache_hash *id,
                           struct cvmcache_object_info *info);
  int (*cvmcache_pread)(const cvmcache_hash *id,
                        uint64_t offset,
                        uint32_t *size,
                        unsigned char *buffer);
  int (*cvmcache_start_txn)(const cvmcache_hash *id,
                            uint64_t txn_id,
                            struct cvmcache_object_info *info);
  int (*cvmcache_write_txn)(uint64_t txn_id,
                            unsigned char *buffer,
                            uint32_t size);
  int (*cvmcache_commit_txn)(uint64_t txn_id);
  int (*cvmcache_abort_txn)(uint64_t txn_id);
};

struct cvmcache_context *cvmcache_init(struct cvmcache_callbacks *callbacks);
int cvmcache_listen(struct cvmcache_context *ctx, char *socket_path);
void cvmcache_process_requests(struct cvmcache_context *ctx);
void cvmcache_terminate(struct cvmcache_context *ctx);

#ifdef __cplusplus
}
#endif

#endif  // CVMFS_LIBCVMFS_CACHE_H_
