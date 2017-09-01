/**
 * This file is part of the CernVM File System.
 *
 * A demo external cache plugin.  All data is stored in std::string.  Feature-
 * complete but quite inefficient.
 */

#define __STDC_FORMAT_MACROS

#include <alloca.h>
#include <inttypes.h>
#include <stdint.h>
#include <unistd.h>

#include <fcntl.h>
#include <errno.h>
#include <cassert>
#include <stdio.h>
#include <stdlib.h>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <map>
#include <string>
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <libgen.h>

#include "libcvmfs_cache.h"
#include "hash.h"

using namespace std;  // NOLINT

const char *g_directory = "/var/lib/cvmfs/posix-upper/";

struct Object {
  struct cvmcache_hash id;
  int fd;
  cvmcache_object_type type;
  int32_t size_data = 0;
  int32_t refcnt;
  int32_t neg_nbytes_written = 0;
  string description;

};

struct TxnInfo {
  struct cvmcache_hash id;
  Object partial_object;
  string path;
};

// List of elements requested by a client.
struct Listing {
  Listing() : type(CVMCACHE_OBJECT_REGULAR), pos(0), elems(NULL) { }
  cvmcache_object_type type;
  uint64_t pos;
  vector<Object> *elems;
};

// Fancy way to organize the hash codes.
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

static int null_getpath(struct cvmcache_hash *id, string *urlpath) {
  shash::Digest<20, shash::kSha1> hash_calc(shash::kSha1, id->digest, shash::kSuffixNone);
  string suffix = hash_calc.MakePath();

  //Build URL
  *urlpath = string(g_directory) + suffix;

  return CVMCACHE_STATUS_OK;
}

static int null_create_tmp(Object& partial_object, string *tmp_path) {
  char *char_path = tmpnam(const_cast<char*>(tmp_path->c_str()));
  string string_path(char_path);
  *tmp_path = string(g_directory) + char_path;
  partial_object.fd = open(tmp_path->c_str(), O_CREAT | O_EXCL);
  if (partial_object.fd <0)
    return CVMCACHE_STATUS_BADCOUNT;
  return CVMCACHE_STATUS_OK;
}

static int null_assert_dir(string *dir_path) {
  struct stat statbuffer;
  int rc = stat(dir_path->c_str(), &statbuffer);
  if (rc < 0 && errno != ENOENT) {
    return -errno;
  } else if (rc == 0 && !S_ISDIR(statbuffer.st_mode)) {
    return -1;
  } else if (rc == 0 && S_ISDIR(statbuffer.st_mode)){
    return CVMCACHE_STATUS_OK;
  }
  if (mkdir(dir_path->c_str(), 0777))
    return -errno;
  return CVMCACHE_STATUS_OK;
}


/**
  * Checks the validity of the hash and reference count associated with the
  * desired data.
  */
static int null_chrefcnt(struct cvmcache_hash *id, int32_t change_by) {
  ComparableHash h(*id);

  string urlpath;
  null_getpath(id, &urlpath);

  if (storage.find(h) == storage.end()) {
    Object obj;
    obj.fd = open(urlpath.c_str(), O_RDONLY);
    if (obj.fd < 0)
      return CVMCACHE_STATUS_NOENTRY;
    obj.refcnt = 1;
    storage[h] = obj;
    return CVMCACHE_STATUS_OK;
  }
  Object obj = storage[h];
  if (change_by > 0){
    if ((obj.refcnt + change_by) == 1){
      obj.fd = open(urlpath.c_str(), O_RDONLY);
      if (obj.fd <0)
        return CVMCACHE_STATUS_BADCOUNT;
      }
  }

  //TODO: check the appropriate error to return.
  if (change_by < 0){
    if ((obj.refcnt + change_by) == 0){
      obj.fd = close(obj.fd);
      if (obj.fd <0)
        return CVMCACHE_STATUS_BADCOUNT;
      }
  }

  obj.refcnt += change_by;
  storage[h] = obj;

  return CVMCACHE_STATUS_OK;
}

/**
  * Retrieves object information from the hash.
  * Implements the same "integrity" check as the previous function.
  */
static int null_obj_info(
  struct cvmcache_hash *id,
  struct cvmcache_object_info *info)
{
  struct stat statbuffer;
  ComparableHash h(*id);
  if (storage.find(h) == storage.end())
    return CVMCACHE_STATUS_NOENTRY;

  Object obj = storage[h];

  if (!(fstat(obj.fd, &statbuffer))){
    info->size = statbuffer.st_size;
    info->type = obj.type;
    info->pinned = obj.refcnt > 0;
    info->description = strdup(obj.description.c_str());
  }else
    return CVMCACHE_STATUS_NOENTRY;

  return CVMCACHE_STATUS_OK;
}


// Copies the contents of the cache in the buffer (?)
static int null_pread(struct cvmcache_hash *id,
                    uint64_t offset,
                    uint32_t *size,
                    unsigned char *buffer)
{
  ComparableHash h(*id);

  Object obj = storage[h];
  if (obj.fd < 0)
    return CVMCACHE_STATUS_NOENTRY;

  // returns an error if the offset is larget than the file size
  if (offset > lseek(obj.fd, 0, SEEK_END))
    return CVMCACHE_STATUS_OUTOFBOUNDS;

  //number of bytes the data occupies
  int numbytes = pread(obj.fd, buffer, *size, offset);
  if (numbytes < 0)
    return CVMCACHE_STATUS_IOERR;
  *size = numbytes;

  return CVMCACHE_STATUS_OK;
}

/**
  * Starts the transaction, by acquiring the *id and info
  * to be transferred to the cache. The transactions are then stored in an
  * array and identified by their id.
  */
static int null_start_txn(
  struct cvmcache_hash *id,
  uint64_t txn_id,
  struct cvmcache_object_info *info)
{
  string tmp_path;
  TxnInfo txn;
  Object partial_object;
  partial_object.id = *id;
  partial_object.type = info->type;
  partial_object.refcnt = 1;
  if (info->size != CVMCACHE_SIZE_UNKNOWN)
    partial_object.size_data = info->size;
  if (info->description != NULL)
    partial_object.description = string(info->description);
  string txn_path = string(g_directory) + "/txn/fetchXXXXXX";
  const unsigned txn_path_len = txn_path.length();
  char template_path[txn_path_len + 1];
  memcpy(template_path, &txn_path[0], txn_path_len);

  partial_object.fd = mkstemp(template_path);
  if (partial_object.fd < 0)
    return -errno;
  txn.id = *id;
  txn.partial_object = partial_object;
  txn.path = template_path;
  transactions[txn_id] = txn;
  return CVMCACHE_STATUS_OK;
}

/**
  * Writes the content of the oject to be acquired in the general transactions
  * array.
  */
static int null_write_txn(
  uint64_t txn_id,
  unsigned char *buffer,
  uint32_t size)
{
  TxnInfo txn = transactions[txn_id];
  int written;

  if (txn.partial_object.neg_nbytes_written > 0)
    txn.partial_object.neg_nbytes_written = 0;

  off_t offset = -txn.partial_object.neg_nbytes_written;
  written = pwrite(txn.partial_object.fd, buffer, size, offset);
  if (written < 0)
    return -errno;
  txn.partial_object.neg_nbytes_written -= size;
  transactions[txn_id] = txn;
  return CVMCACHE_STATUS_OK;
}

/**
  * Writes the transaction content in the cache.
  * Tre transaction information is then erased from the general transactions
  * array and from the buffer.
  */
static int null_commit_txn(uint64_t txn_id) {
  int flushed;
  int update_path;
  string urlpath;
  TxnInfo txn = transactions[txn_id];
  ComparableHash h(txn.id);
  null_getpath(&(txn.partial_object.id), &urlpath);
  flushed = close(txn.partial_object.fd);
  if (flushed < 0)
    return -errno;
  size_t pos = urlpath.rfind('/');
  if (pos == string::npos)
    return -EINVAL;
  string dir_path = urlpath.substr(0, pos);
  int rc = null_assert_dir(&dir_path);
  if (rc < 0)
    return rc;
  update_path = rename(txn.path.c_str(), (&urlpath)->c_str());
  if (update_path < 0)
    return -errno;
  txn.partial_object.fd = open((&urlpath)->c_str(), O_RDONLY);
  if (txn.partial_object.fd <0)
    return -errno;
  storage[h] = txn.partial_object;
  transactions.erase(txn_id);

  return CVMCACHE_STATUS_OK;
}

/**
  * Erases transaction information without committing it to memory.
  */
static int null_abort_txn(uint64_t txn_id) {
  TxnInfo txn = transactions[txn_id];
  txn.partial_object.refcnt = 0;
  txn.partial_object.fd = close(txn.partial_object.fd);
  if (txn.partial_object.fd < 0)
    return -errno;
  transactions.erase(txn_id);
  return CVMCACHE_STATUS_OK;
}

/**
  * Retrives informations regarding the cache, such as the number of pinned
  * bites.
  */
static int null_info(struct cvmcache_info *info) {
  info->size_bytes = uint64_t(-1);
  info->used_bytes = info->pinned_bytes = 0;
  for (map<ComparableHash, Object>::const_iterator i = storage.begin(),
       i_end = storage.end(); i != i_end; ++i)
  {
    info->used_bytes += i->second.size_data;
    // only count the bytes which are being referenced
    if (i->second.refcnt > 0)
      info->pinned_bytes += i->second.size_data;
  }
  info->no_shrink = 0;
  return CVMCACHE_STATUS_OK;
}


static int null_shrink(uint64_t shrink_to, uint64_t *used) {
  struct cvmcache_info info;
  null_info(&info);
  *used = info.used_bytes;
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

    unsigned length = i->second.size_data;
    map<ComparableHash, Object>::iterator delete_me = i++;
    storage.erase(delete_me);
    info.used_bytes -= length;
    if (info.used_bytes <= shrink_to) {
      *used = info.used_bytes;
      return CVMCACHE_STATUS_OK;
    }
  }
  // All other objects
  for (map<ComparableHash, Object>::iterator i = storage.begin(),
       i_end = storage.end(); i != i_end; )
  {
    if (i->second.refcnt > 0) {
      ++i;
      continue;
    }
    unsigned length = i->second.size_data;
    map<ComparableHash, Object>::iterator delete_me = i++;
    storage.erase(delete_me);
    info.used_bytes -= length;
    if (info.used_bytes <= shrink_to) {
      *used = info.used_bytes;
      return CVMCACHE_STATUS_OK;
    }
  }

  *used = info.used_bytes;
  return CVMCACHE_STATUS_PARTIAL;
}

static int null_listing_begin(
  uint64_t lst_id,
  enum cvmcache_object_type type)
{
  Listing lst;
  lst.type = type;
  lst.elems = new vector<Object>();
  for (map<ComparableHash, Object>::const_iterator i = storage.begin(),
       i_end = storage.end(); i != i_end; ++i)
  {
    lst.elems->push_back(i->second);
  }
  listings[lst_id] = lst;
  return CVMCACHE_STATUS_OK;
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
      item->size = (*elems)[lst.pos].size_data;
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

/**
  * Retrives informations regarding the cache, such as the number of pinned
  * bites.x
  */
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
  char *locator = cvmcache_options_get(options, "CVMFS_CACHE_PLUGIN_LOCATOR");
  if (locator == NULL) {
    printf("CVMFS_CACHE_PLUGIN_LOCATOR missing\n");
    cvmcache_options_fini(options);
    return 1;
  }

  string g_str(g_directory);
  int rc = null_assert_dir(&g_str);
  if (rc < 0)
    return rc;
  g_str += "/txn";
  rc = null_assert_dir(&g_str);
  if (rc < 0)
    return rc;


#if 0
  const char *hostpath = cvmcache_options_get(options, "CVMFS_CACHE_XRD_HOST");
  if (hostpath == NULL) {
    printf("CVMFS_CACHE_XRD_HOST missing\n");
    cvmcache_options_fini(options);
    return 1;
  }
  string full_env = string(hostpath) + string(g_directory);
  g_directory = const_cast<char*>(full_env.c_str());
#endif

  cvmcache_spawn_watchdog(NULL);

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
  callbacks.capabilities = CVMCACHE_CAP_ALL_V1;


  ctx = cvmcache_init(&callbacks);
  int retval = cvmcache_listen(ctx, locator);
  if (!retval) {
    fprintf(stderr, "failed to listen on %s\n", locator);
    return 1;
  }
  printf("Listening for cvmfs clients on %s\n", locator);
  printf("NOTE: this process needs to run as user cvmfs\n\n");

  // Starts the I/O processing thread
  cvmcache_process_requests(ctx, 0);

  cvmcache_wait_for(ctx);
  printf("  ... good bye\n");

  cvmcache_options_free(locator);
  cvmcache_options_fini(options);
  cvmcache_terminate_watchdog();
  cvmcache_cleanup_global();
  return 0;
}
