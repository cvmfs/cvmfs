/**
 * This file is part of the CernVM File System.
 *
 * A demo external cache plugin.  All data is stored in std::string.  Feature-
 * complete but quite inefficient.
 */

#define __STDC_FORMAT_MACROS

#include <alloca.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <map>
#include <string>
#include <vector>

#include "libcvmfs_cache.h"

using namespace std;  // NOLINT

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
  Listing() : type(CVMCACHE_OBJECT_REGULAR), pos(0), elems(NULL) { }
  cvmcache_object_type type;
  uint64_t pos;
  vector<Object> *elems;
};

struct ComparableHash {
  ComparableHash() { memset(&hash, 0, sizeof(hash)); }
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
  if (offset > data.length())
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
  transactions.erase(txn_id);
  return CVMCACHE_STATUS_OK;
}

static int null_abort_txn(uint64_t txn_id) {
  transactions.erase(txn_id);
  return CVMCACHE_STATUS_OK;
}

static int null_info(struct cvmcache_info *info) {
  info->size_bytes = uint64_t(-1);
  info->used_bytes = info->pinned_bytes = 0;
  for (map<ComparableHash, Object>::const_iterator i = storage.begin(),
       i_end = storage.end(); i != i_end; ++i)
  {
    info->used_bytes += i->second.data.length();
    if (i->second.refcnt > 0)
      info->pinned_bytes += i->second.data.length();
  }
  info->no_shrink = 0;
  return CVMCACHE_STATUS_OK;
}

static int null_shrink(uint64_t shrink_to, uint64_t *used) {
  struct cvmcache_info info;
  null_info(&info);
  *used = info.used_bytes;
  if (info.used_bytes <= shrink_to)
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
    unsigned length = i->second.data.length();
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
  char *test_mode = cvmcache_options_get(options, "CVMFS_CACHE_PLUGIN_TEST");

  if (!test_mode)
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

  if (test_mode) {
    // Daemonize, print out PID
    pid_t pid;
    int statloc;
    if ((pid = fork()) == 0) {
      if ((pid = fork()) == 0) {
        int null_read = open("/dev/null", O_RDONLY);
        int null_write = open("/dev/null", O_WRONLY);
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

  printf("Listening for cvmfs clients on %s\n", locator);
  printf("NOTE: this process needs to run as user cvmfs\n\n");

  // Starts the I/O processing thread
  cvmcache_process_requests(ctx, 0);

  if (test_mode)
    while (true) sleep(1);

  if (!cvmcache_is_supervised()) {
    printf("Press <R ENTER> to ask clients to release nested catalogs\n");
    printf("Press <Ctrl+D> to quit\n");
    while (true) {
      char buf;
      int retval = read(fileno(stdin), &buf, 1);
      if (retval != 1)
        break;
      if (buf == 'R') {
        printf("  ... asking clients to release nested catalogs\n");
        cvmcache_ask_detach(ctx);
      }
    }
    cvmcache_terminate(ctx);
  }

  cvmcache_wait_for(ctx);
  printf("  ... good bye\n");

  cvmcache_options_free(locator);
  cvmcache_options_fini(options);
  cvmcache_terminate_watchdog();
  cvmcache_cleanup_global();
  return 0;
}
