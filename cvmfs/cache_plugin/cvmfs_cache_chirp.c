/**
 * This file is part of the CernVM File System.
 *
 * An external cache plugin that uses a Chirp server as its backing storage.
 * Due to the remote I/O overhead, this plugin gives poor performance for
 * a single instance, but allows multiple instances to share a Chirp server.
 */

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

#include <uuid/uuid.h>

#include "libcvmfs_cache.h"

#include "cctools/auth_all.h"
#include "cctools/chirp_reli.h"

#define BASEDIR "/"

#define ENTRY BASEDIR "%s/"
#define TXN_PREFIX "txn/"
#define TXN BASEDIR TXN_PREFIX "%s.%" PRIu64 "/"

#define DATA "data"
#define ID "id"
#define REFCOUNT "refcount/%s"
#define TYPE "type"
#define DESC "description"


#define CHECK(expr) do { \
	int64_t _rc = expr; \
	switch (_rc) { \
		case -ENOENT: return CVMCACHE_STATUS_NOENTRY; \
		case -ENOSPC: return CVMCACHE_STATUS_NOSPACE; \
		case -EPERM: return CVMCACHE_STATUS_FORBIDDEN; \
		case -EINVAL: return CVMCACHE_STATUS_OUTOFBOUNDS; \
	} \
	if (_rc < 0) return CVMCACHE_STATUS_IOERR; \
} while (0)

#define STR(ptr) (ptr ? ptr : "null")
#define PTR(ptr) (ptr ? *ptr : NULL)
#define INT(ptr) (ptr ? *ptr : 0)


static char *progname = "chirp_cvmfs_cache";
static char *chirp_host;
static unsigned chirp_timeout;
static char session_id[64];
static char _slop[] = "";


struct chirpcache_hash {
	char str[2 * sizeof(struct cvmcache_hash) + 16];
};

struct chirpcache_buffer {
	char *data;
	size_t length;
};


static void chirpcache_format_hash(const struct cvmcache_hash *id, struct chirpcache_hash *out) {
	assert(id);
	assert(out);
	size_t i;
	for (i = 0; i < sizeof(id->digest); i++) {
		sprintf(&out->str[2*i], "%02x", id->digest[i]);
	}
	sprintf(&out->str[2*i], ".%X", id->algorithm);
}


static void chirpcache_buffer_init(struct chirpcache_buffer *buf) {
	assert(buf);
	memset(buf, 0, sizeof(*buf));
	buf->data = (char *) _slop;
}


static void chirpcache_buffer_free(struct chirpcache_buffer *buf) {
	assert(buf);
	if (buf->data != _slop) free(buf->data);
	buf->data = (char *) _slop;
	buf->length = 0;
}


static void chirpcache_buffer2cstr(struct chirpcache_buffer *buf) {
	assert(buf);
	if (buf->data == _slop) return;
	if (buf->length == 0) {
		free(buf->data);
		buf->data = (char *) _slop;
		return;
	}

	size_t i;
	for (i = 0; i < buf->length; i++) {
		if (buf->data[i] == 0) return;
	}

	void *grown = realloc(buf->data, buf->length + 1);
	if (grown) {
		buf->data[buf->length] = 0;
	} else {
		chirpcache_buffer_free(buf);
	}
}


static void chirpcache_cstr2buffer(struct chirpcache_buffer *buf, const char *str) {
	assert(buf);
	assert(str);
	buf->data = strdup(str);
	buf->length = strlen(buf->data);
}


static time_t stoptime() {
	return time(NULL) + chirp_timeout;
}


static int64_t chirpcache_getbuffer(const char *path, struct chirpcache_buffer *buf, int ignore_enoent) {
#ifdef VERBOSE
	fprintf(stderr, "getbuffer path=%p (%s), buf=%p, ignore_enoent=%d\n", path, STR(path), buf, ignore_enoent);
#endif

	assert(path);
	assert(buf);

	int64_t rc = chirp_reli_getfile_buffer(chirp_host, path, &buf->data, stoptime());
	if (rc < 0) {
		buf->length = 0;
		buf->data = (char *) _slop;
		if (ignore_enoent && errno == ENOENT) {
			rc = 0;
		} else {
			rc = -errno;
			fprintf(stderr, "%s: failed to get buffer %s on %s: %s\n",
				progname, path, chirp_host, strerror(errno));
		}
	} else {
		buf->length = rc;
	}
	return rc;
}


static int64_t chirpcache_putbuffer(char *path, const struct chirpcache_buffer *buf, int ignore_enoent) {
#ifdef VERBOSE
	fprintf(stderr, "putbuffer path=%p (%s), buf=%p, ignore_enoent=%d\n", path, STR(path), buf, ignore_enoent);
#endif

	assert(path);
	assert(buf);

	int64_t rc = chirp_reli_putfile_buffer(chirp_host, path, buf->data, 0644, buf->length, stoptime());
	if (rc < 0) {
		if (ignore_enoent && errno == ENOENT) {
			rc = 0;
		} else {
			rc = -errno;
			if (errno != ENOENT) {
				fprintf(stderr, "%s: failed to put buffer %s on %s: %s\n",
					progname, path, chirp_host, strerror(errno));
			}
		}
	}
	return rc;
}



static int64_t chirpcache_open(struct chirp_file **f, const char *path, int64_t flags, int64_t mode, int ignore_enoent) {
#ifdef VERBOSE
	fprintf(stderr, "open f=%p (%p), path=%p (%s), flags=%" PRIi64 ", mode=%" PRIi64 ", ignore_enoent=%d\n", f, PTR(f), path, STR(path), flags, mode, ignore_enoent);
#endif

	assert(f);
	assert(path);

	int64_t rc = 0;
	struct chirp_file *out = chirp_reli_open(chirp_host, path, flags, mode, stoptime());
	if (out) {
		*f = out;
	} else {
		*f = NULL;
		if (!(ignore_enoent && errno == ENOENT)) {
			rc = -errno;
			fprintf(stderr, "%s: failed to open %s on %s: %s\n",
				progname, path, chirp_host, strerror(errno));
		}
	}
	return rc;
}


static int64_t chirpcache_close(struct chirp_file **f) {
#ifdef VERBOSE
	fprintf(stderr, "close f=%p (%p)\n", f, PTR(f));
#endif

	assert(f);

	int64_t rc = chirp_reli_close(*f, stoptime());
	if (rc < 0) {
		rc = -errno;
		fprintf(stderr, "%s: failed to close file at %p: %s\n",
			progname, *f, strerror(errno));
	}
	*f = NULL;
	return rc;
}


static int64_t chirpcache_pwrite(struct chirp_file **f, const void *buf, int64_t size, int64_t offset) {
#ifdef VERBOSE
	fprintf(stderr, "pwrite f=%p (%p), buf=%p, size=%" PRIi64 ", offset=%" PRIi64 "\n", f, PTR(f), buf, size, offset);
#endif

	assert(f);
	assert(buf);

	int64_t rc = chirp_reli_pwrite(*f, buf, size, offset, stoptime());
	if (rc < 0) {
		rc = -errno;
		fprintf(stderr, "%s: failed to write (%" PRIi64 ",%" PRIi64 ") from file at %p: %s\n",
			progname, size, offset, *f, strerror(errno));
		chirpcache_close(f);
	} else if (rc != size) {
		rc = -EIO;
		fprintf(stderr, "%s: partial write (%" PRIi64 "%" PRIi64 ") to file at %p\n",
			progname, rc, size, f);
		chirpcache_close(f);
	}
	return rc;
}


static int64_t chirpcache_pread(struct chirp_file **f, void *buf, int64_t *size, int64_t offset) {
#ifdef VERBOSE
	fprintf(stderr, "pread f=%p (%p), buf=%p, size=%p (%" PRIi64 "), offset=%" PRIi64 "\n", f, PTR(f), buf, size, INT(size), offset);
#endif

	assert(f);
	assert(size);

	int64_t rc = chirp_reli_pread(*f, buf, *size, offset, stoptime());
	if (rc < 0) {
		rc = -errno;
		fprintf(stderr, "%s: failed to read (%" PRIi64 ",%" PRIi64 ") from file at %p: %s\n",
			progname, *size, offset, *f, strerror(errno));
		chirpcache_close(f);
	} else {
		*size = rc;
	}
	return rc;
}


static int64_t chirpcache_fstat(struct chirp_file **f, struct chirp_stat *info) {
#ifdef VERBOSE
	fprintf(stderr, "fstat f=%p (%p), info=%p\n", f, PTR(f), info);
#endif

	assert(f);
	assert(info);

	int64_t rc = chirp_reli_fstat(*f, info, stoptime());
	if (rc < 0) {
		rc = -errno;
		fprintf(stderr, "%s: failed to stat file at %p: %s\n",
			progname, *f, strerror(errno));
		chirpcache_close(f);
	}
	return rc;
}


static int64_t chirpcache_access(const char *path, int64_t mode, int ignore_enoent) {
#ifdef VERBOSE
	fprintf(stderr, "access path=%p (%s), mode=%o, ignore_enoent=%d\n", path, STR(path), (unsigned) mode, ignore_enoent);
#endif

	assert(path);

	int64_t rc = chirp_reli_access(chirp_host, path, mode, stoptime());
	if (rc < 0) {
		if (ignore_enoent && errno == ENOENT) {
			rc = 0;
		} else {
			rc = -errno;
			fprintf(stderr, "%s: failed to access %s on %s: %s\n",
				progname, path, chirp_host, strerror(errno));
		}
	}
	return rc;
}


static int64_t chirpcache_mkdir(const char *path, int ignore_eexist) {
#ifdef VERBOSE
	fprintf(stderr, "mkdir path=%p (%s), ignore_eexists=%d\n", path, STR(path), ignore_eexist);
#endif


	assert(path);

	int64_t rc = chirp_reli_mkdir(chirp_host, path, 0755, stoptime());
	if (rc < 0) {
		if (ignore_eexist && errno == EEXIST) {
			rc = 0;
		} else {
			rc = -errno;
			fprintf(stderr, "%s: failed to mkdir %s on %s: %s\n",
				progname, path, chirp_host, strerror(errno));
		}
	}
	return rc;
}


static int64_t chirpcache_getdir(const char *path, chirp_dir_t cb, void *arg, int ignore_enoent) {
#ifdef VERBOSE
	fprintf(stderr, "getdir path=%p (%s), cb=%p, arg=%p, ignore_enoent=%d\n", path, STR(path), cb, arg, ignore_enoent);
#endif

	assert(path);

	int64_t rc = chirp_reli_getdir(chirp_host, path, cb, arg, stoptime());
	if (rc < 0) {
		if (ignore_enoent && errno == ENOENT) {
			rc = 0;
		} else {
			rc = -errno;
			fprintf(stderr, "%s: failed to getdir %s on %s: %s\n",
				progname, path, chirp_host, strerror(errno));
		}
	}
	return rc;
}


static int64_t chirpcache_stat(const char *path, struct chirp_stat *info, int ignore_enoent) {
#ifdef VERBOSE
	fprintf(stderr, "stat path=%p (%s), info=%p, ignore_enoent=%d\n", path, STR(path), info, ignore_enoent);
#endif

	assert(path);

	int64_t rc = chirp_reli_stat(chirp_host, path, info, stoptime());
	if (rc < 0) {
		if (ignore_enoent && errno == ENOENT) {
			rc = 0;
		} else {
			rc = -errno;
			fprintf(stderr, "%s: failed to stat %s on %s: %s\n",
				progname, path, chirp_host, strerror(errno));
		}
	}
	return rc;
}


static int64_t chirpcache_rename(const char *src, const char *dst, int ignore_enoent) {
#ifdef VERBOSE
	fprintf(stderr, "rename src=%p (%s), dst=%p (%s), ignore_enoent=%d\n", src, STR(src), dst, STR(dst), ignore_enoent);
#endif

	assert(src);
	assert(dst);

	int64_t rc = chirp_reli_rename(chirp_host, src, dst, stoptime());
	if (rc < 0) {
		if (ignore_enoent && errno == ENOENT) {
			rc = 0;
		} else {
			rc = -errno;
			if (errno != ENOENT && errno != EEXIST && errno != ENOTEMPTY) {
				fprintf(stderr, "%s: failed to rename %s->%s on %s: %s\n",
					progname, src, dst, chirp_host, strerror(errno));
			}
		}
	}
	return rc;
}


static int64_t chirpcache_unlink(const char *path, int ignore_enoent) {
#ifdef VERBOSE
	fprintf(stderr, "unlink path=%p (%s), ignore_enoent=%d\n", path, STR(path), ignore_enoent);
#endif

	assert(path);

	int64_t rc = chirp_reli_unlink(chirp_host, path, stoptime());
	if (rc < 0) {
		if (ignore_enoent && errno == ENOENT) {
			rc = 0;
		} else {
			rc = -errno;
			fprintf(stderr, "%s: failed to unlink %s on %s: %s\n",
				progname, path, chirp_host, strerror(errno));
		}
	}
	return rc;
}


static int64_t chirpcache_rmall(const char *path, int ignore_enoent) {
#ifdef VERBOSE
	fprintf(stderr, "rmall path=%p (%s), ignore_enoent=%d\n", path, STR(path), ignore_enoent);
#endif

	assert(path);

	int64_t rc = chirp_reli_rmall(chirp_host, path, stoptime());
	if (rc < 0) {
		if (ignore_enoent && errno == ENOENT) {
			rc = 0;
		} else {
			rc = -errno;
			fprintf(stderr, "%s: failed to rmall %s on %s: %s\n",
				progname, path, chirp_host, strerror(errno));
		}
	}
	return rc;
}



static int chirpcache_chrefcnt(struct cvmcache_hash *id, int32_t change_by) {
#ifdef VERBOSE
	fprintf(stderr, "chrefcnt id=%p, change_by=%" PRIi32 "\n", id, change_by);
#endif

	assert(id);

	struct chirpcache_hash hash;
	chirpcache_format_hash(id, &hash);

	int rc;
	char path[PATH_MAX];
	struct chirpcache_buffer buf;
	snprintf(path, sizeof(path), ENTRY REFCOUNT, hash.str, session_id);
	CHECK(chirpcache_getbuffer(path, &buf, 1));
	chirpcache_buffer2cstr(&buf);
	rc = atoi(buf.data);
	chirpcache_buffer_free(&buf);

	rc += change_by;
	if (rc < 0) {
		return CVMCACHE_STATUS_BADCOUNT;
	} else if (rc == 0) {
		CHECK(chirpcache_unlink(path, 1));
	} else {
		char str[64];
		snprintf(str, sizeof(str), "%d", rc);
		chirpcache_cstr2buffer(&buf, str);
		CHECK(chirpcache_putbuffer(path, &buf, 0));
		chirpcache_buffer_free(&buf);
	}

	return CVMCACHE_STATUS_OK;
}


static void chirp_obj_info_cb(const char *entry, void *arg) {
#ifdef VERBOSE
	fprintf(stderr, "obj_info_cb entry=%p (%s), arg=%p\n", entry, STR(entry), arg);
#endif

	assert(entry);
	assert(arg);

	if (entry[0] == '.') return;
	++(*(int *) arg);
}


static int chirpcache_obj_info(struct cvmcache_hash *id, struct cvmcache_object_info *info) {
#ifdef VERBOSE
	fprintf(stderr, "obj_info id=%p, info=%p\n", id, info);
#endif

	assert(id);
	assert(info);

	struct chirpcache_hash hash;
	chirpcache_format_hash(id, &hash);

	struct cvmcache_object_info out;
	memcpy(&out.id, id, sizeof(out.id));

	char path[PATH_MAX];
	struct chirp_stat cs;
	snprintf(path, sizeof(path), ENTRY DATA, hash.str);
	CHECK(chirpcache_stat(path, &cs, 0));
	out.size = cs.cst_size;

	struct chirpcache_buffer buf;
	snprintf(path, sizeof(path), ENTRY TYPE, hash.str);
	CHECK(chirpcache_getbuffer(path, &buf, 0));
	chirpcache_buffer2cstr(&buf);
	switch (buf.data[0]) {
		case 'r':
		out.type = CVMCACHE_OBJECT_REGULAR;
		break;
		case 'c':
		out.type = CVMCACHE_OBJECT_CATALOG;
		break;
		case 'v':
		out.type = CVMCACHE_OBJECT_VOLATILE;
		break;
		default:
		fprintf(stderr, "%s: invalid type %s for %s\n",
			progname, buf.data, hash.str);
		chirpcache_buffer_free(&buf);
		return CVMCACHE_STATUS_IOERR;
	}
	chirpcache_buffer_free(&buf);

	snprintf(path, sizeof(path), ENTRY REFCOUNT, hash.str, "");
	CHECK(chirpcache_getdir(path, chirp_obj_info_cb, &out.pinned, 0));
	out.pinned = !!out.pinned;

	snprintf(path, sizeof(path), ENTRY DESC, hash.str);
	CHECK(chirpcache_getbuffer(path, &buf, 1));
	chirpcache_buffer2cstr(&buf);
	out.description = strdup(buf.data);
	chirpcache_buffer_free(&buf);

	memcpy(info, &out, sizeof(*info));
	return CVMCACHE_STATUS_OK;
}


static int chirpcache_read(struct cvmcache_hash *id, uint64_t offset, uint32_t *size, unsigned char *buffer) {
#ifdef VERBOSE
	fprintf(stderr, "pread id=%p, offset=%" PRIu64 ", size=%p (%" PRIu32 "), buffer=%p\n", id, offset, size, INT(size), buffer);
#endif

	assert(id);
	assert(size);
	assert(buffer);

	struct chirpcache_hash hash;
	chirpcache_format_hash(id, &hash);

	char path[PATH_MAX];
	snprintf(path, sizeof(path), ENTRY, hash.str);
	CHECK(chirpcache_access(path, 0, 0));

	snprintf(path, sizeof(path), ENTRY DATA, hash.str);
	struct chirp_file *f;
	CHECK(chirpcache_open(&f, path, O_RDONLY, 0, 0));

	struct chirp_stat cs;
	CHECK(chirpcache_fstat(&f, &cs));
	if ((int) offset > cs.cst_size) {
		return CVMCACHE_STATUS_OUTOFBOUNDS;
	}

	int64_t nbytes = *size < cs.cst_size - offset ? *size : cs.cst_size - offset;
	CHECK(chirpcache_pread(&f, buffer, &nbytes, offset));
	*size = nbytes;

	CHECK(chirpcache_close(&f));

	return CVMCACHE_STATUS_OK;
}


static int chirpcache_start_txn(struct cvmcache_hash *id, uint64_t txn_id, struct cvmcache_object_info *info) {
#ifdef VERBOSE
	fprintf(stderr, "start_txn id=%p, txn_id=%" PRIu64 ", info=%p\n", id, txn_id, info);
#endif

	assert(id);
	assert(info);

	struct chirpcache_hash hash;
	chirpcache_format_hash(id, &hash);

	char path[PATH_MAX];
	snprintf(path, sizeof(path), TXN, session_id, txn_id);
	CHECK(chirpcache_mkdir(path, 0));

	struct chirpcache_buffer buf;
	chirpcache_cstr2buffer(&buf, (const char *) &hash.str);
	snprintf(path, sizeof(path), TXN ID, session_id, txn_id);
	CHECK(chirpcache_putbuffer(path, &buf, 0));
	chirpcache_buffer_free(&buf);

	snprintf(path, sizeof(path), TXN DATA, session_id, txn_id);
	CHECK(chirpcache_putbuffer(path, &buf, 0));

	snprintf(path, sizeof(path), TXN TYPE, session_id, txn_id);
	switch (info->type) {
		case CVMCACHE_OBJECT_REGULAR:
		chirpcache_cstr2buffer(&buf, "r");
		break;
		case CVMCACHE_OBJECT_CATALOG:
		chirpcache_cstr2buffer(&buf, "c");
		break;
		case CVMCACHE_OBJECT_VOLATILE:
		chirpcache_cstr2buffer(&buf, "v");
		break;
		default:
		return CVMCACHE_STATUS_MALFORMED;
	}
	CHECK(chirpcache_putbuffer(path, &buf, 0));
	chirpcache_buffer_free(&buf);

	snprintf(path, sizeof(path), TXN REFCOUNT, session_id, txn_id, "");
	CHECK(chirpcache_mkdir(path, 0));

	chirpcache_cstr2buffer(&buf, "1");
	snprintf(path, sizeof(path), TXN REFCOUNT, session_id, txn_id, session_id);
	CHECK(chirpcache_putbuffer(path, &buf, 0));
	chirpcache_buffer_free(&buf);

	if (info->description) {
		snprintf(path, sizeof(path), TXN DESC, session_id, txn_id);
		chirpcache_cstr2buffer(&buf, info->description);
		CHECK(chirpcache_putbuffer(path, &buf, 0));
		chirpcache_buffer_free(&buf);
	}

	return CVMCACHE_STATUS_OK;
}


static int chirpcache_write_txn(uint64_t txn_id, unsigned char *buffer, uint32_t size) {
#ifdef VERBOSE
	fprintf(stderr, "write_txn txn_id=%" PRIu64 ", buffer=%p, size=%" PRIu32 "\n", txn_id, buffer, size);
#endif

	assert(buffer);

	char path[PATH_MAX];
	snprintf(path, sizeof(path), TXN DATA, session_id, txn_id);

	struct chirp_file *f;
	CHECK(chirpcache_open(&f, path, O_RDWR, 0, 0));

	struct chirp_stat cs;
	CHECK(chirpcache_fstat(&f, &cs));
	CHECK(chirpcache_pwrite(&f, buffer, size, cs.cst_size));
	CHECK(chirpcache_close(&f));

	return CVMCACHE_STATUS_OK;
}


static int chirpcache_commit_txn(uint64_t txn_id) {
	struct chirpcache_buffer buf;
	char src[PATH_MAX];
	chirpcache_buffer_init(&buf);
	snprintf(src, sizeof(src), TXN ID, session_id, txn_id);
	CHECK(chirpcache_getbuffer(src, &buf, 0));
	chirpcache_buffer2cstr(&buf);
	CHECK(chirpcache_unlink(src, 0));

	char dst[PATH_MAX];
	snprintf(src, sizeof(src), TXN, session_id, txn_id);
	snprintf(dst, sizeof(dst), ENTRY, buf.data);
	chirpcache_buffer_free(&buf);

	if (chirpcache_rename(src, dst, 0) < 0) {
		if (errno == EEXIST || errno == ENOTEMPTY) {
			CHECK(chirpcache_rmall(src, 0));
		} else {
			fprintf(stderr, "%s: failed to commit %s->%s on %s: %s\n",
				progname, src, dst, chirp_host, strerror(errno));
			return CVMCACHE_STATUS_IOERR;
		}
	}

	return CVMCACHE_STATUS_OK;
}


static int chirpcache_abort_txn(uint64_t txn_id) {
#ifdef VERBOSE
	fprintf(stderr, "abort_txn txn_id=%" PRIu64 "\n", txn_id);
#endif

	char path[PATH_MAX];
	snprintf(path, sizeof(path), TXN, session_id, txn_id);
	CHECK(chirpcache_rmall(path, 0));
	return CVMCACHE_STATUS_OK;
}


static void chirp_info_cb(const char *entry, void *arg) {
#ifdef VERBOSE
	fprintf(stderr, "info_cb entry=%p (%s), arg=%p\n", entry, STR(entry), arg);
#endif

	assert(entry);
	assert(arg);

	char path[PATH_MAX];
	struct chirp_stat info;
	snprintf(path, sizeof(path), "%s/%s", entry, DATA);
	if (chirpcache_stat(path, &info, 0) < 0) return;

	int rc = 0;
	snprintf(path, sizeof(path), "%s/" REFCOUNT, entry, "");
	if (chirpcache_getdir(path, chirp_obj_info_cb, &rc, 0) < 0) return;

	struct cvmcache_info *ret = arg;
	ret->used_bytes += info.cst_size;
	if (rc > 0) ret->pinned_bytes += info.cst_size;
}


static int chirpcache_info(struct cvmcache_info *info) {
#ifdef VERBOSE
	fprintf(stderr, "info info=%p\n", info);
#endif

	struct cvmcache_info ret;
	memset(&ret, 0, sizeof(ret));
	info->size_bytes = (uint64_t) -1;
	ret.no_shrink = 1;

	CHECK(chirpcache_getdir(BASEDIR, chirp_info_cb, &ret, 0));
	memcpy(info, &ret, sizeof(*info));

	return CVMCACHE_STATUS_OK;
}


static int chirpcache_shrink(uint64_t shrink_to, uint64_t *used) {
#ifdef VERBOSE
	fprintf(stderr, "shrink shrink_to=%" PRIu64 ", used=%p\n", shrink_to, used);
#endif
	return CVMCACHE_STATUS_NOSUPPORT;
}


static int chirpcache_listing_begin(uint64_t lst_id, enum cvmcache_object_type type) {
#ifdef VERBOSE
	fprintf(stderr, "listing_begin lst_id=%" PRIu64 ", type=%d\n", lst_id, type);
#endif
	return CVMCACHE_STATUS_NOSUPPORT;
}


static int chirpcache_listing_next(int64_t lst_id, struct cvmcache_object_info *item) {
#ifdef VERBOSE
	fprintf(stderr, "listing_next lst_id=%" PRIu64 ", item=%p\n", lst_id, item);
#endif
	return CVMCACHE_STATUS_NOSUPPORT;
}


static int chirpcache_listing_end(int64_t lst_id) {
#ifdef VERBOSE
	fprintf(stderr, "listing_end lst_id=%" PRIu64 "\n", lst_id);
#endif
	return CVMCACHE_STATUS_NOSUPPORT;
}


int main(int argc, char *argv[]) {
	struct cvmcache_context *ctx;
	uuid_t uu;
	uuid_generate(uu);
	uuid_unparse(uu, session_id);
#ifdef VERBOSE
	fprintf(stderr, "using session id %s\n", session_id);
#endif

	if (argc < 2) {
		printf("usage: %s <config-file>\n", argv[0]);
		return 1;
	}
	progname = argv[0];

	cvmcache_init_global();

	cvmcache_option_map *options = cvmcache_options_init();
	if (cvmcache_options_parse(options, argv[1]) != 0) {
		fprintf(stderr, "cannot parse options file %s\n", argv[1]);
		return 1;
	}
	char *locator = cvmcache_options_get(options, "CVMFS_CACHE_EXTERNAL_LOCATOR");
	if (locator == NULL) {
		fprintf(stderr, "CVMFS_CACHE_EXTERNAL_LOCATOR missing\n");
		cvmcache_options_fini(options);
		return 1;
	}
	char *timeout = cvmcache_options_get(options, "CVMFS_CACHE_EXTERNAL_CHIRP_TIMEOUT");
	if (timeout) {
		chirp_timeout = atoi(timeout);
	} else {
		chirp_timeout = 5;
	}
#ifdef VERBOSE
	printf("chirp server timeout set to %u\n", chirp_timeout);
#endif

	chirp_host = cvmcache_options_get(options, "CVMFS_CACHE_EXTERNAL_CHIRP_HOST");
	if (chirp_host == NULL) {
		chirp_host = "localhost";
	}
	printf("connecting to %s\n", chirp_host);

	auth_register_all();
	CHECK(chirpcache_mkdir(BASEDIR TXN_PREFIX, 1));

	cvmcache_spawn_watchdog(NULL);

	struct cvmcache_callbacks callbacks;
	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.cvmcache_chrefcnt = chirpcache_chrefcnt;
	callbacks.cvmcache_obj_info = chirpcache_obj_info;
	callbacks.cvmcache_pread = chirpcache_read;
	callbacks.cvmcache_start_txn = chirpcache_start_txn;
	callbacks.cvmcache_write_txn = chirpcache_write_txn;
	callbacks.cvmcache_commit_txn = chirpcache_commit_txn;
	callbacks.cvmcache_abort_txn = chirpcache_abort_txn;
	callbacks.cvmcache_info = chirpcache_info;
	callbacks.cvmcache_shrink = chirpcache_shrink;
	callbacks.cvmcache_listing_begin = chirpcache_listing_begin;
	callbacks.cvmcache_listing_next = chirpcache_listing_next;
	callbacks.cvmcache_listing_end = chirpcache_listing_end;
	callbacks.capabilities = CVMCACHE_CAP_REFCOUNT | CVMCACHE_CAP_INFO;

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

	if (!cvmcache_is_supervised()) {
		printf("Press <R ENTER> to ask clients to release nested catalogs\n");
		printf("Press <Ctrl+D> to quit\n");
		while (true) {
			char buf;
			int retval = read(fileno(stdin), &buf, 1);
			if (retval != 1)
				break;
			if (buf == 'R') {
				printf(" ... asking clients to release nested catalogs\n");
				cvmcache_ask_detach(ctx);
			}
		}
		cvmcache_terminate(ctx);
	}

	cvmcache_wait_for(ctx);
	printf(" ... good bye\n");

	cvmcache_options_free(locator);
	cvmcache_options_fini(options);
	cvmcache_terminate_watchdog();
	cvmcache_cleanup_global();
	return 0;
}
