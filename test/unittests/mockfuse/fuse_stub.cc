/**
 * This file is part of the CernVM File System.
 *
 * libfuse entry points used by mockfuse tests.
 * Uses real libfuse declarations, not cvmfs headers.
 */

#include "fuse_stub.h"

#include <cassert>
#include <cstring>

namespace {

/** Private libfuse fuse_dirent header size. */
const size_t kDirentHeaderSize = 24;

/** Tracks double replies. */
void RecordReply(fuse_req_t req, fuse_req::ReplyKind kind) {
  assert(req != NULL);
  req->n_replies++;
  req->kind = kind;
}

}  // anonymous namespace


int fuse_reply_err(fuse_req_t req, int err) {
  RecordReply(req, fuse_req::kReplyErr);
  req->err = err;
  return 0;
}


void fuse_reply_none(fuse_req_t req) {
  RecordReply(req, fuse_req::kReplyDropped);
}


int fuse_reply_entry(fuse_req_t req, const struct fuse_entry_param *e) {
  RecordReply(req, fuse_req::kReplyEntry);
  req->entry = *e;
  return 0;
}


int fuse_reply_attr(fuse_req_t req, const struct stat *attr,
                    double attr_timeout) {
  RecordReply(req, fuse_req::kReplyAttr);
  req->attr = *attr;
  req->attr_timeout = attr_timeout;
  return 0;
}


int fuse_reply_readlink(fuse_req_t req, const char *link) {
  RecordReply(req, fuse_req::kReplyReadlink);
  req->buf = link;
  return 0;
}


int fuse_reply_open(fuse_req_t req, const struct fuse_file_info *fi) {
  RecordReply(req, fuse_req::kReplyOpen);
  req->fi = *fi;
  return 0;
}


int fuse_reply_buf(fuse_req_t req, const char *buf, size_t size) {
  RecordReply(req, fuse_req::kReplyBuf);
  req->buf.assign(buf, size);
  return 0;
}


int fuse_reply_statfs(fuse_req_t req, const struct statvfs *stbuf) {
  RecordReply(req, fuse_req::kReplyStatfs);
  req->statfs = *stbuf;
  return 0;
}


int fuse_reply_xattr(fuse_req_t req, size_t count) {
  RecordReply(req, fuse_req::kReplyXattr);
  req->xattr_count = count;
  return 0;
}


const struct fuse_ctx *fuse_req_ctx(fuse_req_t req) {
  assert(req != NULL);
  return &req->ctx;
}


int fuse_req_interrupted(fuse_req_t req) {
  assert(req != NULL);
  return req->interrupted;
}


/** libfuse semantics: size probe on NULL, fill only if it fits. */
size_t fuse_add_direntry(fuse_req_t req __attribute__((unused)),
                         char *buf,
                         size_t bufsize,
                         const char *name,
                         const struct stat *stbuf __attribute__((unused)),
                         off_t off __attribute__((unused))) {
  // Header + name + NUL, 8-byte aligned.
  const size_t entlen = kDirentHeaderSize + strlen(name) + 1;
  const size_t entsize = (entlen + 7) & ~static_cast<size_t>(7);
  if ((buf != NULL) && (entsize <= bufsize)) {
    memset(buf, 0, entsize);
    memcpy(buf + kDirentHeaderSize, name, strlen(name));
  }
  return entsize;
}


/** No session fd. */
int fuse_session_fd(struct fuse_session *se __attribute__((unused))) {
  return -1;
}


int fuse_lowlevel_notify_inval_inode(struct fuse_session *se
                                     __attribute__((unused)),
                                     fuse_ino_t ino __attribute__((unused)),
                                     off_t off __attribute__((unused)),
                                     off_t len __attribute__((unused))) {
  return -1;
}


int fuse_lowlevel_notify_inval_entry(struct fuse_session *se
                                     __attribute__((unused)),
                                     fuse_ino_t parent __attribute__((unused)),
                                     const char *name
                                     __attribute__((unused)),
                                     size_t namelen __attribute__((unused))) {
  return -1;
}


#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 16)
int fuse_lowlevel_notify_expire_entry(
    struct fuse_session *se __attribute__((unused)),
    fuse_ino_t parent __attribute__((unused)),
    const char *name __attribute__((unused)),
    size_t namelen __attribute__((unused)),
    enum fuse_expire_flags flags __attribute__((unused))) {
  return -1;
}
#endif
