/**
 * This file is part of the CernVM File System.
 *
 * libfuse low-level API stub for mockfuse tests.
 * Uses the production duplex_fuse.h declarations.
 * Tests define fuse_req to record replies.
 */

#ifndef TEST_UNITTESTS_MOCKFUSE_FUSE_STUB_H_
#define TEST_UNITTESTS_MOCKFUSE_FUSE_STUB_H_

#include "duplex_fuse.h"
#include <sys/stat.h>
#include <sys/statvfs.h>

#include <cstring>
#include <string>

struct fuse_req {
  enum ReplyKind {
    kReplyNone = 0,  ///< unset
    kReplyErr,
    kReplyOpen,
    kReplyAttr,
    kReplyEntry,
    kReplyBuf,
    kReplyDropped,  ///< fuse_reply_none()
    kReplyReadlink,
    kReplyStatfs,
    kReplyXattr
  };

  fuse_req()
      : n_replies(0)
      , kind(kReplyNone)
      , err(-1)
      , attr_timeout(0.0)
      , xattr_count(0)
      , interrupted(0) {
    memset(&fi, 0, sizeof(fi));
    memset(&attr, 0, sizeof(attr));
    memset(&entry, 0, sizeof(entry));
    memset(&statfs, 0, sizeof(statfs));
    // Default unprivileged caller.
    ctx.uid = 1000;
    ctx.gid = 1000;
    ctx.pid = 12345;
    ctx.umask = 022;
  }

  int n_replies;                 ///< reply count
  ReplyKind kind;

  int err;                        ///< fuse_reply_err()
  struct fuse_file_info fi;       ///< fuse_reply_open()
  struct stat attr;               ///< fuse_reply_attr()
  double attr_timeout;            ///< fuse_reply_attr()
  struct fuse_entry_param entry;  ///< fuse_reply_entry()
  std::string buf;                ///< fuse_reply_buf() / fuse_reply_readlink()
  struct statvfs statfs;          ///< fuse_reply_statfs()
  size_t xattr_count;             ///< fuse_reply_xattr()

  struct fuse_ctx ctx;  ///< fuse_req_ctx()
  int interrupted;      ///< fuse_req_interrupted()
};

#endif  // TEST_UNITTESTS_MOCKFUSE_FUSE_STUB_H_
