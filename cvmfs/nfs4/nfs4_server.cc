/**
 * This file is part of the CernVM File System.
 */

#include "nfs4/nfs4_server.h"

#include <errno.h>
#include <inttypes.h>
#include <sys/stat.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>

#include "nfs4/logging.h"

#ifndef ENODATA
#define ENODATA ENOATTR
#endif

namespace nfs4 {

namespace {

const uint32_t kMaxName = 255;
const uint64_t kAttrDirFileIdBit = 1ull << 63;
const uint64_t kNamedAttrFileIdBit = 1ull << 62;

// Attributes this server can return, as bitmap words 0 and 1
const unsigned kSupportedAttrs[] = {FATTR4_SUPPORTED_ATTRS,
                                    FATTR4_TYPE,
                                    FATTR4_FH_EXPIRE_TYPE,
                                    FATTR4_CHANGE,
                                    FATTR4_SIZE,
                                    FATTR4_LINK_SUPPORT,
                                    FATTR4_SYMLINK_SUPPORT,
                                    FATTR4_NAMED_ATTR,
                                    FATTR4_FSID,
                                    FATTR4_UNIQUE_HANDLES,
                                    FATTR4_LEASE_TIME,
                                    FATTR4_RDATTR_ERROR,
                                    FATTR4_ACLSUPPORT,
                                    FATTR4_CANSETTIME,
                                    FATTR4_CASE_INSENSITIVE,
                                    FATTR4_CASE_PRESERVING,
                                    FATTR4_CHOWN_RESTRICTED,
                                    FATTR4_FILEHANDLE,
                                    FATTR4_FILEID,
                                    FATTR4_FILES_AVAIL,
                                    FATTR4_FILES_FREE,
                                    FATTR4_FILES_TOTAL,
                                    FATTR4_HOMOGENEOUS,
                                    FATTR4_MAXFILESIZE,
                                    FATTR4_MAXLINK,
                                    FATTR4_MAXNAME,
                                    FATTR4_MAXREAD,
                                    FATTR4_MAXWRITE,
                                    FATTR4_MODE,
                                    FATTR4_NO_TRUNC,
                                    FATTR4_NUMLINKS,
                                    FATTR4_OWNER,
                                    FATTR4_OWNER_GROUP,
                                    FATTR4_RAWDEV,
                                    FATTR4_SPACE_AVAIL,
                                    FATTR4_SPACE_FREE,
                                    FATTR4_SPACE_TOTAL,
                                    FATTR4_SPACE_USED,
                                    FATTR4_TIME_ACCESS,
                                    FATTR4_TIME_DELTA,
                                    FATTR4_TIME_METADATA,
                                    FATTR4_TIME_MODIFY,
                                    FATTR4_MOUNTED_ON_FILEID};

const unsigned kNumBitmapWords = 2;

void SupportedBitmap(uint32_t *words) {
  words[0] = words[1] = 0;
  for (unsigned i = 0; i < sizeof(kSupportedAttrs) / sizeof(unsigned); ++i)
    words[kSupportedAttrs[i] / 32] |= 1u << (kSupportedAttrs[i] % 32);
}

bool IsSet(const uint32_t *bitmap, unsigned len, unsigned attr) {
  return (attr / 32 < len) && (bitmap[attr / 32] & (1u << (attr % 32)));
}

void PutTime(const Timespec &t, XdrWriter *w) {
  w->PutI64(t.sec);
  w->PutU32(t.nsec);
}

void PutBitmap(const uint32_t *words, unsigned len, XdrWriter *w) {
  // Trailing zero words are dropped, as recommended by RFC 7530
  while (len > 0 && words[len - 1] == 0)
    len--;
  w->PutU32(len);
  for (unsigned i = 0; i < len; ++i)
    w->PutU32(words[i]);
}

uint32_t FileType(const FileHandle &fh, mode_t mode) {
  if (fh.kind == FileHandle::kAttrDir)
    return NF4ATTRDIR;
  if (fh.kind == FileHandle::kNamedAttr)
    return NF4NAMEDATTR;
  if (S_ISDIR(mode))
    return NF4DIR;
  if (S_ISLNK(mode))
    return NF4LNK;
  if (S_ISCHR(mode))
    return NF4CHR;
  if (S_ISBLK(mode))
    return NF4BLK;
  if (S_ISFIFO(mode))
    return NF4FIFO;
  if (S_ISSOCK(mode))
    return NF4SOCK;
  return NF4REG;
}

bool IsValidUtf8(const std::string &s) {
  unsigned i = 0;
  while (i < s.size()) {
    const unsigned char c = s[i];
    unsigned n;
    if (c < 0x80)
      n = 0;
    else if ((c & 0xE0) == 0xC0)
      n = 1;
    else if ((c & 0xF0) == 0xE0)
      n = 2;
    else if ((c & 0xF8) == 0xF0)
      n = 3;
    else
      return false;
    if (i + n >= s.size() && n > 0)
      return false;
    for (unsigned j = 1; j <= n; ++j) {
      if ((static_cast<unsigned char>(s[i + j]) & 0xC0) != 0x80)
        return false;
    }
    i += n + 1;
  }
  return true;
}

uint32_t CheckName(const std::string &name) {
  if (name.empty())
    return NFS4ERR_INVAL;
  if (name.size() > kMaxName)
    return NFS4ERR_NAMETOOLONG;
  if (name == "." || name == ".." || name.find('/') != std::string::npos)
    return NFS4ERR_BADNAME;
  if (!IsValidUtf8(name))
    return NFS4ERR_INVAL;
  return NFS4_OK;
}

std::string NumericId(uint32_t id) {
  char buf[16];
  snprintf(buf, sizeof(buf), "%u", id);
  return buf;
}

/**
 * Permission check against the mode bits.  Write access is never granted.
 */
uint32_t AccessFromMode(const Attr &attr, const Credentials &creds) {
  unsigned perms;
  if (creds.uid == 0) {
    perms = 04 | ((attr.mode & 0111) ? 01 : 0);
  } else if (creds.uid == attr.uid) {
    perms = (attr.mode >> 6) & 07;
  } else {
    bool in_group = (creds.gid == attr.gid);
    for (unsigned i = 0; !in_group && i < creds.gids.size(); ++i)
      in_group = (creds.gids[i] == attr.gid);
    perms = in_group ? (attr.mode >> 3) & 07 : attr.mode & 07;
  }

  uint32_t granted = 0;
  if (perms & 04)
    granted |= ACCESS4_READ;
  if (perms & 01)
    granted |= S_ISDIR(attr.mode) ? ACCESS4_LOOKUP : ACCESS4_EXECUTE;
  return granted;
}

// Errors after which an open-owner's seqid is not advanced (RFC 7530 9.1.7)
bool SeqidAdvances(uint32_t status) {
  switch (status) {
    case NFS4ERR_STALE_CLIENTID:
    case NFS4ERR_STALE_STATEID:
    case NFS4ERR_BAD_STATEID:
    case NFS4ERR_BAD_SEQID:
    case NFS4ERR_BADXDR:
    case NFS4ERR_RESOURCE:
    case NFS4ERR_NOFILEHANDLE:
    case NFS4ERR_MOVED:
      return false;
    default:
      return true;
  }
}

}  // anonymous namespace


uint32_t ErrnoToStatus(int err) {
  switch (err) {
    case 0:
      return NFS4_OK;
    case EPERM:
      return NFS4ERR_PERM;
    case ENOENT:
    case ENODATA:
      return NFS4ERR_NOENT;
    case EACCES:
      return NFS4ERR_ACCESS;
    case ENOTDIR:
      return NFS4ERR_NOTDIR;
    case EISDIR:
      return NFS4ERR_ISDIR;
    case EINVAL:
      return NFS4ERR_INVAL;
    case ENAMETOOLONG:
      return NFS4ERR_NAMETOOLONG;
    case EROFS:
      return NFS4ERR_ROFS;
    case ESTALE:
      return NFS4ERR_STALE;
    case EAGAIN:
    case EBUSY:
      return NFS4ERR_DELAY;
    case ENOSPC:
      return NFS4ERR_NOSPC;
    case EFBIG:
      return NFS4ERR_FBIG;
    case ERANGE:
      return NFS4ERR_TOOSMALL;
    case ELOOP:
      return NFS4ERR_SYMLINK;
    case EMFILE:
    case ENFILE:
      return NFS4ERR_DELAY;
    case EIO:
      return NFS4ERR_IO;
    default:
      Log(kLogLevelWarn, "mapping unexpected errno %d to NFS4ERR_IO", err);
      return NFS4ERR_IO;
  }
}


Nfs4Server::Nfs4Server(Backend *backend, const Options &options)
    : backend_(backend)
    , options_(options)
    , fsid_(backend->FsId())
    , boot_(static_cast<uint32_t>(time(NULL)))
    , next_clientid_(1)
    , next_stateid_(1)
    , last_generation_(0)
    , monitor_stopping_(false)
    , executors_stopping_(false) { }


Nfs4Server::~Nfs4Server() {
  {
    std::lock_guard<std::mutex> lock(state_lock_);
    monitor_stopping_ = true;
  }
  monitor_cond_.notify_all();
  if (monitor_.joinable())
    monitor_.join();
  {
    std::lock_guard<std::mutex> lock(executor_lock_);
    executors_stopping_ = true;
  }
  executor_cond_.notify_all();
  for (unsigned i = 0; i < executors_.size(); ++i)
    executors_[i].join();
}


void Nfs4Server::ExecutorLoop() {
  while (true) {
    TaskPtr task;
    {
      std::unique_lock<std::mutex> lock(executor_lock_);
      executor_cond_.wait(
          lock, [this] { return executors_stopping_ || !tasks_.empty(); });
      if (tasks_.empty())
        return;
      task = tasks_.front();
      tasks_.pop_front();
    }
    XdrReader args(reinterpret_cast<const unsigned char *>(task->args.data()),
                   task->args.size());
    ProcessCompound(task->call, &args, &task->res);
    {
      std::lock_guard<std::mutex> lock(executor_lock_);
      task->done = true;
    }
    task->done_cond.notify_one();
  }
}


void Nfs4Server::RunWithDeadline(const RpcCall &call, XdrReader *args,
                                 XdrWriter *res) {
  TaskPtr task(new Task());
  task->call = call;
  task->args.assign(reinterpret_cast<const char *>(args->cursor()),
                    args->remaining());
  task->call.args = NULL;
  task->call.args_size = 0;

  std::unique_lock<std::mutex> lock(executor_lock_);
  if (executors_.empty()) {
    for (unsigned i = 0; i < options_.executor_threads; ++i)
      executors_.push_back(std::thread(&Nfs4Server::ExecutorLoop, this));
  }
  tasks_.push_back(task);
  executor_cond_.notify_one();
  const bool done = task->done_cond.wait_for(
      lock, std::chrono::milliseconds(options_.delay_after_ms),
      [&task] { return task->done; });
  if (done) {
    res->PutRaw(task->res.buffer().data(), task->res.size());
    return;
  }

  // Still queued or running: the task completes on its own, and the client's
  // retry benefits from the warmed cache (or the open-owner replay cache)
  for (std::deque<TaskPtr>::iterator it = tasks_.begin(); it != tasks_.end();
       ++it) {
    if (*it == task) {
      tasks_.erase(it);
      break;
    }
  }
  lock.unlock();

  XdrReader r(reinterpret_cast<const unsigned char *>(task->args.data()),
              task->args.size());
  std::string tag;
  uint32_t minor_version = 0, num_ops = 0, first_op = 0;
  r.GetOpaque(&tag, kNfs4OpaqueLimit);
  r.GetU32(&minor_version);
  r.GetU32(&num_ops);
  r.GetU32(&first_op);
  Log(kLogLevelDebug, "xid %u delayed", call.xid);
  res->PutU32(NFS4ERR_DELAY);
  res->PutOpaque(tag);
  if (num_ops == 0 || !r.ok()) {
    res->PutU32(0);
    return;
  }
  res->PutU32(1);
  res->PutU32(first_op);
  res->PutU32(NFS4ERR_DELAY);
}


AcceptStat Nfs4Server::Dispatch(const RpcCall &call, XdrReader *args,
                                XdrWriter *res) {
  switch (call.procedure) {
    case kNfsProcNull:
      return kAcceptSuccess;
    case kNfsProcCompound:
      if (options_.delay_after_ms > 0)
        RunWithDeadline(call, args, res);
      else
        ProcessCompound(call, args, res);
      return kAcceptSuccess;
    default:
      return kAcceptProcUnavail;
  }
}


uint32_t Nfs4Server::ProcessCompound(const RpcCall &call, XdrReader *args,
                                     XdrWriter *res) {
  std::string tag;
  uint32_t minor_version = 0, num_ops = 0;
  args->GetOpaque(&tag, kNfs4OpaqueLimit);
  args->GetU32(&minor_version);
  args->GetU32(&num_ops);

  const size_t status_offset = res->ReserveU32();
  res->PutOpaque(tag);
  const size_t count_offset = res->ReserveU32();

  uint32_t status = NFS4_OK;
  uint32_t num_results = 0;
  if (!args->ok()) {
    status = NFS4ERR_BADXDR;
  } else if (minor_version != 0) {
    status = NFS4ERR_MINOR_VERS_MISMATCH;
  } else if (num_ops > options_.max_compound_ops) {
    status = NFS4ERR_RESOURCE;
  } else {
    Compound c;
    c.call = &call;
    for (uint32_t i = 0; i < num_ops; ++i) {
      uint32_t op;
      if (!args->GetU32(&op)) {
        status = NFS4ERR_BADXDR;
        break;
      }
      // The op's result is written in place; on failure it is cut off again
      const size_t op_offset = res->ReserveU32();
      const size_t status_offset_op = res->ReserveU32();
      const size_t body_offset = res->size();
      status = ProcessOp(op, args, &c, res);
      if (status == NFS4_OK && !args->ok())
        status = NFS4ERR_BADXDR;
      Log(kLogLevelDebug, "xid %u op %u -> %u", call.xid, op, status);
      const bool illegal = (status == NFS4ERR_OP_ILLEGAL);
      res->PatchU32(op_offset, illegal ? static_cast<uint32_t>(OP_ILLEGAL) : op);
      res->PatchU32(status_offset_op, status);
      if (status != NFS4_OK) {
        res->Truncate(body_offset);
        if (op == OP_SETATTR)
          res->PutU32(0);  // attrsset is returned on failure, too
      }
      num_results++;
      if (status != NFS4_OK)
        break;
    }
  }

  if (num_results == 0) {
    Log(kLogLevelDebug, "xid %u: compound of %u ops rejected -> %u", call.xid,
        num_ops, status);
  }
  res->PatchU32(status_offset, status);
  res->PatchU32(count_offset, num_results);
  return status;
}


uint32_t Nfs4Server::ProcessOp(uint32_t op, XdrReader *args, Compound *c,
                               XdrWriter *body) {
  switch (op) {
    case OP_ACCESS:
      return OpAccess(args, c, body);
    case OP_CLOSE:
      return OpClose(args, c, body);
    case OP_GETATTR:
      return OpGetattr(args, c, body);
    case OP_GETFH:
      return OpGetfh(c, body);
    case OP_LOOKUP:
      return OpLookup(args, c);
    case OP_LOOKUPP:
      return OpLookupp(c);
    case OP_NVERIFY:
      return OpVerify(args, c, true);
    case OP_VERIFY:
      return OpVerify(args, c, false);
    case OP_OPEN:
      return OpOpen(args, c, body);
    case OP_OPENATTR:
      return OpOpenattr(args, c);
    case OP_OPEN_CONFIRM:
      return OpOpenConfirm(args, c, body);
    case OP_OPEN_DOWNGRADE:
      return OpOpenDowngrade(args, c, body);
    case OP_PUTFH:
      return OpPutfh(args, c);
    case OP_PUTPUBFH:
    case OP_PUTROOTFH:
      c->fh = FileHandle(FileHandle::kObject, fsid_, backend_->RootInode());
      c->has_fh = true;
      return NFS4_OK;
    case OP_READ:
      return OpRead(args, c, body);
    case OP_READDIR:
      return OpReaddir(args, c, body);
    case OP_READLINK:
      return OpReadlink(c, body);
    case OP_RENEW:
      return OpRenew(args);
    case OP_RESTOREFH:
      if (!c->has_saved)
        return NFS4ERR_RESTOREFH;
      c->fh = c->saved;
      c->has_fh = true;
      return NFS4_OK;
    case OP_SAVEFH:
      if (!c->has_fh)
        return NFS4ERR_NOFILEHANDLE;
      c->saved = c->fh;
      c->has_saved = true;
      return NFS4_OK;
    case OP_SECINFO:
      return OpSecinfo(args, c, body);
    case OP_SETCLIENTID:
      return OpSetclientid(args, body);
    case OP_SETCLIENTID_CONFIRM:
      return OpSetclientidConfirm(args);
    case OP_RELEASE_LOCKOWNER:
      return NFS4_OK;
    case OP_DELEGRETURN:
      return OpDelegreturn(args);
    case OP_DELEGPURGE:
    case OP_LOCK:
    case OP_LOCKT:
    case OP_LOCKU:
      return NFS4ERR_NOTSUPP;
    case OP_COMMIT:
    case OP_CREATE:
    case OP_LINK:
    case OP_REMOVE:
    case OP_RENAME:
    case OP_SETATTR:
    case OP_WRITE:
      return c->has_fh ? NFS4ERR_ROFS : NFS4ERR_NOFILEHANDLE;
    default:
      return NFS4ERR_OP_ILLEGAL;
  }
}


//------------------------------------------------------------------------------
// Object resolution


uint32_t Nfs4Server::ResolveXattrName(uint64_t ino, uint32_t hash,
                                      std::string *name) {
  std::vector<std::string> names;
  const int retval = backend_->ListXattr(ino, &names);
  if (retval != 0)
    return ErrnoToStatus(retval);
  for (unsigned i = 0; i < names.size(); ++i) {
    if (HashName(names[i]) == hash) {
      *name = names[i];
      return NFS4_OK;
    }
  }
  return NFS4ERR_STALE;
}


uint32_t Nfs4Server::Resolve(const FileHandle &fh, const Credentials &creds,
                             Object *obj) {
  obj->fh = fh;
  int retval = backend_->GetAttr(fh.ino, &obj->attr);
  if (retval == ENOENT)
    return NFS4ERR_STALE;
  if (retval != 0)
    return ErrnoToStatus(retval);

  if (fh.kind == FileHandle::kAttrDir) {
    obj->attr.mode = S_IFDIR | 0555;
    obj->attr.nlink = 2;
    obj->attr.size = 0;
  } else if (fh.kind == FileHandle::kNamedAttr) {
    const uint32_t status = ResolveXattrName(fh.ino, fh.aux, &obj->xattr_name);
    if (status != NFS4_OK)
      return status;
    std::string value;
    retval = backend_->GetXattr(fh.ino, obj->xattr_name, creds, &value);
    // Unreadable values (e.g. privileged attributes) still get a handle;
    // the error surfaces on READ
    obj->attr.size = (retval == 0) ? value.size() : 0;
    obj->attr.mode = S_IFREG | 0444;
    obj->attr.nlink = 1;
  }
  return NFS4_OK;
}


uint32_t Nfs4Server::LookupChild(const Object &dir, const std::string &name,
                                 const Credentials &creds, Object *child) {
  uint32_t status = CheckName(name);
  if (status != NFS4_OK)
    return status;

  if (dir.fh.kind == FileHandle::kAttrDir) {
    std::vector<std::string> names;
    const int retval = backend_->ListXattr(dir.fh.ino, &names);
    if (retval != 0)
      return ErrnoToStatus(retval);
    for (unsigned i = 0; i < names.size(); ++i) {
      if (names[i] == name) {
        return Resolve(FileHandle(FileHandle::kNamedAttr, fsid_, dir.fh.ino,
                                  HashName(name)),
                       creds, child);
      }
    }
    return NFS4ERR_NOENT;
  }

  if (dir.fh.kind != FileHandle::kObject)
    return NFS4ERR_NOTDIR;
  if (S_ISLNK(dir.attr.mode))
    return NFS4ERR_SYMLINK;
  if (!S_ISDIR(dir.attr.mode))
    return NFS4ERR_NOTDIR;

  const int retval = backend_->Lookup(dir.fh.ino, name, &child->attr);
  if (retval != 0)
    return ErrnoToStatus(retval);
  child->fh = FileHandle(FileHandle::kObject, fsid_, child->attr.ino);
  child->xattr_name.clear();
  return NFS4_OK;
}


uint32_t Nfs4Server::ListChildren(const Object &dir,
                                  std::vector<DirEntry> *entries) {
  if (dir.fh.kind == FileHandle::kAttrDir) {
    std::vector<std::string> names;
    const int retval = backend_->ListXattr(dir.fh.ino, &names);
    if (retval != 0)
      return ErrnoToStatus(retval);
    entries->clear();
    for (unsigned i = 0; i < names.size(); ++i)
      entries->push_back(DirEntry(names[i], dir.fh.ino));
    return NFS4_OK;
  }
  if (dir.fh.kind != FileHandle::kObject || !S_ISDIR(dir.attr.mode))
    return NFS4ERR_NOTDIR;
  return ErrnoToStatus(backend_->ReadDir(dir.fh.ino, entries));
}


uint64_t Nfs4Server::FileId(const Object &obj) const {
  switch (obj.fh.kind) {
    case FileHandle::kAttrDir:
      return obj.fh.ino | kAttrDirFileIdBit;
    case FileHandle::kNamedAttr:
      return (obj.fh.ino ^ (static_cast<uint64_t>(obj.fh.aux) << 32))
             | kNamedAttrFileIdBit;
    default:
      return obj.fh.ino;
  }
}


//------------------------------------------------------------------------------
// Attributes


bool Nfs4Server::DecodeBitmap(XdrReader *r, std::vector<uint32_t> *bitmap) {
  uint32_t len;
  if (!r->GetU32(&len) || len > 8)
    return false;
  bitmap->resize(len);
  for (uint32_t i = 0; i < len; ++i)
    r->GetU32(&(*bitmap)[i]);
  return r->ok();
}


bool Nfs4Server::EncodeFattr(const Object &obj, const uint32_t *request,
                             unsigned request_len, const Credentials &creds,
                             XdrWriter *out) {
  uint32_t supported[kNumBitmapWords];
  SupportedBitmap(supported);
  uint32_t returned[kNumBitmapWords] = {0, 0};
  for (unsigned i = 0; i < kNumBitmapWords && i < request_len; ++i)
    returned[i] = request[i] & supported[i];
  // Errors are reported per op, not in the attributes of a single object
  returned[0] &= ~(1u << FATTR4_RDATTR_ERROR);

  FsStat fs;
  const bool need_fs = IsSet(returned, 2, FATTR4_FILES_AVAIL)
                       || IsSet(returned, 2, FATTR4_FILES_FREE)
                       || IsSet(returned, 2, FATTR4_FILES_TOTAL)
                       || IsSet(returned, 2, FATTR4_SPACE_AVAIL)
                       || IsSet(returned, 2, FATTR4_SPACE_FREE)
                       || IsSet(returned, 2, FATTR4_SPACE_TOTAL);
  if (need_fs)
    backend_->StatFs(&fs);

  const Attr &a = obj.attr;
  XdrWriter v;
  for (unsigned attr = 0; attr < 32 * kNumBitmapWords; ++attr) {
    if (!IsSet(returned, kNumBitmapWords, attr))
      continue;
    switch (attr) {
      case FATTR4_SUPPORTED_ATTRS:
        PutBitmap(supported, kNumBitmapWords, &v);
        break;
      case FATTR4_TYPE:
        v.PutU32(FileType(obj.fh, a.mode));
        break;
      case FATTR4_FH_EXPIRE_TYPE:
        v.PutU32(FH4_PERSISTENT);
        break;
      case FATTR4_CHANGE:
        if (obj.fh.kind == FileHandle::kObject) {
          v.PutU64((static_cast<uint64_t>(a.ctime.sec) << 30) ^ a.ctime.nsec);
        } else {
          v.PutU64(backend_->Generation());
        }
        break;
      case FATTR4_SIZE:
        v.PutU64(a.size);
        break;
      case FATTR4_LINK_SUPPORT:
      case FATTR4_SYMLINK_SUPPORT:
      case FATTR4_UNIQUE_HANDLES:
      case FATTR4_CASE_PRESERVING:
      case FATTR4_CHOWN_RESTRICTED:
      case FATTR4_HOMOGENEOUS:
      case FATTR4_NO_TRUNC:
        v.PutBool(true);
        break;
      case FATTR4_NAMED_ATTR:
        v.PutBool(obj.fh.kind == FileHandle::kObject);
        break;
      case FATTR4_FSID:
        v.PutU64(fsid_);
        v.PutU64(0);
        break;
      case FATTR4_LEASE_TIME:
        v.PutU32(options_.lease_time);
        break;
      case FATTR4_ACLSUPPORT:
        v.PutU32(0);
        break;
      case FATTR4_CANSETTIME:
      case FATTR4_CASE_INSENSITIVE:
        v.PutBool(false);
        break;
      case FATTR4_FILEHANDLE:
        v.PutOpaque(obj.fh.Encode());
        break;
      case FATTR4_FILEID:
      case FATTR4_MOUNTED_ON_FILEID:
        v.PutU64(FileId(obj));
        break;
      case FATTR4_FILES_AVAIL:
        v.PutU64(fs.files_avail);
        break;
      case FATTR4_FILES_FREE:
        v.PutU64(fs.files_free);
        break;
      case FATTR4_FILES_TOTAL:
        v.PutU64(fs.files_total);
        break;
      case FATTR4_MAXFILESIZE:
        v.PutU64(0x7fffffffffffffffull);
        break;
      case FATTR4_MAXLINK:
        v.PutU32(32000);
        break;
      case FATTR4_MAXNAME:
        v.PutU32(kMaxName);
        break;
      case FATTR4_MAXREAD:
      case FATTR4_MAXWRITE:
        v.PutU64(options_.max_read);
        break;
      case FATTR4_MODE:
        v.PutU32(a.mode & 07777);
        break;
      case FATTR4_NUMLINKS:
        v.PutU32(a.nlink);
        break;
      case FATTR4_OWNER:
        v.PutString(NumericId(a.uid));
        break;
      case FATTR4_OWNER_GROUP:
        v.PutString(NumericId(a.gid));
        break;
      case FATTR4_RAWDEV:
        v.PutU32(static_cast<uint32_t>(a.rdev >> 32));
        v.PutU32(static_cast<uint32_t>(a.rdev));
        break;
      case FATTR4_SPACE_AVAIL:
        v.PutU64(fs.bytes_avail);
        break;
      case FATTR4_SPACE_FREE:
        v.PutU64(fs.bytes_free);
        break;
      case FATTR4_SPACE_TOTAL:
        v.PutU64(fs.bytes_total);
        break;
      case FATTR4_SPACE_USED:
        v.PutU64(a.size);
        break;
      case FATTR4_TIME_ACCESS:
        PutTime(a.atime, &v);
        break;
      case FATTR4_TIME_DELTA: {
        Timespec delta;
        delta.nsec = 1;
        PutTime(delta, &v);
        break;
      }
      case FATTR4_TIME_METADATA:
        PutTime(a.ctime, &v);
        break;
      case FATTR4_TIME_MODIFY:
        PutTime(a.mtime, &v);
        break;
      default:
        return false;
    }
  }

  PutBitmap(returned, kNumBitmapWords, out);
  out->PutOpaque(v.buffer());
  (void)creds;
  return true;
}


//------------------------------------------------------------------------------
// Operations


uint32_t Nfs4Server::OpAccess(XdrReader *args, Compound *c, XdrWriter *body) {
  uint32_t requested;
  if (!args->GetU32(&requested))
    return NFS4ERR_BADXDR;
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;
  Object obj;
  const uint32_t status = Resolve(c->fh, c->call->creds, &obj);
  if (status != NFS4_OK)
    return status;

  const uint32_t supported = requested
                             & (ACCESS4_READ | ACCESS4_LOOKUP | ACCESS4_MODIFY
                                | ACCESS4_EXTEND | ACCESS4_DELETE
                                | ACCESS4_EXECUTE);
  body->PutU32(supported);
  body->PutU32(AccessFromMode(obj.attr, c->call->creds) & supported);
  return NFS4_OK;
}


uint32_t Nfs4Server::OpGetattr(XdrReader *args, Compound *c, XdrWriter *body) {
  std::vector<uint32_t> request;
  if (!DecodeBitmap(args, &request))
    return NFS4ERR_BADXDR;
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;
  Object obj;
  const uint32_t status = Resolve(c->fh, c->call->creds, &obj);
  if (status != NFS4_OK)
    return status;
  if (!EncodeFattr(obj, request.empty() ? NULL : &request[0],
                   static_cast<unsigned>(request.size()), c->call->creds,
                   body)) {
    return NFS4ERR_SERVERFAULT;
  }
  return NFS4_OK;
}


uint32_t Nfs4Server::OpGetfh(Compound *c, XdrWriter *body) {
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;
  body->PutOpaque(c->fh.Encode());
  return NFS4_OK;
}


uint32_t Nfs4Server::OpLookup(XdrReader *args, Compound *c) {
  std::string name;
  if (!args->GetString(&name, kNfs4OpaqueLimit))
    return NFS4ERR_BADXDR;
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;
  Object dir, child;
  uint32_t status = Resolve(c->fh, c->call->creds, &dir);
  if (status != NFS4_OK)
    return status;
  status = LookupChild(dir, name, c->call->creds, &child);
  if (status != NFS4_OK)
    return status;
  c->fh = child.fh;
  return NFS4_OK;
}


uint32_t Nfs4Server::OpLookupp(Compound *c) {
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;
  switch (c->fh.kind) {
    case FileHandle::kAttrDir:
      c->fh.kind = FileHandle::kObject;
      return NFS4_OK;
    case FileHandle::kNamedAttr:
      c->fh.kind = FileHandle::kAttrDir;
      c->fh.aux = 0;
      return NFS4_OK;
    default:
      break;
  }

  Object obj;
  const uint32_t status = Resolve(c->fh, c->call->creds, &obj);
  if (status != NFS4_OK)
    return status;
  if (!S_ISDIR(obj.attr.mode))
    return S_ISLNK(obj.attr.mode) ? NFS4ERR_SYMLINK : NFS4ERR_NOTDIR;
  if (c->fh.ino == backend_->RootInode())
    return NFS4ERR_NOENT;
  Attr parent;
  const int retval = backend_->LookupParent(c->fh.ino, &parent);
  if (retval != 0)
    return ErrnoToStatus(retval);
  c->fh = FileHandle(FileHandle::kObject, fsid_, parent.ino);
  return NFS4_OK;
}


uint32_t Nfs4Server::OpVerify(XdrReader *args, Compound *c, bool negate) {
  std::vector<uint32_t> request;
  std::string values;
  if (!DecodeBitmap(args, &request) || !args->GetOpaque(&values))
    return NFS4ERR_BADXDR;
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;

  uint32_t supported[kNumBitmapWords];
  SupportedBitmap(supported);
  for (unsigned i = 0; i < request.size(); ++i) {
    if (request[i] & ~(i < kNumBitmapWords ? supported[i] : 0))
      return NFS4ERR_ATTRNOTSUPP;
  }
  if (IsSet(request.empty() ? NULL : &request[0],
            static_cast<unsigned>(request.size()), FATTR4_RDATTR_ERROR)) {
    return NFS4ERR_INVAL;
  }

  Object obj;
  const uint32_t status = Resolve(c->fh, c->call->creds, &obj);
  if (status != NFS4_OK)
    return status;
  XdrWriter mine;
  EncodeFattr(obj, request.empty() ? NULL : &request[0],
              static_cast<unsigned>(request.size()), c->call->creds, &mine);
  // Skip our bitmap and the opaque length to compare the raw values
  XdrReader r(reinterpret_cast<const unsigned char *>(mine.buffer().data()),
              mine.size());
  std::vector<uint32_t> ignored;
  std::string my_values;
  DecodeBitmap(&r, &ignored);
  r.GetOpaque(&my_values);

  const bool same = (my_values == values);
  if (negate)
    return same ? NFS4ERR_SAME : NFS4_OK;
  return same ? NFS4_OK : NFS4ERR_NOT_SAME;
}


uint32_t Nfs4Server::OpOpenattr(XdrReader *args, Compound *c) {
  bool create_dir;
  if (!args->GetBool(&create_dir))
    return NFS4ERR_BADXDR;
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;
  if (c->fh.kind != FileHandle::kObject)
    return NFS4ERR_INVAL;
  Object obj;
  const uint32_t status = Resolve(c->fh, c->call->creds, &obj);
  if (status != NFS4_OK)
    return status;
  c->fh.kind = FileHandle::kAttrDir;
  c->fh.aux = 0;
  return NFS4_OK;
}


uint32_t Nfs4Server::OpPutfh(XdrReader *args, Compound *c) {
  std::string raw;
  if (!args->GetOpaque(&raw, kNfs4FhSize))
    return NFS4ERR_BADXDR;
  FileHandle fh;
  if (!FileHandle::Decode(raw, &fh))
    return NFS4ERR_BADHANDLE;
  if (fh.fsid != fsid_)
    return NFS4ERR_STALE;
  c->fh = fh;
  c->has_fh = true;
  return NFS4_OK;
}


uint32_t Nfs4Server::OpRead(XdrReader *args, Compound *c, XdrWriter *body) {
  Stateid stateid;
  uint64_t offset;
  uint32_t count;
  if (!GetStateid(args, &stateid) || !args->GetU64(&offset)
      || !args->GetU32(&count)) {
    return NFS4ERR_BADXDR;
  }
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;

  Object obj;
  uint32_t status = Resolve(c->fh, c->call->creds, &obj);
  if (status != NFS4_OK)
    return status;
  if (obj.fh.kind == FileHandle::kAttrDir || S_ISDIR(obj.attr.mode))
    return NFS4ERR_ISDIR;
  if (!S_ISREG(obj.attr.mode))
    return NFS4ERR_INVAL;
  {
    std::lock_guard<std::mutex> lock(state_lock_);
    status = CheckReadStateid(stateid, c->fh);
  }
  if (status != NFS4_OK)
    return status;

  if (count > options_.max_read)
    count = options_.max_read;
  std::string data;
  if (obj.fh.kind == FileHandle::kNamedAttr) {
    std::string value;
    const int retval = backend_->GetXattr(obj.fh.ino, obj.xattr_name,
                                          c->call->creds, &value);
    if (retval != 0)
      return ErrnoToStatus(retval);
    if (offset < value.size())
      data = value.substr(offset, count);
    body->PutBool(offset + data.size() >= value.size());
    body->PutOpaque(data);
    return NFS4_OK;
  }

  if (count > 0) {
    const int retval = backend_->Read(obj.fh.ino, offset, count, &data);
    if (retval != 0)
      return ErrnoToStatus(retval);
  }
  const bool eof = (data.size() < count)
                   || (offset + data.size() >= obj.attr.size);
  body->PutBool(eof);
  body->PutOpaque(data);
  return NFS4_OK;
}


uint32_t Nfs4Server::OpReaddir(XdrReader *args, Compound *c, XdrWriter *body) {
  uint64_t cookie;
  unsigned char verifier[kNfs4VerifierSize];
  uint32_t dircount, maxcount;
  std::vector<uint32_t> request;
  if (!args->GetU64(&cookie)
      || !args->GetFixedOpaque(verifier, kNfs4VerifierSize)
      || !args->GetU32(&dircount) || !args->GetU32(&maxcount)
      || !DecodeBitmap(args, &request)) {
    return NFS4ERR_BADXDR;
  }
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;

  Object dir;
  uint32_t status = Resolve(c->fh, c->call->creds, &dir);
  if (status != NFS4_OK)
    return status;
  if (dir.fh.kind == FileHandle::kNamedAttr || !S_ISDIR(dir.attr.mode))
    return NFS4ERR_NOTDIR;

  XdrWriter my_verifier_w;
  my_verifier_w.PutU64(backend_->Generation());
  const std::string &my_verifier = my_verifier_w.buffer();
  if (cookie == 1 || cookie == 2)
    return NFS4ERR_BAD_COOKIE;
  if (cookie != 0
      && memcmp(verifier, my_verifier.data(), kNfs4VerifierSize) != 0) {
    return NFS4ERR_NOT_SAME;
  }

  std::vector<DirEntry> entries;
  status = ListChildren(dir, &entries);
  if (status != NFS4_OK)
    return status;

  // Cookies 0, 1 and 2 are reserved; entry i has cookie i + 3
  size_t index = (cookie == 0) ? 0 : static_cast<size_t>(cookie - 2);
  if (index > entries.size())
    return NFS4ERR_BAD_COOKIE;

  const bool want_rdattr_error = IsSet(request.empty() ? NULL : &request[0],
                                       static_cast<unsigned>(request.size()),
                                       FATTR4_RDATTR_ERROR);

  body->PutFixedOpaque(my_verifier.data(), kNfs4VerifierSize);
  // Fixed part: verifier, end-of-list marker, eof flag
  const size_t fixed_size = kNfs4VerifierSize + 4 + 4;
  unsigned num_encoded = 0;
  for (; index < entries.size(); ++index) {
    const DirEntry &e = entries[index];
    if (!IsValidUtf8(e.name)) {
      Log(kLogLevelDebug, "skipping non-UTF-8 name in listing");
      continue;
    }

    XdrWriter entry;
    entry.PutBool(true);
    entry.PutU64(index + 3);
    entry.PutString(e.name);
    Object child;
    const uint32_t child_status = LookupChild(dir, e.name, c->call->creds,
                                              &child);
    if (child_status == NFS4_OK) {
      EncodeFattr(child, request.empty() ? NULL : &request[0],
                  static_cast<unsigned>(request.size()), c->call->creds,
                  &entry);
    } else if (want_rdattr_error) {
      uint32_t words[1] = {1u << FATTR4_RDATTR_ERROR};
      PutBitmap(words, 1, &entry);
      XdrWriter value;
      value.PutU32(child_status);
      entry.PutOpaque(value.buffer());
    } else {
      // Vanished between listing and lookup; skip it
      continue;
    }

    if (body->size() + entry.size() + fixed_size - kNfs4VerifierSize
        > maxcount) {
      if (num_encoded == 0)
        return NFS4ERR_TOOSMALL;
      break;
    }
    body->PutRaw(entry.buffer().data(), entry.size());
    num_encoded++;
  }
  body->PutBool(false);
  body->PutBool(index >= entries.size());
  return NFS4_OK;
}


uint32_t Nfs4Server::OpReadlink(Compound *c, XdrWriter *body) {
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;
  Object obj;
  const uint32_t status = Resolve(c->fh, c->call->creds, &obj);
  if (status != NFS4_OK)
    return status;
  if (obj.fh.kind != FileHandle::kObject || !S_ISLNK(obj.attr.mode))
    return S_ISDIR(obj.attr.mode) ? NFS4ERR_ISDIR : NFS4ERR_INVAL;
  std::string target;
  const int retval = backend_->ReadLink(obj.fh.ino, &target);
  if (retval != 0)
    return ErrnoToStatus(retval);
  body->PutString(target);
  return NFS4_OK;
}


uint32_t Nfs4Server::OpSecinfo(XdrReader *args, Compound *c, XdrWriter *body) {
  std::string name;
  if (!args->GetString(&name, kNfs4OpaqueLimit))
    return NFS4ERR_BADXDR;
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;
  Object dir, child;
  uint32_t status = Resolve(c->fh, c->call->creds, &dir);
  if (status != NFS4_OK)
    return status;
  status = LookupChild(dir, name, c->call->creds, &child);
  if (status != NFS4_OK)
    return status;
  // RFC 7530 16.31.4: SECINFO consumes the current file handle
  c->has_fh = false;
  body->PutU32(1);
  body->PutU32(kAuthSys);
  return NFS4_OK;
}


//------------------------------------------------------------------------------
// Client and open state


void Nfs4Server::PutStateid(const Stateid &stateid, XdrWriter *w) {
  w->PutU32(stateid.seqid);
  w->PutFixedOpaque(stateid.other.data(), kStateidOtherSize);
}


bool Nfs4Server::GetStateid(XdrReader *r, Stateid *stateid) {
  unsigned char other[kStateidOtherSize];
  if (!r->GetU32(&stateid->seqid)
      || !r->GetFixedOpaque(other, kStateidOtherSize)) {
    return false;
  }
  stateid->other.assign(reinterpret_cast<char *>(other), kStateidOtherSize);
  return true;
}


std::string Nfs4Server::NewStateidOther() {
  XdrWriter w;
  w.PutU32(boot_);
  w.PutU64(next_stateid_++);
  return w.buffer();
}


uint32_t Nfs4Server::CheckClient(uint64_t clientid) {
  if (static_cast<uint32_t>(clientid >> 32) != boot_)
    return NFS4ERR_STALE_CLIENTID;
  std::map<uint64_t, Client>::const_iterator it = clients_.find(clientid);
  if (it == clients_.end() || !it->second.confirmed)
    return NFS4ERR_STALE_CLIENTID;
  return NFS4_OK;
}


Nfs4Server::SeqidCheck Nfs4Server::CheckSeqid(const OwnerKey &owner,
                                              uint32_t seqid, uint32_t op,
                                              bool create_owner,
                                              uint32_t *status,
                                              std::string *body) {
  std::map<OwnerKey, OpenOwner>::iterator it = owners_.find(owner);
  if (it == owners_.end()) {
    if (!create_owner) {
      *status = NFS4ERR_BAD_SEQID;
      return kSeqidBad;
    }
    return kSeqidProcess;
  }
  if (seqid == it->second.seqid + 1)
    return kSeqidProcess;
  if (seqid == it->second.seqid && it->second.last_op == op) {
    *status = it->second.last_status;
    *body = it->second.last_body;
    return kSeqidReplay;
  }
  *status = NFS4ERR_BAD_SEQID;
  return kSeqidBad;
}


void Nfs4Server::RecordSeqid(const OwnerKey &owner, uint32_t seqid, uint32_t op,
                             uint32_t status, const std::string &body) {
  if (!SeqidAdvances(status))
    return;
  OpenOwner &o = owners_[owner];
  o.seqid = seqid;
  o.last_op = op;
  o.last_status = status;
  o.last_body = body;
}


uint32_t Nfs4Server::FindOpenState(
    const Stateid &stateid, bool exact_seqid,
    std::map<std::string, OpenState>::iterator *it) {
  XdrReader r(reinterpret_cast<const unsigned char *>(stateid.other.data()),
              stateid.other.size());
  uint32_t boot;
  r.GetU32(&boot);
  if (boot != boot_)
    return NFS4ERR_STALE_STATEID;
  *it = open_states_.find(stateid.other);
  if (*it == open_states_.end())
    return NFS4ERR_BAD_STATEID;
  const uint32_t current = (*it)->second.seqid;
  if (stateid.seqid > current)
    return NFS4ERR_BAD_STATEID;
  if (exact_seqid && stateid.seqid < current)
    return NFS4ERR_OLD_STATEID;
  return NFS4_OK;
}


uint32_t Nfs4Server::CheckReadStateid(const Stateid &stateid,
                                      const FileHandle &fh) {
  const std::string zeros(kStateidOtherSize, '\0');
  const std::string ones(kStateidOtherSize, '\xff');
  if ((stateid.seqid == 0 && stateid.other == zeros)
      || (stateid.seqid == 0xffffffffu && stateid.other == ones)) {
    return NFS4_OK;
  }
  std::map<std::string, OpenState>::iterator it;
  const uint32_t status = FindOpenState(stateid, false, &it);
  if (status == NFS4ERR_BAD_STATEID) {
    std::map<std::string, Delegation>::const_iterator d =
        delegations_.find(stateid.other);
    if (d != delegations_.end() && d->second.fh == fh)
      return NFS4_OK;
  }
  if (status != NFS4_OK)
    return status;
  if (!(it->second.fh == fh))
    return NFS4ERR_BAD_STATEID;
  return NFS4_OK;
}


void Nfs4Server::DropClient(uint64_t clientid) {
  clients_.erase(clientid);
  for (std::map<std::string, Delegation>::iterator it = delegations_.begin();
       it != delegations_.end();) {
    if (it->second.clientid == clientid)
      delegations_.erase(it++);
    else
      ++it;
  }
  for (std::map<std::string, OpenState>::iterator it = open_states_.begin();
       it != open_states_.end();) {
    if (it->second.owner.first == clientid)
      open_states_.erase(it++);
    else
      ++it;
  }
  for (std::map<OwnerKey, OpenOwner>::iterator it = owners_.begin();
       it != owners_.end();) {
    if (it->first.first == clientid)
      owners_.erase(it++);
    else
      ++it;
  }
}


void Nfs4Server::PutDelegation(uint64_t clientid, const FileHandle &fh,
                               XdrWriter *w) {
  std::map<uint64_t, Client>::const_iterator client = clients_.find(clientid);
  bool grant = options_.delegations && fh.kind == FileHandle::kObject
               && client != clients_.end() && client->second.callback_ok;
  for (std::map<std::string, Delegation>::const_iterator it =
           delegations_.begin();
       grant && it != delegations_.end();
       ++it) {
    // One delegation per client and file; none while a recall is going on
    if ((it->second.clientid == clientid && it->second.fh == fh)
        || it->second.recalled != 0) {
      grant = false;
    }
  }
  if (!grant || delegations_.size() >= options_.max_stateids) {
    w->PutU32(OPEN_DELEGATE_NONE);
    return;
  }

  Stateid stateid;
  stateid.other = NewStateidOther();
  Delegation &delegation = delegations_[stateid.other];
  delegation.clientid = clientid;
  delegation.fh = fh;
  stateid.seqid = delegation.seqid;

  w->PutU32(OPEN_DELEGATE_READ);
  PutStateid(stateid, w);
  w->PutBool(false);  // recall
  // permissions: none granted in advance, the client still asks ACCESS
  w->PutU32(0);  // ACE4_ACCESS_ALLOWED_ACE_TYPE
  w->PutU32(0);  // flags
  w->PutU32(0);  // access mask
  w->PutString("EVERYONE@");
}


uint32_t Nfs4Server::OpDelegreturn(XdrReader *args) {
  Stateid stateid;
  if (!GetStateid(args, &stateid))
    return NFS4ERR_BADXDR;
  std::lock_guard<std::mutex> lock(state_lock_);
  XdrReader r(reinterpret_cast<const unsigned char *>(stateid.other.data()),
              stateid.other.size());
  uint32_t boot;
  r.GetU32(&boot);
  if (boot != boot_)
    return NFS4ERR_STALE_STATEID;
  if (delegations_.erase(stateid.other) == 0)
    return NFS4ERR_BAD_STATEID;
  return NFS4_OK;
}


void Nfs4Server::StartMonitor() {
  if (!monitor_.joinable())
    monitor_ = std::thread(&Nfs4Server::MonitorLoop, this);
}


void Nfs4Server::MonitorLoop() {
  const unsigned kCallbackTimeoutMs = 5000;
  std::unique_lock<std::mutex> lock(state_lock_);
  while (!monitor_stopping_) {
    monitor_cond_.wait_for(lock, std::chrono::seconds(1));
    if (monitor_stopping_)
      break;

    // Probe new callback paths
    std::vector<uint64_t> probes;
    probes.swap(pending_probes_);
    std::vector<std::pair<uint64_t, std::shared_ptr<CallbackClient> > > cbs;
    for (unsigned i = 0; i < probes.size(); ++i) {
      std::map<uint64_t, Client>::iterator c = clients_.find(probes[i]);
      if (c != clients_.end() && c->second.callback)
        cbs.push_back(std::make_pair(probes[i], c->second.callback));
    }
    lock.unlock();
    std::vector<bool> probe_ok;
    for (unsigned i = 0; i < cbs.size(); ++i)
      probe_ok.push_back(cbs[i].second->Null(kCallbackTimeoutMs));
    const uint64_t generation = backend_->Generation();
    lock.lock();
    for (unsigned i = 0; i < cbs.size(); ++i) {
      std::map<uint64_t, Client>::iterator c = clients_.find(cbs[i].first);
      if (c != clients_.end() && c->second.callback == cbs[i].second) {
        c->second.callback_ok = probe_ok[i];
        Log(kLogLevelDebug, "callback path of client %" PRIx64 " %s",
            cbs[i].first, probe_ok[i] ? "works" : "is down");
      }
    }

    // Recall everything when the exported tree changed; revoke what was not
    // returned within a lease period
    const time_t now = time(NULL);
    const bool changed = (last_generation_ != 0)
                         && (generation != last_generation_);
    last_generation_ = generation;
    struct Recall {
      std::string other;
      uint32_t seqid;
      std::string fh;
      std::shared_ptr<CallbackClient> callback;
    };
    std::vector<Recall> recalls;
    for (std::map<std::string, Delegation>::iterator it = delegations_.begin();
         it != delegations_.end();) {
      Delegation &d = it->second;
      if (d.recalled != 0 && now - d.recalled > options_.lease_time) {
        Log(kLogLevelWarn, "revoking unreturned delegation");
        delegations_.erase(it++);
        continue;
      }
      if (changed && d.recalled == 0) {
        std::map<uint64_t, Client>::iterator c = clients_.find(d.clientid);
        if (c == clients_.end() || !c->second.callback) {
          delegations_.erase(it++);
          continue;
        }
        d.recalled = now;
        Recall recall;
        recall.other = it->first;
        recall.seqid = d.seqid;
        recall.fh = d.fh.Encode();
        recall.callback = c->second.callback;
        recalls.push_back(recall);
      }
      ++it;
    }
    if (recalls.empty())
      continue;

    lock.unlock();
    Log(kLogLevelInfo, "recalling %zu delegations", recalls.size());
    std::vector<std::string> failed;
    const time_t recall_start = time(NULL);
    for (unsigned i = 0; i < recalls.size(); ++i) {
      const int result = recalls[i].callback->Recall(
          recalls[i].seqid, recalls[i].other, recalls[i].fh,
          kCallbackTimeoutMs);
      if (result != NFS4_OK) {
        Log(kLogLevelWarn, "delegation recall failed (%d)", result);
        failed.push_back(recalls[i].other);
      }
    }
    Log(kLogLevelInfo, "sent %zu recalls in %lds, %zu failed", recalls.size(),
        static_cast<long>(time(NULL) - recall_start), failed.size());  // NOLINT
    lock.lock();
    for (unsigned i = 0; i < failed.size(); ++i)
      delegations_.erase(failed[i]);
  }
}


size_t Nfs4Server::NumDelegations() {
  std::lock_guard<std::mutex> lock(state_lock_);
  return delegations_.size();
}


size_t Nfs4Server::NumOpenStates() {
  std::lock_guard<std::mutex> lock(state_lock_);
  return open_states_.size();
}


uint32_t Nfs4Server::OpSetclientid(XdrReader *args, XdrWriter *body) {
  unsigned char verifier[kNfs4VerifierSize];
  std::string name, netid, addr;
  uint32_t cb_program, cb_ident;
  args->GetFixedOpaque(verifier, kNfs4VerifierSize);
  args->GetOpaque(&name, kNfs4OpaqueLimit);
  args->GetU32(&cb_program);
  args->GetString(&netid);
  args->GetString(&addr);
  args->GetU32(&cb_ident);
  if (!args->ok())
    return NFS4ERR_BADXDR;
  const std::string verf(reinterpret_cast<char *>(verifier), kNfs4VerifierSize);

  std::lock_guard<std::mutex> lock(state_lock_);
  uint64_t clientid = 0;
  for (std::map<uint64_t, Client>::iterator it = clients_.begin();
       it != clients_.end();
       ++it) {
    if (it->second.name == name && it->second.confirmed
        && it->second.verifier == verf) {
      // Same client instance, e.g. callback update: keep the client id
      clientid = it->first;
      break;
    }
  }
  if (clientid == 0) {
    for (std::map<uint64_t, Client>::iterator it = clients_.begin();
         it != clients_.end();) {
      if (it->second.name == name && !it->second.confirmed)
        clients_.erase(it++);
      else
        ++it;
    }
    clientid = (static_cast<uint64_t>(boot_) << 32) | next_clientid_++;
    Client &client = clients_[clientid];
    client.name = name;
    client.verifier = verf;
  }

  XdrWriter confirm;
  confirm.PutU64((static_cast<uint64_t>(next_clientid_++) << 32)
                 ^ static_cast<uint64_t>(time(NULL)));
  clients_[clientid].confirm = confirm.buffer();
  if (options_.delegations) {
    clients_[clientid].callback.reset(
        new CallbackClient(cb_program, cb_ident, netid, addr));
    clients_[clientid].callback_ok = false;
  }
  body->PutU64(clientid);
  body->PutFixedOpaque(confirm.buffer().data(), kNfs4VerifierSize);
  return NFS4_OK;
}


uint32_t Nfs4Server::OpSetclientidConfirm(XdrReader *args) {
  uint64_t clientid;
  unsigned char confirm[kNfs4VerifierSize];
  if (!args->GetU64(&clientid)
      || !args->GetFixedOpaque(confirm, kNfs4VerifierSize)) {
    return NFS4ERR_BADXDR;
  }
  std::lock_guard<std::mutex> lock(state_lock_);
  std::map<uint64_t, Client>::iterator it = clients_.find(clientid);
  if (it == clients_.end()
      || it->second.confirm
             != std::string(reinterpret_cast<char *>(confirm),
                            kNfs4VerifierSize)) {
    return NFS4ERR_STALE_CLIENTID;
  }
  if (options_.delegations && it->second.callback
      && it->second.callback->valid()) {
    pending_probes_.push_back(clientid);
    StartMonitor();
    monitor_cond_.notify_all();
  }
  if (it->second.confirmed)
    return NFS4_OK;

  it->second.confirmed = true;
  const std::string name = it->second.name;
  // A rebooted client replaces its previous incarnation and its state
  std::vector<uint64_t> previous;
  for (std::map<uint64_t, Client>::iterator i = clients_.begin();
       i != clients_.end();
       ++i) {
    if (i->first != clientid && i->second.name == name)
      previous.push_back(i->first);
  }
  for (unsigned i = 0; i < previous.size(); ++i)
    DropClient(previous[i]);
  return NFS4_OK;
}


uint32_t Nfs4Server::OpRenew(XdrReader *args) {
  uint64_t clientid;
  if (!args->GetU64(&clientid))
    return NFS4ERR_BADXDR;
  std::lock_guard<std::mutex> lock(state_lock_);
  return CheckClient(clientid);
}


uint32_t Nfs4Server::OpOpen(XdrReader *args, Compound *c, XdrWriter *body) {
  uint32_t seqid, share_access, share_deny, open_type, claim_type;
  uint64_t clientid;
  std::string owner, name;
  args->GetU32(&seqid);
  args->GetU32(&share_access);
  args->GetU32(&share_deny);
  args->GetU64(&clientid);
  args->GetOpaque(&owner, kNfs4OpaqueLimit);
  args->GetU32(&open_type);
  if (!args->ok())
    return NFS4ERR_BADXDR;
  if (open_type == OPEN4_CREATE) {
    // The create arguments are not needed: the file system is read-only
    return c->has_fh ? NFS4ERR_ROFS : NFS4ERR_NOFILEHANDLE;
  }
  if (!args->GetU32(&claim_type))
    return NFS4ERR_BADXDR;
  switch (claim_type) {
    case CLAIM_NULL:
      args->GetString(&name, kNfs4OpaqueLimit);
      break;
    case CLAIM_PREVIOUS: {
      uint32_t delegation_type;
      args->GetU32(&delegation_type);
      break;
    }
    case CLAIM_DELEGATE_CUR: {
      // The client converts local opens before returning a delegation
      Stateid delegation;
      GetStateid(args, &delegation);
      args->GetString(&name, kNfs4OpaqueLimit);
      break;
    }
    default:
      // State of a previous server instance is not kept
      return NFS4ERR_RECLAIM_BAD;
  }
  if (!args->ok())
    return NFS4ERR_BADXDR;
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;

  const OwnerKey owner_key(clientid, owner);
  std::string replay_body;
  uint32_t status;
  {
    std::lock_guard<std::mutex> lock(state_lock_);
    status = CheckClient(clientid);
    if (status != NFS4_OK)
      return status;
    const SeqidCheck check = CheckSeqid(owner_key, seqid, OP_OPEN, true,
                                        &status, &replay_body);
    if (check == kSeqidBad)
      return status;
    if (check == kSeqidReplay) {
      body->PutRaw(replay_body.data(), replay_body.size());
      return status;
    }
  }

  Object target;
  Object dir;
  if (claim_type == CLAIM_NULL || claim_type == CLAIM_DELEGATE_CUR) {
    status = Resolve(c->fh, c->call->creds, &dir);
    if (status == NFS4_OK)
      status = LookupChild(dir, name, c->call->creds, &target);
  } else {
    status = Resolve(c->fh, c->call->creds, &target);
  }
  if (status == NFS4_OK) {
    if ((share_access & OPEN4_SHARE_ACCESS_WRITE)
        || !(share_access & OPEN4_SHARE_ACCESS_READ)) {
      status = NFS4ERR_ROFS;
    } else if (target.fh.kind == FileHandle::kAttrDir
               || S_ISDIR(target.attr.mode)) {
      status = NFS4ERR_ISDIR;
    } else if (S_ISLNK(target.attr.mode)) {
      status = NFS4ERR_SYMLINK;
    } else if (!S_ISREG(target.attr.mode)) {
      status = NFS4ERR_INVAL;
    } else if (!(AccessFromMode(target.attr, c->call->creds) & ACCESS4_READ)) {
      status = NFS4ERR_ACCESS;
    }
  }

  XdrWriter result;
  std::lock_guard<std::mutex> lock(state_lock_);
  if (status == NFS4_OK) {
    // Repeated opens of a file by the same owner share one stateid
    std::map<std::string, OpenState>::iterator it = open_states_.begin();
    for (; it != open_states_.end(); ++it) {
      if (it->second.owner == owner_key && it->second.fh == target.fh)
        break;
    }
    if (it == open_states_.end()) {
      if (open_states_.size() >= options_.max_stateids) {
        Log(kLogLevelWarn, "too many open NFS stateids (%u)", options_.max_stateids);
        return NFS4ERR_RESOURCE;
      }
      const std::string other = NewStateidOther();
      it = open_states_.insert(std::make_pair(other, OpenState())).first;
      it->second.owner = owner_key;
      it->second.fh = target.fh;
    }
    it->second.seqid++;
    it->second.access |= share_access;

    Stateid stateid;
    stateid.seqid = it->second.seqid;
    stateid.other = it->first;
    PutStateid(stateid, &result);
    // change_info4: nothing changes in a read-only directory
    result.PutBool(true);
    result.PutU64(0);
    result.PutU64(0);
    // No OPEN4_RESULT_CONFIRM: a confirm round trip buys nothing here
    result.PutU32(OPEN4_RESULT_LOCKTYPE_POSIX);
    result.PutU32(0);  // attrset
    if (claim_type == CLAIM_NULL)
      PutDelegation(clientid, target.fh, &result);
    else
      result.PutU32(OPEN_DELEGATE_NONE);
    c->fh = target.fh;
  }
  RecordSeqid(owner_key, seqid, OP_OPEN, status, result.buffer());
  if (status == NFS4_OK)
    body->PutRaw(result.buffer().data(), result.size());
  return status;
}


uint32_t Nfs4Server::OpOpenConfirm(XdrReader *args, Compound *c,
                                   XdrWriter *body) {
  Stateid stateid;
  uint32_t seqid;
  if (!GetStateid(args, &stateid) || !args->GetU32(&seqid))
    return NFS4ERR_BADXDR;
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;

  std::lock_guard<std::mutex> lock(state_lock_);
  std::map<std::string, OpenState>::iterator it;
  uint32_t status = FindOpenState(stateid, true, &it);
  if (status != NFS4_OK)
    return status;
  const OwnerKey owner = it->second.owner;
  std::string replay_body;
  const SeqidCheck check = CheckSeqid(owner, seqid, OP_OPEN_CONFIRM, false,
                                      &status, &replay_body);
  if (check == kSeqidBad)
    return status;
  if (check == kSeqidReplay) {
    body->PutRaw(replay_body.data(), replay_body.size());
    return status;
  }

  it->second.seqid++;
  Stateid result;
  result.seqid = it->second.seqid;
  result.other = it->first;
  XdrWriter w;
  PutStateid(result, &w);
  RecordSeqid(owner, seqid, OP_OPEN_CONFIRM, NFS4_OK, w.buffer());
  body->PutRaw(w.buffer().data(), w.size());
  return NFS4_OK;
}


uint32_t Nfs4Server::OpOpenDowngrade(XdrReader *args, Compound *c,
                                     XdrWriter *body) {
  Stateid stateid;
  uint32_t seqid, share_access, share_deny;
  if (!GetStateid(args, &stateid) || !args->GetU32(&seqid)
      || !args->GetU32(&share_access) || !args->GetU32(&share_deny)) {
    return NFS4ERR_BADXDR;
  }
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;

  std::lock_guard<std::mutex> lock(state_lock_);
  std::map<std::string, OpenState>::iterator it;
  uint32_t status = FindOpenState(stateid, true, &it);
  if (status != NFS4_OK)
    return status;
  const OwnerKey owner = it->second.owner;
  std::string replay_body;
  const SeqidCheck check = CheckSeqid(owner, seqid, OP_OPEN_DOWNGRADE, false,
                                      &status, &replay_body);
  if (check == kSeqidBad)
    return status;
  if (check == kSeqidReplay) {
    body->PutRaw(replay_body.data(), replay_body.size());
    return status;
  }

  XdrWriter w;
  if ((share_access & ~it->second.access) != 0) {
    status = NFS4ERR_INVAL;
  } else {
    it->second.access = share_access;
    it->second.seqid++;
    Stateid result;
    result.seqid = it->second.seqid;
    result.other = it->first;
    PutStateid(result, &w);
    status = NFS4_OK;
  }
  RecordSeqid(owner, seqid, OP_OPEN_DOWNGRADE, status, w.buffer());
  body->PutRaw(w.buffer().data(), w.size());
  return status;
}


uint32_t Nfs4Server::OpClose(XdrReader *args, Compound *c, XdrWriter *body) {
  uint32_t seqid;
  Stateid stateid;
  if (!args->GetU32(&seqid) || !GetStateid(args, &stateid))
    return NFS4ERR_BADXDR;
  if (!c->has_fh)
    return NFS4ERR_NOFILEHANDLE;

  uint64_t forget_ino = 0;
  {
    std::lock_guard<std::mutex> lock(state_lock_);
    std::map<std::string, OpenState>::iterator it;
    uint32_t status = FindOpenState(stateid, true, &it);
    if (status == NFS4ERR_BAD_STATEID) {
      // The stateid is gone after a CLOSE, so a retransmitted CLOSE is only
      // recognizable by the owner's cached reply
      for (std::map<OwnerKey, OpenOwner>::const_iterator o = owners_.begin();
           o != owners_.end();
           ++o) {
        const OpenOwner &owner = o->second;
        if (owner.last_op == OP_CLOSE && owner.seqid == seqid
            && owner.last_body.size() == 4 + kStateidOtherSize
            && owner.last_body.compare(4, kStateidOtherSize, stateid.other)
                   == 0) {
          body->PutRaw(owner.last_body.data(), owner.last_body.size());
          return owner.last_status;
        }
      }
    }
    if (status != NFS4_OK)
      return status;
    const OwnerKey owner = it->second.owner;
    std::string replay_body;
    const SeqidCheck check = CheckSeqid(owner, seqid, OP_CLOSE, false, &status,
                                        &replay_body);
    if (check == kSeqidBad)
      return status;
    if (check == kSeqidReplay) {
      body->PutRaw(replay_body.data(), replay_body.size());
      return status;
    }

    Stateid result;
    result.seqid = it->second.seqid + 1;
    result.other = it->first;
    const FileHandle fh = it->second.fh;
    open_states_.erase(it);
    bool still_open = false;
    for (it = open_states_.begin(); it != open_states_.end(); ++it) {
      if (it->second.fh.ino == fh.ino) {
        still_open = true;
        break;
      }
    }
    if (!still_open && fh.kind == FileHandle::kObject)
      forget_ino = fh.ino;

    XdrWriter w;
    PutStateid(result, &w);
    RecordSeqid(owner, seqid, OP_CLOSE, NFS4_OK, w.buffer());
    body->PutRaw(w.buffer().data(), w.size());
  }
  if (forget_ino != 0)
    backend_->Forget(forget_ino);
  return NFS4_OK;
}

}  // namespace nfs4
