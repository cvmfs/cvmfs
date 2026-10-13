/**
 * This file is part of the CernVM File System.
 */

#include "nfs4/fuse_backend.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <sys/resource.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include "nfs4/fuse_shim.h"
#include "nfs4/logging.h"

namespace nfs4 {

namespace {

// Directory listings are fetched from cvmfs in slices of this size
const size_t kReaddirChunk = 256 * 1024;

Timespec ToTimespec(time_t sec, long nsec) {  // NOLINT(runtime/int)
  Timespec t;
  t.sec = sec;
  t.nsec = static_cast<uint32_t>(nsec);
  return t;
}

void StatToAttr(const struct stat &info, Attr *attr) {
  attr->ino = info.st_ino;
  attr->mode = info.st_mode;
  attr->nlink = static_cast<uint32_t>(info.st_nlink);
  attr->uid = info.st_uid;
  attr->gid = info.st_gid;
  attr->size = info.st_size;
  attr->rdev = info.st_rdev;
#ifdef __APPLE__
  attr->atime = ToTimespec(info.st_atimespec.tv_sec, info.st_atimespec.tv_nsec);
  attr->mtime = ToTimespec(info.st_mtimespec.tv_sec, info.st_mtimespec.tv_nsec);
  attr->ctime = ToTimespec(info.st_ctimespec.tv_sec, info.st_ctimespec.tv_nsec);
#else
  attr->atime = ToTimespec(info.st_atim.tv_sec, info.st_atim.tv_nsec);
  attr->mtime = ToTimespec(info.st_mtim.tv_sec, info.st_mtim.tv_nsec);
  attr->ctime = ToTimespec(info.st_ctim.tv_sec, info.st_ctim.tv_nsec);
#endif
}

/** Errno of a request that did not produce the expected kind of reply */
int ReplyError(const fuse_req &req, fuse_req::Reply expected) {
  if (req.reply == expected)
    return 0;
  if (req.reply == fuse_req::kErr)
    return req.err != 0 ? req.err : EIO;
  Log(kLogLevelError, "unexpected FUSE reply type %d (expected %d)",
      static_cast<int>(req.reply), static_cast<int>(expected));
  return EIO;
}

// Layout of libfuse's directory entries (struct fuse_dirent)
const size_t kDirentHeader = 24;

}  // anonymous namespace


FuseBackend::FuseBackend(const struct fuse_lowlevel_ops *ops, uint64_t fsid)
    : ops_(ops)
    , fsid_(fsid)
    , max_open_files_(kMaxOpenFiles)
    , generation_(1)
    , generation_checked_(0) {
  struct rlimit limit;
  if (getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur != RLIM_INFINITY) {
    const rlim_t share = limit.rlim_cur / 8;
    if (share < max_open_files_)
      max_open_files_ = (share < 16) ? 16 : static_cast<unsigned>(share);
  }
}


FuseBackend::~FuseBackend() { ReleaseAll(); }


uint64_t FuseBackend::Generation() {
  const time_t now = time(NULL);
  std::lock_guard<std::mutex> guard(generation_lock_);
  if (now == generation_checked_)
    return generation_;
  generation_checked_ = now;

  // The root catalog revision changes with every publication
  const std::string name = "user.revision";
  Credentials root;
  root.uid = root.gid = 0;
  std::string value;
  if (Xattr(FUSE_ROOT_ID, &name, root, &value) == 0)
    generation_ = strtoull(value.c_str(), NULL, 10) + 1;
  return generation_;
}


int FuseBackend::GetAttr(uint64_t ino, Attr *attr) {
  fuse_req req;
  ops_->getattr(&req, ino, NULL);
  const int err = ReplyError(req, fuse_req::kAttr);
  if (err != 0)
    return err;
  StatToAttr(req.attr, attr);
  attr->ino = ino;
  return 0;
}


int FuseBackend::Lookup(uint64_t parent, const std::string &name, Attr *attr) {
  fuse_req req;
  ops_->lookup(&req, parent, name.c_str());
  const int err = ReplyError(req, fuse_req::kEntry);
  if (err != 0)
    return err;
  // A zero inode is a cacheable negative entry
  if (req.entry.ino == 0)
    return ENOENT;
  StatToAttr(req.entry.attr, attr);
  attr->ino = req.entry.ino;
  return 0;
}


int FuseBackend::LookupParent(uint64_t ino, Attr *attr) {
  return Lookup(ino, "..", attr);
}


int FuseBackend::ReadLink(uint64_t ino, std::string *target) {
  fuse_req req;
  ops_->readlink(&req, ino);
  const int err = ReplyError(req, fuse_req::kReadlink);
  if (err != 0)
    return err;
  target->swap(req.buf);
  return 0;
}


int FuseBackend::ReadDir(uint64_t ino, std::vector<DirEntry> *entries) {
  struct fuse_file_info fi;
  memset(&fi, 0, sizeof(fi));
  fi.flags = O_RDONLY;
  {
    fuse_req req;
    ops_->opendir(&req, ino, &fi);
    const int err = ReplyError(req, fuse_req::kOpen);
    if (err != 0)
      return err;
    fi = req.fi;
  }

  entries->clear();
  int err = 0;
  off_t offset = 0;
  while (true) {
    fuse_req req;
    ops_->readdir(&req, ino, kReaddirChunk, offset, &fi);
    err = ReplyError(req, fuse_req::kBuf);
    if (err != 0 || req.buf.empty())
      break;

    size_t pos = 0;
    while (pos + kDirentHeader <= req.buf.size()) {
      const char *p = req.buf.data() + pos;
      uint64_t entry_ino;
      uint32_t namelen;
      memcpy(&entry_ino, p, 8);
      memcpy(&namelen, p + 16, 4);
      const size_t entry_size = (kDirentHeader + namelen + 7) & ~size_t(7);
      if (pos + kDirentHeader + namelen > req.buf.size())
        break;
      const std::string name(p + kDirentHeader, namelen);
      if (name != "." && name != "..")
        entries->push_back(DirEntry(name, entry_ino));
      pos += entry_size;
    }
    if (pos == 0) {
      // Not even one entry fits into the slice
      err = EIO;
      break;
    }
    offset += pos;
  }

  fuse_req req;
  ops_->releasedir(&req, ino, &fi);
  return err;
}


void FuseBackend::EvictLocked(
    std::vector<std::pair<uint64_t, struct fuse_file_info> > *to_close) {
  std::list<uint64_t>::iterator it = lru_.begin();
  while (open_files_.size() > max_open_files_ && it != lru_.end()) {
    std::map<uint64_t, OpenFile>::iterator f = open_files_.find(*it);
    if (f->second.pins > 0) {
      ++it;
      continue;
    }
    to_close->push_back(std::make_pair(f->first, f->second.fi));
    open_files_.erase(f);
    it = lru_.erase(it);
  }
}


void FuseBackend::CloseFiles(
    const std::vector<std::pair<uint64_t, struct fuse_file_info> > &files) {
  for (unsigned i = 0; i < files.size(); ++i) {
    fuse_req req;
    struct fuse_file_info fi = files[i].second;
    ops_->release(&req, files[i].first, &fi);
  }
}


int FuseBackend::AcquireFile(uint64_t ino, struct fuse_file_info *fi) {
  const uint64_t generation = Generation();
  std::vector<std::pair<uint64_t, struct fuse_file_info> > to_close;
  {
    std::lock_guard<std::mutex> guard(lock_);
    std::map<uint64_t, OpenFile>::iterator f = open_files_.find(ino);
    if (f != open_files_.end()) {
      if (f->second.generation == generation || f->second.pins > 0) {
        // A file handle keeps serving the revision it was opened in; a
        // newer revision is picked up once nobody reads from it
        f->second.pins++;
        f->second.last_used = time(NULL);
        lru_.splice(lru_.end(), lru_, f->second.lru_pos);
        *fi = f->second.fi;
        return 0;
      }
      to_close.push_back(std::make_pair(ino, f->second.fi));
      lru_.erase(f->second.lru_pos);
      open_files_.erase(f);
    }
  }
  CloseFiles(to_close);
  to_close.clear();

  struct fuse_file_info new_fi;
  memset(&new_fi, 0, sizeof(new_fi));
  new_fi.flags = O_RDONLY;
  fuse_req req;
  ops_->open(&req, ino, &new_fi);
  int err = ReplyError(req, fuse_req::kOpen);
  if (err == EMFILE || err == ENFILE) {
    // Make room by closing all cached files that are not being read
    ReleaseIdle(0);
    req = fuse_req();
    new_fi.fh = 0;
    ops_->open(&req, ino, &new_fi);
    err = ReplyError(req, fuse_req::kOpen);
  }
  if (err == EMFILE || err == ENFILE) {
    Log(kLogLevelWarn, "out of file descriptors opening inode %" PRIu64,
        ino);
    return EAGAIN;
  }
  if (err != 0)
    return err;

  {
    std::lock_guard<std::mutex> guard(lock_);
    std::map<uint64_t, OpenFile>::iterator f = open_files_.find(ino);
    if (f != open_files_.end()) {
      // Another thread opened the file concurrently; use its handle
      to_close.push_back(std::make_pair(ino, req.fi));
      f->second.pins++;
      *fi = f->second.fi;
    } else {
      OpenFile &file = open_files_[ino];
      file.fi = req.fi;
      file.generation = generation;
      file.pins = 1;
      file.last_used = time(NULL);
      file.lru_pos = lru_.insert(lru_.end(), ino);
      *fi = req.fi;
      EvictLocked(&to_close);
    }
  }
  CloseFiles(to_close);
  return 0;
}


void FuseBackend::ReleaseFile(uint64_t ino) {
  std::lock_guard<std::mutex> guard(lock_);
  std::map<uint64_t, OpenFile>::iterator f = open_files_.find(ino);
  if (f != open_files_.end() && f->second.pins > 0)
    f->second.pins--;
}


int FuseBackend::Read(uint64_t ino, uint64_t offset, uint32_t size,
                      std::string *data) {
  struct fuse_file_info fi;
  int err = AcquireFile(ino, &fi);
  if (err != 0)
    return err;
  fuse_req req;
  ops_->read(&req, ino, size, offset, &fi);
  ReleaseFile(ino);
  err = ReplyError(req, fuse_req::kBuf);
  if (err != 0)
    return err;
  data->swap(req.buf);
  return 0;
}


void FuseBackend::Forget(uint64_t ino) {
  std::vector<std::pair<uint64_t, struct fuse_file_info> > to_close;
  {
    std::lock_guard<std::mutex> guard(lock_);
    std::map<uint64_t, OpenFile>::iterator f = open_files_.find(ino);
    if (f == open_files_.end() || f->second.pins > 0)
      return;
    to_close.push_back(std::make_pair(ino, f->second.fi));
    lru_.erase(f->second.lru_pos);
    open_files_.erase(f);
  }
  CloseFiles(to_close);
}


void FuseBackend::ReleaseIdle(unsigned max_idle) {
  const time_t now = time(NULL);
  std::vector<std::pair<uint64_t, struct fuse_file_info> > to_close;
  {
    std::lock_guard<std::mutex> guard(lock_);
    // The LRU list is ordered by last use
    std::list<uint64_t>::iterator it = lru_.begin();
    while (it != lru_.end()) {
      std::map<uint64_t, OpenFile>::iterator f = open_files_.find(*it);
      if (now - f->second.last_used < static_cast<time_t>(max_idle))
        break;
      if (f->second.pins > 0) {
        ++it;
        continue;
      }
      to_close.push_back(std::make_pair(f->first, f->second.fi));
      open_files_.erase(f);
      it = lru_.erase(it);
    }
  }
  CloseFiles(to_close);
}


void FuseBackend::ReleaseAll() {
  std::vector<std::pair<uint64_t, struct fuse_file_info> > to_close;
  {
    std::lock_guard<std::mutex> guard(lock_);
    for (std::map<uint64_t, OpenFile>::iterator f = open_files_.begin();
         f != open_files_.end(); ++f) {
      to_close.push_back(std::make_pair(f->first, f->second.fi));
    }
    open_files_.clear();
    lru_.clear();
  }
  CloseFiles(to_close);
}


int FuseBackend::StatFs(FsStat *stat) {
  fuse_req req;
  ops_->statfs(&req, FUSE_ROOT_ID);
  const int err = ReplyError(req, fuse_req::kStatfs);
  if (err != 0)
    return err;
  const uint64_t frsize = req.statfs.f_frsize ? req.statfs.f_frsize
                                              : req.statfs.f_bsize;
  stat->bytes_total = static_cast<uint64_t>(req.statfs.f_blocks) * frsize;
  stat->bytes_free = static_cast<uint64_t>(req.statfs.f_bfree) * frsize;
  stat->bytes_avail = static_cast<uint64_t>(req.statfs.f_bavail) * frsize;
  stat->files_total = req.statfs.f_files;
  stat->files_free = req.statfs.f_ffree;
  stat->files_avail = req.statfs.f_favail;
  return 0;
}


/**
 * listxattr (name == NULL) or getxattr, following the FUSE protocol: first
 * ask for the size, then fetch the value.
 */
int FuseBackend::Xattr(uint64_t ino, const std::string *name,
                       const Credentials &creds, std::string *value) {
  for (unsigned attempt = 0; attempt < 3; ++attempt) {
    fuse_req size_req;
    size_req.ctx.uid = creds.uid;
    size_req.ctx.gid = creds.gid;
    if (name)
      ops_->getxattr(&size_req, ino, name->c_str(), 0);
    else
      ops_->listxattr(&size_req, ino, 0);
    int err = ReplyError(size_req, fuse_req::kXattr);
    if (err != 0)
      return err;
    if (value == NULL)
      return 0;
    if (size_req.xattr_size == 0) {
      value->clear();
      return 0;
    }

    fuse_req req;
    req.ctx = size_req.ctx;
    if (name)
      ops_->getxattr(&req, ino, name->c_str(), size_req.xattr_size);
    else
      ops_->listxattr(&req, ino, size_req.xattr_size);
    err = ReplyError(req, fuse_req::kBuf);
    // The value can grow between the two calls (e.g. user.uptime)
    if (err == ERANGE)
      continue;
    if (err != 0)
      return err;
    value->swap(req.buf);
    return 0;
  }
  return ERANGE;
}


int FuseBackend::ListXattr(uint64_t ino, std::vector<std::string> *names) {
  std::string buf;
  const int err = Xattr(ino, NULL, Credentials(), &buf);
  if (err != 0)
    return err;
  names->clear();
  size_t start = 0;
  for (size_t i = 0; i < buf.size(); ++i) {
    if (buf[i] == '\0') {
      if (i > start)
        names->push_back(buf.substr(start, i - start));
      start = i + 1;
    }
  }
  return 0;
}


int FuseBackend::GetXattr(uint64_t ino, const std::string &name,
                          const Credentials &creds, std::string *value) {
  return Xattr(ino, &name, creds, value);
}

}  // namespace nfs4
