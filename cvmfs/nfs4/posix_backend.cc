/**
 * This file is part of the CernVM File System.
 */

#include "nfs4/posix_backend.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <climits>

#include "nfs4/filehandle.h"

namespace nfs4 {

namespace {

ssize_t ListXattrRaw(const std::string &path, char *buf, size_t size) {
#ifdef __APPLE__
  return listxattr(path.c_str(), buf, size, XATTR_NOFOLLOW);
#else
  return llistxattr(path.c_str(), buf, size);
#endif
}

ssize_t GetXattrRaw(const std::string &path, const std::string &name, char *buf,
                    size_t size) {
#ifdef __APPLE__
  return getxattr(path.c_str(), name.c_str(), buf, size, 0, XATTR_NOFOLLOW);
#else
  return lgetxattr(path.c_str(), name.c_str(), buf, size);
#endif
}

Timespec ToTimespec(time_t sec, long nsec) {  // NOLINT(runtime/int)
  Timespec t;
  t.sec = sec;
  t.nsec = static_cast<uint32_t>(nsec);
  return t;
}

}  // anonymous namespace


PosixBackend *PosixBackend::Create(const std::string &root) {
  PosixBackend *backend = new PosixBackend(root);
  Attr attr;
  if (backend->StatPath(root, &attr) != 0 || !S_ISDIR(attr.mode)) {
    delete backend;
    return NULL;
  }
  backend->root_ino_ = attr.ino;
  backend->fsid_ = (static_cast<uint64_t>(HashName(root)) << 32) | 1;
  backend->paths_[attr.ino] = root;
  return backend;
}


uint64_t PosixBackend::Generation() {
  Attr attr;
  if (StatPath(root_, &attr) != 0)
    return 1;
  return (static_cast<uint64_t>(attr.mtime.sec) << 30) ^ attr.mtime.nsec;
}


int PosixBackend::StatPath(const std::string &path, Attr *attr) {
  struct stat info;
  if (lstat(path.c_str(), &info) != 0)
    return errno;
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
  return 0;
}


bool PosixBackend::PathOf(uint64_t ino, std::string *path) {
  std::lock_guard<std::mutex> guard(lock_);
  std::map<uint64_t, std::string>::const_iterator it = paths_.find(ino);
  if (it == paths_.end())
    return false;
  *path = it->second;
  return true;
}


int PosixBackend::GetAttr(uint64_t ino, Attr *attr) {
  std::string path;
  if (!PathOf(ino, &path))
    return ESTALE;
  const int retval = StatPath(path, attr);
  if (retval == 0 && attr->ino != ino)
    return ESTALE;
  return (retval == ENOENT) ? ESTALE : retval;
}


int PosixBackend::Lookup(uint64_t parent, const std::string &name, Attr *attr) {
  std::string path;
  if (!PathOf(parent, &path))
    return ESTALE;
  path += "/" + name;
  const int retval = StatPath(path, attr);
  if (retval != 0)
    return retval;
  // Hard links map to the last path seen, which is good enough for testing
  std::lock_guard<std::mutex> guard(lock_);
  paths_[attr->ino] = path;
  return 0;
}


int PosixBackend::LookupParent(uint64_t ino, Attr *attr) {
  std::string path;
  if (!PathOf(ino, &path))
    return ESTALE;
  if (path == root_)
    return ENOENT;
  const std::string parent = path.substr(0, path.rfind('/'));
  const int retval = StatPath(parent, attr);
  if (retval != 0)
    return retval;
  std::lock_guard<std::mutex> guard(lock_);
  paths_[attr->ino] = parent;
  return 0;
}


int PosixBackend::ReadLink(uint64_t ino, std::string *target) {
  std::string path;
  if (!PathOf(ino, &path))
    return ESTALE;
  char buf[PATH_MAX];
  const ssize_t len = readlink(path.c_str(), buf, sizeof(buf));
  if (len < 0)
    return errno;
  target->assign(buf, len);
  return 0;
}


int PosixBackend::ReadDir(uint64_t ino, std::vector<DirEntry> *entries) {
  std::string path;
  if (!PathOf(ino, &path))
    return ESTALE;
  DIR *dir = opendir(path.c_str());
  if (dir == NULL)
    return errno;
  entries->clear();
  struct dirent *d;
  while ((d = readdir(dir)) != NULL) {
    const std::string name = d->d_name;
    if (name == "." || name == "..")
      continue;
    entries->push_back(DirEntry(name, d->d_ino));
  }
  closedir(dir);
  return 0;
}


int PosixBackend::Read(uint64_t ino, uint64_t offset, uint32_t size,
                       std::string *data) {
  if (read_delay_ms_ > 0) {
    bool first;
    {
      std::lock_guard<std::mutex> guard(lock_);
      first = fetched_.insert(ino).second;
    }
    if (first)
      usleep(read_delay_ms_ * 1000);
  }
  std::string path;
  if (!PathOf(ino, &path))
    return ESTALE;
  const int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0)
    return errno;
  data->resize(size);
  size_t total = 0;
  while (total < size) {
    const ssize_t n = pread(fd, &(*data)[total], size - total, offset + total);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      const int err = errno;
      close(fd);
      return err;
    }
    if (n == 0)
      break;
    total += n;
  }
  close(fd);
  data->resize(total);
  return 0;
}


int PosixBackend::StatFs(FsStat *stat) {
  struct statvfs info;
  if (statvfs(root_.c_str(), &info) != 0)
    return errno;
  stat->bytes_total = static_cast<uint64_t>(info.f_blocks) * info.f_frsize;
  stat->bytes_free = static_cast<uint64_t>(info.f_bfree) * info.f_frsize;
  stat->bytes_avail = static_cast<uint64_t>(info.f_bavail) * info.f_frsize;
  stat->files_total = info.f_files;
  stat->files_free = info.f_ffree;
  stat->files_avail = info.f_favail;
  return 0;
}


int PosixBackend::ListXattr(uint64_t ino, std::vector<std::string> *names) {
  std::string path;
  if (!PathOf(ino, &path))
    return ESTALE;
  names->clear();
  const ssize_t size = ListXattrRaw(path, NULL, 0);
  if (size < 0)
    return (errno == ENOTSUP) ? 0 : errno;
  std::string buf(size, '\0');
  const ssize_t len = ListXattrRaw(path, &buf[0], buf.size());
  if (len < 0)
    return errno;
  size_t start = 0;
  for (ssize_t i = 0; i < len; ++i) {
    if (buf[i] == '\0') {
      if (static_cast<size_t>(i) > start)
        names->push_back(buf.substr(start, i - start));
      start = i + 1;
    }
  }
  return 0;
}


int PosixBackend::GetXattr(uint64_t ino, const std::string &name,
                           const Credentials & /* creds */,
                           std::string *value) {
  std::string path;
  if (!PathOf(ino, &path))
    return ESTALE;
  const ssize_t size = GetXattrRaw(path, name, NULL, 0);
  if (size < 0)
    return errno;
  value->resize(size);
  const ssize_t len = GetXattrRaw(path, name, &(*value)[0], value->size());
  if (len < 0)
    return errno;
  value->resize(len);
  return 0;
}

}  // namespace nfs4
