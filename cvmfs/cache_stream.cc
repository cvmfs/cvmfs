/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cache_stream.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <string>

#include "clientctx.h"
#include "network/download.h"
#include "network/sink.h"
#include "quota.h"
#include "util/mutex.h"
#include "util/smalloc.h"


namespace {

class StreamingSink : public cvmfs::Sink {
 public:
  StreamingSink(void *buf, uint64_t size, uint64_t offset)
    : Sink(false /* is_owner */)
    , pos_(0)
    , window_buf_(buf)
    , window_size_(size)
    , window_offset_(offset)
  { }

  virtual ~StreamingSink() {}

  virtual int64_t Write(const void *buf, uint64_t sz) {
    uint64_t old_pos = pos_;
    pos_ += sz;

    if (!window_buf_)
      return static_cast<int64_t>(sz);

    if (pos_ < window_offset_)
      return static_cast<int64_t>(sz);

    if (old_pos >= (window_offset_ + window_size_))
      return static_cast<int64_t>(sz);

    uint64_t copy_offset = std::max(old_pos, window_offset_);
    uint64_t inbuf_offset = copy_offset - old_pos;
    uint64_t outbuf_offset = copy_offset - window_offset_;
    uint64_t copy_size =
      std::min(sz - inbuf_offset, window_size_ - outbuf_offset);

    memcpy(reinterpret_cast<unsigned char *>(window_buf_) + outbuf_offset,
           reinterpret_cast<const unsigned char *>(buf) + inbuf_offset,
           copy_size);

    return static_cast<int64_t>(sz);
  }

  virtual int Reset() {
    pos_ = 0;
    return 0;
  }

  virtual int Purge() { return Reset(); }
  virtual bool IsValid() {
    return (window_buf_ != NULL) || (window_size_ == 0);
  }
  virtual int Flush() { return 0; }
  virtual bool Reserve(size_t /* size */) { return true; }
  virtual bool RequiresReserve() { return false; }
  virtual std::string Describe() {
    std::string result =  "Streaming sink that is ";
    result += IsValid() ? "valid"  : "invalid";
    return result;
  }

  int64_t GetNBytesStreamed() const { return static_cast<int64_t>(pos_); }

 private:
  uint64_t pos_;
  void *window_buf_;
  uint64_t window_size_;
  uint64_t window_offset_;
};  // class StreamingSink

}  // anonymous namespace


download::DownloadManager *StreamingCacheManager::SelectDownloadManager(
  const FdInfo &info)
{
  if (info.label.IsExternal())
    return external_download_mgr_;
  return regular_download_mgr_;
}

int64_t StreamingCacheManager::Stream(
  const FdInfo &info,
  void *buf,
  uint64_t size,
  uint64_t offset)
{
  StreamingSink sink(buf, size, offset);
  std::string url;
  if (info.label.IsExternal()) {
    url = info.label.path;
  } else {
    url = "/data/" + info.object_id.MakePath();
  }
  bool is_zipped = info.label.zip_algorithm == zlib::kZlibDefault;

  download::JobInfo download_job(&url, is_zipped, true /* probe_hosts */,
                                 &info.object_id, &sink);
  download_job.SetExtraInfo(&info.label.path);
  download_job.SetRangeOffset(info.label.range_offset);
  download_job.SetRangeSize(static_cast<int64_t>(info.label.size));
  ClientCtx *ctx = ClientCtx::GetInstance();
  if (ctx->IsSet()) {
    ctx->Get(download_job.GetUidPtr(),
             download_job.GetGidPtr(),
             download_job.GetPidPtr(),
             download_job.GetInterruptCuePtr());
  }

  SelectDownloadManager(info)->Fetch(&download_job);

  if (download_job.error_code() != download::kFailOk) {
    return -EIO;
  }

  return sink.GetNBytesStreamed();
}


StreamingCacheManager::StreamingCacheManager(
  unsigned max_open_fds,
  CacheManager *cache_mgr,
  download::DownloadManager *regular_download_mgr,
  download::DownloadManager *external_download_mgr)
  : cache_mgr_(cache_mgr)
  , regular_download_mgr_(regular_download_mgr)
  , external_download_mgr_(external_download_mgr)
  , fd_table_(max_open_fds, FdInfo())
{
  lock_fd_table_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_fd_table_, NULL);
  assert(retval == 0);

  delete quota_mgr_;
  quota_mgr_ = cache_mgr_->quota_mgr();
}

StreamingCacheManager::~StreamingCacheManager() {
  pthread_mutex_destroy(lock_fd_table_);
  free(lock_fd_table_);
  quota_mgr_ = NULL;  // gets deleted by cache_mgr_
}

std::string StreamingCacheManager::Describe() {
  return "Streaming shim, underlying cache manager:\n" + cache_mgr_->Describe();
}

bool StreamingCacheManager::AcquireQuotaManager(QuotaManager *quota_mgr) {
  bool result = cache_mgr_->AcquireQuotaManager(quota_mgr);
  if (result)
    quota_mgr_ = cache_mgr_->quota_mgr();
  return result;
}

int StreamingCacheManager::Open(const LabeledObject &object) {
  int fd_in_cache_mgr = cache_mgr_->Open(object);
  if (fd_in_cache_mgr >= 0) {
    MutexLockGuard lock_guard(lock_fd_table_);
    return fd_table_.OpenFd(FdInfo(fd_in_cache_mgr));
  }

  if (fd_in_cache_mgr != -ENOENT)
    return fd_in_cache_mgr;

  if (object.label.IsCatalog() || object.label.IsPinned() ||
      object.label.IsCertificate())
  {
    return -ENOENT;
  }

  MutexLockGuard lock_guard(lock_fd_table_);
  return fd_table_.OpenFd(FdInfo(object));
}

int StreamingCacheManager::PlantFd(int fd_in_cache_mgr) {
  MutexLockGuard lock_guard(lock_fd_table_);
  return fd_table_.OpenFd(FdInfo(fd_in_cache_mgr));
}

int64_t StreamingCacheManager::GetSize(int fd) {
  FdInfo info;
  {
    MutexLockGuard lock_guard(lock_fd_table_);
    info = fd_table_.GetHandle(fd);
  }

  if (!info.IsValid())
    return -EBADF;

  if (info.fd_in_cache_mgr >= 0)
    return cache_mgr_->GetSize(info.fd_in_cache_mgr);

  return Stream(info, NULL, 0, 0);
}

int StreamingCacheManager::Dup(int fd) {
  FdInfo info;

  MutexLockGuard lock_guard(lock_fd_table_);
  info = fd_table_.GetHandle(fd);

  if (!info.IsValid())
    return -EBADF;

  if (info.fd_in_cache_mgr >= 0) {
    int dup_fd = cache_mgr_->Dup(info.fd_in_cache_mgr);
    if (dup_fd < 0)
      return dup_fd;
    return fd_table_.OpenFd(FdInfo(dup_fd));
  }

  return fd_table_.OpenFd(FdInfo(LabeledObject(info.object_id, info.label)));
}

int StreamingCacheManager::Close(int fd) {
  FdInfo info;
  {
    MutexLockGuard lock_guard(lock_fd_table_);
    info = fd_table_.GetHandle(fd);
    if (!info.IsValid())
      return -EBADF;
    fd_table_.CloseFd(fd);
  }

  if (info.fd_in_cache_mgr >= 0)
    return cache_mgr_->Close(info.fd_in_cache_mgr);

  return 0;
}

int64_t StreamingCacheManager::Pread(
  int fd, void *buf, uint64_t size, uint64_t offset)
{
  FdInfo info;
  {
    MutexLockGuard lock_guard(lock_fd_table_);
    info = fd_table_.GetHandle(fd);
  }

  if (!info.IsValid())
    return -EBADF;

  if (info.fd_in_cache_mgr >= 0)
    return cache_mgr_->Pread(info.fd_in_cache_mgr, buf, size, offset);

  int64_t nbytes_streamed = Stream(info, buf, size, offset);
  if (nbytes_streamed < 0)
    return nbytes_streamed;
  if (static_cast<uint64_t>(nbytes_streamed) < offset)
    return 0;
  if (static_cast<uint64_t>(nbytes_streamed) > (offset + size))
    return size;
  return nbytes_streamed - offset;
}

int StreamingCacheManager::Readahead(int fd) {
  FdInfo info;
  {
    MutexLockGuard lock_guard(lock_fd_table_);
    info = fd_table_.GetHandle(fd);
  }

  if (!info.IsValid())
    return -EBADF;

  if (info.fd_in_cache_mgr >= 0)
    return cache_mgr_->Readahead(info.fd_in_cache_mgr);

  return 0;
}

int StreamingCacheManager::OpenFromTxn(void *txn) {
  int fd = cache_mgr_->OpenFromTxn(txn);
  if (fd < 0)
    return fd;

  MutexLockGuard lock_guard(lock_fd_table_);
  return fd_table_.OpenFd(FdInfo(fd));
}

void *StreamingCacheManager::DoSaveState() {
  SavedState *state = new SavedState();
  state->fd_table = fd_table_.Clone();
  state->state_backing_cachemgr = cache_mgr_->SaveState(-1);
  return state;
}

int StreamingCacheManager::DoRestoreState(void *data) {
  // When DoRestoreState is called, we have fd 0 assigned to the root file
  // catalog
  FdInfo handle_root = fd_table_.GetHandle(0);

  SavedState *state = reinterpret_cast<SavedState *>(data);

  int new_backing_root_fd =
    cache_mgr_->RestoreState(-1, state->state_backing_cachemgr);
  fd_table_.AssignFrom(*state->fd_table);

  int new_root_fd = -1;
  if (handle_root.IsValid()) {
    if (new_backing_root_fd >= 0)
      handle_root.fd_in_cache_mgr = new_backing_root_fd;
    new_root_fd = fd_table_.OpenFd(handle_root);
    // There must be a free file descriptor because the root file catalog gets
    // closed before a reload
    assert(new_root_fd >= 0);
  }
  return new_root_fd;
}

bool StreamingCacheManager::DoFreeState(void *data) {
  SavedState *state = reinterpret_cast<SavedState *>(data);
  cache_mgr_->FreeState(-1, state->state_backing_cachemgr);
  delete state->fd_table;
  delete state;
  return true;
}

CacheManager *StreamingCacheManager::MoveOutBackingCacheMgr(int *root_fd) {
  *root_fd = fd_table_.GetHandle(0).fd_in_cache_mgr;
  return cache_mgr_.Release();
}
